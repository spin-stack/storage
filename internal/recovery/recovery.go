// Package recovery rebuilds a volume's published chain on a host that has never seen it,
// from the object store alone (v6 §14, §23.4). It is internal/publisher in reverse: qcow
// owns a volume's local chain and decides *when* something happens to it; this is *what*
// happens, and it is the half that needs a key and a bucket.
//
// The refusal is the product here, not the rebuild: without it a volume with published
// commits, placed on a host holding no local copy, gets `qemu-img create` and its guest
// gets a blank disk. Half a volume is worse than no volume, because a guest will boot it.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrIncomplete means this volume has published commits and this host could not assemble
// all of them: a manifest or layer that is not there, a digest that does not match, a
// chain that does not resolve. It is a refusal and never a partial chain — the one thing
// the caller must not do is create a fresh empty layer for a volume that already has one.
//
// commit.ErrNoHead is the other answer and means the volume has never published, so an
// empty chain is *correct*; it is returned wrapped and the two are told apart with
// errors.Is. One error type for both would make "the bucket is unreachable" and "this
// volume is new" the same sentence.
var ErrIncomplete = errors.New("recovery: this volume's published chain could not be rebuilt in full")

// partSuffix names a layer that is still being downloaded. A layer only ever appears
// under its real name complete: a crash mid-fetch leaves a `.part` nobody looks for,
// where truncating the real path would leave a file that Exists, that the state record
// may vouch for, and that a guest would boot.
const partSuffix = ".part"

// Keys hands over a volume's wrapped key material; agent.Loop in production.
type Keys interface {
	VolumeKeys(ctx context.Context, volumeID string) (agent.VolumeKeys, error)
}

// Files is the filesystem a rebuild needs, by absolute path. Not simio's disk.Disk: that
// namespace is relative names rooted at the data directory, and a component straddling the
// two is the `--data-dir applied twice` defect with a second process added to it.
// real.Paths is the production implementation.
type Files interface {
	// Paths is embedded whole because qcow.ReadState/WriteState take it: this package
	// and qcow.Manager both write `state.json`, and two spellings of one format is how
	// the two would come to disagree about which layers this host holds.
	qcow.Paths
	// Create truncates or creates, and its Close makes the bytes durable — the file and
	// the directory entry both. A layer that reached the page cache and not the platter
	// is a chain that opens today and is short a layer after a power cut.
	Create(path string) (io.WriteCloser, error)
	Rename(oldPath, newPath string) error
	Remove(path string) error
}

// Runner runs qemu-img and returns its standard output. A qcow2 parser of our own is
// forbidden (v6 §7): every question about an offline image is another process's answer.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Recoverer rebuilds published chains.
type Recoverer struct {
	root    string
	qemuImg string
	store   objectstore.Store
	kms     crypto.KMS
	keys    Keys
	files   Files
	run     Runner
	clk     clock.Clock
	rec     *obs.Recorder
}

// New returns a Recoverer rooted at the Agent's data directory.
func New(root, qemuImg string, store objectstore.Store, kms crypto.KMS,
	keys Keys, files Files, run Runner,
) *Recoverer {
	return &Recoverer{root: root, qemuImg: qemuImg, store: store, kms: kms,
		keys: keys, files: files, run: run}
}

// WithTelemetry attaches the clock and recorder the §28 numbers are written with, and
// returns the Recoverer so a binary wires it in one expression. Separate from New because
// a Recoverer that measures nothing rebuilds exactly the same chain.
func (r *Recoverer) WithTelemetry(clk clock.Clock, rec *obs.Recorder) *Recoverer {
	r.clk, r.rec = clk, rec
	return r
}

// elapsed is the whole of this package's relationship with time: a nil clock is a caller
// that is not measuring, and the duration is then zero rather than a branch at the one
// record site.
func (r *Recoverer) elapsed(since clock.Instant) float64 {
	if r.clk == nil {
		return 0
	}
	return r.clk.Now().Sub(since).Seconds()
}

func (r *Recoverer) now() clock.Instant {
	if r.clk == nil {
		return 0
	}
	return r.clk.Now()
}

// Restore rebuilds volumeID's published chain on this host and returns what a caller must
// build the new tip over.
//
// A wrapped commit.ErrNoHead means the volume has never published: it is new, and an
// empty chain is the right answer. Every other error is ErrIncomplete and means refuse.
//
// What it deliberately does not do: create the tip, and write `active/current`. Which file
// a VM is launched against is one decision and it stays in the one package that owns the
// layout of the thing being launched.
func (r *Recoverer) Restore(ctx context.Context, volumeID string, sizeBytes int64) (qcow.Restored, error) {
	return r.RestoreFrom(ctx, qcow.Lineage{VolumeID: volumeID}, sizeBytes)
}

// RestoreFrom rebuilds a volume that may descend from others.
//
// With no ancestry it is Restore. With one it rebuilds each generation in turn, oldest
// first, up to the commit that generation's snapshot named, and then this volume's own
// published chain on top of them — in that order, because a layer is repointed at a
// parent already on disk.
//
// Every generation is walked, not just the nearest: a clone's own manifests state only
// what it wrote, so the bytes of a grandparent are named by nothing the parent published.
// Stopping at the first ancestor rebuilds a chain missing everything the older ones wrote
// and reports success — a volume advertised as a copy, delivered with holes.
//
// Each generation's commit is *named*, not read from that volume's HEAD, and the
// difference is the whole promise of §20: a clone is the volume as it was at that
// snapshot, and its HEAD is whatever it has published since. Reading HEAD would deliver
// Thursday for a clone of Tuesday, with nothing anywhere reporting the difference.
func (r *Recoverer) RestoreFrom(ctx context.Context, l qcow.Lineage, sizeBytes int64) (qcow.Restored, error) {
	started := r.now()
	base, parentTip, beneath := "", "", 0
	for _, a := range l.Ancestry {
		// `local` stays this volume: every generation's layers land under the volume
		// being served, so nothing here writes into another volume's directory — and
		// `source` is whose objects are read and whose id sealed them, which is what
		// keeps each generation's key binding its own.
		got, n, err := r.rebuild(ctx, l.VolumeID, a.VolumeID, a.CommitID, sizeBytes, parentTip, beneath)
		if err != nil {
			return qcow.Restored{}, err
		}
		base, parentTip, beneath = got.Base, got.Base, beneath+n
	}
	head, _, err := commit.ReadHead(ctx, r.store, l.VolumeID)
	switch {
	case errors.Is(err, commit.ErrNoHead) && parentTip != "":
		// A clone that has published nothing of its own: its ancestry is the whole of
		// what it holds, and the new tip goes straight on top.
		r.rec.Observe(ctx, "recovery_duration_seconds", r.elapsed(started), obs.String("volume", l.VolumeID))
		return qcow.Restored{Base: base, VirtualSize: sizeBytes,
			HeadCommitID: l.Ancestry[len(l.Ancestry)-1].CommitID}, nil
	case errors.Is(err, commit.ErrNoHead):
		return qcow.Restored{}, fmt.Errorf("recovery: volume %s: %w", l.VolumeID, err)
	case err != nil:
		return qcow.Restored{}, fmt.Errorf("%w: reading the HEAD of volume %s: %w", ErrIncomplete, l.VolumeID, err)
	}
	got, _, err := r.rebuild(ctx, l.VolumeID, l.VolumeID, head.CommitID, sizeBytes, parentTip, beneath)
	if err != nil {
		return got, err
	}
	// Only a rebuild that finished is timed. A refusal has a duration too, and mixing the
	// two would make the series answer "how long does a restore take" with the time it
	// takes to fail — which is the number nobody is asking for.
	r.rec.Observe(ctx, "recovery_duration_seconds", r.elapsed(started), obs.String("volume", l.VolumeID))
	return got, nil
}

// rebuild materialises one chain: `source` is the volume whose published objects are read
// and whose id the layers were sealed under, `local` is the volume whose directory they
// land in. They differ for every generation of a clone's ancestry, and `local` is the same
// volume throughout it: a rebuild never writes into a volume it does not own.
func (r *Recoverer) rebuild(ctx context.Context, local, source, commitID string, sizeBytes int64, onTopOf string, beneath int) (qcow.Restored, int, error) {
	manifests, err := r.walk(ctx, source, commitID, beneath)
	if err != nil {
		return qcow.Restored{}, 0, err
	}
	// A generation that names no commit walks to nothing, and every line below assumes at
	// least one manifest. Refused rather than indexed: `ancestry` arrives over the wire as
	// a repeated message, so an entry with an empty commit_id is a shape any producer can
	// put on it, and `manifests[len-1]` on an empty walk panics the Agent's reconcile loop
	// for every volume on the host, not only this one.
	// A generation that names no commit walks to nothing, and every line below assumes at
	// least one manifest. Refused rather than indexed: `ancestry` arrives over the wire as
	// a repeated message, so an entry with an empty commit_id is a shape any producer can
	// put on it, and `manifests[len-1]` on an empty walk panics the Agent's reconcile loop
	// for every volume on the host, not only this one.
	if len(manifests) == 0 {
		return qcow.Restored{}, 0, fmt.Errorf("%w: volume %s names no commit to rebuild volume %s to",
			ErrIncomplete, local, source)
	}
	// The head commit is the last element, the walk being oldest-first. The size comes
	// from its manifest and not from the Head object, which names a commit and holds
	// nothing else: the manifest is where a recovery running without a catalog can still
	// read how big the guest's disk is.
	headManifest := manifests[len(manifests)-1]
	if err := checkGeometry(source, sizeBytes, headManifest, manifests); err != nil {
		return qcow.Restored{}, 0, err
	}
	enc, err := r.encryption(ctx, local, source)
	if err != nil {
		return qcow.Restored{}, 0, err
	}
	// The host's one layers directory, and this volume's own — which holds the record and
	// the pointer, and which used to be made as a side effect of making its layers
	// directory underneath it.
	for _, dir := range []string{qcow.LayersDir(r.root), qcow.VolumeDir(r.root, local)} {
		if err := r.files.MkdirAll(dir); err != nil {
			return qcow.Restored{}, 0, fmt.Errorf("%w: making %s for volume %s: %w", ErrIncomplete, dir, local, err)
		}
	}
	// What this host already holds, so a re-placement skips most downloads. It cannot be
	// replaced by hashing the local files: `qemu-img rebase -u` rewrites a layer's header,
	// so a repointed layer no longer hashes to the object it came from (measured: 40 bytes
	// differ, same length). Its liability is a state file that is intact but wrong — the
	// framing catches corruption, not authorship.
	st, err := qcow.ReadState(r.files, r.root, local)
	if err != nil {
		return qcow.Restored{}, 0, fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	// What this host already holds, over every volume's record and not just this one's.
	// Layers live in one directory for the whole host (qcow.LayersDir says why), so a
	// layer another volume vouches for is a layer this restore can use as it stands.
	//
	// That is what makes a clone on the same host as its parent free: §18 asks that it
	// reuse the local files, and it reuses them now rather than copying them. The copy this
	// replaced was never about the bytes, it was about the path — `qemu-img rebase -u`
	// rewrites a layer's header to name its parent, so under a directory per volume every
	// clone needed its own rebased file, and a hundred clones of one volume needed a
	// hundred copies of its history. Under one directory the header is written once and is
	// already correct for everyone.
	//
	// Still a record and never a stat: a file at the right path is not evidence that it is
	// the layer the manifest names.
	held, err := qcow.HeldCommits(r.files, r.root)
	if err != nil {
		return qcow.Restored{}, 0, fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	parent := onTopOf
	for _, m := range manifests {
		path := qcow.LayerImage(r.root, m.Layer.LayerID)
		downloaded, err := r.materialize(ctx, enc, m, path, held[m.CommitID] == m.Layer.LayerID)
		if err != nil {
			return qcow.Restored{}, 0, err
		}
		if downloaded > 0 {
			// The sealed length, which is what crossed the network. Counted per layer as
			// it lands rather than once at the end: a restore that is refused halfway
			// still cost the bytes it pulled, and that is the cost an operator is
			// watching.
			r.rec.Count(ctx, "recovery_download_bytes_total", downloaded, obs.String("volume", local))
		}
		if err := r.repoint(ctx, m, path, parent); err != nil {
			return qcow.Restored{}, 0, err
		}
		// One write per layer rather than one at the end: a crash mid-restore then costs
		// the layers not yet fetched and not the ones already on disk. It is also what
		// makes the file safe to keep — the sweep frees a layer no record names, and a
		// clone that has downloaded three of its ancestors and not the fourth must not
		// have those three collected out from under it on the next cycle.
		if !st.Names("", path) {
			st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: m.CommitID, LayerID: m.Layer.LayerID})
			if err := qcow.WriteState(r.files, r.root, local, st); err != nil {
				return qcow.Restored{}, 0, fmt.Errorf("%w: recording commit %s: %w", ErrIncomplete, m.CommitID, err)
			}
		}
		parent = path
	}

	// The whole chain, once, offline. Every layer was checked against its own parent as
	// it landed; this is the check that the result resolves end to end, and it is a
	// different question — plain `qemu-img info` exits 0 on an image whose backing file
	// is gone, and `--backing-chain` exits 1 (both measured against the pinned 11.1.1).
	// It is safe here and only here: this runs on an offline image, and the walk opens
	// every backing file, which would fail on the write lock of a live one.
	chain, err := r.inspectChain(ctx, parent)
	if err != nil {
		return qcow.Restored{}, 0, err
	}
	for _, layer := range chain {
		// Before the tip is built and long before a guest is launched, which is the whole
		// point: the same corrupt layer is refused on the *second* open — qcow.Open's
		// adopt-a-local-chain branch walks the chain and checks this — and until now it
		// was served on the first. Detection after the guest is the ordering §29 forbids.
		//
		// It costs nothing extra: this walk already opens every layer, which is why the
		// check is here and not in a pass of its own.
		if layer.Specific.Data.Corrupt {
			return qcow.Restored{}, 0, fmt.Errorf("%w: volume %s rebuilt a chain whose layer %s has the qcow2 corrupt flag set",
				ErrIncomplete, local, layer.Filename)
		}
	}
	if len(chain) != len(manifests)+beneath {
		return qcow.Restored{}, 0, fmt.Errorf("%w: volume %s rebuilt to %d commits and %s walks %d layers",
			ErrIncomplete, local, len(manifests), parent, len(chain))
	}
	return qcow.Restored{Base: parent, VirtualSize: headManifest.VirtualSize, HeadCommitID: commitID}, len(manifests), nil
}

// walk follows parent_commit_id back from HEAD and returns the chain oldest-first, the
// order it must be rebuilt in: a layer is repointed at a parent already on disk.
//
// `beneath` is how many layers earlier generations of this lineage already put under
// this one, and qcow.MaxLayers bounds their sum rather than one generation's history:
// what the measurement bounds is how many backing files a single image opens, and a
// per-generation bound would let a depth-D lineage build D times it.
//
// A repeated commit id is a cycle in the bucket — commit.Publish shipped one self-parent
// bug — and following it is an infinite download. A repeated *layer* id is two commits
// claiming one file, which would rebase a layer onto itself.
func (r *Recoverer) walk(ctx context.Context, volumeID, headCommitID string, beneath int) ([]commit.Manifest, error) {
	var newestFirst []commit.Manifest
	seenCommit := map[string]bool{}
	seenLayer := map[string]bool{}
	for id := headCommitID; id != ""; {
		if seenCommit[id] {
			return nil, fmt.Errorf("%w: volume %s's history reaches commit %s twice", ErrIncomplete, volumeID, id)
		}
		if beneath+len(newestFirst) == qcow.MaxLayers {
			return nil, fmt.Errorf("%w: volume %s reaches more than %d layers to restore",
				ErrIncomplete, volumeID, qcow.MaxLayers)
		}
		seenCommit[id] = true
		// ReadManifest already refuses an object that describes a different volume or
		// commit, so a manifest restored under the wrong prefix is caught before any of
		// its layers is attributed to this guest.
		m, err := commit.ReadManifest(ctx, r.store, volumeID, id)
		if err != nil {
			return nil, fmt.Errorf("%w: volume %s: %w", ErrIncomplete, volumeID, err)
		}
		if seenLayer[m.Layer.LayerID] {
			return nil, fmt.Errorf("%w: volume %s has two commits naming layer %s",
				ErrIncomplete, volumeID, m.Layer.LayerID)
		}
		seenLayer[m.Layer.LayerID] = true
		newestFirst = append(newestFirst, m)
		id = m.ParentCommitID
	}
	oldestFirst := make([]commit.Manifest, len(newestFirst))
	for i, m := range newestFirst {
		oldestFirst[len(newestFirst)-1-i] = m
	}
	return oldestFirst, nil
}

// checkGeometry takes the volume's size from the commit and not from the catalog. A
// manifest differing from the head's would mean a resize, which this system does not have;
// a caller differing from it means the catalog row and the bucket disagree about how big
// somebody's disk is. The number decides the size of the device the guest is handed.
func checkGeometry(volumeID string, sizeBytes int64, head commit.Manifest, manifests []commit.Manifest) error {
	want := head.VirtualSize
	for _, m := range manifests {
		if m.VirtualSize != want {
			return fmt.Errorf("%w: volume %s's commit %s reconstructs %d bytes and commit %s reconstructs %d",
				ErrIncomplete, volumeID, m.CommitID, m.VirtualSize, head.CommitID, want)
		}
	}
	if sizeBytes != want {
		return fmt.Errorf("%w: volume %s is %d bytes in the catalog and %d bytes at commit %s",
			ErrIncomplete, volumeID, sizeBytes, want, head.CommitID)
	}
	return nil
}

// materialize puts one layer's plaintext qcow2 at path and reports how many sealed bytes
// it had to download to do it — zero when the layer was already here or copied from
// another volume on this disk.
//
// held — the durable record that this host already has this commit's layer — is believed
// only together with the file being there.
func (r *Recoverer) materialize(ctx context.Context, enc *crypto.Encryption, m commit.Manifest, path string, held bool) (int64, error) {
	there, err := r.files.Exists(path)
	if err != nil {
		return 0, fmt.Errorf("%w: looking for %s: %w", ErrIncomplete, path, err)
	}
	if held {
		if there {
			return 0, nil
		}
	} else if there {
		// The path is taken and nothing here says it holds *this* commit's layer. Layers
		// are shared between volumes now, so the download that would follow does not
		// replace a file of this volume's — it replaces one every volume reading through
		// it depends on, and a rename into place is not something a chain under a running
		// guest survives.
		//
		// It is refused rather than worked around. In an honest system a layer id names
		// one layer's plaintext for ever, so this shape means a record and a manifest
		// disagree about which commit owns the id — a fork restored here, a data directory
		// copied between machines, a forged manifest — and picking a winner is choosing
		// which volume gets the wrong bytes. Refusing costs a cycle: if nothing vouches
		// for the file at all, the sweep collects it and the next restore proceeds.
		return 0, fmt.Errorf("%w: commit %s says its layer is %s, and this host already holds a file there that no record ties to this commit",
			ErrIncomplete, m.CommitID, m.Layer.LayerID)
	}
	part := path + partSuffix
	w, err := r.files.Create(part)
	if err != nil {
		return 0, fmt.Errorf("%w: creating %s: %w", ErrIncomplete, part, err)
	}
	// commit.Fetch checks the digest over the bytes as stored before unsealing and
	// GCM-authenticates every frame; there must be one answer to "is this the object the
	// manifest named".
	if err := commit.Fetch(ctx, r.store, enc, m, w); err != nil {
		_ = w.Close()
		r.discard(part)
		return 0, fmt.Errorf("%w: layer %s of commit %s: %w", ErrIncomplete, m.Layer.LayerID, m.CommitID, err)
	}
	if err := w.Close(); err != nil {
		r.discard(part)
		return 0, fmt.Errorf("%w: finishing %s: %w", ErrIncomplete, part, err)
	}
	if err := r.files.Rename(part, path); err != nil {
		r.discard(part)
		return 0, fmt.Errorf("%w: renaming %s: %w", ErrIncomplete, part, err)
	}
	return m.Layer.SizeBytes, nil
}

// discard drops a partial download. Its own failure is not reported: the caller is
// already returning the reason the restore was refused, and a leftover `.part` is
// garbage, not a chain — the next attempt truncates it.
func (r *Recoverer) discard(part string) { _ = r.files.Remove(part) }

// repoint makes a layer's header name the parent where it actually landed, then checks it.
//
// `-F qcow2` is mandatory: without it qemu-img exits 1 with "backing file format must be
// specified" (measured against the pinned 11.1.1). `-u` rewrites the header and touches no
// cluster. Run unconditionally, including on a layer already held: it is idempotent.
//
// The check afterwards is not defensive: `rebase -u` onto a wrong-but-existing parent is
// accepted in complete silence (measured: a later `convert -O raw` succeeded with no
// warning). A chain root that claims a backing file is a clone lineage, which the commit
// protocol does not express yet.
func (r *Recoverer) repoint(ctx context.Context, m commit.Manifest, path, parent string) error {
	if parent != "" {
		if _, err := r.run.Run(ctx, r.qemuImg,
			"rebase", "-u", "-f", "qcow2", "-b", parent, "-F", "qcow2", path); err != nil {
			return fmt.Errorf("%w: repointing %s at %s: %w", ErrIncomplete, path, parent, err)
		}
	}
	info, err := r.inspect(ctx, path)
	if err != nil {
		return err
	}
	if info.FullBackingFilename != parent {
		return fmt.Errorf("%w: commit %s's layer is backed by %q, want %q",
			ErrIncomplete, m.CommitID, info.FullBackingFilename, parent)
	}
	return nil
}

// imageInfo is the part of `qemu-img info --output=json` this package reads.
type imageInfo struct {
	Format      string `json:"format"`
	VirtualSize int64  `json:"virtual-size"`
	Filename    string `json:"filename"`
	// FullBackingFilename is the backing path as the header records it, resolved to an
	// absolute one. It is empty for a chain root.
	FullBackingFilename string `json:"full-backing-filename"`
	// Specific carries qcow2's own corrupt bit, which QEMU sets when it finds an
	// inconsistency it could not resolve. It rides through the object store intact — the
	// bit is inside the image the Agent sealed, so the layer's digest matches its
	// manifest and every integrity check here passes — and the chain walk below is where
	// a rebuilt volume gets to see it before a guest does.
	Specific struct {
		Data struct {
			Corrupt bool `json:"corrupt"`
		} `json:"data"`
	} `json:"format-specific"`
}

// inspect asks about one image and opens no backing file. Plain `info` rather than
// `--backing-chain` per layer: the parent is there by construction at this point, and
// keeping the per-layer question cheap is what makes the whole-chain walk at the end
// affordable.
func (r *Recoverer) inspect(ctx context.Context, path string) (imageInfo, error) {
	out, err := r.run.Run(ctx, r.qemuImg, "info", "--output=json", path)
	if err != nil {
		return imageInfo{}, fmt.Errorf("%w: inspecting %s: %w", ErrIncomplete, path, err)
	}
	var info imageInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return imageInfo{}, fmt.Errorf("%w: decoding qemu-img info for %s: %w", ErrIncomplete, path, err)
	}
	return info, nil
}

// inspectChain walks the whole chain from path down to its root. The output is an array
// of the same objects plain info returns, newest first, and the command fails outright if
// any link is missing — which is the point of running it.
func (r *Recoverer) inspectChain(ctx context.Context, path string) ([]imageInfo, error) {
	out, err := r.run.Run(ctx, r.qemuImg, "info", "--output=json", "--backing-chain", path)
	if err != nil {
		return nil, fmt.Errorf("%w: walking the chain from %s: %w", ErrIncomplete, path, err)
	}
	var chain []imageInfo
	if err := json.Unmarshal(out, &chain); err != nil {
		return nil, fmt.Errorf("%w: decoding the chain walk of %s: %w", ErrIncomplete, path, err)
	}
	return chain, nil
}

// encryption fetches the volume's key, unwraps it, and binds it to the volume — once for
// the whole walk, which is publisher.encryption in reverse service. The unwrapped key is
// not kept beyond the restore that needed it.
// The two ids are the same volume except for a clone, and that is the whole of §10's
// shared-lineage key. `holder` is whose wrapped DEK this host was handed; `sealedBy` is
// the volume whose id the layers' nonces were derived from (crypto.layerNonce binds it).
//
// A clone can open its parent's layers because controlplane.Clone rewraps the *same* DEK
// bytes under the child's id: unwrapping with the clone's id yields the parent's key. The
// nonce is the part that does not follow, so it is passed separately rather than assumed.
func (r *Recoverer) encryption(ctx context.Context, holder, sealedBy string) (*crypto.Encryption, error) {
	holderID, err := uuid.Parse(holder)
	if err != nil {
		return nil, fmt.Errorf("%w: volume id %q: %w", ErrIncomplete, holder, err)
	}
	bindTo, err := uuid.Parse(sealedBy)
	if err != nil {
		return nil, fmt.Errorf("%w: volume id %q: %w", ErrIncomplete, sealedBy, err)
	}
	keys, err := r.keys.VolumeKeys(ctx, holder)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s: %w", ErrIncomplete, holder, err)
	}
	dek, err := r.kms.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID, holderID)
	if err != nil {
		return nil, fmt.Errorf("%w: unwrapping the DEK of volume %s: %w", ErrIncomplete, holder, err)
	}
	enc, err := crypto.NewEncryption(dek, bindTo)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s: %w", ErrIncomplete, holder, err)
	}
	return enc, nil
}

// Current is the newest commit the object store holds for this volume.
//
// It is a HEAD read and nothing else: the question "is the chain on this disk still the
// published one" has to be cheap enough to ask on every open, and a rebuild is not.
func (r *Recoverer) Current(ctx context.Context, volumeID string) (string, error) {
	head, _, err := commit.ReadHead(ctx, r.store, volumeID)
	if err != nil {
		return "", err
	}
	return head.CommitID, nil
}

// Absent is the Recovery a host has when it was started with no object store. It is a
// type rather than a nil or a caller-computed bool in qcow.Deps.Recovery, because what a
// wiring change forgets is the decision between refusing a volume with published commits
// elsewhere and serving it empty.
type Absent struct{}

// Restore answers that the volume has never published: a deployment with no object store
// has published nothing, so every volume in it is new. Refusing instead broke every
// single-machine deployment. The case that refusal was for — a host that did publish and
// is now started against no store — is caught by qcow.Open reading this host's own
// state.json, which names the commits whose layers it holds.
func (Absent) RestoreFrom(_ context.Context, l qcow.Lineage, _ int64) (qcow.Restored, error) {
	// Except for a clone, which cannot be born empty however loudly this host has no
	// object store: a volume advertised as a copy of another one, served blank, is the
	// oldest defect in this repository (DEV-0007). ErrNoHead would mean "new, start
	// empty"; this must refuse instead.
	if l.Cloned() {
		parent := l.Ancestry[len(l.Ancestry)-1]
		return qcow.Restored{}, fmt.Errorf("%w: volume %s clones %s at commit %s (%d generation(s) of lineage) and this Agent was started with no object store to read it from",
			ErrIncomplete, l.VolumeID, parent.VolumeID, parent.CommitID, len(l.Ancestry))
	}
	return qcow.Restored{}, fmt.Errorf("%w: this Agent was started with no object store", commit.ErrNoHead)
}

// Current answers the same way and for the same reason.
func (Absent) Current(context.Context, string) (string, error) {
	return "", fmt.Errorf("%w: this Agent was started with no object store", commit.ErrNoHead)
}
