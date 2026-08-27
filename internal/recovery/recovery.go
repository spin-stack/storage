// Package recovery rebuilds a volume's published chain on a host that has never seen it,
// from the object store alone (v6 §14, §23.4).
//
// It is the mirror image of internal/publisher: the same collaborators, the opposite
// direction. publisher turns a sealed layer on this disk into a commit in the bucket;
// this turns a commit chain in the bucket into layers on this disk, repointed at each
// other, ready for internal/qcow to mint a tip over. The split is the same one: qcow owns
// a volume's local chain and decides *when* something happens to it; this is *what*
// happens, and it is the half that needs a key and a bucket.
//
// # Why this exists at all
//
// Without it, a volume with published commits, placed on a host that holds no local copy,
// gets `qemu-img create` and its guest gets a blank disk — silent data loss, and the same
// shape as the clone that read zeros. So the refusal is the product here, not the
// rebuild: every failure below returns ErrIncomplete and leaves nothing behind that could
// be mistaken for a chain. Half a volume is worse than no volume, because a guest will
// boot it.
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
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrIncomplete means this volume has published commits and this host could not assemble
// all of them: a manifest that is not there, a layer that is not there, a digest that
// does not match, a chain that does not resolve. It is a refusal and never a partial
// chain — the caller's only correct response is to refuse the volume, because the one
// thing it must not do is create a fresh empty layer for a volume that already has one.
//
// commit.ErrNoHead is the other answer and it is not this one: it means the volume has
// never published anything, so an empty chain is *correct*. It is returned wrapped, and
// the two are told apart with errors.Is. A single error type covering both would make
// "the bucket is unreachable" and "this volume is new" the same sentence, which is the
// defect this package was written to close.
var ErrIncomplete = errors.New("recovery: this volume's published chain could not be rebuilt in full")

// maxRestoreDepth bounds the walk.
//
// 301 layers open fine in both qemu-img and qemu-system — measured — at one file
// descriptor and about 140 KiB of RSS per layer *in every process that opens the chain*,
// so the real ceiling is the default 1024-descriptor limit and it is reached by the VM
// rather than here. Refusing at 256 turns "the fleet quietly built a chain nobody can
// open" into a loud refusal well before that. §19's compaction, whose example trigger is
// 32 layers, is what is supposed to keep the number an order of magnitude below this.
const maxRestoreDepth = 256

// partSuffix names a layer that is still being downloaded. A layer only ever appears
// under its real name complete: a crash mid-fetch leaves a `.part` nobody looks for,
// where truncating the real path would leave a file that Exists, that the state record
// may vouch for, and that a guest would boot.
const partSuffix = ".part"

// Keys hands over a volume's wrapped key material. It is agent.Loop in production — the
// same object publisher.Keys names, declared again here because the two packages consume
// it independently and neither is the other's client.
type Keys interface {
	VolumeKeys(ctx context.Context, volumeID string) (agent.VolumeKeys, error)
}

// Files is the filesystem a rebuild needs, by absolute path.
//
// Deliberately not simio's disk.Disk: that namespace is relative names rooted at the data
// directory, this one is absolute paths handed to another process, and a component
// straddling the two is the `--data-dir applied twice` defect with a second process added
// to it. real.Paths is the production implementation, the same one qcow.Paths takes.
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

// Runner runs qemu-img and returns its standard output. Structurally qcow.Runner —
// real.Runner satisfies both — declared here because this is where it is consumed, and
// because a qcow2 parser of our own is forbidden (v6 §7): every question about an offline
// image is another process's answer.
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
}

// New returns a Recoverer rooted at the Agent's data directory.
func New(root, qemuImg string, store objectstore.Store, kms crypto.KMS,
	keys Keys, files Files, run Runner,
) *Recoverer {
	return &Recoverer{root: root, qemuImg: qemuImg, store: store, kms: kms,
		keys: keys, files: files, run: run}
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
	// HEAD first, and nothing before it. This is the hot path for every volume that was
	// created a second ago, it is asked once per volume per host, and it must cost
	// exactly one request — not a key fetch, not a MkdirAll.
	head, _, err := commit.ReadHead(ctx, r.store, volumeID)
	switch {
	case errors.Is(err, commit.ErrNoHead):
		return qcow.Restored{}, fmt.Errorf("recovery: volume %s: %w", volumeID, err)
	case err != nil:
		// Including a bucket that is not there. objectstore separates that from a key
		// that is not there precisely so this line can refuse instead of reading an
		// unreachable bucket as "nothing was ever written".
		return qcow.Restored{}, fmt.Errorf("%w: reading the HEAD of volume %s: %w", ErrIncomplete, volumeID, err)
	}

	manifests, err := r.walk(ctx, volumeID, head.CommitID)
	if err != nil {
		return qcow.Restored{}, err
	}
	// The head commit is the last element, the walk being oldest-first. The size comes
	// from its manifest and not from the Head object, which names a commit and holds
	// nothing else: the manifest is where a recovery running without a catalog can still
	// read how big the guest's disk is.
	headManifest := manifests[len(manifests)-1]
	if err := checkGeometry(volumeID, sizeBytes, headManifest, manifests); err != nil {
		return qcow.Restored{}, err
	}
	enc, err := r.encryption(ctx, volumeID)
	if err != nil {
		return qcow.Restored{}, err
	}
	if err := r.files.MkdirAll(qcow.LayersDir(r.root, volumeID)); err != nil {
		return qcow.Restored{}, fmt.Errorf("%w: making the layer directory for volume %s: %w", ErrIncomplete, volumeID, err)
	}
	// What this host already holds, from the record qcow.Manager writes on every
	// successful publish. It is what makes a re-placement onto a host that has most of
	// the chain skip most of the downloads, and it cannot be replaced by hashing the
	// local files: `qemu-img rebase -u` rewrites a layer's header, so a repointed layer
	// no longer hashes to the object it came from — measured, 40 bytes differ, same
	// length. That is also its liability: a state file that is intact but wrong (hand
	// edited, or restored from another host's backup) makes a restore skip a download it
	// should have made. The framing catches corruption, not authorship.
	st, err := qcow.ReadState(r.files, r.root, volumeID)
	if err != nil {
		return qcow.Restored{}, fmt.Errorf("%w: %w", ErrIncomplete, err)
	}
	held := make(map[string]string, len(st.Commits))
	for _, c := range st.Commits {
		held[c.CommitID] = c.LayerID
	}

	parent := ""
	for _, m := range manifests {
		path := qcow.LayerImage(r.root, volumeID, m.Layer.LayerID)
		known := held[m.CommitID] == m.Layer.LayerID
		if err := r.materialize(ctx, enc, m, path, known); err != nil {
			return qcow.Restored{}, err
		}
		if err := r.repoint(ctx, m, path, parent); err != nil {
			return qcow.Restored{}, err
		}
		if !known {
			// One write per layer that is new to this host, rather than one at the end:
			// a crash mid-restore then costs the layers not yet fetched and not the ones
			// already on disk. A layer that was already recorded is not appended again —
			// the list would grow by the whole chain on every re-placement.
			st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: m.CommitID, LayerID: m.Layer.LayerID})
			if err := qcow.WriteState(r.files, r.root, volumeID, st); err != nil {
				return qcow.Restored{}, fmt.Errorf("%w: recording commit %s: %w", ErrIncomplete, m.CommitID, err)
			}
			held[m.CommitID] = m.Layer.LayerID
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
		return qcow.Restored{}, err
	}
	if len(chain) != len(manifests) {
		return qcow.Restored{}, fmt.Errorf("%w: volume %s rebuilt to %d commits and %s walks %d layers",
			ErrIncomplete, volumeID, len(manifests), parent, len(chain))
	}
	return qcow.Restored{Base: parent, VirtualSize: headManifest.VirtualSize, HeadCommitID: head.CommitID}, nil
}

// walk follows parent_commit_id back from HEAD and returns the chain oldest-first, which
// is the order it has to be rebuilt in: a layer is repointed at a parent that is already
// on disk.
//
// Both refusals here are about a history that cannot be walked rather than one that is
// merely long. A repeated commit id is a cycle in the bucket — commit.Publish has already
// shipped one self-parent bug, caught by a cycle test — and following it is an infinite
// download. A repeated *layer* id is two commits claiming one file, which would rebase a
// layer onto itself.
func (r *Recoverer) walk(ctx context.Context, volumeID, headCommitID string) ([]commit.Manifest, error) {
	var newestFirst []commit.Manifest
	seenCommit := map[string]bool{}
	seenLayer := map[string]bool{}
	for id := headCommitID; id != ""; {
		if seenCommit[id] {
			return nil, fmt.Errorf("%w: volume %s's history reaches commit %s twice", ErrIncomplete, volumeID, id)
		}
		if len(newestFirst) == maxRestoreDepth {
			return nil, fmt.Errorf("%w: volume %s has more than %d commits to restore",
				ErrIncomplete, volumeID, maxRestoreDepth)
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

// checkGeometry takes the volume's size from the commit and not from the catalog.
//
// A manifest whose virtual size differs from the head's would mean a resize, which this
// system does not have; a caller whose size differs from the head's means the catalog row
// and the bucket disagree about how big somebody's disk is. Neither is guessed at: the
// number decides how big a device the guest is handed, and the commit is the thing that
// describes the bytes.
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

// materialize puts one layer's plaintext qcow2 at path.
//
// held is the durable record saying this host already has this commit's layer, and it is
// only believed together with the file being there. That pair is the whole of "a
// re-placement onto a host that has most of the chain does not download all of it": the
// layer was either published by this host or fetched by an earlier restore, and in both
// cases it is already backed by the local parent.
func (r *Recoverer) materialize(ctx context.Context, enc *crypto.Encryption, m commit.Manifest, path string, held bool) error {
	if held {
		there, err := r.files.Exists(path)
		if err != nil {
			return fmt.Errorf("%w: looking for %s: %w", ErrIncomplete, path, err)
		}
		if there {
			return nil
		}
	}
	part := path + partSuffix
	w, err := r.files.Create(part)
	if err != nil {
		return fmt.Errorf("%w: creating %s: %w", ErrIncomplete, part, err)
	}
	// commit.Fetch checks the digest over the bytes as stored *before* unsealing one of
	// them, then GCM-authenticates every frame with the volume and layer id as additional
	// data — so a layer moved under another layer's manifest fails to open rather than
	// decrypting into the wrong chain. Not reimplemented here; there must be one answer
	// to "is this the object the manifest named".
	if err := commit.Fetch(ctx, r.store, enc, m, w); err != nil {
		_ = w.Close()
		r.discard(part)
		return fmt.Errorf("%w: layer %s of commit %s: %w", ErrIncomplete, m.Layer.LayerID, m.CommitID, err)
	}
	if err := w.Close(); err != nil {
		r.discard(part)
		return fmt.Errorf("%w: finishing %s: %w", ErrIncomplete, part, err)
	}
	if err := r.files.Rename(part, path); err != nil {
		r.discard(part)
		return fmt.Errorf("%w: renaming %s: %w", ErrIncomplete, part, err)
	}
	return nil
}

// discard drops a partial download. Its own failure is not reported: the caller is
// already returning the reason the restore was refused, and a leftover `.part` is
// garbage, not a chain — the next attempt truncates it.
func (r *Recoverer) discard(part string) { _ = r.files.Remove(part) }

// repoint makes a layer's header name the parent where it actually landed, and then
// checks that it does.
//
// `-F qcow2` is mandatory, not decoration: without it qemu-img exits 1 with "backing file
// format must be specified" (measured against the pinned 11.1.1). `-u` is what makes this
// O(1) — it rewrites the header and does not touch a cluster, which is correct because
// the parent's *contents* are the same bytes wherever the file sits.
//
// It is run unconditionally, including on a layer that was skipped as already held: it is
// one process, it is idempotent, and a conditional here is a branch that can be wrong.
//
// The check afterwards is not defensive. `rebase -u` onto a wrong-but-existing parent is
// accepted in complete silence (measured: a later `convert -O raw` succeeded with no
// warning), which is the same hazard qcow.Rotate guards with the same comparison. A chain
// root that claims a backing file is a clone lineage, which the commit protocol does not
// express yet, and it is refused rather than guessed at.
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
func (r *Recoverer) encryption(ctx context.Context, volumeID string) (*crypto.Encryption, error) {
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return nil, fmt.Errorf("%w: volume id %q: %w", ErrIncomplete, volumeID, err)
	}
	keys, err := r.keys.VolumeKeys(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s: %w", ErrIncomplete, volumeID, err)
	}
	dek, err := r.kms.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID, id)
	if err != nil {
		return nil, fmt.Errorf("%w: unwrapping the DEK of volume %s: %w", ErrIncomplete, volumeID, err)
	}
	enc, err := crypto.NewEncryption(dek, id)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s: %w", ErrIncomplete, volumeID, err)
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

// Absent is the Recovery a host has when it was started with no object store.
//
// It exists so that "this Agent cannot publish" and "this Agent may hand a guest a blank
// disk" stay different sentences. qcow.Deps.Recovery is not optional, and this is why: a
// nil there, or a bool the caller computes, is a thing a wiring change forgets — and the
// thing it forgets is the one that decides whether a volume with published commits
// somewhere else is refused or served empty.
//
// Every question it is asked is answered "I could not look", which qcow turns into a
// refusal. A volume that genuinely has no history is refused too, on a host configured
// this way, and that is the right trade: the alternative is guessing, and the guess that
// is wrong hands a guest somebody's data as zeros.
type Absent struct{}

// Restore answers that the volume has never published.
//
// It is the honest answer and not a shrug: a deployment with no object store has never
// published anything, so no volume in it has a history and every one of them is new. The
// first version of this refused instead, on the argument that a host which cannot look
// must not guess — and it broke every single-machine deployment, which is a configuration
// this system supports and documents.
//
// What that argument was really about is a host that *did* publish and is now started
// against no store. That case is caught where the evidence is: this host's own state.json
// records the commits whose layers it holds, and qcow.Open refuses a volume whose record
// names commits it cannot reach. Ignorance is distinguished from knowledge by the file
// this host wrote, not by the flag it was started with.
func (Absent) Restore(context.Context, string, int64) (qcow.Restored, error) {
	return qcow.Restored{}, fmt.Errorf("%w: this Agent was started with no object store", commit.ErrNoHead)
}

// Current answers the same way and for the same reason.
func (Absent) Current(context.Context, string) (string, error) {
	return "", fmt.Errorf("%w: this Agent was started with no object store", commit.ErrNoHead)
}
