package qcow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
)

// compactSuffix is what the flattened image is called while `qemu-img convert` is writing
// it. The final name is only ever a complete image: a crash mid-convert leaves this, which
// no record names and no chain reads through, and the sweep collects it.
const compactSuffix = ".compacting"

// MaxLayers is the deepest chain anything in this system builds or opens — the ceiling
// §19's compaction exists to keep a volume away from, and a hard bound rather than a
// policy: a chain past it cannot be rebuilt on another host, so a volume that reached it
// would be one nothing can recover.
//
// 301 layers open fine in both qemu-img and qemu-system — measured — at one file
// descriptor and about 140 KiB of RSS per layer *in every process that opens the chain*,
// so the real ceiling is the default 1024-descriptor limit and it is reached by the VM
// rather than by a rebuild. Refusing at 256 turns "the fleet quietly built a chain nobody
// can open" into a loud refusal well before that.
//
// It bounds the whole lineage and not one generation's history, which is what
// controlplane.MaxChainDepth is derived from.
const MaxLayers = 256

// DefaultCompaction is §19's policy, and it is on by default for a reason that is not
// tidiness: without it a chain grows until it passes MaxLayers, and then the volume cannot
// be rebuilt anywhere. A collapse is what keeps that from being reachable, so shipping the
// size trigger on (DefaultRotateAtBytes) and the collapse off would be shipping a fleet
// that slowly makes its own volumes unrecoverable.
//
// 32 layers is §19's own example and it is an eighth of the ceiling. The headroom is the
// point rather than the number: a collapse waits for the guest to detach — QEMU holds
// every file of a live chain and §5 forbids going around it — so between the policy firing
// and the collapse running, the volume goes on rotating. 224 layers of room is what a
// long attach is given.
//
// AtBytes is left at zero. Depth is the constraint with a hard bound behind it; a second
// threshold measured against nothing would be choosing, which is what §19 says not to do.
var DefaultCompaction = CompactionPolicy{AtLayers: 32}

// CompactionPolicy is when a volume's chain has grown far enough that collapsing it is
// worth doing (v6 §19: chains cannot grow without limit).
//
// Zero on both disables it entirely, which is free: no chain is evaluated and no layer is
// stat'd. DefaultCompaction is what a binary uses.
type CompactionPolicy struct {
	// AtLayers is the chain depth — the tip plus every layer under it — at or above
	// which a chain is due for compaction. Zero leaves depth out of the decision.
	AtLayers int
	// AtBytes is §19's total_incremental_size: the bytes of the published layers a
	// `qemu-img convert` would read. It is what the collapse would cost, not what the
	// volume occupies, because that is the number the decision is about. Zero leaves
	// size out of the decision.
	AtBytes int64
}

// Validate reports a policy that could not mean anything.
func (p CompactionPolicy) Validate() error {
	switch {
	case p.AtLayers < 0 || p.AtBytes < 0:
		return fmt.Errorf("qcow: a compaction policy cannot be negative, got %d layers and %d bytes", p.AtLayers, p.AtBytes)
	case p.AtLayers == 1:
		// A one-layer chain is already a root: a threshold of 1 would call every volume
		// due for a compaction that would collapse nothing.
		return fmt.Errorf("qcow: a compaction threshold of %d layer is below the one layer every volume has", p.AtLayers)
	}
	return nil
}

func (p CompactionPolicy) set() bool { return p.AtLayers > 0 || p.AtBytes > 0 }

// compaction is what this host will do about a chain that has grown past the policy.
type compaction struct {
	// chainDepth is how many layers a guest reads through, and localDiskBytes is what
	// this volume occupies on this host. They are v6 §21's two chain numbers.
	chainDepth     int
	localDiskBytes int64
	// collapse is the layers the convert flattens, oldest first. They are exactly the
	// published ones: the tip is being written to (v6 §5 forbids an offline tool on it)
	// and a sealed layer that is not published yet is a commit that has not happened.
	collapse []string
	// intoCommitID is the newest commit the collapsed layers reconstruct. The new root
	// replaces it, and reconstructing exactly what it reconstructed is the whole of what
	// makes the collapse safe.
	intoCommitID string
	virtualSize  int64
	// readBytes is what the convert reads: every byte of every layer it collapses.
	readBytes int64
	// writeAtMostBytes is an upper bound and not a measurement. A convert writes one
	// cluster where the chain holds several versions of it and skips the holes, so the
	// result cannot exceed the smaller of what it reads and the guest's disk — and where
	// inside that it lands depends on how the guest overwrote itself, which nothing
	// offline can answer without reading the allocation of every layer.
	writeAtMostBytes int64
	// trigger names the threshold that fired, because the operator's next question is
	// which one and the two mean different things: depth is read amplification, bytes are
	// disk.
	trigger string
}

// publishedPrefix is the layers of the commits this host holds, oldest first, with the
// commit the topmost one reconstructs and what reading all of them costs.
//
// It is the collapse set, and it comes from the record rather than from the chain because
// the record is the only thing that knows which layers are *published* — a question about
// the object store that no local file answers. That the two agree is not assumed:
// checkCollapseSet walks the files before a byte is converted.
func publishedPrefix(p Paths, root, volumeID string, st State) (layers []string, into string, readBytes int64, err error) {
	for _, published := range st.Commits {
		size, err := p.Size(LayerImage(root, published.LayerID))
		if err != nil {
			return nil, "", 0, fmt.Errorf("qcow: measuring layer %s of volume %s to plan a compaction: %w",
				published.LayerID, volumeID, err)
		}
		layers = append(layers, published.LayerID)
		readBytes += size
		into = published.CommitID
	}
	return layers, into, readBytes, nil
}

// planCompaction decides whether a chain is due and, when it is, what collapsing it
// involves. A nil plan and a nil error is a chain that is not due — including every chain
// when no policy is set.
func planCompaction(p Paths, policy CompactionPolicy, root, volumeID, tip string, st State, virtualSize int64) (*compaction, error) {
	if !policy.set() {
		return nil, nil
	}
	depth := chainDepth(st, tip)
	if policy.AtBytes == 0 && depth < policy.AtLayers {
		// Depth is free — it comes out of the record — and the bytes are one stat per
		// layer per volume per cycle. With no size threshold set there is nothing to
		// spend them on.
		return nil, nil
	}
	c := compaction{chainDepth: depth, virtualSize: virtualSize}
	var err error
	c.collapse, c.intoCommitID, c.readBytes, err = publishedPrefix(p, root, volumeID, st)
	if err != nil {
		return nil, err
	}
	local, err := layerBytes(p, root, volumeID)
	if err != nil {
		return nil, err
	}
	c.localDiskBytes = local
	switch {
	case policy.AtLayers > 0 && depth >= policy.AtLayers && policy.AtBytes > 0 && c.readBytes >= policy.AtBytes:
		c.trigger = "chain_depth+incremental_bytes"
	case policy.AtLayers > 0 && depth >= policy.AtLayers:
		c.trigger = "chain_depth"
	case policy.AtBytes > 0 && c.readBytes >= policy.AtBytes:
		c.trigger = "incremental_bytes"
	default:
		return nil, nil
	}
	if len(c.collapse) < 2 {
		// Due, and there is nothing to collapse: one published layer is already the root
		// a convert would produce, and the layers above it are the tip a guest is writing
		// to and at most one sealed layer that is not a commit yet.
		return nil, nil
	}
	c.writeAtMostBytes = c.readBytes
	if virtualSize < c.writeAtMostBytes {
		c.writeAtMostBytes = virtualSize
	}
	return &c, nil
}

// chainDepth is how many layers a guest reading the tip reads through: every published
// commit, every layer sealed under the tip and not published yet, and the tip itself.
//
// Derived from this host's record rather than from `qemu-img info --backing-chain`, which
// is the only other source and cannot be used: a guest holds the tip's write lock, so the
// walk is refused (v6 §5 forbids it anyway). The record can over-count a chain this host
// no longer serves — clearFork keeps Commits after a fork is re-derived — which is the
// direction an operator can act on.
func chainDepth(st State, tip string) int {
	return len(st.Commits) + len(st.SealedBelow(LayerIDOfImage(tip))) + 1
}

// layerBytes is what this volume's layers occupy on this host — v6 §21's
// local_disk_bytes. The directory is listed rather than the record walked, because the
// number an operator is watching is the disk's and not the record's: an orphan overlay a
// rotation left behind occupies space no record names.
func layerBytes(p Paths, root, volumeID string) (int64, error) {
	dir := LayersDir(root)
	names, err := p.List(dir)
	if err != nil {
		return 0, fmt.Errorf("qcow: listing %s to measure what volume %s occupies: %w", dir, volumeID, err)
	}
	var total int64
	for _, name := range names {
		if filepath.Ext(name) != layerSuffix {
			continue
		}
		size, err := p.Size(filepath.Join(dir, name))
		if err != nil {
			return 0, fmt.Errorf("qcow: measuring %s of volume %s: %w", name, volumeID, err)
		}
		total += size
	}
	return total, nil
}

// compact collapses a chain that is past its policy into one flattened image, publishes it
// as a new immutable root, and repoints the local chain at it (v6 §19).
//
// # What is collapsed, and what is never touched
//
// The published prefix, and nothing above it. The tip is under a guest — v6 §5 forbids an
// offline tool on it and qemu-img would be refused on QEMU's write lock anyway — and a
// sealed layer that is not published yet is a commit that has not happened, so putting
// either into a root would publish bytes no commit ever claimed. Everything the prefix
// contains is already durable, so the convert reads files whose content nothing can change
// under it while it runs.
//
// # The three steps, and why the last one waits
//
//	convert → publish (PUT layer, PUT manifest, CAS HEAD) → repoint the chain at the root
//
// The rebase is what makes a collapse repeatable. Without it the record says [root] while
// the layer above the prefix still reads through the files the root replaced: the next
// collapse walks that chain, finds layers no commit names, and refuses — once a heartbeat,
// for the life of the volume, with the message a genuine fork produces. So the record of a
// collapse is cleared by the rebase and by nothing else, and until then no second one is
// planned.
//
// `qemu-img rebase -u` is one header write, and it is how every other repointing here is
// done (internal/recovery does it per layer of a rebuilt chain). What it needs is the file,
// and a running QEMU holds a lock on every file of the chain it has open — the tip and each
// backing layer under it. There is no version of this that touches those while the guest
// runs, so a collapse whose volume is attached publishes its root and then waits. Nothing
// is lost by waiting: the root is durable, the guest is on a chain that is complete, and
// the numbers §21 reports go on describing the layers actually being read.
//
// # Nothing is deleted
//
// The collapsed layers stay on disk and in the bucket. The guest reads through them until
// the rebase, and reclaiming them afterwards is the sweep's and the GC's job — doing it
// here would tie a transformation to a deletion.
//
// # What the new commit claims
//
// Exactly what a commit always claims: this state is reconstructible without the host. The
// root's layer is the flattened prefix, so it reconstructs, byte for byte, what
// ReplacesCommitID reconstructed — and it says so by carrying no parent, which is what
// makes a recovery stop there instead of walking and downloading the prefix it replaces.
// The layers above it compose unchanged: an overlay addresses absolute offsets in the
// guest's disk, so a delta over the prefix is the same delta over an image the prefix
// flattens to.
func (m *Manager) compact(ctx context.Context, v *volume, st State) error {
	if m.pub == nil || v.pending != nil {
		// A host that publishes nothing has no published prefix to collapse, and one that
		// still owes the object store a sealed layer publishes that first: v6 §11 keeps at
		// most one layer in flight, and a root that landed while an older layer was still
		// owed would put a commit into the history before its predecessor.
		return nil
	}
	if st.Compacting == nil {
		plan, err := planCompaction(m.paths, m.cfg.Compaction, m.cfg.Root, v.id, v.chain.Active, st, v.chain.SizeBytes)
		if err != nil {
			return fmt.Errorf("volume %s: %w", v.id, err)
		}
		if plan == nil {
			return nil
		}
		if err := m.startCompaction(v, &st, plan, LayerIDOfImage(v.chain.Active)); err != nil {
			return err
		}
	}
	if !st.Compacting.Published {
		if err := m.collapse(ctx, v, &st); err != nil {
			return err
		}
		if st.Compacting == nil {
			// Abandoned: the history moved under the plan and nothing was published. The
			// next cycle plans over what the history is now.
			return nil
		}
	}
	return m.rebase(ctx, v, *st.Compacting)
}

// startCompaction names the root that is about to be built, durably, before anything is
// converted or published.
//
// `tip` is the layer the rebase will have to move, and it is the right one because
// reconcile has already adopted everything sealed under it: a host with a sealed layer it
// has not published has a pending commit, and compact returns above without planning
// anything. So there is nothing between the tip and the prefix.
//
// The record is what makes the whole thing idempotent across a crash: without it a host
// that died after the CAS would find HEAD naming a commit it has no memory of — a published
// history it did not write, which is the definition of a stale chain, and the volume would
// be refused on the next open.
func (m *Manager) startCompaction(v *volume, st *State, plan *compaction, tip string) error {
	st.Compacting = &CompactedRoot{
		CommitID: ids.New().String(), LayerID: ids.New().String(),
		ReplacesCommitID: plan.intoCommitID, RebaseLayerID: tip,
	}
	if err := WriteState(m.paths, m.cfg.Root, v.id, *st); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	// The plan is stated here and not at the end, because this is the line that is there
	// when a collapse does not finish: what it is going to read, what it may write, and
	// which threshold decided. v6 §21's two chain numbers are in it for the same reason —
	// they are what an operator is watching when a host runs out of disk.
	slog.Info("collapsing this volume's published prefix into a new immutable root",
		"volume_id", v.id, "commit_id", st.Compacting.CommitID, "layer_id", st.Compacting.LayerID,
		"chain_depth", plan.chainDepth, "local_disk_bytes", plan.localDiskBytes,
		"trigger", plan.trigger, "collapsed_layers", len(plan.collapse),
		"collapsed_oldest", plan.collapse[0],
		"collapsed_newest", plan.collapse[len(plan.collapse)-1],
		"into_commit_id", plan.intoCommitID, "virtual_size", plan.virtualSize,
		"read_bytes", plan.readBytes, "write_bytes_at_most", plan.writeAtMostBytes)
	return nil
}

// collapse converts the prefix and publishes it as the commit the record already names.
//
// The prefix is re-derived from the record every time rather than stored with the plan,
// which is what makes an unfinished collapse survive a policy an operator has raised out of
// reach: the threshold decides whether a collapse *starts*, and the root this host began is
// finished whatever the threshold says now.
func (m *Manager) collapse(ctx context.Context, v *volume, st *State) error {
	root := *st.Compacting
	prefix, into, _, err := publishedPrefix(m.paths, m.cfg.Root, v.id, *st)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	if into != root.ReplacesCommitID {
		// A commit landed under this collapse: an ordinary rotation, published by this
		// host between the cycle that planned the root and this one. The flattened bytes
		// reconstruct the older commit, so publishing them now would move HEAD off a
		// commit that returned SUCCESS onto one that does not contain it.
		return m.abandonCompaction(v, st, "this host published commit "+into+" after the collapse was planned")
	}
	image, err := m.flatten(ctx, v, prefix, root.LayerID)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	bytes, err := m.paths.Size(image)
	if err != nil {
		return fmt.Errorf("qcow: measuring the flattened root %s of volume %s: %w", image, v.id, err)
	}
	layer := SealedLayer{
		VolumeID: v.id, LayerID: root.LayerID, CommitID: root.CommitID, Path: image,
		Epoch: v.epoch, PlainBytes: bytes,
		// The guest's disk, never the flattened file's length. A manifest's virtual size
		// is what a restore recreates the tip at, so a root claiming the size of its own
		// qcow2 is a commit that returned SUCCESS and that nothing can rebuild.
		VirtualSize: v.chain.SizeBytes, ReplacesCommitID: root.ReplacesCommitID,
	}
	if err := m.pub.Publish(ctx, layer); err != nil {
		switch {
		case errors.Is(err, commit.ErrRootSuperseded):
			// The object store's own statement of the check above, for the window this
			// host cannot see from its record: HEAD is not the commit these bytes
			// reconstruct.
			return m.abandonCompaction(v, st, err.Error())
		case errors.Is(err, commit.ErrHeadMoved):
			// Another host is this volume's writer. A collapse is a publish, so it meets
			// the same fence as any other one, and for the same reason: the alternative is
			// this host serving a guest whose writes can never land.
			return errors.Join(
				m.recordFenced(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err.Error()),
				m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err))
		}
		return fmt.Errorf("volume %s: publishing the compacted root %s: %w", v.id, root.CommitID, err)
	}
	st.Compacting.Published = true
	if err := WriteState(m.paths, m.cfg.Root, v.id, *st); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	slog.Info("this volume's published prefix is now one immutable root; the local chain reads through it once the guest lets go of the layers it replaces",
		"volume_id", v.id, "commit_id", root.CommitID, "layer_id", root.LayerID,
		"layer", image, "wrote_bytes", bytes, "replaces_commit_id", root.ReplacesCommitID)
	return nil
}

// abandonCompaction drops a collapse that was never published. It is not an error: the next
// cycle plans one over the history as it is now.
func (m *Manager) abandonCompaction(v *volume, st *State, why string) error {
	root := *st.Compacting
	st.Compacting = nil
	if err := WriteState(m.paths, m.cfg.Root, v.id, *st); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	// The flattened image is left where the sweep finds it: its id is minted after the tip
	// and no record names it any more, which is exactly what that sweep removes.
	slog.Warn("abandoning a chain collapse: it was never published and the commit it flattens is no longer the one this host's history ends at",
		"volume_id", v.id, "commit_id", root.CommitID, "layer_id", root.LayerID,
		"replaces_commit_id", root.ReplacesCommitID, "why", why)
	return nil
}

// flatten runs the convert and returns the path of the finished root.
//
// The chain is walked first, and that walk is the check the act rests on: the collapse set
// comes out of this host's record, and a record can name commits of a history this host no
// longer serves. `qemu-img info --backing-chain` on the top of the prefix says what the
// *files* are — the layers, in order, that a convert will actually read — and unless that
// is exactly the set the record claims, nothing is converted. It is also where a live file
// would be caught: the walk opens every layer, and QEMU's write lock refuses it.
//
// A root that is already there is a crash between the rename and the publish. It is used as
// it is: the convert is deterministic over immutable inputs, so the file on disk is the
// file this would write again, and it was checked before it was given that name.
func (m *Manager) flatten(ctx context.Context, v *volume, prefix []string, layerID string) (string, error) {
	image := LayerImage(m.cfg.Root, layerID)
	there, err := m.paths.Exists(image)
	if err != nil {
		return "", fmt.Errorf("qcow: looking for %s: %w", image, err)
	}
	if there {
		return image, nil
	}
	from := LayerImage(m.cfg.Root, prefix[len(prefix)-1])
	if err := m.checkCollapseSet(ctx, v, prefix, from); err != nil {
		return "", err
	}
	// Not bounded by ProbeTimeout: that number is what one `qemu-img info` or one QMP
	// exchange may take, and reading a whole chain and writing a whole image is neither.
	// The cycle's own context is the bound, and a convert it cuts short leaves the
	// temporary file, which the next cycle replaces.
	tmp := image + compactSuffix
	if _, err := m.run.Run(ctx, m.cfg.QemuImg, "convert", "-O", "qcow2", from, tmp); err != nil {
		return "", fmt.Errorf("qcow: converting the published prefix of volume %s into %s: %w", v.id, tmp, err)
	}
	if err := m.checkRoot(ctx, tmp, v.chain.SizeBytes); err != nil {
		// The temporary file is removed rather than left: it is not the root it was
		// supposed to be, and the next cycle must convert again rather than find it.
		if rmErr := m.paths.Remove(tmp); rmErr != nil {
			return "", errors.Join(err, rmErr)
		}
		return "", err
	}
	if err := m.paths.Rename(tmp, image); err != nil {
		return "", fmt.Errorf("qcow: naming the compacted root of volume %s: %w", v.id, err)
	}
	return image, nil
}

// checkCollapseSet refuses a set whose layers are not the chain the convert would read.
func (m *Manager) checkCollapseSet(ctx context.Context, v *volume, prefix []string, from string) error {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	chain, err := inspectChain(ctx, m.run, m.cfg.QemuImg, from)
	if err != nil {
		return err
	}
	walked := make(map[string]bool, len(chain))
	for _, layer := range chain {
		walked[filepath.Clean(layer.Filename)] = true
	}
	// Named one by one and not counted, because the answer an operator needs is *which*
	// layer this host recorded as published that the chain does not read through — a count
	// says a fork happened and not where.
	for _, id := range prefix {
		if want := LayerImage(m.cfg.Root, id); !walked[filepath.Clean(want)] {
			return fmt.Errorf("%w: volume %s records layer %s as published, and the chain under %s does not read through it",
				ErrChainMismatch, v.id, id, from)
		}
	}
	if len(chain) != len(prefix) {
		return fmt.Errorf("%w: volume %s records %d published layers under %s and the chain walks %d",
			ErrChainMismatch, v.id, len(prefix), from, len(chain))
	}
	// The walk is top-first and the prefix is oldest-first: same layers, same order.
	for i, layer := range chain {
		want := LayerImage(m.cfg.Root, prefix[len(prefix)-1-i])
		if filepath.Clean(layer.Filename) != filepath.Clean(want) {
			return fmt.Errorf("%w: volume %s would collapse %s, and the chain under %s has %s in that place",
				ErrChainMismatch, v.id, want, from, layer.Filename)
		}
	}
	if got := chain[0].VirtualSize; got != v.chain.SizeBytes {
		return fmt.Errorf("%w: the published prefix of volume %s is %d bytes and its chain is %d",
			ErrChainMismatch, v.id, got, v.chain.SizeBytes)
	}
	return nil
}

// checkRoot is what the convert produced, before it is given a layer's name: a whole image
// of the volume's size, standing on nothing.
//
// The backing file is the load-bearing half. A root that quietly kept one would publish a
// commit whose layer needs a file no recovery is ever told to fetch — a commit that
// returned SUCCESS and reconstructs a hole.
func (m *Manager) checkRoot(ctx context.Context, image string, sizeBytes int64) error {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	info, err := inspect(ctx, m.run, m.cfg.QemuImg, image)
	if err != nil {
		return err
	}
	switch {
	case info.Format != "qcow2":
		return fmt.Errorf("%w: the compacted root %s is a %s image, not qcow2", ErrChainMismatch, image, info.Format)
	case info.VirtualSize != sizeBytes:
		return fmt.Errorf("%w: the compacted root %s is %d bytes and the chain it replaces is %d",
			ErrChainMismatch, image, info.VirtualSize, sizeBytes)
	case info.FullBackingFilename != "":
		return fmt.Errorf("%w: the compacted root %s is backed by %q, so it is not a root",
			ErrChainMismatch, image, info.FullBackingFilename)
	case info.Specific.Data.Corrupt:
		return fmt.Errorf("%w: the compacted root %s has the qcow2 corrupt flag set, so a commit naming it could never be restored",
			ErrChainMismatch, image)
	}
	return nil
}

// rebase repoints the layer above the collapsed prefix at the root that replaced it, which
// is what makes the chain shorter and a second collapse possible.
//
// It cannot run while a guest is attached, and nothing here tries. Whether one is, is QMP's
// answer and not a guess; a socket that stopped answering while a VM runs reads as
// unattached here, and then qemu-img's refusal on the write lock is what stops it — the
// same protection Open's offline branch already stands on.
func (m *Manager) rebase(ctx context.Context, v *volume, root CompactedRoot) error {
	if v.attached {
		if !v.awaitingRebase {
			v.awaitingRebase = true
			slog.Info("this volume's compacted root is published and its chain still reads through the layers that root replaces: repointing them needs the files, and a guest holds every file of the chain it has open",
				"volume_id", v.id, "commit_id", root.CommitID,
				"layer", LayerImage(m.cfg.Root, root.RebaseLayerID))
		}
		return nil
	}
	onto := LayerImage(m.cfg.Root, root.LayerID)
	if err := m.repoint(ctx, LayerImage(m.cfg.Root, root.RebaseLayerID), onto, v.chain.Active); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	if err := m.recordCollapsed(v, root); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	v.awaitingRebase = false
	slog.Info("this volume's chain reads through its compacted root; the layers it replaces are left for the GC",
		"volume_id", v.id, "commit_id", root.CommitID, "tip", v.chain.Active, "root", onto)
	return nil
}

// repoint rewrites one layer's backing pointer and then reads the chain back to prove it.
//
// `-u` is what a rebase onto an already-flattened prefix means: the root holds those bytes,
// so no cluster is copied and only the header is rewritten. It also does not open the
// backing file, so a path that is wrong — a stale id, a file that is not there — survives
// the command itself with an exit code of zero. The walk is what catches that, and it
// starts at the *tip*, because the claim is about what a guest reads: the bottom of the
// chain under the tip is the root, and nothing under it is opened.
func (m *Manager) repoint(ctx context.Context, image, onto, tip string) error {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	if _, err := m.run.Run(ctx, m.cfg.QemuImg, "rebase", "-u", "-f", "qcow2", "-b", onto, "-F", "qcow2", image); err != nil {
		return fmt.Errorf("qcow: repointing %s at the compacted root %s: %w", image, onto, err)
	}
	chain, err := inspectChain(ctx, m.run, m.cfg.QemuImg, tip)
	if err != nil {
		return err
	}
	if bottom := chain[len(chain)-1]; filepath.Clean(bottom.Filename) != filepath.Clean(onto) {
		return fmt.Errorf("%w: the chain under %s reaches %s, and the compacted root is %s",
			ErrChainMismatch, tip, bottom.Filename, onto)
	}
	// And the root's immediate child is the layer that was repointed. The check above is
	// not enough on its own and the difference is everything a rotation can put between
	// them: repointing the *tip* also leaves the root at the bottom, while unhooking every
	// layer in between — each of which holds a published commit and the guest's writes.
	// A plant that did exactly that left the whole package green.
	if len(chain) < 2 {
		return fmt.Errorf("%w: the chain under %s is one layer after a rebase onto %s", ErrChainMismatch, tip, onto)
	}
	if above := chain[len(chain)-2]; filepath.Clean(above.Filename) != filepath.Clean(image) {
		return fmt.Errorf("%w: %s sits directly on the compacted root and %s is what was repointed onto it, so the layers between them are no longer read",
			ErrChainMismatch, above.Filename, image)
	}
	return nil
}

// recordCollapsed replaces this host's record of the prefix with the root that supersedes
// it, in the one moment where the record and the files on disk agree again.
//
// The old entries go because the layers they name are not commits this host holds any more.
// Commits published *after* the collapse began are kept: they sit above the root and they
// are still the history. Keeping the prefix instead would make the next cycle plan the same
// collapse for ever, which is the failure this whole path exists to end.
//
// Layers is cut back to the rebased layer in the same breath, and it has to be: "sealed and
// not published" is derived as (the layers seen as tips) − (the tip) − (the published
// ones), so a prefix layer that stayed in Layers after leaving Commits would read as a run
// of sealed layers this host owes — and the next cycle would publish a *published* layer
// again as a new commit.
//
// LastCommitAt is deliberately not touched. It is the RPO — the age of the newest *guest*
// writes that are durable — and a compaction publishes nothing the guest wrote, so moving
// it would let a volume that has not committed for an hour look fresh and postpone the
// commit that would have made it true.
func (m *Manager) recordCollapsed(v *volume, root CompactedRoot) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return err
	}
	at := slices.IndexFunc(st.Commits, func(c CommitLayer) bool { return c.CommitID == root.ReplacesCommitID })
	if at < 0 {
		// The commit the root flattens is not in the list the root is replacing, so which
		// of these entries the root supersedes cannot be worked out. Rewriting anyway
		// would either drop commits that are still the history or keep layers the root
		// contains — and the record is what the next collapse is planned from.
		return fmt.Errorf("%w: the compacted root %s replaces commit %s, which this host does not record",
			ErrChainMismatch, root.CommitID, root.ReplacesCommitID)
	}
	st.Compacting = nil
	st.Commits = append([]CommitLayer{{CommitID: root.CommitID, LayerID: root.LayerID}}, st.Commits[at+1:]...)
	st.trimLayers(root.RebaseLayerID)
	return WriteState(m.paths, m.cfg.Root, v.id, st)
}
