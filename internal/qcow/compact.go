package qcow

import (
	"fmt"
	"log/slog"
	"path/filepath"
)

// CompactionPolicy is when a volume's chain has grown far enough that collapsing it is
// worth doing (v6 §19: chains cannot grow without limit).
//
// Both numbers are zero unless a caller sets them, and nothing in this tree chooses one.
// §19 says the policy is "ajustada por medición" and offers `layer_count >= 32` and
// `total_incremental_size >= 20 GiB` as examples; no chain has been measured in
// production, so those stay examples in the document rather than becoming defaults here.
// Zero on both is the honest state and it is also free: no chain is evaluated and no
// layer is stat'd.
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

// compaction is what this host would do about a chain that has grown past the policy,
// and it is only ever logged.
//
// Nothing here runs `qemu-img convert`, and nothing here removes a layer. Collapsing a
// chain publishes a new immutable root: HEAD comes to name a commit whose bytes no other
// host has ever seen, every snapshot that names an older commit depends on the layers it
// replaces, and the volume's whole history rests on one convert nobody watched. That is a
// publish path and a data-loss risk, so the decision, the plan and the numbers are
// produced here and the act belongs to a human.
type compaction struct {
	// chainDepth is how many layers a guest reads through, and localDiskBytes is what
	// this volume occupies on this host. They are v6 §21's two chain numbers.
	chainDepth     int
	localDiskBytes int64
	// collapse is the layers a convert would flatten, oldest first. They are exactly the
	// published ones: the tip is being written to (v6 §5 forbids an offline tool on it)
	// and a sealed layer that is not published yet is a commit that has not happened.
	collapse []string
	// intoCommitID is the newest commit the collapsed layers reconstruct, which is what
	// the new root would have to be published as a replacement for.
	intoCommitID string
	virtualSize  int64
	// readBytes is what a convert would read: every byte of every layer it collapses.
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

// planCompaction decides whether a chain is due and, when it is, what collapsing it
// would involve. A nil plan and a nil error is a chain that is not due — including every
// chain when no policy is set.
//
// The depth is derived from this host's record rather than from `qemu-img info
// --backing-chain`, which is the only other source and cannot be used: a guest holds the
// tip's write lock, so the walk is refused (v6 §5 forbids it anyway). The record can name
// commits of a chain this host no longer serves — clearFork keeps Commits after a fork is
// re-derived, so the next rebuild can skip a download — and then the depth over-counts
// *and* the collapse set names layers the tip does not read through. The second half is
// the one that matters, because the collapse set is what a human would hand to a convert:
// establishing that every layer named here is in the tip's backing chain is the first
// thing the review of an act has to do, and nothing offline can do it from here.
func planCompaction(p Paths, policy CompactionPolicy, root, volumeID, tip string, st State, virtualSize int64) (*compaction, error) {
	if !policy.set() {
		return nil, nil
	}
	depth := len(st.Commits) + len(st.sealedBelow(LayerIDOfImage(tip))) + 1
	if policy.AtBytes == 0 && depth < policy.AtLayers {
		// Depth is free — it comes out of the record — and the bytes are one stat per
		// layer per volume per cycle. With no size threshold set there is nothing to
		// spend them on.
		return nil, nil
	}
	c := compaction{chainDepth: depth, virtualSize: virtualSize}
	for _, published := range st.Commits {
		size, err := p.Size(LayerImage(root, volumeID, published.LayerID))
		if err != nil {
			return nil, fmt.Errorf("qcow: measuring layer %s of volume %s to plan a compaction: %w",
				published.LayerID, volumeID, err)
		}
		c.collapse = append(c.collapse, published.LayerID)
		c.readBytes += size
		c.intoCommitID = published.CommitID
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
		// to and at most one sealed layer that is not a commit yet. Saying so would be a
		// warning an operator can do nothing about.
		return nil, nil
	}
	c.writeAtMostBytes = c.readBytes
	if virtualSize < c.writeAtMostBytes {
		c.writeAtMostBytes = virtualSize
	}
	return &c, nil
}

// layerBytes is what this volume's layers occupy on this host — v6 §21's
// local_disk_bytes. The directory is listed rather than the record walked, because the
// number an operator is watching is the disk's and not the record's: an orphan overlay a
// rotation left behind occupies space no record names.
func layerBytes(p Paths, root, volumeID string) (int64, error) {
	dir := LayersDir(root, volumeID)
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

// noteCompaction reports a chain that is due for compaction, so that an operator meets it
// as a log line rather than as a host that ran out of disk or a guest reading through
// forty layers.
//
// Once per depth and not once per cycle: the condition, once true, stays true until a
// human acts on it — nothing here collapses anything — so a line per heartbeat would be
// the same sentence for ever. A rotation deepens the chain by one and says it again,
// which is the shape of the thing getting worse.
func (m *Manager) noteCompaction(v *volume, st State) error {
	plan, err := planCompaction(m.paths, m.cfg.Compaction, m.cfg.Root, v.id, v.chain.Active, st, v.chain.SizeBytes)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	if plan == nil || plan.chainDepth == v.compactedDepth {
		return nil
	}
	v.compactedDepth = plan.chainDepth
	slog.Warn("this volume's chain is past its compaction threshold, and nothing here compacts it: collapsing it publishes a new root, which is a publish and belongs to a human",
		"volume_id", v.id,
		"chain_depth", plan.chainDepth,
		"local_disk_bytes", plan.localDiskBytes,
		"trigger", plan.trigger,
		"collapse_layers", len(plan.collapse),
		"collapse_oldest", plan.collapse[0],
		"collapse_newest", plan.collapse[len(plan.collapse)-1],
		"into_commit_id", plan.intoCommitID,
		"virtual_size", plan.virtualSize,
		"read_bytes", plan.readBytes,
		"write_bytes_at_most", plan.writeAtMostBytes)
	return nil
}
