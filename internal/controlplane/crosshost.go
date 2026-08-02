package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// CrossHostClone is the result of placing a clone on a host that did not have the
// data: the new volume record, the state rebuilt on the destination, and what the
// rebuild cost (progress feeds the operation's visible progress and the measured
// cold RTO, §28.1/§29.4).
type CrossHostClone struct {
	Volume   metadata.Volume
	View     *cow.IntervalMap
	Progress materialize.Progress
}

// CloneCrossHost creates a clone on destHost by full materialization from the
// object store (§20 cross-host, §22.3 cold): the parent snapshot's state is rebuilt
// there from S3 alone — the source host is never contacted — and only a complete,
// verified materialization records the clone.
//
// There is no reservation to take and none to release (ADR-0017). The destination is
// charged when the volume row naming it exists, and not before, so a clone that
// fails at any point leaks nothing: there is no delta anybody has to remember to
// reverse, and no leadership change that can strand one.
//
// The §28.2 bound is checked twice, for two different reasons. The advisory check
// here refuses before a cold materialization is started against a host that plainly
// cannot hold the result. The authoritative one is a predicate of CreateVolume,
// because the decision was taken against a fleet read that a drain or another clone
// may have shared: two clones that both fetch and one that is refused at the write
// is wasted work, and it is the only shape in which the ceiling cannot be exceeded.
func CloneCrossHost(ctx context.Context, md metadata.Store, store objectstore.Store,
	mat *materialize.Materializer, policy placement.Policy, term int64,
	parentSnapshotID, newVolumeID, destHost string,
) (CrossHostClone, error) {
	snap, err := md.GetSnapshot(ctx, parentSnapshotID)
	if err != nil {
		return CrossHostClone{}, err
	}
	parent, err := md.GetVolume(ctx, snap.VolumeID)
	if err != nil {
		return CrossHostClone{}, err
	}
	dest, err := md.GetHost(ctx, destHost)
	if err != nil {
		return CrossHostClone{}, err
	}

	// A materialization that fills the destination's NVMe is worse than one that is
	// refused, so ask before fetching (§28.2).
	if !policy.Admits(dest, parent.SizeBytes) {
		return CrossHostClone{}, fmt.Errorf("%w: host %s holds %d of %d committed bytes and cannot take %d",
			metadata.ErrCapacityExceeded, destHost, dest.NVMeCommittedBytes, policy.Limit(dest), parent.SizeBytes)
	}

	view, prog, err := mat.FromSnapshot(ctx, snap.VolumeID, parentSnapshotID)
	if err != nil {
		return CrossHostClone{}, err
	}

	clone, err := Clone(ctx, md, store, term, parentSnapshotID, newVolumeID, destHost,
		&metadata.CapacityBound{HostID: destHost, AddBytes: parent.SizeBytes, Limit: policy.Limit(dest)})
	if err != nil {
		return CrossHostClone{}, err
	}
	return CrossHostClone{Volume: clone, View: view, Progress: prog}, nil
}
