package controlplane

import (
	"context"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
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
// object store (§20 cross-host, §22.3 cold): capacity is committed on the
// destination *before* the fetch starts, the parent snapshot's state is rebuilt
// there from S3 alone — the source host is never contacted — and only a complete,
// verified materialization records the clone.
//
// If anything fails the reservation is released, so a failed move never leaks
// committed capacity. Whether destHost *should* take the volume is the placement
// policy's decision (internal/placement); this function commits what that decision
// implies — and re-states the policy's bound as a predicate of the reservation
// itself, because the decision was taken against a fleet read that a drain or
// another clone may have been holding at the same time (§28.2).
func CloneCrossHost(ctx context.Context, md metadata.Store, mat *materialize.Materializer,
	policy placement.Policy, term int64, parentSnapshotID, newVolumeID, destHost string,
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

	// Reserve first: a materialization that fills the destination's NVMe is worse
	// than one that is refused (§28.2).
	if err := md.CommitHostCapacity(ctx, term, destHost, metadata.CapacityChange{
		DeltaBytes: parent.SizeBytes, Limit: policy.Limit(dest),
	}); err != nil {
		return CrossHostClone{}, err
	}
	release := func() {
		// Best-effort rollback; a stale term means another CP owns the accounting.
		// Unbounded: giving bytes back must not be refused by the ceiling.
		_ = md.CommitHostCapacity(ctx, term, destHost, metadata.CapacityChange{DeltaBytes: -parent.SizeBytes})
	}

	view, prog, err := mat.FromSnapshot(ctx, snap.VolumeID, parentSnapshotID)
	if err != nil {
		release()
		return CrossHostClone{}, err
	}

	clone, err := Clone(ctx, md, term, parentSnapshotID, newVolumeID, destHost)
	if err != nil {
		release()
		return CrossHostClone{}, err
	}
	return CrossHostClone{Volume: clone, View: view, Progress: prog}, nil
}
