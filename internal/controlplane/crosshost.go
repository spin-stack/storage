package controlplane

import (
	"context"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
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
// implies.
func CloneCrossHost(ctx context.Context, md metadata.Store, mat *materialize.Materializer,
	term int64, parentSnapshotID, newVolumeID, destHost string,
) (CrossHostClone, error) {
	snap, err := md.GetSnapshot(ctx, parentSnapshotID)
	if err != nil {
		return CrossHostClone{}, err
	}
	parent, err := md.GetVolume(ctx, snap.VolumeID)
	if err != nil {
		return CrossHostClone{}, err
	}

	// Reserve first: a materialization that fills the destination's NVMe is worse
	// than one that is refused (§28.2).
	if err := md.CommitHostCapacity(ctx, term, destHost, parent.SizeBytes); err != nil {
		return CrossHostClone{}, err
	}
	release := func() {
		// Best-effort rollback; a stale term means another CP owns the accounting.
		_ = md.CommitHostCapacity(ctx, term, destHost, -parent.SizeBytes)
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
