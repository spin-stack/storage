package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// RequestSnapshot asks the host serving a volume to freeze a copy of it under a name (§19).
// It writes one catalog row in CREATING and returns; the Agent takes the snapshot, because
// it is the only thing holding the volume's write path.
//
// There is no call to a host and no operation row: the Agent converges on desired state
// (ADR-0021) and the row's own state is the record of how far it has got, so a Control Plane
// that dies between this write and the Agent noticing loses nothing.
//
// The volume must be ACTIVE. Snapshotting one being fenced or promoted would freeze a view
// whose writer may be taken away, under an epoch the fleet has left.
func RequestSnapshot(ctx context.Context, md metadata.Store, term int64, volumeID, snapshotID, requestID string) (metadata.Snapshot, error) {
	v, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return metadata.Snapshot{}, err
	}
	if v.State != lifecycle.VolumeActive {
		return metadata.Snapshot{}, fmt.Errorf("controlplane: volume %s is %s, not ACTIVE: only a volume with a live writer can be snapshotted (§19)",
			volumeID, v.State)
	}
	if v.PrimaryHostID == "" {
		return metadata.Snapshot{}, fmt.Errorf("controlplane: volume %s has no primary host to take snapshot %s", volumeID, snapshotID)
	}

	snap := metadata.Snapshot{
		SnapshotID: snapshotID,
		VolumeID:   volumeID,
		// The epoch the request is made under. The Agent reports the commit it named, and
		// a commit from a writer the fleet moved past is not this volume's (§12.3).
		Epoch: v.CurrentEpoch,
		// The parent link: a snapshot of a clone descends from the clone's own parent
		// snapshot, so a chain read from the catalog is the chain the objects form.
		ParentSnapshotID: v.ParentSnapshotID,
		State:            lifecycle.SnapshotCreating,
		RequestID:        requestID,
		// CommitID and SourceHostID are deliberately empty: they are
		// facts only the host that takes the snapshot can know, and guessing them here
		// would put a number in the catalog that no copy corresponds to. PublishSnapshot
		// stamps them from the Agent's report.
	}
	if err := md.CreateSnapshot(ctx, term, snap); err != nil {
		return metadata.Snapshot{}, err
	}
	return snap, nil
}
