package controlplane

import (
	"context"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Clone creates a new, independent volume from a parent snapshot as a same-host
// clone (§20): it is pure metadata — a new active child at epoch 1 that reuses the
// parent snapshot's already-durable objects, with no data copy. Cross-host
// materialization is Phase 11. The clone inherits the parent's size, block size,
// durability, and DEK (so it can read the shared base), and increments the chain
// depth (§20.1). Returns the new volume's descriptor-shaped record.
//
// bound is the §28.2 ceiling the new volume is placed under (ADR-0017): creating it
// is what charges newHostID, so it is the write the bound belongs to. A same-host
// clone that the caller has already admitted may pass nil.
func Clone(ctx context.Context, md metadata.Store, term int64, parentSnapshotID, newVolumeID, newHostID string,
	bound *metadata.CapacityBound,
) (metadata.Volume, error) {
	snap, err := md.GetSnapshot(ctx, parentSnapshotID)
	if err != nil {
		return metadata.Volume{}, err
	}
	parent, err := md.GetVolume(ctx, snap.VolumeID)
	if err != nil {
		return metadata.Volume{}, err
	}
	clone := metadata.Volume{
		VolumeID:      newVolumeID,
		SizeBytes:     parent.SizeBytes,
		Durability:    parent.Durability,
		BlockSize:     parent.BlockSize,
		CurrentEpoch:  1, // a fresh active child
		State:         lifecycle.VolumeActive,
		PrimaryHostID: newHostID,
		ChainDepth:    parent.ChainDepth + 1,
		DEKWrapped:    parent.DEKWrapped,
		KEKID:         parent.KEKID,
	}
	if err := md.CreateVolume(ctx, term, clone, bound); err != nil {
		return metadata.Volume{}, err
	}
	return clone, nil
}
