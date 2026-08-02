package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/simio/objectstore"

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
func Clone(ctx context.Context, md metadata.Store, store objectstore.Store, term int64,
	parentSnapshotID, newVolumeID, newHostID string, bound *metadata.CapacityBound,
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
		// A clone shares the parent's DEK (§19: the chain's objects are the parent's
		// until the child writes), so it must share the *version* that names it —
		// crypto.DevKMS binds it as GCM AAD, and a clone carrying the key without the
		// version is a volume nobody can open.
		DEKKeyID: parent.DEKKeyID,
		// And the link itself. ChainDepth above says a chain exists; these say what is
		// on the other end of it, which is what the clone's Agent needs to find the
		// objects it reads through. Without them the clone starts an empty WAL under
		// its own id, finds nothing under that id in the object store, and serves
		// zeros for everything the parent ever wrote (DEV-0007).
		ParentSnapshotID: parentSnapshotID,
		ParentVolumeID:   snap.VolumeID,
	}
	if err := md.CreateVolume(ctx, term, clone, bound); err != nil {
		return metadata.Volume{}, err
	}

	// The descriptor, for the same reason provisioning writes one: §22.5's
	// rebuild-metadata reconstructs volumes from these objects, and a clone with no
	// descriptor is a volume a restore silently loses — along with the chain link that
	// is the difference between reading its parent's data and reading zeros.
	//
	// Reported, not rolled back, exactly as provisioning does it: deleting the row here
	// would need a term-guarded delete that does not exist, and would turn one
	// repairable inconsistency into two writes that can each fail.
	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID:         clone.VolumeID,
		SizeBytes:        clone.SizeBytes,
		BlockSize:        clone.BlockSize,
		Durability:       clone.Durability,
		CurrentEpoch:     clone.CurrentEpoch,
		ChainDepth:       clone.ChainDepth,
		KEKID:            clone.KEKID,
		DEKWrapped:       clone.DEKWrapped,
		DEKKeyID:         clone.DEKKeyID,
		ParentSnapshotID: clone.ParentSnapshotID,
	}); err != nil {
		return clone, fmt.Errorf("writing the descriptor for clone %s (the row exists; rebuild-metadata cannot see it until this succeeds): %w",
			clone.VolumeID, err)
	}
	return clone, nil
}
