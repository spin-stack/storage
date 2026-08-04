package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/simio/objectstore"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
)

// Clone creates a new, independent volume from a parent snapshot (§20): it is pure
// metadata — a new active child at epoch 1 that reuses the parent snapshot's
// already-durable objects, with no data copy. The clone inherits the parent's size,
// block size, and DEK (so it can read the shared base), and increments the
// chain depth (§20.1). Returns the new volume's descriptor-shaped record.
//
// **Where it lands is decided here, not passed in.** policy.Choose implements §20's
// three steps — source host, a host with the data cached, any host with capacity — and
// step 1 is most of the boot-time story under ADR-0026: a cross-host clone pays a full
// download from the object store, with no warm standby and no lazy loading to shorten
// it, while a same-host clone reads local NVMe. The snapshot's source_host_id is what
// makes that possible, and it is a fact rather than a guess because the host that took
// the snapshot stamped it (§19, increment 3b).
//
// Two things it deliberately is not. Same-host is a *preference*: Choose falls through
// when that host is full, cordoned or gone, and making it mandatory would couple
// scheduling to a host with no obligation to be up. And the locality is time-bounded —
// the source host holds the data only while it still holds the volume, so once the
// source stops, step 1 buys nothing and the clone pays the download.
//
// The §28.2 ceiling travels with the write rather than being checked here (ADR-0017):
// Choose is pure and advisory, so two callers reading the same fleet pick the same
// destination and both commit. Limit is the same number Choose admitted against, handed
// to the statement that places the bytes.
func Clone(ctx context.Context, md metadata.Store, store objectstore.Store, policy placement.Policy,
	term int64, parentSnapshotID, newVolumeID string,
) (metadata.Volume, error) {
	snap, err := md.GetSnapshot(ctx, parentSnapshotID)
	if err != nil {
		return metadata.Volume{}, err
	}
	if snap.State != lifecycle.SnapshotPublished {
		// A clone of a snapshot whose objects are not written yet reads zeros for
		// everything its parent wrote — DEV-0007's shape, reached through the catalog
		// instead of through a missing field.
		return metadata.Volume{}, fmt.Errorf("controlplane: snapshot %s is %s, not PUBLISHED: nothing has been written for a clone to read",
			parentSnapshotID, snap.State)
	}
	parent, err := md.GetVolume(ctx, snap.VolumeID)
	if err != nil {
		return metadata.Volume{}, err
	}
	hosts, err := md.ListHosts(ctx)
	if err != nil {
		return metadata.Volume{}, err
	}
	newHostID, err := policy.Choose(hosts, placement.Request{
		SizeBytes: parent.SizeBytes,
		// The host that took the snapshot still has its data on local NVMe.
		SourceHostID: snap.SourceHostID,
	})
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: placing a clone of snapshot %s: %w", parentSnapshotID, err)
	}
	bound := &metadata.CapacityBound{HostID: newHostID, AddBytes: parent.SizeBytes}
	for _, h := range hosts {
		if h.HostID == newHostID {
			bound.Limit = policy.Limit(h)
		}
	}
	clone := metadata.Volume{
		VolumeID:      newVolumeID,
		SizeBytes:     parent.SizeBytes,
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
