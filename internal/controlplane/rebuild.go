package controlplane

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// RebuildSummary is what one rebuild found and recorded.
type RebuildSummary struct {
	Volumes   int
	Snapshots int
}

// RebuildMetadata reconstructs the volume and snapshot catalog from the object store
// alone (§22.5, INV-20). It is the reader `descriptor.Write` had been feeding since
// increment 4 deleted the previous one, and it is why those objects are written at all:
// without it a lost PostgreSQL is unrecoverable even though every byte of every volume
// is intact in the bucket.
//
// **It is much smaller than the one ADR-0026 removed.** That version walked an epoch
// chain, a recovery point per epoch and a contiguous prefix of WAL objects. A volume's
// state in S3 is now one descriptor plus one manifest, so this reads two objects per
// volume and nothing has to be reassembled.
//
// # What it restores, and what it cannot
//
// It restores what the objects state: geometry, the wrapped DEK with the
// version that names it, the chain depth and the parent link, and the snapshots that
// exist under each volume's prefix. It does **not** restore placement — no volume comes
// back with a primary host — because no object records one. A rebuilt catalog therefore
// describes volumes nobody is serving, which is the honest outcome: after losing the
// database you know what exists, not who was running it, and an operator re-places them.
//
// Two facts changed meaning with ADR-0026 and are worth stating where they are used:
// the descriptor's `current_epoch` is now **authoritative** rather than "last known"
// (the epoch object it deferred to was deleted with the fencing chain), and a snapshot
// manifest's existence *is* its PUBLISHED state, because it is written create-only and
// never changes (INV-16).
//
// # Why three passes
//
// `volumes.parent_snapshot_id` references `snapshots`, and `snapshots.volume_id`
// references `volumes`. Neither can go first. So: every volume without its parent link,
// then every snapshot, then the clones again with the link — which the catalog's own
// upsert is built for (`parent_snapshot_id = COALESCE(existing, excluded)`, never
// cleared by a converging write).
//
// Running it twice, or from two operators at once, converges rather than aborting;
// `CreateVolume` and `CreateSnapshot` are both idempotent by design, and this passes no
// capacity bound because it is recording volumes that already occupy their hosts.
func RebuildMetadata(ctx context.Context, md metadata.Store, store objectstore.Store, term int64) (RebuildSummary, error) {
	var sum RebuildSummary

	descs, err := listDescriptors(ctx, store)
	if err != nil {
		return sum, err
	}

	// Pass 1: the volumes, with no parent link yet.
	for _, d := range descs {
		v := volumeFromDescriptor(d)
		v.ParentSnapshotID = ""
		if err := md.CreateVolume(ctx, term, v, nil); err != nil {
			return sum, fmt.Errorf("controlplane: recording volume %s: %w", d.VolumeID, err)
		}
		sum.Volumes++
	}

	// Pass 2: the snapshots, now that their volume rows exist.
	for _, d := range descs {
		n, err := rebuildSnapshots(ctx, md, store, term, d)
		if err != nil {
			return sum, err
		}
		sum.Snapshots += n
	}

	// Pass 3: the clones' parent links, now that their parents exist.
	for _, d := range descs {
		if d.ParentSnapshotID == "" {
			continue
		}
		if err := md.CreateVolume(ctx, term, volumeFromDescriptor(d), nil); err != nil {
			return sum, fmt.Errorf("controlplane: linking volume %s to snapshot %s: %w",
				d.VolumeID, d.ParentSnapshotID, err)
		}
	}
	return sum, nil
}

func volumeFromDescriptor(d descriptor.Descriptor) metadata.Volume {
	return metadata.Volume{
		VolumeID:  d.VolumeID,
		SizeBytes: d.SizeBytes,
		BlockSize: d.BlockSize,
		// Authoritative now: the epoch object this used to defer to went with the
		// fencing chain (ADR-0026). It is still the fencing token a later promotion
		// would build on, so it must not come back lower than it was — which the
		// catalog's upsert enforces with GREATEST rather than trusting this value.
		CurrentEpoch: d.CurrentEpoch,
		// ACTIVE with no primary: the volume exists and nobody is serving it. There is
		// no object that records placement, and inventing one would make a rebuilt
		// catalog claim a host is writing when nothing is.
		State:            lifecycle.VolumeActive,
		ChainDepth:       d.ChainDepth,
		ParentSnapshotID: d.ParentSnapshotID,
		DEKWrapped:       d.DEKWrapped,
		KEKID:            d.KEKID,
		DEKKeyID:         d.DEKKeyID,
	}
}

// listDescriptors reads every volume descriptor in the bucket. A descriptor that fails
// its own digest check stops the rebuild rather than being skipped: a catalog rebuilt
// from some of the bucket, silently, is worse than no rebuild — the operator would have
// no way to know which volumes are missing.
func listDescriptors(ctx context.Context, store objectstore.Store) ([]descriptor.Descriptor, error) {
	objs, err := store.List(ctx, "volumes/")
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing volume descriptors: %w", err)
	}
	var out []descriptor.Descriptor
	for _, o := range objs {
		volumeID, ok := volumeOfDescriptorKey(o.Key)
		if !ok {
			continue
		}
		d, err := descriptor.Read(ctx, store, volumeID)
		if err != nil {
			return nil, fmt.Errorf("controlplane: reading %s: %w", o.Key, err)
		}
		if d.VolumeID != volumeID {
			// The object says it belongs to another volume. That is what a bucket
			// copied with the wrong prefix looks like, and acting on it would record a
			// volume under an id whose data lives somewhere else.
			return nil, fmt.Errorf("controlplane: %s describes volume %s", o.Key, d.VolumeID)
		}
		out = append(out, d)
	}
	return out, nil
}

// volumeOfDescriptorKey extracts the volume id from `volumes/<id>/descriptor.json`.
func volumeOfDescriptorKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, "volumes/")
	if !ok {
		return "", false
	}
	id, ok := strings.CutSuffix(rest, "/descriptor.json")
	if !ok || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}

// rebuildSnapshots records every published snapshot under one volume's prefix.
func rebuildSnapshots(ctx context.Context, md metadata.Store, store objectstore.Store, term int64, d descriptor.Descriptor) (int, error) {
	u, err := ids.Parse(d.VolumeID)
	if err != nil {
		return 0, fmt.Errorf("controlplane: volume %q is not a uuid (INV-22): %w", d.VolumeID, err)
	}
	vol := [16]byte(u)
	prefix := image.Prefix(vol) + "snapshots/"
	objs, err := store.List(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("controlplane: listing the snapshots of %s: %w", d.VolumeID, err)
	}
	n := 0
	for _, o := range objs {
		snapID, ok := strings.CutSuffix(strings.TrimPrefix(o.Key, prefix), ".json")
		if !ok || snapID == "" {
			continue
		}
		// The manifest only. Materialising the snapshot would download and decrypt every
		// chunk to learn a sequence number that is in the manifest, and it would need the
		// KEK — a rebuild must work for an operator who has the bucket and the database
		// backup, not the key material.
		man, err := image.ReadSnapshotManifest(ctx, store, vol, snapID)
		if errors.Is(err, image.ErrNotPublished) {
			continue // listed and then gone; nothing to record
		}
		if err != nil {
			return n, fmt.Errorf("controlplane: reading snapshot %s of %s: %w", snapID, d.VolumeID, err)
		}
		if man.VolumeID != d.VolumeID {
			return n, fmt.Errorf("controlplane: %s describes volume %s", o.Key, man.VolumeID)
		}
		if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
			SnapshotID: snapID,
			VolumeID:   d.VolumeID,
			// The manifest exists, and it is written create-only and never changes
			// (INV-16), so its existence *is* PUBLISHED. There is no CREATING snapshot
			// in a bucket: a request that never completed left no object.
			State:          lifecycle.SnapshotPublished,
			TargetSequence: int64(man.Sequence),
			// The epoch the volume is at, not one the manifest records — sequences are
			// numbered within an epoch (§12.3) and the snapshot's own is not in S3.
			// Recorded rather than left zero because a zero epoch is a claim too.
			Epoch:       d.CurrentEpoch,
			ManifestKey: o.Key,
			// SourceHostID stays empty: which host froze it is not in any object, and
			// after a catastrophe that host may not exist. §20's placement rule 1 falls
			// through to steps 2 and 3, which is what it is for.
			RequestID: ids.New().String(),
		}); err != nil {
			return n, fmt.Errorf("controlplane: recording snapshot %s of %s: %w", snapID, d.VolumeID, err)
		}
		n++
	}
	return n, nil
}
