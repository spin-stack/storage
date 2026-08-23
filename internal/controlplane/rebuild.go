package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// RebuildSummary is what one rebuild found and recorded.
//
// There is no Snapshots count any more, and the absence is the report: the objects a
// snapshot's existence was read out of were the chunked image's manifests, which went
// with the local block engine. Keeping a field that is structurally zero would have a
// rebuild print "0 snapshots" at an operator who has just lost their catalog, and that
// sentence is indistinguishable from "your bucket holds no snapshots".
type RebuildSummary struct {
	Volumes int
}

// RebuildMetadata reconstructs the volume catalog from the object store alone
// (§22.5, INV-20). It is the reader `descriptor.Write` had been feeding since
// increment 4 deleted the previous one, and it is why those objects are written at all:
// without it a lost PostgreSQL is unrecoverable even though every byte of every volume
// is intact in the bucket.
//
// # What it restores, and what it cannot
//
// It restores what the descriptor states: geometry, the wrapped DEK with the version
// that names it, the epoch, and the chain depth. It does **not** restore placement — no
// volume comes back with a primary host — because no object records one. A rebuilt
// catalog therefore describes volumes nobody is serving, which is the honest outcome:
// after losing the database you know what exists, not who was running it, and an
// operator re-places them.
//
// # One pass, and what the other two used to do
//
// It used to be three, because it also recorded every snapshot (pass 2) and then linked
// each clone to its parent snapshot (pass 3), and `volumes.parent_snapshot_id` and
// `snapshots.volume_id` reference each other so neither could go first.
//
// Both of those read the chunked image's manifests — the per-volume `manifest.json` for
// the published sequence, and `image/<vol>/snapshots/*.json` for the snapshots — and
// that format is withdrawn with the local block engine. There is nothing left in the
// bucket to read them out of, and inventing the numbers is the one thing a rebuild must
// not do: `published_sequence` and `durable_sequence` are the two floors an Agent checks
// at attach, so a rebuild that guesses high refuses to serve a volume that is fine, and
// one that guesses low hands a guest a blank device for a volume it has written into.
//
// So a rebuilt volume comes back with zeroed sequences and no parent link, and the
// commit protocol that replaces the manifest is what will restore both. A clone whose
// parent link is not restored still has its data — the link is a catalog fact, and the
// descriptor still carries the parent id for whoever re-establishes it.
//
// Running it twice, or from two operators at once, converges rather than aborting;
// `CreateVolume` is idempotent by design, and this passes no capacity bound because it
// is recording volumes that already occupy their hosts.
func RebuildMetadata(ctx context.Context, md metadata.Store, store objectstore.Store, term int64) (RebuildSummary, error) {
	var sum RebuildSummary

	descs, err := listDescriptors(ctx, store)
	if err != nil {
		return sum, err
	}
	for _, d := range descs {
		v := volumeFromDescriptor(d)
		// Cleared, not carried: `volumes.parent_snapshot_id` references a snapshots row,
		// and nothing records snapshots any more, so writing the link would fail the
		// foreign key on every clone in the bucket and abort the rebuild.
		v.ParentSnapshotID = ""
		if err := md.CreateVolume(ctx, term, v, nil); err != nil {
			return sum, fmt.Errorf("controlplane: recording volume %s: %w", d.VolumeID, err)
		}
		sum.Volumes++
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
	objs, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing volume descriptors: %w", err)
	}
	var out []descriptor.Descriptor
	for _, o := range objs {
		volumeID, ok := descriptor.VolumeOfKey(o.Key)
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
