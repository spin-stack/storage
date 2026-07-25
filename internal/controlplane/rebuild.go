package controlplane

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// RebuildMetadata reconstructs the volume rows in a metadata.Store from the
// self-describing S3 layout (§22.5, INV-20): for every volume descriptor it reads
// the epoch object (the authority for the current epoch, §5.8) and recreates the
// volume. Returns the number of volumes rebuilt. Idempotent: a volume that already
// exists is skipped.
func RebuildMetadata(ctx context.Context, store objectstore.Store, epochs *epoch.Store, md metadata.Store, term int64) (int, error) {
	ids, err := descriptor.ListVolumeIDs(ctx, store)
	if err != nil {
		return 0, err
	}
	rebuilt := 0
	for _, id := range ids {
		if _, err := md.GetVolume(ctx, id); err == nil {
			continue // already present
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return rebuilt, err
		}

		d, err := descriptor.Read(ctx, store, id)
		if err != nil {
			return rebuilt, fmt.Errorf("rebuild %s: %w", id, err)
		}

		// The epoch object is authoritative for the current epoch; fall back to the
		// descriptor's last-known value if the object is missing.
		currentEpoch := d.CurrentEpoch
		if ep, _, err := epochs.Current(ctx, id); err == nil {
			currentEpoch = int64(ep)
		} else if !errors.Is(err, objectstore.ErrNotFound) {
			return rebuilt, fmt.Errorf("rebuild %s epoch: %w", id, err)
		}

		if err := md.CreateVolume(ctx, term, metadata.Volume{
			VolumeID:     d.VolumeID,
			SizeBytes:    d.SizeBytes,
			Durability:   d.Durability,
			BlockSize:    d.BlockSize,
			CurrentEpoch: currentEpoch,
			// A rebuilt row knows nothing about ownership: the CP re-attaches through
			// the normal path (which fences via the epoch object), so the volume comes
			// back DETACHED rather than in an invented state (§7, §22.5).
			State:      lifecycle.VolumeDetached,
			ChainDepth: d.ChainDepth,
			DEKWrapped: d.DEKWrapped,
			KEKID:      d.KEKID,
		}); err != nil {
			return rebuilt, fmt.Errorf("rebuild %s create: %w", id, err)
		}
		rebuilt++
	}
	return rebuilt, nil
}
