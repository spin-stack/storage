package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/snapshot"
)

// RebuildResult is what a rebuild reconstructed, and — just as important — what it
// could not. A number on its own reads as completeness; the operator running this
// during an incident needs to know which state is simply not in S3 (§22.5).
type RebuildResult struct {
	Volumes   int
	Snapshots int
	// Repaired names the volumes whose row was already there but disagreed with the
	// object store, and was corrected from it. A PITR restore rewinds
	// volumes.current_epoch while the epoch object still carries the epoch that was
	// actually granted, and that row is unattachable until something reconciles it —
	// so "already present" is not the same as "already right", and the count of rows
	// *created* says nothing about it.
	Repaired []string
	// Conflicting names the volumes whose row is *ahead* of the object store. The
	// rebuild will not rewind a fencing token from a descriptor that may itself be
	// stale (§12.4), so it cannot repair these: they need a human, and silence about
	// them would be the same failure Repaired exists to end.
	Conflicting []string
	// NotReconstructible names the tables the object store cannot speak for. They
	// have to come back from a PostgreSQL PITR restore or from the fleet
	// re-registering itself.
	NotReconstructible []string
}

// RebuildMetadata reconstructs the Control-Plane rows that the self-describing S3
// layout can speak for (§22.5, INV-20): every volume descriptor (with the epoch
// object as the authority for the current epoch, §5.8) and every published snapshot
// manifest, which is the catalog clones and restores are anchored to.
//
// It is idempotent — an operator may run it twice — and it reports what it could not
// rebuild rather than leaving a count that looks complete.
func RebuildMetadata(ctx context.Context, store objectstore.Store, epochs *epoch.Store, md metadata.Store, term int64) (RebuildResult, error) {
	res := RebuildResult{NotReconstructible: []string{
		"hosts",       // the fleet re-registers by heartbeat (§28.2)
		"host_leases", // leases are re-granted; a stale one must never be restored
		"operations",  // in-flight reconciliation is not durable state (§7)
	}}
	ids, err := descriptor.ListVolumeIDs(ctx, store)
	if err != nil {
		return res, err
	}
	for _, id := range ids {
		cur, err := md.GetVolume(ctx, id)
		present := err == nil // already there; it may still disagree with S3
		if err != nil && !errors.Is(err, metadata.ErrNotFound) {
			return res, err
		}

		d, err := descriptor.Read(ctx, store, id)
		if err != nil {
			return res, fmt.Errorf("rebuild %s: %w", id, err)
		}

		// The epoch object is authoritative for the current epoch; fall back to the
		// descriptor's last-known value if the object is missing.
		currentEpoch := d.CurrentEpoch
		if ep, _, err := epochs.Current(ctx, id); err == nil {
			currentEpoch = int64(ep)
		} else if !errors.Is(err, objectstore.ErrNotFound) {
			return res, fmt.Errorf("rebuild %s epoch: %w", id, err)
		}

		// CreateVolume converges without regressing (§22.5): it raises the epoch and
		// the size to what S3 says, refreshes the columns the descriptor owns, and
		// leaves ownership and the §7 lifecycle state alone. So the same write both
		// creates a missing row and repairs a stale one, and running it for a row
		// that already agrees is a no-op.
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
			// The version travels with the wrapped key or the rebuilt volume cannot
			// be opened: crypto.DevKMS binds it as GCM AAD, so unwrapping with the
			// wrong one fails outright. This is the whole reason §22.5's descriptor
			// carries it rather than only the catalog.
			DEKKeyID: d.DEKKeyID,
			// No §28.2 bound (ADR-0017): rebuild-metadata records volumes that
			// already exist and already occupy their hosts. A ceiling that refused
			// to write them would leave the catalog short of reality, which is the
			// one thing this run exists to prevent.
		}, nil); err != nil {
			return res, fmt.Errorf("rebuild %s create: %w", id, err)
		}
		switch {
		case !present:
			res.Volumes++
		case cur.CurrentEpoch > currentEpoch:
			// The row claims an epoch the object store never recorded. Converging
			// cannot fix that direction and rewinding the fence is not the rebuild's
			// call, so name it instead of reporting a clean run.
			res.Conflicting = append(res.Conflicting, id)
		case cur.CurrentEpoch < currentEpoch || cur.SizeBytes < d.SizeBytes:
			res.Repaired = append(res.Repaired, id)
		}

		n, err := rebuildSnapshots(ctx, store, md, term, id)
		if err != nil {
			return res, err
		}
		res.Snapshots += n
	}
	return res, nil
}

// rebuildSnapshots recreates the catalog rows for one volume from the manifests
// under snapshots/<volume>/. A manifest in S3 *is* a published snapshot — it is
// written create-only as the last step of publication (§19) — so its presence is the
// evidence, and its contents carry everything the row needs.
func rebuildSnapshots(ctx context.Context, store objectstore.Store, md metadata.Store, term int64, volumeID string) (int, error) {
	infos, err := store.List(ctx, "snapshots/"+volumeID+"/")
	if err != nil {
		return 0, err
	}
	rebuilt := 0
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, "/manifest.json") {
			continue
		}
		body, err := store.Get(ctx, info.Key)
		if err != nil {
			return rebuilt, fmt.Errorf("rebuild snapshot %s: %w", info.Key, err)
		}
		var m snapshot.Manifest
		if err := json.Unmarshal(body, &m); err != nil {
			return rebuilt, fmt.Errorf("rebuild snapshot %s: %w", info.Key, err)
		}
		if !m.DigestMatches() {
			// The manifest is immutable (INV-16); one that no longer matches its own
			// digest is corrupt, and a catalog row would give it authority.
			return rebuilt, fmt.Errorf("rebuild snapshot %s: root digest mismatch", info.Key)
		}
		if _, err := md.GetSnapshot(ctx, m.SnapshotID); err == nil {
			continue // already in the catalog
		} else if !errors.Is(err, metadata.ErrNotFound) {
			return rebuilt, err
		}
		if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
			SnapshotID: m.SnapshotID, VolumeID: m.VolumeID, ParentSnapshotID: m.ParentSnapshotID,
			Epoch: int64(m.Epoch), TargetSequence: int64(m.TargetSequence), RootDigest: m.RootDigest,
			State: lifecycle.SnapshotPublished, ManifestKey: info.Key,
			// The client request id is not in S3; the snapshot id is itself a UUIDv7
			// and is unique, so it stands in for the idempotency key of a request that
			// completed long ago.
			RequestID: m.SnapshotID,
		}); err != nil {
			return rebuilt, fmt.Errorf("rebuild snapshot %s: %w", m.SnapshotID, err)
		}
		rebuilt++
	}
	return rebuilt, nil
}
