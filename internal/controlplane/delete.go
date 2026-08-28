package controlplane

import (
	"context"
	"errors"
	"fmt"

	"log/slog"

	"github.com/google/uuid"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrVolumeAttached refuses the delete of a volume a host is still serving. A sentinel
// because it is the one refusal here an operator fixes with a single command: detach it.
var ErrVolumeAttached = errors.New("controlplane: the volume is still placed on a host")

// DeleteVolume destroys a volume: its key material first, then the map from the volume to
// its layers, then the catalog row.
//
// # What "deleted" means
//
// Crypto-shred. The layers themselves are **not** touched: commit.LayerKey puts them under
// a global content-addressed prefix so a clone's chain can reference layers its parent
// wrote, and deleting them under a volume's name would delete its clones' data. With the
// DEK gone the bytes are noise — which is why the shred has to be complete. Two limits:
//
//   - objectstore.Store.Delete is a *reversible marker* by design (INV-14); permanent
//     removal belongs to the bucket's lifecycle policy. A deployment that has not
//     configured one has shredded nothing until the descriptor's non-current versions
//     expire, and any claim otherwise is false.
//   - The shred is **lineage-scoped, not volume-scoped**. A clone shares the parent's DEK
//     bytes (only the wrap differs), so deleting a parent whose descendants exist would
//     destroy no secret at all — one more reason step 1 refuses it. See the re-wrap in
//     Clone for the constraint this puts on a future FLATTEN.
//
// # Order, decided by what a crash in the middle leaves behind
//
//  1. Preconditions. Nothing is written before a refusal.
//  2. `volumes/<id>/descriptor.json` — the key material — first. It is also what
//     -rebuild-metadata lists volumes from, so a crash later leaves an unopenable,
//     unlistable orphan. Row-first instead would let the next -rebuild-metadata resurrect
//     the deleted volume from its descriptor: the shred defeated by the repair tool.
//  3. `HEAD` and the commit manifests — the map from a volume to bytes you have just
//     claimed to destroy.
//  4. The catalog row last, term-guarded: `volumes.dek_wrapped` is the second copy of the
//     key.
//
// A crash between 2 and 4 leaves a row whose descriptor is gone — invisible to
// -rebuild-metadata and removable only through this verb. The runbook: run the delete
// again.
func DeleteVolume(ctx context.Context, md metadata.Store, store objectstore.Store,
	kms KeyChecker, term int64, volumeID string,
) (Shred, error) {
	vol, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return Shred{}, err
	}
	// Placement is this command's precondition and not the catalog's: metadata.Store
	// says so, because a row cannot see whether an Agent is still serving the device.
	if vol.PrimaryHostID != "" {
		return Shred{}, fmt.Errorf("%w: volume %s is placed on host %s. Detach it first — deleting its key while a guest is writing "+
			"turns the next commit into unopenable bytes", ErrVolumeAttached, volumeID, vol.PrimaryHostID)
	}
	// The bucket's half of the descendant refusal. The catalog's own (ErrHasDescendants, at
	// step 4) is not enough alone: a rebuild that lost the clone's parent link, or a clone
	// whose row is not recorded yet, is invisible to it, and the bucket is the authority a
	// rebuild trusts (INV-20).
	if err := refuseIfAnythingDescends(ctx, store, kms, volumeID); err != nil {
		return Shred{}, err
	}
	// Whether this delete is a shred at all, decided before anything is removed. Reported
	// and not refused: refusing was the first answer and it deadlocked deletion — a parent
	// cannot go while a descendant exists (the check above), so a clone that could not go
	// while its parent existed made a cloned lineage undeletable for ever. What was wrong was
	// the claim, not the removal.
	shred, err := lineageShred(ctx, store, volumeID)
	if err != nil {
		return Shred{}, err
	}

	// Step 2: the key material. ErrNotFound is tolerated here and at every Delete below — it
	// is what a re-run after a crash sees, and treating "already gone" as a failure would make
	// the only path that can finish the job the one path that refuses to.
	if err := store.Delete(ctx, descriptor.Key(volumeID)); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return Shred{}, fmt.Errorf("controlplane: deleting the key material at %s: %w", descriptor.Key(volumeID), err)
	}

	// Step 3: the map from the volume to its layers.
	if err := store.Delete(ctx, commit.HeadKey(volumeID)); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return Shred{}, fmt.Errorf("controlplane: deleting %s: %w", commit.HeadKey(volumeID), err)
	}
	manifests, err := store.List(ctx, commitPrefix(volumeID))
	if err != nil {
		return Shred{}, fmt.Errorf("controlplane: listing the commits of volume %s: %w", volumeID, err)
	}
	for _, o := range manifests {
		if err := store.Delete(ctx, o.Key); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
			return Shred{}, fmt.Errorf("controlplane: deleting %s: %w", o.Key, err)
		}
	}

	// Step 4: the row, and the second copy of the wrapped key with it.
	if err := md.DeleteVolume(ctx, term, volumeID); err != nil {
		return Shred{}, fmt.Errorf("controlplane: deleting the row of volume %s (its objects are already gone; re-run the delete): %w", volumeID, err)
	}
	return shred, nil
}

// commitPrefix is where one volume's commit manifests live. It is derived from
// commit.ManifestKey rather than spelled out, for the reason descriptor.Prefix exists:
// a listing of the wrong prefix answers "nothing" rather than failing, and this caller
// reads that answer as "this volume published no commits".
func commitPrefix(volumeID string) string {
	key := commit.ManifestKey(volumeID, "x")
	return key[:len(key)-len("x.json")]
}

// Shred says what a delete destroyed. It is returned rather than logged because the one
// thing that must not happen is an operator assuming a secret is gone when it is not, and
// a caller cannot branch on a log line.
type Shred struct {
	// KeyDestroyed is whether this delete removed the last wrap of the volume's DEK. When
	// it is false the layers this volume published stay readable, with the key the volume
	// named in SharedWith still publishes.
	KeyDestroyed bool
	// SharedWith is the live relative holding the same key, empty when KeyDestroyed.
	SharedWith string
}

// lineageShred reports whether deleting this volume destroys its key: a lineage shares one
// DEK's bytes (see Clone's re-wrap), so the secret only stops existing when the last wrap of
// it does.
//
// The parent is looked for in the bucket rather than in the catalog, for the reason the
// descendant check gives one direction up: a rebuild that lost the link, or a row never
// recorded, is invisible to the catalog.
func lineageShred(ctx context.Context, store objectstore.Store, volumeID string) (Shred, error) {
	d, err := descriptor.Read(ctx, store, volumeID)
	if errors.Is(err, objectstore.ErrNotFound) {
		// Already gone: no key left here to share.
		return Shred{KeyDestroyed: true}, nil
	}
	if err != nil {
		return Shred{}, fmt.Errorf("controlplane: reading the descriptor of %s to see whose key it shares: %w", volumeID, err)
	}
	if d.ParentVolumeID == "" {
		return Shred{KeyDestroyed: true}, nil
	}
	if _, err := descriptor.Read(ctx, store, d.ParentVolumeID); err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			// The parent is gone, so this volume is the last holder of the key and
			// deleting it destroys the secret. That is a shred.
			return Shred{KeyDestroyed: true}, nil
		}
		return Shred{}, fmt.Errorf("controlplane: reading the descriptor of %s's parent %s: %w", volumeID, d.ParentVolumeID, err)
	}
	return Shred{SharedWith: d.ParentVolumeID}, nil
}

// refuseIfAnythingDescends is the bucket-side descendant check: no other volume's
// descriptor may name this one as its parent.
//
// It is a full listing of descriptor.Prefix, which is O(volumes) per delete. That is
// accepted: a delete is an operator action, not a data-path one, and the alternative is
// an index of the inverse link — a second place a lineage is recorded, which would then
// be the thing that can be wrong at exactly the moment it matters.
func refuseIfAnythingDescends(ctx context.Context, store objectstore.Store, kms KeyChecker, volumeID string) error {
	objs, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return fmt.Errorf("controlplane: listing volume descriptors to check what descends from %s: %w", volumeID, err)
	}
	for _, o := range objs {
		other, ok := descriptor.VolumeOfKey(o.Key)
		if !ok || other == volumeID {
			continue
		}
		id, perr := ids.Parse(other)
		if perr != nil {
			continue
		}
		d, err := descriptor.Read(ctx, store, other)
		if err != nil {
			// Not skipped. A descriptor that cannot be read is one whose parent link
			// cannot be stated, and deleting a key on the strength of "probably not a
			// descendant" is the one thing this check exists to prevent.
			return fmt.Errorf("controlplane: reading %s while checking what descends from %s: %w", o.Key, volumeID, err)
		}
		// Only a descriptor this fleet wrapped counts as a descendant. `parent_volume_id` is
		// unauthenticated, so a forged descriptor claiming descent blocked a real volume's
		// crypto-shred for ever, with the operator told to FLATTEN a volume that does not
		// exist. The wrap is the one thing a forger cannot produce.
		//
		// A descriptor that does not verify is skipped rather than refused: it cannot be ours,
		// so it cannot be reading through the volume being deleted. Logged, because an object
		// under this prefix that is not ours is worth an operator's attention.
		if _, kerr := kms.UnwrapDEK(d.DEKWrapped, d.DEKKeyID, uuid.UUID(id)); kerr != nil {
			slog.Warn("an object under the volumes prefix carries key material this fleet did not wrap; it is not a descendant and is being ignored",
				"key", o.Key, "checking", volumeID, "error", kerr)
			continue
		}
		if d.ParentVolumeID == volumeID {
			return fmt.Errorf("%w: volume %s reads through %s. FLATTEN %s first — it re-uploads what the clone inherited "+
				"under a fresh key and returns it to depth 0 — then delete %s",
				metadata.ErrHasDescendants, other, volumeID, other, volumeID)
		}
	}
	return nil
}
