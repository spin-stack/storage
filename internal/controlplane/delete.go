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

// ErrVolumeAttached refuses the delete of a volume a host is still serving.
//
// A sentinel because it is the one refusal here an operator can act on with a single
// command — detach it — where ErrHasDescendants asks for a FLATTEN and everything else
// says the bucket or the catalog is wrong.
var ErrVolumeAttached = errors.New("controlplane: the volume is still placed on a host")

// DeleteVolume destroys a volume: its key material first, then the map from the volume
// to its layers, then the catalog row.
//
// # What "deleted" means, exactly
//
// Crypto-shred. The layers themselves are **not** touched: `commit.LayerKey` puts them
// under a global content-addressed prefix precisely so a clone's chain can reference
// layers its parent wrote, and deleting them under a volume's name would delete its
// clones' data. With the DEK gone the bytes are noise, and that is the entire argument
// for why leaving them is safe — which is also why the shred has to be complete.
//
// Two limits, said out loud rather than implied:
//
//   - `objectstore.Store.Delete` is a *reversible marker* by design (INV-14): the bytes
//     stay, `Restore` brings them back, and permanent removal belongs to the bucket's
//     lifecycle policy — the same argument `metadata.Store.DeleteVolume` already makes
//     about there being no retention column. So this makes the wrapped DEK unreachable
//     through this interface and hands the actual destruction to that policy. A
//     deployment that has not configured it has not shredded anything, and any claim
//     otherwise is false until the descriptor's non-current versions expire.
//   - The shred is **lineage-scoped, not volume-scoped**. A clone shares the parent's
//     DEK bytes (only the wrap differs), so deleting a parent whose descendants exist
//     would destroy no secret at all — which is one more reason step 1 refuses it. See
//     the re-wrap in Clone for the constraint this puts on a future FLATTEN.
//
// # Order, decided by what a crash in the middle leaves behind
//
//  1. Preconditions. Nothing is written before a refusal.
//  2. `volumes/<id>/descriptor.json` — the key material — before anything else. It is
//     also what `-rebuild-metadata` lists volumes from, so killing it first means a
//     crash anywhere later leaves an unopenable, unlistable orphan. The reverse order
//     (row first) leaves the wrapped DEK in the bucket with no row to drive a retry, and
//     the next `-rebuild-metadata` would resurrect the deleted volume from its
//     descriptor: the shred defeated by the repair tool.
//  3. `HEAD` and the commit manifests. Not key material — they are the map from a volume
//     to its layers, and leaving a map to bytes you have just claimed to destroy is what
//     makes an operator's "it's gone" wrong.
//  4. The catalog row last, term-guarded. It is the only durable record of what is still
//     to be finished, and `volumes.dek_wrapped` is the second copy of the key.
//
// A crash between 2 and 4 leaves a row whose descriptor is gone. Re-running is
// idempotent — this is driven by the id, not by a listing — but `-rebuild-metadata` will
// not see the volume any more, so the row is only removable through this verb. The
// runbook is: run the delete again.
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
	// The bucket's half of the descendant refusal. The catalog has its own
	// (ErrHasDescendants, at step 4, enforced in both directions and by two foreign keys
	// in Postgres), and it is not enough on its own here: a rebuild that lost the clone's
	// parent link, or a clone whose row has not been recorded yet, is invisible to it,
	// and the bucket is the authority a rebuild trusts (INV-20). This is what
	// descriptor.Prefix and VolumeOfKey were given for.
	if err := refuseIfAnythingDescends(ctx, store, kms, volumeID); err != nil {
		return Shred{}, err
	}
	// Whether this delete is a shred at all, decided before anything is removed.
	//
	// A clone is handed its parent's DEK *bytes* — only the wrap differs — because that
	// is what lets it read the layers its parent published (v6 §10). So deleting one
	// member of a live lineage destroys no secret: its layers stay readable with the
	// wrap its relative still publishes, which an adversary demonstrated by reading every
	// byte back out.
	//
	// It is reported and not refused, and that was a real choice. Refusing was the first
	// answer and it deadlocked deletion outright: a parent cannot go while a descendant
	// exists (the check above, which is right), so a clone that could not go while its
	// parent existed made a cloned lineage undeletable for ever. What was wrong was never
	// the removal — it was the *claim*. Deleting a clone is a removal; deleting the last
	// holder of the key is the shred; and this says which one happened rather than
	// letting an operator assume.
	shred, err := lineageShred(ctx, store, volumeID)
	if err != nil {
		return Shred{}, err
	}

	// Step 2: the key material, before anything else.
	// ErrNotFound is tolerated here and at every Delete below: it is what a re-run of a
	// delete that crashed after step 2 sees, and the runbook for that crash is to run the
	// delete again. Treating "already gone" as a failure would make the only path that
	// can finish the job the one path that refuses to.
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

// lineageShred reports whether deleting this volume destroys its key.
//
// A clone is handed its parent's DEK *bytes* — only the wrap differs (see Clone's
// re-wrap) — because that is what lets it read the layers its parent published. The
// consequence is that no single member of a lineage can be crypto-shredded: the secret
// only stops existing when the last wrap of it does.
//
// The parent is looked for in the bucket rather than in the catalog, for the reason the
// descendant check gives one direction up: a rebuild that lost the link, or a row that
// was never recorded, is invisible to the catalog, and the bucket is the authority a
// rebuild trusts.
func lineageShred(ctx context.Context, store objectstore.Store, volumeID string) (Shred, error) {
	d, err := descriptor.Read(ctx, store, volumeID)
	if errors.Is(err, objectstore.ErrNotFound) {
		// Already gone, which is what a re-run of a delete that crashed after step 2
		// sees. There is no key left here to share, and the runbook for that crash is to
		// run the delete again.
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
		// Only a descriptor this fleet wrapped counts as a descendant. `parent_volume_id`
		// is an unauthenticated field in an object anyone with the bucket can create, so
		// a forged descriptor claiming descent blocked the crypto-shred of a real volume
		// for ever — and the instruction the operator was handed was to FLATTEN a volume
		// that does not exist. The wrap is the one thing a forger cannot produce: it is
		// sealed under this fleet's KEK, bound to the volume that carries it.
		//
		// A descriptor that does not verify is skipped rather than refused. Skipping is
		// safe in the direction that matters: it cannot be one of ours, so it cannot be
		// reading through the volume being deleted, so it is not a descendant. It is
		// logged, because an object under this prefix that is not ours is worth an
		// operator's attention even when it changes nothing.
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
