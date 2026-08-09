package lineage

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrHasDescendants means a volume still has descendants in the bucket, so deleting it
// would break them. See Delete.
var ErrHasDescendants = errors.New("lineage: this volume still has descendants, and they read through it")

// Removed is what one delete marked, for the operator watching it.
type Removed struct {
	// Descriptor, Manifest report whether that object was there to mark. Both are
	// false on a re-run of a delete that already finished, which is how a re-run
	// distinguishes itself from a first one without keeping any state.
	Descriptor bool
	Manifest   bool
	// Snapshots and Chunks are how many objects of each kind were marked.
	Snapshots int
	Chunks    int
	// ChunkBytes is what the chunk objects occupied, as the listing reported it —
	// the storage this delete actually reclaims once the lifecycle expires them.
	ChunkBytes int64
	// InheritedFrom is the parent volume of a clone, empty for a volume that
	// descended from nothing. It is the honest half of ChunkBytes: a clone's own
	// writes live in its *ancestry's* chunk store, which this delete does not touch,
	// so for a clone the number above is what an interrupted flatten left behind and
	// not what the volume wrote. See Delete.
	InheritedFrom string
}

// Delete marks every object a volume owns, in the exact inverse of the order that
// wrote them.
//
// It is the only operation in this repository that removes anything, and it removes
// nothing permanently: `objectstore.Store.Delete` places a reversible marker and the
// interface deliberately has no way to reach past it (INV-14, and `real.NewS3Store`
// refuses a bucket without versioning, so the guarantee is structural rather than
// promised). "A couple of days of recovery, then gone" is therefore a **lifecycle rule
// on the bucket**, not code here — the deletion decision
// — and the one thing this package can do about it is not offer an alternative.
// docs/plan/RUNBOOK.md carries what the deployment owes and what shortening it costs.
//
// # The order is the inverse of publish, and that is the whole safety argument
//
// A publish writes chunks and then the manifest, so that a manifest which exists
// always resolves (image.Publish). A delete therefore runs backwards:
//
//		volumes/<vol>/descriptor.json
//		image/<vol>/snapshots/*.json
//		image/<vol>/manifest.json
//		chunks/<vol>/*
//
//	  - **The descriptor goes first** because it is what makes the volume exist to
//	    `-rebuild-metadata` (controlplane.listDescriptors). A rebuild that runs
//	    mid-delete must not resurrect a volume whose image is already gone; with the
//	    descriptor marked first it simply does not see it. Rejected: the descriptor
//	    last — a rebuild in that window brings the volume back ACTIVE, unplaced and
//	    unreadable, which is a catalog row an operator has no way to interpret.
//	  - **The chunks go last** because the reverse leaves a live manifest naming bytes
//	    that are gone, which loadManifest correctly refuses — turning a delete in
//	    progress into a volume that reports corruption. Unreferenced chunks cost
//	    storage; a manifest with holes costs an incident. TestADeleteNeverLeavesAManifestOverMissingChunks
//	    interrupts this at every step and asserts exactly that difference.
//
// Every step treats ErrNotFound as success, which is what makes the whole thing
// re-runnable: the interface says a Delete on a key that is already marked or was
// never there is ErrNotFound, and real.S3Store HEADs first so that the real backend
// and the sim agree about it.
//
// # What it refuses, and why the bucket answers rather than the catalog
//
// A volume with descendants is refused. A clone reads its ancestry on every attach
// (Walk, Compose) — publishing stopped flattening — so deleting a parent would leave
// its clones refusing every read. The question is asked of the *bucket*: the catalog's
// `volumes.parent_snapshot_id` is write-once by construction, so it keeps naming a
// parent a flatten has already dissolved, and a delete that believed it would refuse
// for ever exactly the volumes FLATTEN exists to unblock. Descendants lists the
// descriptors instead. The catalog check is still there and still matters — it is
// metadata.DeleteVolume's ErrHasDescendants, protecting the snapshot rows — but it is
// the second line, not the first.
//
// The caller flattens those descendants and runs this again
// (the deletion decision). This does not flatten them itself: a
// flatten needs the volume's DEK and a detached volume, which are a key file and a
// catalog read, and neither belongs behind a function whose argument is a bucket.
//
// # Which chunk store it may touch, which is the one thing here that could lose data
//
// It marks `chunks/<this volume>/` and never `chunks/<its ancestor>/`. A chunk store
// is named by a *lineage root* (image.ChunksPrefix), so the only volumes whose bytes
// live under this volume's id are this volume and its descendants — and there are
// none, by the refusal above. That is what makes the whole prefix safe to mark without
// reading a single manifest, and it is why this needs no reference counting and no set
// difference over a lineage's manifests (both of which the deletion decision
// rejects, the first by name).
//
// **The consequence is that deleting a clone reclaims almost nothing**, and it is
// stated in Removed.InheritedFrom rather than hidden: a clone writes into its
// ancestry's chunk store, so its bytes stay there, named by no manifest, until the
// root is deleted. The route to reclaiming them is the one the rest of this package
// already provides — flatten the clone first, which re-homes its dataset under its own
// id, then delete it. Rejected: taking the set difference between this volume's
// manifests and every other manifest in the lineage. It is computable, it costs a read
// of every manifest of every volume in the lineage, and it is precisely the kind of
// derived reachability whose one wrong answer costs data rather than storage —
// the asymmetry that governs anything that reclaims: an object collected one cycle late
// costs storage, an object collected one cycle early costs data.
func Delete(ctx context.Context, store objectstore.Store, volumeID string) (Removed, error) {
	var res Removed
	u, err := ids.Parse(volumeID)
	if err != nil {
		return res, fmt.Errorf("lineage: volume %q is not a uuid: %w", volumeID, err)
	}
	vol := [16]byte(u)

	kids, err := Descendants(ctx, store, volumeID)
	if err != nil {
		return res, err
	}
	if len(kids) > 0 {
		return res, fmt.Errorf("%w: %s is read through by %v. Flatten each of them first (-flatten-volume), which is what makes a clone owe nothing to its ancestry",
			ErrHasDescendants, volumeID, kids)
	}

	// Read before the first mark, because the descriptor is the first thing to go and
	// it is the only object that says whether this volume wrote into somebody else's
	// chunk store. A re-run finds it already marked and reports nothing about the
	// lineage, which is correct: by then there is nothing left to warn about.
	switch self, derr := descriptor.Read(ctx, store, volumeID); {
	case derr == nil:
		res.InheritedFrom = self.ParentVolumeID
	case errors.Is(derr, objectstore.ErrNotFound):
	default:
		return res, fmt.Errorf("lineage: volume %s: reading %s before deleting it: %w",
			volumeID, descriptor.Key(volumeID), derr)
	}

	gone, err := mark(ctx, store, descriptor.Key(volumeID))
	if err != nil {
		return res, err
	}
	res.Descriptor = gone

	// The snapshots before the manifest, because both are read the same way and a
	// snapshot manifest outliving the volume's own is the same "names chunks that are
	// gone" hazard one step later.
	snaps, err := store.List(ctx, image.Prefix(vol)+"snapshots/")
	if err != nil {
		return res, fmt.Errorf("lineage: volume %s: listing its snapshots: %w", volumeID, err)
	}
	for _, o := range snaps {
		gone, err := mark(ctx, store, o.Key)
		if err != nil {
			return res, err
		}
		if gone {
			res.Snapshots++
		}
	}

	if gone, err := mark(ctx, store, image.ManifestKey(vol)); err != nil {
		return res, err
	} else if gone {
		res.Manifest = true
	}

	// Last, and only now: from here on nothing in the bucket names these bytes.
	//
	// The listing is what bounds the work, and an eventually consistent one under-reports
	// rather than over-reports (objectstore.Store.List). A key it misses is a key nothing
	// references, which costs storage until the delete is re-run — the one orphan class
	// the deletion decision accepts, and the reason a re-run is cheap.
	chunks, err := store.List(ctx, image.ChunksPrefix(vol))
	if err != nil {
		return res, fmt.Errorf("lineage: volume %s: listing its chunk store: %w", volumeID, err)
	}
	for _, o := range chunks {
		gone, err := mark(ctx, store, o.Key)
		if err != nil {
			return res, err
		}
		if gone {
			res.Chunks++
			res.ChunkBytes += o.Size
		}
	}
	return res, nil
}

// mark places a delete marker and reports whether there was anything to mark.
//
// ErrNotFound is success, not an error, and that single line is what makes a delete
// re-runnable: an operator who re-runs a command they are not sure completed must be
// told it worked, and the alternative — remembering which keys this process already
// marked — is progress state that can disagree with the bucket.
func mark(ctx context.Context, store objectstore.Store, key string) (bool, error) {
	switch err := store.Delete(ctx, key); {
	case err == nil:
		return true, nil
	case errors.Is(err, objectstore.ErrNotFound):
		return false, nil
	default:
		return false, fmt.Errorf("lineage: marking %s: %w", key, err)
	}
}

// Descendants is Walk's inverse: the volumes whose descriptor names volumeID as their
// parent, in id order (INV-02). It answers one link down, not the whole subtree, and
// that is sufficient — flattening a direct child makes it a lineage root, at which
// point its own children descend from *it* and no longer from anything above.
//
// It costs a listing of `volumes/` and a GET per descriptor, which is what
// `-rebuild-metadata` already pays and is affordable for an admin one-shot. The
// alternative — asking the catalog, which has both links indexed — is what
// the deletion decision originally proposed and it is wrong here for a reason
// that arrived with FLATTEN: `volumes.parent_snapshot_id` cannot be written to NULL by
// a converging create, so the catalog goes on naming a parent that the bucket says was
// left behind. Asking it would make the refusal permanent for exactly the volumes that
// have already done the work to lift it.
//
// **A descriptor it cannot read stops the scan.** A volume whose descriptor is corrupt
// or unreachable might be a descendant, and "might be" is the answer a delete has to
// treat as yes: the failure it prevents is a clone whose whole ancestry disappears
// under it. Same rule and same reason as controlplane.listDescriptors, which refuses to
// rebuild a catalog from part of a bucket.
func Descendants(ctx context.Context, store objectstore.Store, volumeID string) ([]string, error) {
	objs, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return nil, fmt.Errorf("lineage: listing volume descriptors to find what descends from %s: %w", volumeID, err)
	}
	var kids []string
	for _, o := range objs {
		id, ok := descriptor.VolumeOfKey(o.Key)
		if !ok || id == volumeID {
			continue
		}
		d, err := descriptor.Read(ctx, store, id)
		if err != nil {
			return nil, fmt.Errorf("lineage: reading %s while checking whether %s descends from %s: %w",
				o.Key, id, volumeID, err)
		}
		if d.ParentVolumeID == volumeID {
			kids = append(kids, id)
		}
	}
	sort.Strings(kids)
	return kids, nil
}
