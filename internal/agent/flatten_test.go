package agent_test

import (
	"crypto/rand"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TestAFlattenedCloneIsServedFromItsOwnImage is FLATTEN at the seam it exists for: a real
// VolumeManager, a real WAL and a real device on either side of the operator's one-shot,
// asserting what a guest reads back.
//
// Three things are proven here that no assertion inside internal/lineage can reach.
//
// **The clone reads the same bytes it read before**, out of a device, at offsets chosen so
// that each one fails differently: a range only the parent ever wrote (a flatten that did
// not write down the ancestry answers zeros), a range only the clone wrote (a flatten that
// wrote down the ancestry and lost the volume's own layers answers zeros), and a range the
// parent wrote and the clone overwrote (a flatten that composed them in the wrong order
// answers the parent's byte).
//
// **It reads them with every object the parent owns deleted.** That is the whole point of
// the operation — DELETION-AND-RECLAIM-SPEC's answer B is that a delete of a volume with
// descendants flattens them first — and "self-contained" is not a claim a manifest can
// make, it is one a missing parent has to fail to refute.
//
// **And it does so while the desired state still names the parent.** This is not laziness
// in the fixture, it is the state a fleet is actually in after a flatten:
// `volumes.parent_snapshot_id` is write-once by construction (CreateVolume's COALESCE), so
// the catalog goes on describing a lineage the volume has left until somebody rebuilds it
// from the bucket. The Agent obeys the descriptor (lineage.Walk), and if it did not, it
// would resolve a lineage root that is no longer this volume's and fail to find a single
// chunk of its own image.
func TestAFlattenedCloneIsServedFromItsOwnImage(t *testing.T) {
	const (
		parentOnly = int64(0)                 // the parent wrote it; the clone never touched it
		cloneOnly  = int64(4 * testBlockSize) // only the clone wrote it
		shared     = int64(8 * testBlockSize) // the parent wrote it and the clone overwrote it
	)
	store := sim.NewObjectStore()
	kms, dek, wrapped := lineageKeys(t)

	// The parent: a volume that descends from nothing, writes, is snapshotted, and stops —
	// which is what puts its image and its snapshot in the bucket.
	parent := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-flatten-parent")
	p := desiredVolume(t, 1)
	writeDescriptor(t, store, p.GetVolumeId(), lineageLink{})
	pdev := serveClone(t, parent, p)
	writeBlock(t, pdev, parentOnly, 0xA1)
	writeBlock(t, pdev, shared, 0xA1)
	snapID := ids.New().String()
	if _, err := parent.Snapshot(t.Context(), p.GetVolumeId(), snapID); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := parent.Close(t.Context()); err != nil {
		t.Fatalf("closing the parent's Agent: %v", err)
	}

	// The clone: it reads through the parent, writes two blocks of its own, and stops —
	// leaving an image that is a delta over the parent's snapshot.
	c := desiredVolume(t, 1)
	descendsFrom(t, store, c, lineageLink{volume: p.GetVolumeId(), snapshot: snapID})
	first := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-flatten-clone-1")
	cdev := serveClone(t, first, c)
	readBlock(t, cdev, parentOnly, 0xA1, "the clone inherits its parent's bytes before anything is flattened")
	writeBlock(t, cdev, cloneOnly, 0xC3)
	writeBlock(t, cdev, shared, 0xC3)
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("closing the clone's first Agent: %v", err)
	}

	// The operator's one-shot. It runs against the bucket with no Agent involved, which is
	// the shape the decision settled on: the volume must be detached for its manifest to be
	// safe to replace, and a detached volume is in no host's desired state.
	res, err := lineage.Flatten(t.Context(), store, rand.Reader, cloneEncryption(t, dek, c.GetVolumeId()), c.GetVolumeId())
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	if res.Ancestors != 1 {
		t.Fatalf("the flatten reports %d ancestors left behind, want 1", res.Ancestors)
	}

	// Everything the parent names goes away: its image, its snapshot and its descriptor. Its
	// chunks stay, because they belong to the lineage rather than to it (image.ChunksPrefix)
	// — and after the flatten they are not what the clone reads anyway, which is what the
	// next line finds out.
	for _, prefix := range []string{image.Prefix(uuidOf(t, p.GetVolumeId())), "volumes/" + p.GetVolumeId() + "/"} {
		objs, lerr := store.List(t.Context(), prefix)
		if lerr != nil {
			t.Fatalf("listing %s: %v", prefix, lerr)
		}
		if len(objs) == 0 {
			t.Fatalf("nothing under %s: the fixture never built the parent this test deletes", prefix)
		}
		for _, o := range objs {
			if derr := store.Delete(t.Context(), o.Key); derr != nil {
				t.Fatalf("deleting %s: %v", o.Key, derr)
			}
		}
	}

	// A fresh data directory, so the local WAL cannot answer and only the bucket can — and
	// the same desired state as before, parent link and all, because that is what the
	// Control Plane still sends.
	second := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-flatten-clone-2")
	again := serveClone(t, second, c)
	readBlock(t, again, parentOnly, 0xA1, "a range only the parent ever wrote, read after the parent's objects were deleted")
	readBlock(t, again, cloneOnly, 0xC3, "a range only the clone wrote")
	readBlock(t, again, shared, 0xC3, "a range the parent wrote and the clone overwrote: the clone's byte wins")
}

// cloneEncryption is what cmd/control-plane builds from the catalog's wrapped DEK: the one
// key the lineage shares, bound to the volume being flattened.
func cloneEncryption(t *testing.T, dek crypto.DEK, volumeID string) *wal.Encryption {
	t.Helper()
	enc, err := wal.NewEncryption(dek, uuidOf(t, volumeID))
	if err != nil {
		t.Fatalf("binding the DEK to %s: %v", volumeID, err)
	}
	return enc
}
