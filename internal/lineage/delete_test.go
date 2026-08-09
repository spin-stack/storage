package lineage_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TestADeletedVolumeStopsAnsweringAndTheBucketStillHoldsIt is the whole shape of a
// delete, asserted from outside: afterwards nothing under the volume's own names answers
// a Get and its image cannot be loaded — and every one of those keys is still
// restorable, because a delete here places a marker and the interface has no way to
// reach past one (INV-14).
//
// The restore runs in the **inverse of the delete order**, so the volume becomes visible
// to a reader and to -rebuild-metadata only once its bytes are back. What says it worked
// is the five offsets read out of the restored image, not the absence of an error from
// Restore.
func TestADeletedVolumeStopsAnsweringAndTheBucketStillHoldsIt(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, enc := buildLineage(t, store, dek)

	// The youngest of the chain is the one with no descendants, so it is the one a
	// delete may take. It is also a clone, which is the case that reclaims nothing: its
	// bytes are in its ancestry's chunk store and this delete does not touch them.
	keys := keysUnder(t, store, "image/"+vol.id+"/", "volumes/"+vol.id+"/", "chunks/"+vol.id+"/")
	if len(keys) == 0 {
		t.Fatal("the fixture wrote no object under the volume's own names")
	}

	res, err := lineage.Delete(t.Context(), store, vol.id)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !res.Descriptor || !res.Manifest {
		t.Errorf("the delete reports descriptor=%v manifest=%v; the fixture wrote both", res.Descriptor, res.Manifest)
	}
	// The honest half: a clone's own writes live under its ancestry's lineage, so this
	// delete reclaimed no chunk at all — and it says whose store they are in rather than
	// letting an operator read "deleted" as "reclaimed".
	if res.Chunks != 0 || res.InheritedFrom != vol.chain[0].Volume {
		t.Errorf("the delete reports %d chunk objects and a lineage inherited from %q; want 0 and %s",
			res.Chunks, res.InheritedFrom, vol.chain[0].Volume)
	}

	for _, k := range keys {
		if _, err := store.Get(t.Context(), k); !errors.Is(err, objectstore.ErrNotFound) {
			t.Errorf("%s still answers a Get after the delete: %v", k, err)
		}
	}
	// The manifest is what a reader starts from, and the difference between "gone" and
	// "broken" is which error it is handed.
	_, _, _, err = image.Load(t.Context(), store, enc, image.Ident{Volume: vol.u, Lineage: vol.root}, nil)
	if !errors.Is(err, image.ErrNotPublished) {
		t.Errorf("loading the deleted volume's image: %v, want ErrNotPublished", err)
	}
	if _, err := descriptor.Read(t.Context(), store, vol.id); !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("reading the deleted volume's descriptor: %v, want ErrNotFound", err)
	}

	// **The ancestry is untouched**, and this is the assertion that a delete cannot lose
	// somebody else's data. The clone's bytes are in its lineage root's chunk store,
	// shared with both ancestors; a delete that reasoned "this volume's chunks" from the
	// lineage rather than from the volume's own id would wipe that store and take two
	// live volumes with it. Read through the chain rather than checking key existence:
	// what matters is that the ancestors still answer, not that objects are present.
	ancestry, err := lineage.Compose(t.Context(), store, enc, vol.root, vol.chain, nil)
	if err != nil {
		t.Fatalf("composing the deleted clone's ancestry, which the delete must not have touched: %v", err)
	}
	assertReads(t, ancestry, map[int64]byte{
		offRootOnly: 0xA1, offMiddleOnly: 0xB2, offErased: 0x00,
		offShared: 0xA1, offOwn: 0x00, offOwnErased: 0xA1,
	}, "the ancestry of a deleted clone")

	// Twice is once: a second run finds everything already marked and says it did
	// nothing, rather than failing on a key that is not there.
	again, err := lineage.Delete(t.Context(), store, vol.id)
	if err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if again.Descriptor || again.Manifest || again.Snapshots != 0 || again.Chunks != 0 {
		t.Errorf("the second delete claims to have marked something: %+v", again)
	}

	// The recovery window, which is the bucket's and not this repository's: every one of
	// those keys comes back, in the reverse of the order they went.
	for i := len(keys) - 1; i >= 0; i-- {
		if err := store.Restore(t.Context(), keys[i]); err != nil {
			t.Fatalf("restoring %s: %v", keys[i], err)
		}
	}
	restored, _ := readThroughTheChain(t, store, enc, vol)
	assertReads(t, restored, wholeChain, "after the deleted keys were restored")
}

// wholeChain is what the youngest volume of the fixture answers when it is read through
// its ancestry — the same map the flatten tests assert, named because three tests here
// compare against it.
var wholeChain = map[int64]byte{
	offRootOnly: 0xA1, offMiddleOnly: 0xB2, offErased: 0x00,
	offShared: 0xC3, offOwn: 0xC3, offOwnErased: 0x00,
}

// TestDeleteRefusesWhileSomethingDescendsFromTheVolume is the refusal, and the assertion
// is that it cost nothing: an operator who deletes the wrong id must not find the right
// volume half-marked.
//
// It also pins where the answer comes from. Nothing in the bucket says "a clone exists"
// except that clone's own descriptor, and this is the read that finds it.
func TestDeleteRefusesWhileSomethingDescendsFromTheVolume(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, _ := buildLineage(t, store, dek)
	root := vol.chain[len(vol.chain)-1]
	before := listAll(t, store)

	_, err := lineage.Delete(t.Context(), store, root.Volume)
	if !errors.Is(err, lineage.ErrHasDescendants) {
		t.Fatalf("deleting a volume with a clone: %v, want ErrHasDescendants", err)
	}
	// The message has to name the volume the operator must deal with, or the refusal is
	// a dead end.
	if mid := vol.chain[0].Volume; !strings.Contains(err.Error(), mid) {
		t.Errorf("the refusal does not name the descendant %s: %v", mid, err)
	}
	if after := listAll(t, store); after != before {
		t.Errorf("a refused delete changed the bucket:\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// TestAFlattenedCloneOutlivesItsWholeAncestry pins the answer
// end to end: flatten the clone, then delete every volume it used to read through, and
// read its bytes back.
//
// It is what makes FLATTEN the precondition of deleting a parent rather than a
// suggestion, and it proves the refusal is *temporary* — Descendants asks the bucket, so
// a flatten lifts it. Asking the catalog would not: volumes.parent_snapshot_id can never
// be written back to NULL by a converging create, so a delete that believed the catalog
// would refuse for ever exactly the volumes a flatten has already freed.
func TestAFlattenedCloneOutlivesItsWholeAncestry(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, enc := buildLineage(t, store, dek)
	mid, root := vol.chain[0], vol.chain[1]

	if _, err := lineage.Delete(t.Context(), store, mid.Volume); !errors.Is(err, lineage.ErrHasDescendants) {
		t.Fatalf("deleting the clone's parent before the flatten: %v, want ErrHasDescendants", err)
	}
	if _, err := lineage.Flatten(t.Context(), store, rand.Reader, enc, vol.id); err != nil {
		t.Fatalf("Flatten: %v", err)
	}

	// The parent, then the grandparent — and not the other way round, which is the
	// order an operator has to follow and the order this asserts.
	if _, err := lineage.Delete(t.Context(), store, root.Volume); !errors.Is(err, lineage.ErrHasDescendants) {
		t.Fatalf("deleting the grandparent while the parent still descends from it: %v", err)
	}
	if _, err := lineage.Delete(t.Context(), store, mid.Volume); err != nil {
		t.Fatalf("deleting the flattened clone's parent: %v", err)
	}
	res, err := lineage.Delete(t.Context(), store, root.Volume)
	if err != nil {
		t.Fatalf("deleting the grandparent: %v", err)
	}
	// The root is its own lineage, so this is the delete that actually reclaims: the
	// chunk store the whole chain shared was under its id.
	if res.Chunks == 0 || res.ChunkBytes == 0 {
		t.Errorf("deleting the lineage root reclaimed %d chunk objects (%d bytes); its chunk store held the chain's data",
			res.Chunks, res.ChunkBytes)
	}

	alone, _, _, err := image.Load(t.Context(), store, enc, image.OwnLineage(vol.u), nil)
	if err != nil {
		t.Fatalf("loading the flattened clone after its whole ancestry was deleted: %v", err)
	}
	assertReads(t, alone, wholeChain, "after every ancestor was deleted")
}

// TestADeleteNeverLeavesAManifestOverMissingChunks is the ordering, and it is why the
// order is written down rather than left to whoever implements it.
//
// A publish writes chunks and then the manifest, so a manifest that exists always
// resolves (image.Publish). This interrupts a delete after every single object it marks
// and asks the one question that matters at each of those points: does what is left read
// correctly, or does it read *wrong*? Two answers are acceptable — the volume loads and
// returns its bytes, or it has no published image — and one is not: a manifest that
// still loads, naming chunks whose bytes have gone.
//
// Planting the publish order (chunks first, manifest after — which is what "delete it
// the way you wrote it" produces) turns it red inside the chunk loop.
func TestADeleteNeverLeavesAManifestOverMissingChunks(t *testing.T) {
	// A volume that descends from nothing, because it is the only kind whose delete
	// touches all four kinds of object: a clone's chunks are in its ancestry's store, so
	// an interruption inside a clone's chunk loop would have nothing to interrupt.
	dek := newDEK(t)
	build := func(t *testing.T) (*sim.ObjectStore, string, [16]byte, *wal.Encryption) {
		t.Helper()
		store := sim.NewObjectStore()
		id, u := newVolume(t)
		enc := encryptionFor(t, dek, u)
		view := cow.NewIntervalMap()
		for _, off := range []int64{offRootOnly, offErased, offShared, offOwnErased} {
			view.Overwrite(uint64(off), repeat(0xA1))
		}
		if _, err := image.Publish(t.Context(), store, rand.Reader, enc, image.OwnLineage(u), view, nil, 9, ""); err != nil {
			t.Fatalf("publishing the volume's image: %v", err)
		}
		publishSnapshotOf(t, store, dek, u, u, view, nil)
		writeDescriptor(t, store, id, "", "", 0)
		return store, id, u, enc
	}
	// What the volume answers out of its own image: the four ranges it wrote, and zeros
	// where the fixture's chain used to have something and this volume does not.
	want := map[int64]byte{
		offRootOnly: 0xA1, offErased: 0xA1, offShared: 0xA1, offOwnErased: 0xA1,
		offMiddleOnly: 0x00, offOwn: 0x00,
	}

	// How many objects one uninterrupted delete marks, so the loop below stops at every
	// point the real one could stop at.
	store, id, _, _ := build(t)
	full, err := lineage.Delete(t.Context(), store, id)
	if err != nil {
		t.Fatalf("the uninterrupted delete: %v", err)
	}
	steps := boolInt(full.Descriptor) + full.Snapshots + boolInt(full.Manifest) + full.Chunks
	if full.Chunks == 0 || !full.Manifest || !full.Descriptor {
		t.Fatalf("the fixture is not what this test needs: %+v", full)
	}

	for stop := 1; stop <= steps; stop++ {
		t.Run(fmt.Sprintf("interrupted after %d of %d marks", stop, steps), func(t *testing.T) {
			store, id, u, enc := build(t)
			cut := &stopAfter{Store: store, left: stop}
			if _, err := lineage.Delete(t.Context(), cut, id); err != nil && !errors.Is(err, errInterrupted) {
				t.Fatalf("the interrupted delete failed for another reason: %v", err)
			}

			// The reading a fresh Agent would perform: this volume descends from
			// nothing, so its own image is the whole of it.
			view, _, _, lerr := image.Load(t.Context(), store, enc, image.OwnLineage(u), nil)
			switch {
			case errors.Is(lerr, image.ErrNotPublished):
				// The manifest is gone. Whatever is left under the volume's names is
				// unreferenced storage, which is the acceptable half of the
				// asymmetry: an object collected late costs storage, an object
				// collected early costs data.
			case lerr != nil:
				t.Fatalf("after %d of %d marks the image is neither readable nor gone: %v", stop, steps, lerr)
			default:
				assertReads(t, view, want,
					fmt.Sprintf("after %d of %d marks the manifest is still there, so it must still resolve", stop, steps))
			}
		})
	}
}

// TestDescendantsRefusesADescriptorItCannotRead is the fail-closed half of the scan. A
// volume whose descriptor is corrupt might be a descendant, and "might be" is what a
// delete has to treat as yes: the failure it prevents is a clone whose whole ancestry
// disappears under it.
func TestDescendantsRefusesADescriptorItCannotRead(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, _ := buildLineage(t, store, dek)
	root := vol.chain[len(vol.chain)-1]

	// A descriptor that fails its own digest check — a truncated write, a bit flip in
	// the bucket, anything that means nobody can state its contents.
	if _, err := store.Put(t.Context(), descriptor.Key(vol.id), []byte("not a descriptor"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := lineage.Descendants(t.Context(), store, root.Volume); err == nil {
		t.Fatal("the descendant scan skipped a descriptor it could not read")
	}
	if _, err := lineage.Delete(t.Context(), store, root.Volume); err == nil {
		t.Fatal("a delete ran with a descriptor it could not read")
	}
}

// errInterrupted is what stopAfter answers with once its budget is spent. It is a
// distinct error rather than a cancelled context so that a delete failing for a real
// reason cannot be mistaken for the interruption this test arranged.
var errInterrupted = errors.New("test: the delete was interrupted here")

// stopAfter lets a fixed number of deletes through and then refuses, which is what a
// process killed part-way through a delete looks like from the bucket's side.
type stopAfter struct {
	objectstore.Store
	left int
}

func (s *stopAfter) Delete(ctx context.Context, key string) error {
	if s.left <= 0 {
		return errInterrupted
	}
	s.left--
	return s.Store.Delete(ctx, key)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// keysUnder is every object under the given prefixes, in listing order. It is the record
// a restore replays, and taking it from the bucket rather than composing it by hand is
// what makes the restore assert something about the delete instead of about this test's
// idea of what the delete touched.
func keysUnder(t *testing.T, store objectstore.Store, prefixes ...string) []string {
	t.Helper()
	var keys []string
	for _, p := range prefixes {
		objs, err := store.List(t.Context(), p)
		if err != nil {
			t.Fatalf("listing %s: %v", p, err)
		}
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
	}
	return keys
}
