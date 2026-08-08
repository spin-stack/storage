package image_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A snapshot is a *frozen* copy, and this is the property that makes the word mean
// anything: the source keeps writing immediately afterwards (§19 — a snapshot is a
// number, not an event), so a snapshot that picked up those later writes would be a copy
// of no particular moment.
func TestASnapshotDoesNotSeeWritesThatFollowIt(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	before := bytes.Repeat([]byte{0xAA}, 1024)
	live := cow.NewIntervalMap()
	live.Overwrite(0, before)

	// The seal: the snapshot keeps the map as it stands, the volume gets a fresh layer
	// over it. That is one pointer, which is where §2's "~0 pause" comes from.
	frozen := live
	live = cow.NewIntervalMapOver(frozen)

	if _, err := image.PublishSnapshot(ctx, store, rand.Reader, nil, image.OwnLineage(vol), frozen, nil, 7, "snap-1"); err != nil {
		t.Fatalf("PublishSnapshot: %v", err)
	}

	// The source carries on, over the top of the same range.
	after := bytes.Repeat([]byte{0xBB}, 1024)
	live.Overwrite(0, after)

	view, man, err := image.LoadSnapshot(ctx, store, nil, image.OwnLineage(vol), "snap-1")
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if man.Sequence != 7 {
		t.Fatalf("the snapshot froze at sequence %d, want 7", man.Sequence)
	}
	got := make([]byte, len(before))
	view.Read(0, got)
	if !bytes.Equal(got, before) {
		t.Fatalf("the snapshot reads %x…, want the bytes at the moment it was taken (%x…)", got[:8], before[:8])
	}
	// And the live volume did move on, or the test above would pass for the wrong reason.
	live.Read(0, got)
	if !bytes.Equal(got, after) {
		t.Fatal("the source volume did not take the later write; the freeze proved nothing")
	}
}

// §5.2/INV-16: a published snapshot never changes. It is create-only rather than CASed,
// which is the difference between it and the volume's own manifest — one is a named point
// in the past, the other is where the volume resumes.
func TestASnapshotCannotBeRepublished(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	v := cow.NewIntervalMap()
	v.Overwrite(0, bytes.Repeat([]byte{0x11}, 512))
	if _, err := image.PublishSnapshot(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 1, "snap-1"); err != nil {
		t.Fatal(err)
	}

	other := cow.NewIntervalMap()
	other.Overwrite(0, bytes.Repeat([]byte{0x22}, 512))
	if _, err := image.PublishSnapshot(ctx, store, rand.Reader, nil, image.OwnLineage(vol), other, nil, 2, "snap-1"); !errors.Is(err, image.ErrSnapshotExists) {
		t.Fatalf("a published snapshot was overwritten: %v, want ErrSnapshotExists", err)
	}

	// And it still holds what it held.
	view, _, err := image.LoadSnapshot(ctx, store, nil, image.OwnLineage(vol), "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 512)
	view.Read(0, got)
	if !bytes.Equal(got, bytes.Repeat([]byte{0x11}, 512)) {
		t.Fatal("the refused republish changed the snapshot anyway")
	}
}

// Snapshots share the volume's chunks, which is what makes §2's primary use case —
// frequent cloning from snapshots — affordable. A second snapshot of an unchanged volume
// must cost no new bytes at all.
func TestASecondSnapshotOfAnUnchangedVolumeUploadsNothing(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	v := cow.NewIntervalMap()
	v.Overwrite(0, bytes.Repeat([]byte{0x5A}, 8192))
	if _, err := image.PublishSnapshot(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 1, "snap-1"); err != nil {
		t.Fatal(err)
	}
	first, _ := store.List(ctx, image.ChunksPrefix(vol))

	if _, err := image.PublishSnapshot(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 2, "snap-2"); err != nil {
		t.Fatal(err)
	}
	second, _ := store.List(ctx, image.ChunksPrefix(vol))

	if len(second) != len(first) {
		t.Fatalf("a second snapshot of an unchanged volume added %d chunk(s); it must add none",
			len(second)-len(first))
	}
}

// A snapshot nobody took is not a broken image, and callers have to be able to tell the
// difference — a clone naming a snapshot that does not exist is a Control Plane problem, not
// a corrupt object.
func TestLoadingAnAbsentSnapshot(t *testing.T) {
	if _, _, err := image.LoadSnapshot(t.Context(), sim.NewObjectStore(), nil, image.OwnLineage(vol7()), "nope"); !errors.Is(err, image.ErrNotPublished) {
		t.Fatalf("want ErrNotPublished for a snapshot nobody took, got %v", err)
	}
}
