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

// The whole of V1's fencing, in one test.
//
// ADR-0026 withdrew the lease-gated ACK, the promotion protocol and the fencing wait,
// and kept exactly one obligation: two incarnations of a volume must not both publish.
// Two hosts each uploading at stop is a lost update with no error anywhere — the second
// manifest simply replaces the first, and the writes the first host held are gone with
// nobody able to tell.
//
// One compare-and-set on one key is the entirety of that protection, which is why it
// gets a test of its own rather than a line in a larger one.
func TestASecondPublisherIsRefusedRatherThanOverwriting(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	first := cow.NewIntervalMap()
	first.Overwrite(0, bytes.Repeat([]byte{0x11}, 512))
	etag, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), first, nil, 1, "")
	if err != nil {
		t.Fatalf("the first publish: %v", err)
	}

	// A second incarnation that never saw the first one's manifest. This is a host that
	// booted the volume believing it had none — the exact split-brain the CAS is for.
	second := cow.NewIntervalMap()
	second.Overwrite(0, bytes.Repeat([]byte{0x22}, 512))
	if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), second, nil, 1, ""); !errors.Is(err, image.ErrSuperseded) {
		t.Fatalf("a second publisher overwrote the first: want ErrSuperseded, got %v", err)
	}

	// And the first host's data is still what the volume holds. Asserting the error is
	// not enough: a refusal that had already replaced the manifest would satisfy it.
	loaded, _, _, err := image.Load(ctx, store, nil, image.OwnLineage(vol), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 512)
	loaded.Read(0, got)
	if !bytes.Equal(got, bytes.Repeat([]byte{0x11}, 512)) {
		t.Fatalf("the refused publish took the volume anyway: read %x…", got[:8])
	}

	// The legitimate case still works: the same host, publishing again against the ETag
	// it holds. A fence that also blocks the writer it protects is not a fence.
	third := cow.NewIntervalMap()
	third.Overwrite(0, bytes.Repeat([]byte{0x33}, 512))
	if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), third, nil, 2, etag); err != nil {
		t.Fatalf("the rightful writer was refused its own volume: %v", err)
	}
}

// A stale ETag is the same failure wearing different clothes: a host that published,
// lost the volume, and comes back holding a manifest that has moved on.
func TestAStaleETagIsRefused(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	v := cow.NewIntervalMap()
	v.Overwrite(0, bytes.Repeat([]byte{0xAA}, 256))
	stale, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 2, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), v, nil, 3, stale); !errors.Is(err, image.ErrSuperseded) {
		t.Fatalf("a stale ETag published anyway: want ErrSuperseded, got %v", err)
	}
}

// A volume nobody has published is not an error: it is the first boot, and the only
// case that would otherwise be impossible.
func TestAVolumeWithNoImageIsNotAFailure(t *testing.T) {
	if _, _, _, err := image.Load(t.Context(), sim.NewObjectStore(), nil, image.OwnLineage(vol7()), nil); !errors.Is(err, image.ErrNotPublished) {
		t.Fatalf("want ErrNotPublished for a volume that never stopped, got %v", err)
	}
}
