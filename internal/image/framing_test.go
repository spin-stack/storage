package image_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"pgregory.net/rapid"
)

// §25.2's second half, which this format could not answer until DEV-0025: a manifest is
// JSON, so a flipped bit in `"offset":1024` yields 1025 and decodes clean.
//
// The exposure is specific, and it is why this is worth a format change rather than a
// note. A manifest names chunks that verify themselves — the key is the digest of the
// plaintext — and it names a volume that readManifest checks. What has no second opinion
// is everything that *places* data: the offsets and lengths, and, since a manifest became
// a delta over an ancestry, the tombstones. A moved tombstone does not misplace this
// volume's bytes; it uncovers an ancestor's.
func TestAManifestBitFlipIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := t.Context()
		store := sim.NewObjectStore()
		var vol [16]byte
		vol[0] = 0xC1
		view := cow.NewIntervalMap()
		view.Overwrite(0, bytes.Repeat([]byte{0x5A}, 4096))
		view.Overwrite(1<<20, bytes.Repeat([]byte{0x6B}, 512))
		if _, err := image.Publish(ctx, store, rand.Reader, nil,
			image.Ident{Volume: vol, Lineage: vol}, view, nil, 7, ""); err != nil {
			rt.Fatalf("publish: %v", err)
		}
		key := image.ManifestKey(vol)

		full, err := store.Get(ctx, key)
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		i := rapid.IntRange(0, len(full)-1).Draw(rt, "byte")
		bit := rapid.IntRange(0, 7).Draw(rt, "bit")
		corrupted := append([]byte(nil), full...)
		corrupted[i] ^= 1 << bit
		if bytes.Equal(corrupted, full) {
			return
		}
		if _, err := store.Put(ctx, key, corrupted, objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put corrupted: %v", err)
		}
		// Load and not a bare parse: what must be refused is the *use* of the manifest,
		// and a check that only proved the parser said no would pass for a format that
		// parsed a wrong offset happily.
		if _, _, _, err := image.Load(ctx, store, nil, image.Ident{Volume: vol, Lineage: vol}, nil); err == nil {
			rt.Fatalf("a bit flipped at byte %d (bit %d) read back as a usable image", i, bit)
		}
	})
}

// Bare JSON — the shape of this format before DEV-0025 — is refused rather than
// tolerated. Nothing is deployed, so there is no such object anywhere to be lenient for,
// and a lenient branch would leave the hole open permanently for the sake of a manifest
// that does not exist. internal/descriptor made the same call for the same reason.
func TestAManifestWithNoDigestLineIsRefused(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	var vol [16]byte
	vol[0] = 0xC2
	view := cow.NewIntervalMap()
	view.Overwrite(0, bytes.Repeat([]byte{0x77}, 4096))
	if _, err := image.Publish(ctx, store, rand.Reader, nil,
		image.Ident{Volume: vol, Lineage: vol}, view, nil, 1, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	key := image.ManifestKey(vol)
	framedBody, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// Strip the digest line: everything after the first newline is the old format.
	nl := bytes.IndexByte(framedBody, '\n')
	if nl < 0 {
		t.Fatal("the published manifest carries no digest line at all")
	}
	if _, err := store.Put(ctx, key, framedBody[nl+1:], objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := image.Load(ctx, store, nil, image.Ident{Volume: vol, Lineage: vol}, nil); err == nil {
		t.Fatal("a manifest with no digest line was accepted")
	}
}
