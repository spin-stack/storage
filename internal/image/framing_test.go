package image_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/framed"
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

// The reason format_version exists, stated as a test rather than as a comment.
//
// These objects are JSON, and `json.Unmarshal` silently discards fields it does not
// know. So the failure this guards is not a parse error — it is the absence of one: an
// Agent meeting a manifest from a newer format decodes it cleanly, drops whatever was
// added, and serves the volume. The fields most likely to be added to a manifest are
// the ones that place data, and a tombstone this binary never heard of is an ancestor's
// bytes coming back at an offset the guest freed.
//
// The version is bumped in the stored bytes rather than by writing a Manifest with a
// different value, because Write stamps the constant over whatever the caller sets —
// which is deliberate, and means the only way to produce a wrong-version object is the
// way the world produces one: another binary wrote it.
func TestAManifestFromAnotherFormatVersionIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(payload []byte) []byte
		wantErr error
	}{
		{
			name: "a newer version tells the operator to roll this host forward",
			mutate: func(p []byte) []byte {
				return bytes.Replace(p, []byte(`"format_version":1`), []byte(`"format_version":2`), 1)
			},
			wantErr: framed.ErrFormatTooNew,
		},
		{
			// Unreachable today — the constant has only ever been 1 — and the branch is
			// asserted anyway, because the one time somebody sees this message it will
			// be during an incident and there will be no second chance to get it right.
			name: "an older version is a different error, and a different remedy",
			mutate: func(p []byte) []byte {
				return bytes.Replace(p, []byte(`"format_version":1`), []byte(`"format_version":0`), 1)
			},
			wantErr: framed.ErrFormatTooOld,
		},
		{
			// What every object written before this field existed looks like. Refused
			// rather than read as generation 1: nothing is deployed, so there is no such
			// object to be lenient for, and the lenient branch would outlive the reason.
			name:    "no version field at all is refused, not assumed",
			mutate:  func(p []byte) []byte { return bytes.Replace(p, []byte(`"format_version":1,`), nil, 1) },
			wantErr: framed.ErrFormatTooOld,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := sim.NewObjectStore()
			var vol [16]byte
			vol[0] = 0xC3
			view := cow.NewIntervalMap()
			view.Overwrite(0, bytes.Repeat([]byte{0x33}, 4096))
			if _, err := image.Publish(ctx, store, rand.Reader, nil,
				image.Ident{Volume: vol, Lineage: vol}, view, nil, 3, ""); err != nil {
				t.Fatalf("publish: %v", err)
			}
			key := image.ManifestKey(vol)

			// Re-frame after mutating: the digest covers the bytes as stored, so a
			// mutation that skipped it would be refused as corruption and this test
			// would pass without ever reaching the version check.
			body, err := store.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := framed.Unframe(body)
			if err != nil {
				t.Fatal(err)
			}
			mutated := tc.mutate(payload)
			if bytes.Equal(mutated, payload) {
				t.Fatalf("the mutation changed nothing; the payload was %s", payload)
			}
			if _, err := store.Put(ctx, key, framed.Frame(mutated), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}

			_, _, _, err = image.Load(ctx, store, nil, image.Ident{Volume: vol, Lineage: vol}, nil)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Load = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
