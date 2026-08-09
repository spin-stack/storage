package descriptor_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The descriptor is an on-S3 format, so CLAUDE.md requires a serialize/replay property
// test over arbitrary truncations and bit corruptions (§25.2). It had none until
// dek_key_id was added to it; these are the tests that change owes.

func genDescriptor(t *rapid.T) descriptor.Descriptor {
	wrapped := rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(t, "dek_wrapped")
	// One draw decides both halves of the chain link, because a descriptor naming a
	// snapshot with no volume is half a link — a state agent.parentView refuses rather
	// than stores, and one no writer produces. Two independent draws would generate it
	// half the time and prove something about a shape that does not exist.
	var parentSnapshot, parentVolume string
	if rapid.Bool().Draw(t, "cloned") {
		parentSnapshot = ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(t, "parent_snapshot_ms")), rand.Reader).String()
		parentVolume = ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(t, "parent_volume_ms")), rand.Reader).String()
	}
	return descriptor.Descriptor{
		// Drawn, and deliberately allowed to be wrong: Write stamps the current version
		// over whatever the caller set, and a generator that always supplied the right
		// one could not tell that apart from Write trusting its argument. The round trip
		// below asserts the stamp, not the echo.
		FormatVersion: rapid.IntRange(0, 9).Draw(t, "format_version"),
		VolumeID:      ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(t, "ms")), rand.Reader).String(),
		SizeBytes:     int64(rapid.IntRange(1, 1<<40).Draw(t, "size")),
		BlockSize:     int32(rapid.IntRange(512, 1<<20).Draw(t, "block")),
		CurrentEpoch:  int64(rapid.IntRange(0, 1<<20).Draw(t, "epoch")),
		ChainDepth:    int32(rapid.IntRange(0, 32).Draw(t, "chain")),
		KEKID:         rapid.StringMatching(`[a-z0-9-]{1,16}`).Draw(t, "kek_id"),
		DEKWrapped:    wrapped,
		// Never 0: a descriptor carrying 0 describes a volume nothing can open, and
		// the write paths refuse it (metadata.CheckDEKKeyID). Generating it here would
		// be testing a state the system does not produce.
		DEKKeyID: uint32(rapid.IntRange(1, 1<<31).Draw(t, "dek_key_id")),
		// The chain link is drawn — rather than left at its zero value — because a field
		// that is always empty in the generator is a field the truncation and corruption
		// tests below never reach: `omitempty` keeps it out of the bytes entirely, so
		// there is nothing to truncate in the middle of and nothing to flip a bit in.
		// That is the same gap this file was written to close for dek_key_id.
		ParentSnapshotID: parentSnapshot,
		ParentVolumeID:   parentVolume,
	}
}

// TestDescriptorRoundTrips is the base case every corruption test needs: without it, a
// truncation test that always errors would pass against a Read that never worked.
func TestDescriptorRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := genDescriptor(rt)
		store := sim.NewObjectStore()
		ctx := t.Context()
		if err := descriptor.Write(ctx, store, want); err != nil {
			rt.Fatalf("write: %v", err)
		}
		got, err := descriptor.Read(ctx, store, want.VolumeID)
		if err != nil {
			rt.Fatalf("read: %v", err)
		}
		if got.DEKKeyID != want.DEKKeyID {
			rt.Fatalf("dek_key_id %d survived as %d", want.DEKKeyID, got.DEKKeyID)
		}
		if !bytes.Equal(got.DEKWrapped, want.DEKWrapped) {
			rt.Fatalf("dek_wrapped did not survive: %x -> %x", want.DEKWrapped, got.DEKWrapped)
		}
		// The version is stamped by Write, not echoed from the caller, so whatever the
		// generator drew is gone by the time it is read back. Asserted before the
		// whole-struct comparison because that comparison would otherwise report it as
		// "the round trip changed the descriptor", which is exactly what it should do.
		if got.FormatVersion != framed.FormatVersion {
			rt.Fatalf("Write stamped format_version %d over a drawn %d; want %d",
				got.FormatVersion, want.FormatVersion, framed.FormatVersion)
		}

		// Every other field, compared as a whole rather than one assertion per field:
		// a field added to this struct and forgotten by json is caught here.
		gotBlank, wantBlank := got, want
		gotBlank.DEKWrapped, wantBlank.DEKWrapped = nil, nil
		gotBlank.FormatVersion, wantBlank.FormatVersion = 0, 0
		if !reflect.DeepEqual(gotBlank, wantBlank) {
			rt.Fatalf("round trip changed the descriptor:\n want %+v\n  got %+v", want, got)
		}
	})
}

// TestDescriptorTruncationIsDetected: cut the object at any byte and Read must fail.
// A descriptor is JSON, so a truncated one loses its closing brace — but "must" is the
// point: the failure mode this rules out is a short read that decodes to a descriptor
// with default values, which would rebuild a volume at size 0 with no key at all.
func TestDescriptorTruncationIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		d := genDescriptor(rt)
		store := sim.NewObjectStore()
		ctx := t.Context()
		if err := descriptor.Write(ctx, store, d); err != nil {
			rt.Fatalf("write: %v", err)
		}
		full, err := store.Get(ctx, descriptor.Key(d.VolumeID))
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		at := rapid.IntRange(0, len(full)-1).Draw(rt, "truncate_at")
		if _, err := store.Put(ctx, descriptor.Key(d.VolumeID), full[:at], objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put truncated: %v", err)
		}
		if got, err := descriptor.Read(ctx, store, d.VolumeID); err == nil {
			rt.Fatalf("a descriptor truncated to %d/%d bytes decoded as %+v", at, len(full), got)
		}
	})
}

// TestDescriptorBitFlipIsDetected is what DEV-0015 was: until the digest landed, JSON
// carried no checksum, so flipping a digit in size_bytes yielded a different and
// perfectly valid descriptor — and §22.5's rebuild-metadata would recreate the volume
// at the wrong size, which re-running nothing repairs.
//
// Written as the general case rather than for the three fields that were exposed: any
// bit, at any offset, must make Read fail. A flip inside a whitespace-free JSON object
// either breaks the syntax or changes a value, and both must be caught — the second is
// the one that was silent.
func TestDescriptorBitFlipIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		d := genDescriptor(rt)
		store := sim.NewObjectStore()
		ctx := t.Context()
		if err := descriptor.Write(ctx, store, d); err != nil {
			rt.Fatalf("write: %v", err)
		}
		full, err := store.Get(ctx, descriptor.Key(d.VolumeID))
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
		if _, err := store.Put(ctx, descriptor.Key(d.VolumeID), corrupted, objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put corrupted: %v", err)
		}
		if got, rerr := descriptor.Read(ctx, store, d.VolumeID); rerr == nil {
			rt.Fatalf("a bit flipped at byte %d (bit %d) read back as a valid descriptor: %+v", i, bit, got)
		}
	})
}

// TestADescriptorWithNoDigestIsRefused — bare JSON, which is the shape of the format as
// it stood before DEV-0015 was closed, and it is refused rather than tolerated: nothing is
// deployed, so there is no such object anywhere to be lenient for, and a lenient branch
// would leave the hole open permanently for the sake of a volume that does not exist.
func TestADescriptorWithNoDigestIsRefused(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	d := descriptor.Descriptor{
		VolumeID: ids.New().String(), SizeBytes: 1 << 30, BlockSize: 4096,
		KEKID: "k", DEKWrapped: []byte{1}, DEKKeyID: 1,
	}
	body, err := json.Marshal(d) // marshalled directly: bare JSON, with no digest line
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, descriptor.Key(d.VolumeID), body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := descriptor.Read(ctx, store, d.VolumeID); !errors.Is(err, descriptor.ErrCorruptDescriptor) {
		t.Fatalf("want ErrCorruptDescriptor, got %v", err)
	}
}

// TestCorruptedKeyMaterialCannotUnwrap is the *other* half, and it holds independently
// of the digest: key material is self-detecting wherever it travels, including in a
// catalog row that never passed through this object.
//
// What this proves is that the two fields this increment cares about are
// self-detecting: dek_wrapped is an AEAD ciphertext and dek_key_id is bound to it as
// additional authenticated data (crypto.DevKMS.WrapDEK), so corrupting *either* makes
// the unwrap fail rather than yielding a key that decrypts nothing recognisable. A
// silently wrong DEK would decrypt every replayed record to garbage that still passes
// as bytes; ErrUnwrap says which object is broken.
func TestCorruptedKeyMaterialCannotUnwrap(t *testing.T) {
	var kek [crypto.DEKSize]byte
	if _, err := rand.Read(kek[:]); err != nil {
		t.Fatal(err)
	}
	kms := crypto.NewDevKMS(kek, "kek-1")

	rapid.Check(t, func(rt *rapid.T) {
		keyID := uint32(rapid.IntRange(1, 1<<20).Draw(rt, "key_id"))
		dek, err := crypto.GenerateDEK(rand.Reader, keyID)
		if err != nil {
			rt.Fatalf("dek: %v", err)
		}
		wrapped, err := kms.WrapDEK(rand.Reader, dek)
		if err != nil {
			rt.Fatalf("wrap: %v", err)
		}
		// The control: intact material unwraps to the same key.
		back, err := kms.UnwrapDEK(wrapped, keyID)
		if err != nil {
			rt.Fatalf("the intact DEK did not unwrap: %v", err)
		}
		if back.Key != dek.Key || back.KeyID != keyID {
			rt.Fatalf("unwrap returned a different key")
		}

		switch rapid.SampledFrom([]string{"ciphertext", "version"}).Draw(rt, "corrupt") {
		case "ciphertext":
			i := rapid.IntRange(0, len(wrapped)-1).Draw(rt, "byte")
			bit := rapid.IntRange(0, 7).Draw(rt, "bit")
			corrupted := append([]byte(nil), wrapped...)
			corrupted[i] ^= 1 << bit
			if _, err := kms.UnwrapDEK(corrupted, keyID); !errors.Is(err, crypto.ErrUnwrap) {
				rt.Fatalf("a bit flipped in dek_wrapped[%d] unwrapped anyway: %v", i, err)
			}
		case "version":
			// The version travelling separately from the ciphertext is exactly the
			// shape that could go wrong silently — a descriptor whose dek_key_id was
			// rewritten while dek_wrapped was not. It cannot: the version is the AAD.
			other := keyID ^ uint32(1<<rapid.IntRange(0, 19).Draw(rt, "version_bit"))
			if other == keyID || other == 0 {
				return
			}
			if _, err := kms.UnwrapDEK(wrapped, other); !errors.Is(err, crypto.ErrUnwrap) {
				rt.Fatalf("a DEK wrapped under version %d unwrapped as version %d: %v", keyID, other, err)
			}
		}
	})
}

// TestADescriptorUnderTheWrongKeyIsRefused is the half the digest cannot cover. The frame
// proves the bytes are the bytes somebody wrote; it says nothing about where they were
// written, so a descriptor copied under another volume's prefix — a restore that fanned
// out wrongly, a bucket somebody rearranged — verifies perfectly and describes the wrong
// volume's identity, geometry, wrapped key and parent link.
//
// It is asserted through Write and a Put of the same object under a second key, so the
// bytes are the production writer's rather than the test's: a hand-built body could fail
// this for a reason the real format never produces.
func TestADescriptorUnderTheWrongKeyIsRefused(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	mine := ids.New().String()
	theirs := ids.New().String()

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: mine, SizeBytes: 1 << 20, BlockSize: 512, CurrentEpoch: 1, DEKKeyID: 1,
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	body, err := store.Get(ctx, descriptor.Key(mine))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := store.Put(ctx, descriptor.Key(theirs), body, objectstore.PutOptions{}); err != nil {
		t.Fatalf("put under the other volume's key: %v", err)
	}

	// The control: the same bytes still open under their own key. Without it a failure
	// below would also be consistent with a descriptor this test wrote wrongly.
	if _, err := descriptor.Read(ctx, store, mine); err != nil {
		t.Fatalf("the descriptor no longer reads under its own key: %v", err)
	}
	got, err := descriptor.Read(ctx, store, theirs)
	if err == nil {
		t.Fatalf("volume %s read a descriptor describing volume %s", theirs, got.VolumeID)
	}
}

// The manifest's version check and this one are two call sites of one primitive, so this
// is here to prove the *call* exists rather than to re-prove framed.CheckVersion.
//
// It matters more here than it looks: this object is what `-rebuild-metadata` reads to
// reconstruct a volume the database no longer describes (INV-20), and it carries the
// volume's geometry, its wrapped DEK and the id of the key that opens it. A descriptor
// from a newer format decoding cleanly with unknown fields dropped is a catalog rebuilt
// wrong from an object that looked fine.
func TestADescriptorFromAnotherFormatVersionIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		wantErr error
	}{
		{"newer", `"format_version":2`, framed.ErrFormatTooNew},
		{"older", `"format_version":0`, framed.ErrFormatTooOld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := sim.NewObjectStore()
			d := descriptor.Descriptor{
				VolumeID: ids.New().String(), SizeBytes: 1 << 20, BlockSize: 4096, DEKKeyID: 1,
			}
			if err := descriptor.Write(ctx, store, d); err != nil {
				t.Fatal(err)
			}
			body, err := store.Get(ctx, descriptor.Key(d.VolumeID))
			if err != nil {
				t.Fatal(err)
			}
			payload, err := framed.Unframe(body)
			if err != nil {
				t.Fatal(err)
			}
			// Re-framed, so the digest still matches and the read reaches the version
			// check instead of stopping at corruption.
			mutated := bytes.Replace(payload, []byte(`"format_version":1`), []byte(tc.to), 1)
			if bytes.Equal(mutated, payload) {
				t.Fatalf("the mutation changed nothing: %s", payload)
			}
			if _, err := store.Put(ctx, descriptor.Key(d.VolumeID), framed.Frame(mutated), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := descriptor.Read(ctx, store, d.VolumeID); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Read = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
