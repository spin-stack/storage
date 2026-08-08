package image_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func vol7() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

// drawIdent draws the pair an image is keyed by: a volume, and the lineage whose chunk
// store holds its bytes.
//
// Half the draws are a volume that descends from nothing, where the two ids are equal —
// and that is the case that hides a bug, because every operation keyed by the wrong one
// of two equal ids still works. The other half is a clone, whose ids differ, which is
// the only shape in which the round trip can tell the manifest's owner from the chunk
// store's.
func drawIdent(rt *rapid.T) image.Ident {
	id := image.OwnLineage(vol7())
	if rapid.Bool().Draw(rt, "clone") {
		root := vol7()
		root[0] = 0xA0
		id.Lineage = root
	}
	return id
}

// drawEncryption draws whether this run seals its chunks, and binds the DEK the way an
// Agent does. §15 allows the unsealed mode for dev only; both are exercised because the
// truncation and corruption arms below detect a damaged chunk by different means in each
// (the digest and the length check when it is cleartext, the GCM tag when it is not).
func drawEncryption(rt *rapid.T, id image.Ident) *wal.Encryption {
	if !rapid.Bool().Draw(rt, "encrypted") {
		return nil
	}
	dek, err := crypto.GenerateDEK(&ramp{1}, 7)
	if err != nil {
		rt.Fatalf("GenerateDEK: %v", err)
	}
	enc, err := wal.NewEncryption(dek, id.Volume)
	if err != nil {
		rt.Fatalf("NewEncryption: %v", err)
	}
	return enc
}

// drawView draws a sequence of writes and discards over an 8 KiB volume.
func drawView(rt *rapid.T) *cow.IntervalMap {
	const size = 8192
	view := cow.NewIntervalMap()
	for range rapid.IntRange(0, 12).Draw(rt, "ops") {
		off := uint64(rapid.IntRange(0, size-1).Draw(rt, "off"))
		n := rapid.IntRange(1, 1024).Draw(rt, "len")
		if off+uint64(n) > size {
			n = size - int(off)
		}
		if rapid.Bool().Draw(rt, "discard") {
			view.Clear(off, uint64(n))
		} else {
			view.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, "b"))}, n))
		}
	}
	return view
}

// §25.2 for the image format: publish/load is total and exact, and it lands where it says.
//
// For any sequence of writes and discards, an image published from a view and loaded back
// answers Read identically at every offset. This is the property the format exists to
// have — an image that is merely "close" loses guest data silently, since the difference
// between a byte the guest wrote and a zero is invisible to everything downstream.
//
// Since the chunk store moved to the lineage the property has a second half, and it is
// asserted on the bucket rather than on the round trip: the chunks are under the
// *lineage's* prefix and the volume's own prefix holds nothing but its manifest. A
// publisher that put the bytes under the volume would round-trip perfectly against a
// loader making the same mistake — which is precisely the shape of format defect that
// only an assertion about the outside can catch.
func TestPublishLoadRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const size = 8192
		ctx := t.Context()
		store := sim.NewObjectStore()
		id := drawIdent(rt)
		enc := drawEncryption(rt, id)
		view := drawView(rt)

		if _, err := image.Publish(ctx, store, rand.Reader, enc, id, view, 42, ""); err != nil {
			rt.Fatalf("Publish: %v", err)
		}
		loaded, man, _, err := image.Load(ctx, store, enc, id)
		if err != nil {
			rt.Fatalf("Load: %v", err)
		}
		if man.Sequence != 42 {
			rt.Fatalf("sequence = %d, want 42", man.Sequence)
		}

		want, got := make([]byte, size), make([]byte, size)
		view.Read(0, want)
		loaded.Read(0, got)
		if !bytes.Equal(want, got) {
			for i := range want {
				if want[i] != got[i] {
					rt.Fatalf("byte %d: published %d, loaded %d", i, want[i], got[i])
				}
			}
		}

		// Where the bytes landed. The keys are spelled out rather than taken from the
		// package: a test that asks the code under test where it put something asserts
		// nothing about where that is.
		volPrefix := "image/" + format.UUIDString(id.Volume) + "/"
		own, err := store.List(ctx, volPrefix)
		if err != nil {
			rt.Fatalf("listing %s: %v", volPrefix, err)
		}
		for _, o := range own {
			if o.Key != volPrefix+"manifest.json" {
				rt.Fatalf("object %s is under the volume's prefix; only its manifest belongs there", o.Key)
			}
		}
		chunkPrefix := "chunks/" + format.UUIDString(id.Lineage) + "/"
		chunks, err := store.List(ctx, chunkPrefix)
		if err != nil {
			rt.Fatalf("listing %s: %v", chunkPrefix, err)
		}
		// Distinct digests, not entries: two ranges holding identical bytes are two
		// entries in the manifest and one object in the bucket, which is the dedup
		// working rather than a chunk gone missing.
		digests := map[string]bool{}
		for _, c := range man.Chunks {
			digests[c.Digest] = true
		}
		if len(chunks) != len(digests) {
			rt.Fatalf("the manifest names %d distinct chunks and %s holds %d", len(digests), chunkPrefix, len(chunks))
		}
	})
}

// Corruption is detected, never applied. A chunk that does not hash to its key, that is
// the wrong length, or whose seal does not authenticate, fails the load — because the
// alternative is a view with a hole in it, and a hole reads as zeros, which is
// indistinguishable from a range the guest never wrote.
func TestLoadRefusesCorruptedChunks(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := t.Context()
		store := sim.NewObjectStore()
		id := drawIdent(rt)
		enc := drawEncryption(rt, id)

		view := cow.NewIntervalMap()
		view.Overwrite(0, bytes.Repeat([]byte{0xAB}, 2048))
		if _, err := image.Publish(ctx, store, rand.Reader, enc, id, view, 1, ""); err != nil {
			rt.Fatal(err)
		}

		objs, err := store.List(ctx, image.ChunksPrefix(id.Lineage))
		if err != nil || len(objs) == 0 {
			rt.Fatalf("no chunks to corrupt: %v", err)
		}
		key := objs[0].Key
		body, err := store.Get(ctx, key)
		if err != nil {
			rt.Fatal(err)
		}

		// Either flip a bit or truncate — the two shapes §25.2 names.
		if rapid.Bool().Draw(rt, "truncate") {
			n := rapid.IntRange(0, len(body)-1).Draw(rt, "keep")
			body = body[:n]
		} else {
			i := rapid.IntRange(0, len(body)-1).Draw(rt, "byte")
			body[i] ^= 1 << rapid.IntRange(0, 7).Draw(rt, "bit")
		}
		// Unconditional Put is how the corruption gets in: it is what a backend
		// silently returning wrong bytes looks like from here.
		if _, err := store.Put(ctx, key, body, objectstore.PutOptions{}); err != nil {
			rt.Fatal(err)
		}

		if _, _, _, err := image.Load(ctx, store, enc, id); err == nil {
			rt.Fatal("a corrupted chunk loaded without complaint; the guest would be served zeros or wrong bytes")
		}
	})
}

// The binding is still a binding: a chunk opens for its lineage and for nothing else.
//
// This is the assertion that distinguishes "we moved the key" from "we removed the
// protection", and it is why the AAD changed rather than being dropped. Both directions
// are here because either alone is satisfied by a mistake:
//
//   - **it opens for a clone.** The clone below uploads nothing at all — its view is its
//     parent's, so every chunk its manifest names is one its parent sealed — and it reads
//     every byte back. An AAD that still named the writing volume would fail this, which
//     is what §1.2 of CHUNK-ADDRESSING-SPEC reproduced before the change.
//   - **it does not open for a stranger.** The same chunk objects and the same manifest
//     are copied under an unrelated volume's identity, *with the same DEK*, and the load
//     fails. The shared DEK is what makes this an assertion about the AAD: with a
//     different key it would fail for a reason that says nothing about the binding, which
//     is the mistake CHUNK-ADDRESSING-SPEC §1.2's control line exists to rule out.
//
// The content is drawn rather than fixed because a binding that happened to hold for one
// plaintext and not another would be a stranger defect than the one being tested for.
func TestAChunkOpensForItsLineageAndForNothingElse(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := t.Context()
		store := sim.NewObjectStore()

		parent, clone, stranger := volumeID(0xA0), volumeID(0xC0), volumeID(0xD0)
		// One DEK for all three. A clone inherits its parent's (controlplane.Clone); the
		// stranger is given it too, so that nothing below can fail merely for want of a
		// key.
		dek, err := crypto.GenerateDEK(&ramp{1}, 7)
		if err != nil {
			rt.Fatalf("GenerateDEK: %v", err)
		}
		enc, err := wal.NewEncryption(dek, parent)
		if err != nil {
			rt.Fatalf("NewEncryption: %v", err)
		}

		view := drawView(rt)
		if _, err := image.Publish(ctx, store, rand.Reader, enc, image.OwnLineage(parent), view, 1, ""); err != nil {
			rt.Fatalf("the parent's publish: %v", err)
		}
		before, err := store.List(ctx, image.ChunksPrefix(parent))
		if err != nil {
			rt.Fatal(err)
		}

		// The clone publishes the same view: every chunk it names is one the parent
		// sealed, so if it can read its own image back it is reading its parent's chunks.
		cloneID := image.Ident{Volume: clone, Lineage: parent}
		if _, err := image.Publish(ctx, store, rand.Reader, enc, cloneID, view, 2, ""); err != nil {
			rt.Fatalf("the clone's publish: %v", err)
		}
		after, err := store.List(ctx, image.ChunksPrefix(parent))
		if err != nil {
			rt.Fatal(err)
		}
		if len(after) != len(before) {
			rt.Fatalf("the clone uploaded %d new chunk objects; it wrote nothing its parent had not already stored", len(after)-len(before))
		}
		loaded, man, _, err := image.Load(ctx, store, enc, cloneID)
		if err != nil {
			rt.Fatalf("a clone could not open the chunks its parent sealed: %v", err)
		}
		want, got := make([]byte, 8192), make([]byte, 8192)
		view.Read(0, want)
		loaded.Read(0, got)
		if !bytes.Equal(want, got) {
			rt.Fatal("the clone read back something other than what its parent published")
		}
		if len(man.Chunks) == 0 && len(before) > 0 {
			rt.Fatal("the clone's manifest names no chunks, so nothing above was tested")
		}

		// Now the stranger: the same bytes, the same digests, the same DEK, its own
		// lineage. This is a bucket copied under the wrong prefix, or a volume claiming
		// another lineage's data.
		for _, o := range after {
			body, err := store.Get(ctx, o.Key)
			if err != nil {
				rt.Fatal(err)
			}
			digest := o.Key[len(image.ChunksPrefix(parent)):]
			if _, err := store.Put(ctx, image.ChunksPrefix(stranger)+digest, body, objectstore.PutOptions{}); err != nil {
				rt.Fatal(err)
			}
		}
		man.VolumeID = format.UUIDString(stranger)
		body, err := json.Marshal(man)
		if err != nil {
			rt.Fatal(err)
		}
		if _, err := store.Put(ctx, image.ManifestKey(stranger), body, objectstore.PutOptions{}); err != nil {
			rt.Fatal(err)
		}
		strangerEnc, err := wal.NewEncryption(dek, stranger)
		if err != nil {
			rt.Fatal(err)
		}
		_, _, _, err = image.Load(ctx, store, strangerEnc, image.OwnLineage(stranger))
		if len(man.Chunks) > 0 && err == nil {
			rt.Fatal("an unrelated volume opened a chunk sealed for another lineage: the blast radius of a chunk is the bucket, not the lineage")
		}
	})
}
