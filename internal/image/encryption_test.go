package image_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

type ramp struct{ b byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

func encFor(t *testing.T, vol [16]byte) *wal.Encryption {
	t.Helper()
	dek, err := crypto.GenerateDEK(&ramp{1}, 7)
	if err != nil {
		t.Fatal(err)
	}
	e, err := wal.NewEncryption(dek, vol)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// §5.10/INV-15 for the image: nothing leaves the host in cleartext.
//
// Asserted on the bucket, not on the call — a Publish that took an Encryption and
// forgot to use it would satisfy any assertion on its arguments, and that is exactly
// the defect this test exists for. The first version of this package wrote plaintext
// chunks and its round-trip property test passed happily.
func TestPublishedChunksAreCiphertext(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()
	enc := encFor(t, vol)

	secret := bytes.Repeat([]byte("GUEST-SECRET"), 64)
	view := cow.NewIntervalMap()
	view.Overwrite(0, secret)

	if _, err := image.Publish(ctx, store, &ramp{9}, enc, image.OwnLineage(vol), view, nil, 1, ""); err != nil {
		t.Fatal(err)
	}

	// Both prefixes, because the chunks left image/<volume>/ when the chunk store moved
	// to the lineage: a listing of the volume's prefix alone now reaches the manifest and
	// nothing else, which would make this test pass over an object that never carried
	// guest bytes in the first place.
	objs, err := store.List(ctx, image.Prefix(vol))
	if err != nil {
		t.Fatalf("listing the volume's prefix: %v", err)
	}
	chunks, err := store.List(ctx, image.ChunksPrefix(vol))
	if err != nil || len(chunks) == 0 {
		t.Fatalf("no chunk objects under %s: %v", image.ChunksPrefix(vol), err)
	}
	for _, o := range append(objs, chunks...) {
		body, err := store.Get(ctx, o.Key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte("GUEST-SECRET")) {
			t.Fatalf("object %s carries the guest's plaintext", o.Key)
		}
	}

	// And it is not encrypted-to-noise: the same image loads back through the same key.
	loaded, _, _, err := image.Load(ctx, store, enc, image.OwnLineage(vol), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(secret))
	loaded.Read(0, got)
	if !bytes.Equal(got, secret) {
		t.Fatal("the encrypted image did not load back what was published")
	}
}

// A nonce is used for exactly one plaintext, and this is the mechanism that guarantees
// it: a chunk whose key exists is never re-sealed. Publishing the same content twice
// must leave the ciphertext untouched — if it were re-encrypted, a second nonce would be
// drawn for the same plaintext, which is harmless, but the *implementation* that
// re-encrypts is one step from deriving a nonce and reusing it.
func TestAnUnchangedChunkIsNeverResealed(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()
	enc := encFor(t, vol)

	view := cow.NewIntervalMap()
	view.Overwrite(0, bytes.Repeat([]byte{0x5A}, 4096))

	etag, err := image.Publish(ctx, store, &ramp{3}, enc, image.OwnLineage(vol), view, nil, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	objs, _ := store.List(ctx, image.ChunksPrefix(vol))
	if len(objs) != 1 {
		t.Fatalf("expected one chunk, got %d", len(objs))
	}
	before, _ := store.Get(ctx, objs[0].Key)

	// Publish the identical view again, with a different random source. A re-seal would
	// produce different bytes.
	if _, err := image.Publish(ctx, store, &ramp{200}, enc, image.OwnLineage(vol), view, nil, 2, etag); err != nil {
		t.Fatal(err)
	}
	after, _ := store.Get(ctx, objs[0].Key)
	if !bytes.Equal(before, after) {
		t.Fatal("an unchanged chunk was re-sealed: the same plaintext now exists under two nonces")
	}
}

// The wrong key fails closed rather than returning noise the guest would be served as
// its own data.
func TestLoadWithTheWrongKeyFailsClosed(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	view := cow.NewIntervalMap()
	view.Overwrite(0, bytes.Repeat([]byte{0x7E}, 1024))
	if _, err := image.Publish(ctx, store, &ramp{1}, encFor(t, vol), image.OwnLineage(vol), view, nil, 1, ""); err != nil {
		t.Fatal(err)
	}

	other, err := crypto.GenerateDEK(&ramp{99}, 7)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := wal.NewEncryption(other, vol)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := image.Load(ctx, store, wrong, image.OwnLineage(vol), nil); err == nil {
		t.Fatal("an image loaded under the wrong key; the guest would be served ciphertext as its own data")
	}
}
