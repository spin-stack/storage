package crypto_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/crypto"
)

const frame = 1024

func layerKey(t *testing.T) *crypto.Encryption {
	t.Helper()
	dek, err := crypto.GenerateDEK(rand.Reader, 1)
	if err != nil {
		t.Fatalf("generating a DEK: %v", err)
	}
	enc, err := crypto.NewEncryption(dek, vol)
	if err != nil {
		t.Fatalf("binding it to a volume: %v", err)
	}
	return enc
}

// keyFor is the same key bound to a different volume, which is how a layer is asked to
// open under an identity that is not its own.
func keyFor(t *testing.T, d *crypto.Encryption, volumeID [16]byte) *crypto.Encryption {
	t.Helper()
	enc, err := crypto.NewEncryption(d.DEK, volumeID)
	if err != nil {
		t.Fatalf("rebinding: %v", err)
	}
	return enc
}

var (
	vol   = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	layer = [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
)

func seal(t *testing.T, d *crypto.Encryption, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := d.SealLayer(layer, frame, bytes.NewReader(plain), &out); err != nil {
		t.Fatalf("sealing %d bytes: %v", len(plain), err)
	}
	return out.Bytes()
}

// TestSealLayerRoundTrips covers the boundaries a frame-based format gets wrong: empty,
// one byte, one byte either side of a frame, and a plaintext that is an exact multiple
// of the frame — the last being the case a length check alone gets wrong, because
// nothing about the final frame's *size* says it is final.
func TestSealLayerRoundTrips(t *testing.T) {
	t.Parallel()
	d := layerKey(t)
	for _, n := range []int{0, 1, frame - 1, frame, frame + 1, 3 * frame, 3*frame + 7} {
		plain := make([]byte, n)
		if _, err := rand.Read(plain); err != nil {
			t.Fatalf("drawing plaintext: %v", err)
		}
		sealed := seal(t, d, plain)
		// One frame per full frame's worth, plus one for the remainder — and one for a
		// plaintext of nothing, because a layer with no final frame is not a layer.
		frames := (n + frame - 1) / frame
		if frames == 0 {
			frames = 1
		}
		if want := n + frames*crypto.TagSize; len(sealed) != want {
			t.Errorf("%d bytes sealed to %d bytes in %d frames, want %d", n, len(sealed), frames, want)
		}
		var got bytes.Buffer
		if err := d.OpenLayer(layer, frame, bytes.NewReader(sealed), &got); err != nil {
			t.Fatalf("opening %d bytes: %v", n, err)
		}
		if !bytes.Equal(got.Bytes(), plain) {
			t.Errorf("%d bytes did not survive the round trip", n)
		}
	}
}

// TestSealLayerIsDeterministic is the property the whole retry story rests on: the same
// layer sealed twice is the same object, so the same digest, so the same
// content-addressed key. Without it every interrupted upload leaves an orphan behind
// that only a sweep could find.
func TestSealLayerIsDeterministic(t *testing.T) {
	t.Parallel()
	d := layerKey(t)
	plain := make([]byte, 3*frame+11)
	if _, err := rand.Read(plain); err != nil {
		t.Fatalf("drawing plaintext: %v", err)
	}
	if !bytes.Equal(seal(t, d, plain), seal(t, d, plain)) {
		t.Fatal("the same layer sealed twice produced different bytes")
	}
}

// TestSealLayerIsBoundToItsIdentity: a layer moved under another volume's or another
// layer's name does not decrypt into that chain. The AAD is what makes an object that
// is intact and correctly digested still refuse to be somebody else's layer.
func TestSealLayerIsBoundToItsIdentity(t *testing.T) {
	t.Parallel()
	d := layerKey(t)
	plain := make([]byte, 2*frame)
	sealed := seal(t, d, plain)
	other := [16]byte{9, 9, 9}

	tests := []struct {
		name       string
		key        *crypto.Encryption
		lay        [16]byte
		frameBytes int
	}{
		{"another volume", keyFor(t, d, other), layer, frame},
		{"another layer", d, other, frame},
		{"a different framing", d, layer, frame * 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got bytes.Buffer
			if err := tt.key.OpenLayer(tt.lay, tt.frameBytes, bytes.NewReader(sealed), &got); err == nil {
				t.Fatal("it opened")
			}
		})
	}
}

// TestSealLayerRefusesARearrangedStream: the frame index is in the AAD, so a valid
// layer's own frames put back in a different order is not a valid layer. Duplication is
// the case worth naming — it is what a retrying uploader with a bug produces, and the
// result would otherwise be a qcow2 with a cluster repeated where another should be.
func TestSealLayerRefusesARearrangedStream(t *testing.T) {
	t.Parallel()
	d := layerKey(t)
	// Not an exact multiple of the frame, so the last frame is a short one and the three
	// before it are full. A multiple would make the third frame *both* full and final,
	// and "drop the last frame" would then leave a stream that is legitimately whole —
	// which is how the first version of this test passed while proving nothing.
	plain := make([]byte, 3*frame+13)
	if _, err := rand.Read(plain); err != nil {
		t.Fatalf("drawing plaintext: %v", err)
	}
	sealed := seal(t, d, plain)
	full := frame + crypto.TagSize
	f := [][]byte{sealed[0:full], sealed[full : 2*full], sealed[2*full : 3*full], sealed[3*full:]}

	tests := []struct {
		name  string
		order [][]byte
	}{
		{"two frames swapped", [][]byte{f[1], f[0], f[2], f[3]}},
		{"a frame repeated", [][]byte{f[0], f[0], f[2], f[3]}},
		{"the final frame dropped", [][]byte{f[0], f[1], f[2]}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var in, got bytes.Buffer
			for _, part := range tt.order {
				in.Write(part)
			}
			if err := d.OpenLayer(layer, frame, bytes.NewReader(in.Bytes()), &got); err == nil {
				t.Fatal("it opened")
			}
		})
	}
}

// TestSealedLayerTruncationIsDetected — §25.2's first half, over the sealed stream
// rather than over a manifest. It matters here because the object's own SHA-256 lives in
// the manifest, and a recovery assembling a chain reads layers it may not have manifests
// for yet.
func TestSealedLayerTruncationIsDetected(t *testing.T) {
	d := layerKey(t)
	plain := make([]byte, 3*frame+13)
	if _, err := rand.Read(plain); err != nil {
		t.Fatalf("drawing plaintext: %v", err)
	}
	var out bytes.Buffer
	if err := d.SealLayer(layer, frame, bytes.NewReader(plain), &out); err != nil {
		t.Fatalf("sealing: %v", err)
	}
	sealed := out.Bytes()

	rapid.Check(t, func(rt *rapid.T) {
		at := rapid.IntRange(0, len(sealed)-1).Draw(rt, "truncate_at")
		var got bytes.Buffer
		err := d.OpenLayer(layer, frame, bytes.NewReader(sealed[:at]), &got)
		if err == nil {
			rt.Fatalf("a layer truncated to %d/%d bytes opened", at, len(sealed))
		}
		// And what did reach the writer is a prefix of the truth. Frames are opened
		// whole before any of their bytes are written, so a failure part-way through
		// leaves a short plaintext and never a wrong one.
		if !bytes.HasPrefix(plain, got.Bytes()) {
			rt.Fatalf("a truncated layer produced %d bytes that are not a prefix of the plaintext", got.Len())
		}
	})
}

// TestSealedLayerBitFlipIsDetected — §25.2's second half. Any bit, at any offset:
// ciphertext, tag, it makes no difference, because GCM authenticates the whole frame.
func TestSealedLayerBitFlipIsDetected(t *testing.T) {
	d := layerKey(t)
	plain := make([]byte, 2*frame+5)
	if _, err := rand.Read(plain); err != nil {
		t.Fatalf("drawing plaintext: %v", err)
	}
	var out bytes.Buffer
	if err := d.SealLayer(layer, frame, bytes.NewReader(plain), &out); err != nil {
		t.Fatalf("sealing: %v", err)
	}
	sealed := out.Bytes()

	rapid.Check(t, func(rt *rapid.T) {
		at := rapid.IntRange(0, len(sealed)-1).Draw(rt, "byte")
		bit := rapid.IntRange(0, 7).Draw(rt, "bit")
		corrupt := bytes.Clone(sealed)
		corrupt[at] ^= 1 << bit
		var got bytes.Buffer
		if err := d.OpenLayer(layer, frame, bytes.NewReader(corrupt), &got); err == nil {
			rt.Fatalf("a layer with bit %d of byte %d flipped opened", bit, at)
		}
	})
}
