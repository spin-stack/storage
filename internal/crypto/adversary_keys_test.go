package crypto_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
)

func adversaryEncryption(t *testing.T) *crypto.Encryption {
	t.Helper()
	var key [crypto.DEKSize]byte
	for i := range key {
		key[i] = byte(i * 7)
	}
	var vol [16]byte
	copy(vol[:], "volume-aaaaaaaa")
	enc, err := crypto.NewEncryption(crypto.DEK{Key: key, KeyID: 1}, vol)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func adversaryLayerID() [16]byte {
	var id [16]byte
	copy(id[:], "layer-bbbbbbbbb")
	return id
}

func advSeal(t *testing.T, enc *crypto.Encryption, id [16]byte, frameBytes int, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := enc.SealLayer(id, frameBytes, bytes.NewReader(plain), &out); err != nil {
		t.Fatalf("SealLayer(%d): %v", frameBytes, err)
	}
	return out.Bytes()
}

// Two sealings of ONE layer at two frame sizes must not be two ciphertexts under one
// (key, nonce). `frame_bytes` travels in the manifest so it can be changed, and a restart
// between sealing and publishing republishes a layer that was already sealed once — so
// the two sizes meet on one layer id. Demonstrated the way the break is used: an observer
// with the two objects and no key recovers the XOR of the two plaintexts. The binding is
// now in crypto.layerNonce.
func TestAdversaryFrameSizeIsNotBoundSoOneNonceSealsTwoPlaintexts(t *testing.T) {
	enc, id := adversaryEncryption(t), adversaryLayerID()
	const small = 32 << 10

	first := bytes.Repeat([]byte("A"), 96<<10)
	second := append(bytes.Repeat([]byte("S"), 48<<10), bytes.Repeat([]byte("E"), 48<<10)...)

	a := advSeal(t, enc, id, crypto.LayerFrameBytes, first) // 64 KiB frames
	b := advSeal(t, enc, id, small, second)                 // 32 KiB frames

	recovered := make([]byte, small)
	for i := range recovered {
		recovered[i] = a[i] ^ b[i]
	}
	want := make([]byte, small)
	for i := range want {
		want[i] = first[i] ^ second[i]
	}
	if bytes.Equal(recovered, want) {
		t.Fatalf("layer %x of this volume was sealed twice under one GCM nonce: "+
			"xor-ing the first %d bytes of the two objects yields plaintext1 xor plaintext2 "+
			"with no key involved. The same two ciphertexts also give up the GHASH subkey, "+
			"which forges tags for every other frame under this DEK", id, small)
	}
}

// The control: two frames at the *same* size are the deterministic re-seal the retry
// story rests on, and two different plaintexts at that size must not leak their xor.
func TestAdversaryControlSameFrameSizeDoesNotLeak(t *testing.T) {
	enc, id := adversaryEncryption(t), adversaryLayerID()
	first := bytes.Repeat([]byte("A"), 96<<10)

	a := advSeal(t, enc, id, crypto.LayerFrameBytes, first)
	again := advSeal(t, enc, id, crypto.LayerFrameBytes, first)
	if !bytes.Equal(a, again) {
		t.Fatal("sealing the same layer twice is not deterministic; the retry story needs it to be")
	}
	var out bytes.Buffer
	if err := enc.OpenLayer(id, crypto.LayerFrameBytes, bytes.NewReader(a), &out); err != nil {
		t.Fatalf("OpenLayer: %v", err)
	}
	if !bytes.Equal(out.Bytes(), first) {
		t.Fatal("round trip lost bytes")
	}
}

// A single-frame layer has no split for a wrong frame size to fail on, so the reader's
// half of the frame-size binding is absent exactly where it would be needed: it has to
// come from the nonce, not from the framing.
func TestAdversarySingleFrameLayerOpensUnderAnyFrameSize(t *testing.T) {
	enc, id := adversaryEncryption(t), adversaryLayerID()
	plain := []byte("one frame's worth")

	sealed := advSeal(t, enc, id, crypto.LayerFrameBytes, plain)

	var out bytes.Buffer
	err := enc.OpenLayer(id, 4*crypto.LayerFrameBytes, bytes.NewReader(sealed), &out)
	if err == nil && bytes.Equal(out.Bytes(), plain) {
		t.Fatalf("a layer sealed at %d-byte frames opened cleanly at %d-byte frames: "+
			"the frame size the manifest records is not authenticated by anything",
			crypto.LayerFrameBytes, 4*crypto.LayerFrameBytes)
	}
}
