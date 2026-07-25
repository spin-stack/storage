package crypto_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/crypto"
)

// fixedReader yields deterministic bytes for DEK/KEK generation in tests.
type fixedReader struct{ b byte }

func (r *fixedReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

func testDEK(t *testing.T, keyID uint32) crypto.DEK {
	t.Helper()
	d, err := crypto.GenerateDEK(&fixedReader{b: 1}, keyID)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSealOpenRoundTrip(t *testing.T) {
	d := testDEK(t, 1)
	var vol [16]byte
	plaintext := []byte("guest page data 8 KiB...")

	ct, tag, err := d.Seal(vol, 3, 42, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if len(ct) != len(plaintext) {
		t.Fatalf("ciphertext length %d != plaintext %d", len(ct), len(plaintext))
	}
	if bytes.Contains(ct, plaintext) {
		t.Fatal("ciphertext must not contain the plaintext")
	}

	got, err := d.Open(vol, 3, 42, ct, tag)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round-trip mismatch: %q", got)
	}
}

func TestTamperFailsClosed(t *testing.T) {
	d := testDEK(t, 1)
	var vol [16]byte
	ct, tag, _ := d.Seal(vol, 1, 1, []byte("secret"))

	flippedCT := append([]byte(nil), ct...)
	flippedCT[0] ^= 0x01
	flippedTag := tag
	flippedTag[0] ^= 0x01

	tests := []struct {
		name       string
		ct         []byte
		tag        [crypto.TagSize]byte
		epoch, seq uint64
	}{
		{"ciphertext bit flip", flippedCT, tag, 1, 1},
		{"tag bit flip", ct, flippedTag, 1, 1},
		{"wrong seq (aad/nonce mismatch)", ct, tag, 1, 2},
		{"wrong epoch (aad/nonce mismatch)", ct, tag, 2, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := d.Open(vol, tc.epoch, tc.seq, tc.ct, tc.tag); !errors.Is(err, crypto.ErrOpen) {
				t.Fatalf("want ErrOpen, got %v", err)
			}
		})
	}
}

// TestNonceUniqueness is the §15.2 safety property: distinct (volume, epoch, seq)
// produce distinct ciphertext for the same plaintext (i.e., distinct nonces), so a
// monotonic sequence never reuses a nonce within an epoch.
func TestNonceUniqueness(t *testing.T) {
	d := testDEK(t, 1)
	pt := []byte("same plaintext everywhere")
	rapid.Check(t, func(t *rapid.T) {
		var v1, v2 [16]byte
		v1[0] = byte(rapid.IntRange(0, 3).Draw(t, "v1"))
		v2[0] = byte(rapid.IntRange(0, 3).Draw(t, "v2"))
		e1 := uint64(rapid.IntRange(0, 5).Draw(t, "e1"))
		e2 := uint64(rapid.IntRange(0, 5).Draw(t, "e2"))
		s1 := uint64(rapid.IntRange(0, 100).Draw(t, "s1"))
		s2 := uint64(rapid.IntRange(0, 100).Draw(t, "s2"))

		ct1, _, _ := d.Seal(v1, e1, s1, pt)
		ct2, _, _ := d.Seal(v2, e2, s2, pt)

		sameID := v1 == v2 && e1 == e2 && s1 == s2
		if sameID {
			if !bytes.Equal(ct1, ct2) {
				t.Fatal("same (vol,epoch,seq) must be deterministic")
			}
		} else if bytes.Equal(ct1, ct2) {
			t.Fatalf("distinct identity produced identical ciphertext (nonce reuse!): v=%v/%v e=%d/%d s=%d/%d",
				v1[0], v2[0], e1, e2, s1, s2)
		}
	})
}

func TestWrapUnwrapRoundTrip(t *testing.T) {
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(0xA0 + i)
	}
	kms := crypto.NewDevKMS(kek, "kek-1")
	dek := testDEK(t, 7)

	wrapped, err := kms.WrapDEK(&fixedReader{b: 100}, dek)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wrapped, dek.Key[:]) {
		t.Fatal("wrapped DEK must not contain the raw key")
	}

	got, err := kms.UnwrapDEK(wrapped, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Key != dek.Key || got.KeyID != 7 {
		t.Fatal("unwrapped DEK differs from the original")
	}
}

func TestUnwrapWrongKEKFails(t *testing.T) {
	var kek1, kek2 [crypto.DEKSize]byte
	kek1[0], kek2[0] = 1, 2
	a := crypto.NewDevKMS(kek1, "kek-1")
	b := crypto.NewDevKMS(kek2, "kek-2")

	wrapped, _ := a.WrapDEK(&fixedReader{b: 5}, testDEK(t, 1))
	if _, err := b.UnwrapDEK(wrapped, 1); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("unwrap with wrong KEK: want ErrUnwrap, got %v", err)
	}
}

func TestUnwrapWrongKeyIDFails(t *testing.T) {
	var kek [crypto.DEKSize]byte
	kms := crypto.NewDevKMS(kek, "kek-1")
	wrapped, _ := kms.WrapDEK(&fixedReader{b: 9}, testDEK(t, 3))
	// keyID is bound as AAD; unwrapping under a different id must fail.
	if _, err := kms.UnwrapDEK(wrapped, 4); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("unwrap wrong keyID: want ErrUnwrap, got %v", err)
	}
}

func TestKEKID(t *testing.T) {
	var kek [crypto.DEKSize]byte
	if got := crypto.NewDevKMS(kek, "kek-42").KEKID(); got != "kek-42" {
		t.Fatalf("KEKID = %q, want kek-42", got)
	}
}

// shortReader yields n bytes then EOF, to exercise randomness-failure paths.
type shortReader struct{ n int }

func (r *shortReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	k := min(r.n, len(p))
	r.n -= k
	return k, nil
}

func TestGenerateDEKShortReaderFails(t *testing.T) {
	if _, err := crypto.GenerateDEK(&shortReader{n: 4}, 1); err == nil {
		t.Fatal("GenerateDEK with a short reader must fail")
	}
}

func TestWrapShortNonceReaderFails(t *testing.T) {
	var kek [crypto.DEKSize]byte
	kms := crypto.NewDevKMS(kek, "kek-1")
	if _, err := kms.WrapDEK(&shortReader{n: 0}, testDEK(t, 1)); err == nil {
		t.Fatal("WrapDEK with no wrap randomness must fail")
	}
}

func TestUnwrapTooShortFails(t *testing.T) {
	var kek [crypto.DEKSize]byte
	kms := crypto.NewDevKMS(kek, "kek-1")
	if _, err := kms.UnwrapDEK([]byte{1, 2, 3}, 1); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("unwrap short blob: want ErrUnwrap, got %v", err)
	}
}
