package crypto_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
)

func testKEK(t *testing.T) *crypto.DevKMS {
	t.Helper()
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(i)
	}
	return crypto.NewDevKMS(kek, "kek-test")
}

// volID returns a distinct 16-byte volume identity.
func volID(b byte) [16]byte {
	var v [16]byte
	for i := range v {
		v[i] = b + byte(i)
	}
	return v
}

// A wrapped DEK opens for the volume it was wrapped for, and for nothing else — the
// descriptor-swap defect at the level where it can be fixed. Until the volume was in the
// AAD, every wrap in a fleet was made under the same four bytes (KeyID 1), so a blob moved
// between two digest-framed descriptors unwrapped perfectly under the victim's id.
func TestAWrappedDEKOpensOnlyForItsOwnVolume(t *testing.T) {
	kms := testKEK(t)
	mine, yours := volID(1), volID(200)
	dek := testDEK(t, 1)

	wrapped, err := kms.WrapDEK(&fixedReader{b: 9}, dek, mine)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	got, err := kms.UnwrapDEK(wrapped, dek.KeyID, mine)
	if err != nil {
		t.Fatalf("a DEK does not unwrap for the volume it was wrapped for: %v", err)
	}
	if got.Key != dek.Key || got.KeyID != dek.KeyID {
		t.Fatal("the round trip did not return the key that went in")
	}

	// The swap: the same bytes, presented as another volume's.
	if _, err := kms.UnwrapDEK(wrapped, dek.KeyID, yours); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("volume %x's wrapped DEK opened under volume %x: a single PUT of one "+
			"unauthenticated descriptor re-keys a live volume, and its published layers stop "+
			"opening for good. Got err=%v", mine, yours, err)
	}
	// And the version half of the AAD still binds.
	if _, err := kms.UnwrapDEK(wrapped, dek.KeyID+1, mine); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("a DEK unwrapped under the wrong key version: %v", err)
	}
}

// The stored bytes did not change shape when the volume joined the AAD: AAD is an input
// to GCM and is never stored. `descriptor.json` gains no field and `volumes.dek_wrapped`
// gains no byte, which is what makes this not an on-S3 format change.
func TestAWrapIsStillNonceCiphertextTag(t *testing.T) {
	kms := testKEK(t)
	wrapped, err := kms.WrapDEK(&fixedReader{b: 3}, testDEK(t, 1), volID(1))
	if err != nil {
		t.Fatal(err)
	}
	if want := crypto.NonceSize + crypto.DEKSize + crypto.TagSize; len(wrapped) != want {
		t.Fatalf("a wrapped DEK is %d bytes, want %d (nonce||ciphertext||tag)", len(wrapped), want)
	}
}

// Nothing short of the whole object opens, and no single bit may be flipped in it.
//
// The wrapped DEK is the one piece of key material that travels in a cleartext object,
// so "refused, never silently wrong" has to hold at every truncation and every bit.
func TestATruncatedOrFlippedWrapIsRefused(t *testing.T) {
	kms := testKEK(t)
	vol, dek := volID(1), testDEK(t, 1)
	wrapped, err := kms.WrapDEK(&fixedReader{b: 5}, dek, vol)
	if err != nil {
		t.Fatal(err)
	}

	for n := range len(wrapped) {
		if _, err := kms.UnwrapDEK(wrapped[:n], dek.KeyID, vol); !errors.Is(err, crypto.ErrUnwrap) {
			t.Fatalf("a wrap truncated to %d bytes was accepted: %v", n, err)
		}
	}
	for i := range wrapped {
		for bit := range 8 {
			flipped := bytes.Clone(wrapped)
			flipped[i] ^= 1 << bit
			if _, err := kms.UnwrapDEK(flipped, dek.KeyID, vol); !errors.Is(err, crypto.ErrUnwrap) {
				t.Fatalf("byte %d bit %d flipped and the wrap still opened: %v", i, bit, err)
			}
		}
	}
}

// A lineage shares one DEK's *bytes* while every volume in it carries its own wrap.
// That is the property that lets the volume be bound at all: re-wrapping at clone is a
// change of ciphertext, not of key, so the child still opens the parent's layers.
func TestTwoVolumesCanShareOneKeyUnderTwoWraps(t *testing.T) {
	kms := testKEK(t)
	parent, child := volID(1), volID(100)
	dek := testDEK(t, 1)

	pw, err := kms.WrapDEK(&fixedReader{b: 9}, dek, parent)
	if err != nil {
		t.Fatal(err)
	}
	cw, err := kms.WrapDEK(&fixedReader{b: 77}, dek, child)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(pw, cw) {
		t.Fatal("the two wraps are byte-identical, so one of them is not bound to its volume")
	}
	fromChild, err := kms.UnwrapDEK(cw, dek.KeyID, child)
	if err != nil {
		t.Fatalf("the clone cannot open its own wrap: %v", err)
	}
	if fromChild.Key != dek.Key {
		t.Fatal("the clone's wrap does not hold the lineage key, so it cannot read its parent's layers")
	}
	// The child's wrap is still not a passkey to the parent's identity.
	if _, err := kms.UnwrapDEK(cw, dek.KeyID, parent); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("the clone's wrap opened as the parent's: %v", err)
	}
}

// A wrap made under one KEK does not open under another, which is the check that keeps
// "you passed the wrong -kek-file" distinguishable from "somebody forged this object" —
// the two are reported separately by controlplane.checkKey.
func TestAWrapDoesNotOpenUnderAnotherKEK(t *testing.T) {
	var other [crypto.DEKSize]byte
	for i := range other {
		other[i] = byte(255 - i)
	}
	wrapped, err := testKEK(t).WrapDEK(&fixedReader{b: 2}, testDEK(t, 1), volID(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crypto.NewDevKMS(other, "kek-other").UnwrapDEK(wrapped, 1, volID(1)); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("a wrap opened under a KEK that did not make it: %v", err)
	}
}
