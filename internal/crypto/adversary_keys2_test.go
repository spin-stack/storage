package crypto_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
)

func adversaryKMS(t *testing.T, seed byte) *crypto.DevKMS {
	t.Helper()
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = seed ^ byte(i)
	}
	return crypto.NewDevKMS(kek, crypto.KEKID(kek))
}

func advVol(n byte) [16]byte {
	var v [16]byte
	v[0], v[6] = n, 0x70 // a v7 nibble, so it is the shape ids.Parse admits
	return v
}

// A DEK can be wrapped with no key version, and everything downstream refuses it.
//
// KeyID 0 is not a version: crypto.ErrUnversionedKey says it is the reserved marker for
// "this payload is cleartext", NewEncryption refuses to bind a DEK carrying it, and
// metadata.CheckDEKKeyID refuses to store one. WrapDEK is the one place key material is
// minted and it is the only one of the three that accepts it — it seals the key, binds
// the 0 as AAD, and hands back a blob that unwraps perfectly and can never be used.
//
// The failure is not an attacker's, it is the next caller's. Provision hardcodes 1 and
// Clone inherits the parent's version, so no path reaches it today; the paths that are
// coming do. A FLATTEN mints a fresh DEK (Clone's own comment says it must, or the
// delete verb's promise is false), and a rotation mints one per version. A minting path
// that forgets to set the version gets a wrap, a row, a descriptor and a green
// provisioning run, and discovers at the guest's first read that NewEncryption will not
// bind the key it has been storing. Refusing at the wrap costs one comparison and moves
// that discovery to the line that made the mistake.
func TestAdversaryAWrapCanBeMintedWithNoKeyVersion(t *testing.T) {
	kms := adversaryKMS(t, 1)
	var dek crypto.DEK
	for i := range dek.Key {
		dek.Key[i] = byte(i)
	}
	dek.KeyID = 0 // the reserved "this is cleartext" marker

	wrapped, err := kms.WrapDEK(&adversaryRamp{}, dek, advVol(1))
	if err == nil {
		back, uerr := kms.UnwrapDEK(wrapped, 0, advVol(1))
		t.Fatalf("WrapDEK sealed a DEK at version 0 — the marker that means the payload is "+
			"plaintext — into %d storable bytes, and it unwraps (%v). NewEncryption refuses to "+
			"bind that key and metadata.CheckDEKKeyID refuses to store it, so this is key "+
			"material that is minted successfully and can never be used",
			len(wrapped), func() error { _ = back; return uerr }())
	}
}

// The controls. Each is a way the binding could have been weaker than it reads, and each
// must fail closed; a red one here would be a hole in the fix itself rather than beside
// it.
func TestAdversaryTheWrapOpensOnlyForItsExactIdentity(t *testing.T) {
	kms := adversaryKMS(t, 1)
	dek, err := crypto.GenerateDEK(&adversaryRamp{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := kms.WrapDEK(&adversaryRamp{}, dek, advVol(1))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		unwrap  func() (crypto.DEK, error)
		wantErr bool
	}{
		{"its own volume and version", func() (crypto.DEK, error) {
			return kms.UnwrapDEK(wrapped, 3, advVol(1))
		}, false},
		{"another volume", func() (crypto.DEK, error) {
			return kms.UnwrapDEK(wrapped, 3, advVol(2))
		}, true},
		{"another version", func() (crypto.DEK, error) {
			return kms.UnwrapDEK(wrapped, 4, advVol(1))
		}, true},
		{"another KEK", func() (crypto.DEK, error) {
			return adversaryKMS(t, 9).UnwrapDEK(wrapped, 3, advVol(1))
		}, true},
		// The volume id is 16 bytes of AAD, so the near miss is the one worth naming: a
		// single flipped bit in the id, which is what a truncated or mistyped uuid looks
		// like from here.
		{"one bit of the volume id", func() (crypto.DEK, error) {
			v := advVol(1)
			v[15] ^= 1
			return kms.UnwrapDEK(wrapped, 3, v)
		}, true},
		// And the wrap itself, byte for byte: nothing about the ciphertext may be
		// rearranged into a different volume's key.
		{"the nonce swapped for another wrap's", func() (crypto.DEK, error) {
			// A different byte source, so the other wrap gets a different nonce. Two
			// wraps drawn from two fresh ramps would share one, which is a property of
			// the fixture and not of the KMS.
			other, werr := kms.WrapDEK(&adversaryRamp{n: 200}, dek, advVol(2))
			if werr != nil {
				t.Fatal(werr)
			}
			mixed := append(append([]byte{}, other[:crypto.NonceSize]...), wrapped[crypto.NonceSize:]...)
			return kms.UnwrapDEK(mixed, 3, advVol(1))
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.unwrap()
			switch {
			case tt.wantErr && err == nil:
				t.Fatalf("a wrap made for volume %x at version 3 opened anyway, into a usable key", advVol(1))
			case !tt.wantErr && err != nil:
				t.Fatalf("the wrap did not open for its own volume: %v", err)
			case !tt.wantErr && !bytes.Equal(got.Key[:], dek.Key[:]):
				t.Fatal("the wrap opened into the wrong key bytes")
			}
		})
	}
}

type adversaryRamp struct{ n byte }

func (r *adversaryRamp) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}
