package crypto

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

// ErrUnwrap is returned when a wrapped DEK cannot be authenticated (wrong KEK or
// tampering).
var ErrUnwrap = errors.New("crypto: DEK unwrap failed")

// KMS wraps and unwraps DEKs with a KEK it holds. In production this is AWS
// KMS/Vault; on-prem the minimum acceptable is a KEK per host in a file (§15.1).
type KMS interface {
	// WrapDEK returns the DEK sealed under the KEK, bound to volumeID. r supplies
	// wrap randomness.
	WrapDEK(r io.Reader, dek DEK, volumeID [16]byte) ([]byte, error)
	// UnwrapDEK recovers a DEK from its wrapped form, authenticating it against the
	// version and the volume it was wrapped for.
	UnwrapDEK(wrapped []byte, keyID uint32, volumeID [16]byte) (DEK, error)
	// KEKID identifies the KEK (stored as kek_id, §8).
	KEKID() string
}

// DevKMS is the development KMS: the KEK lives in memory (or a file with strict
// permissions). It is only for the `local` dev mode (§6.2); production uses a real
// KMS behind the same interface.
type DevKMS struct {
	kek   [DEKSize]byte
	kekID string
}

// NewDevKMS returns a dev KMS holding kek.
func NewDevKMS(kek [DEKSize]byte, kekID string) *DevKMS {
	return &DevKMS{kek: kek, kekID: kekID}
}

// KEKID identifies the KEK.
func (k *DevKMS) KEKID() string { return k.kekID }

func (k *DevKMS) gcm() (cipher.AEAD, error) {
	d := DEK{Key: k.kek}
	return d.gcm()
}

// wrapAAD is what a wrapped DEK proves about itself: the key version it is, and the
// volume it belongs to. AAD is an input to GCM and is never stored, so binding the volume
// costs no byte in `descriptor.json` and changes no on-S3 format.
//
// The volume half closes a three-line exploit. `descriptor.json` carries `dek_wrapped`
// and is only digest-framed, and a digest is not authentication; every volume here gets
// KeyID 1, so with the version as the whole AAD every wrap in the fleet was made under
// one AAD, and a single PUT of one volume's wrapped DEK into another's descriptor re-keyed
// a live volume through -rebuild-metadata. No structural check on the object can see it —
// the swap keeps `volume_id` honest — only the KEK can, being the one thing a
// bucket-writing adversary does not hold.
//
// Rejected: binding the *lineage root* instead, so a clone could keep its parent's
// ciphertext byte for byte. The reader learns the root from a field the same adversary
// writes, and making it trustworthy needs a new authority for "which lineage is this";
// re-wrapping at clone gets the property with no format change. This does not decide
// which volume's bytes a key may open — crypto.Encryption and the layer nonce do, one step
// later, which is why re-wrapping a shared DEK under the child's id breaks no lineage.
func wrapAAD(keyID uint32, volumeID [16]byte) []byte {
	b := make([]byte, 4+len(volumeID))
	binary.LittleEndian.PutUint32(b[:4], keyID)
	copy(b[4:], volumeID[:])
	return b
}

// WrapDEK seals the DEK key material under the KEK: nonce || (ciphertext+tag). The keyID
// and the volume are bound as additional authenticated data (wrapAAD).
//
// volumeID is a required argument rather than a field on DEK so that no caller can
// forget it: every call site is a compile error until it says which volume this wrap
// is for.
func (k *DevKMS) WrapDEK(r io.Reader, dek DEK, volumeID [16]byte) ([]byte, error) {
	// KeyID 0 is the reserved marker for "this payload is cleartext" (ErrUnversionedKey),
	// so a DEK at version 0 is not a key version. Wrapping one produces storable bytes that
	// NewEncryption will not bind and metadata.CheckDEKKeyID will not store — refused here,
	// at the line that mints the material rather than at the volume that needs it.
	if dek.KeyID == 0 {
		return nil, ErrUnversionedKey
	}
	g, err := k.gcm()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(r, nonce); err != nil {
		return nil, fmt.Errorf("crypto: wrap nonce: %w", err)
	}
	sealed := g.Seal(nil, nonce, dek.Key[:], wrapAAD(dek.KeyID, volumeID))
	return append(nonce, sealed...), nil
}

// UnwrapDEK recovers the DEK from wrapped, authenticating against keyID and volumeID.
//
// A wrapped DEK belonging to another volume fails here, closed, rather than succeeding
// with the wrong key. The error names the volume that was asked for, because the caller
// holding a descriptor needs to know which one disagreed.
func (k *DevKMS) UnwrapDEK(wrapped []byte, keyID uint32, volumeID [16]byte) (DEK, error) {
	if len(wrapped) < NonceSize+TagSize {
		return DEK{}, fmt.Errorf("%w for volume %s: %d wrapped bytes is shorter than a nonce and a tag",
			ErrUnwrap, uuid.UUID(volumeID), len(wrapped))
	}
	g, err := k.gcm()
	if err != nil {
		return DEK{}, err
	}
	nonce := wrapped[:NonceSize]
	key, err := g.Open(nil, nonce, wrapped[NonceSize:], wrapAAD(keyID, volumeID))
	if err != nil || len(key) != DEKSize {
		return DEK{}, fmt.Errorf("%w for volume %s at key version %d: the wrapped bytes were not sealed under this KEK for this volume",
			ErrUnwrap, uuid.UUID(volumeID), keyID)
	}
	var d DEK
	copy(d.Key[:], key)
	d.KeyID = keyID
	return d, nil
}
