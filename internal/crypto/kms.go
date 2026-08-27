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
// volume it belongs to. Nothing of it is stored — AAD is an input to GCM — so binding
// the volume costs no byte in `descriptor.json` and changes no on-S3 format.
//
// The volume half was added on 2026-08-27, and its absence was a hole with a
// three-line exploit. `volumes/<id>/descriptor.json` carries `dek_wrapped` and is only
// digest-framed; internal/framed says out loud that a digest is not authentication,
// "anything that can write the object can write a matching digest". Every volume this
// deployment provisions gets KeyID 1, so with the version as the whole AAD *every wrap
// in the fleet was made under the same AAD*: a single PUT of one volume's wrapped DEK
// into another volume's descriptor re-keyed a live volume through -rebuild-metadata,
// and every layer it had already published stopped opening. No structural check on the
// object could ever see it — descriptor.Read already refuses an object naming another
// volume, and the swap keeps `volume_id` honest. Only the KEK can see it, because the
// KEK is the one thing a bucket-writing adversary does not hold.
//
// Rejected: binding the *lineage root* instead, so that a clone could keep its parent's
// ciphertext byte for byte. The reader has to learn the root from somewhere, and every
// candidate — `descriptor.ParentVolumeID`, or an explicit `dek_owner_volume_id` — is
// written by the same adversary, who would simply point the victim at their own root.
// Making it trustworthy needs a new authority for "which lineage is this": a descriptor
// field, a `metadata.Volume` field and a `volumes` column, to defend a property that
// re-wrapping at clone gets with no format change at all. Per-volume needs no
// independent statement: the reader already knows which volume it asked for.
//
// This does not decide *which volume's bytes a key may open* — that is one step later
// and already per-volume: `crypto.Encryption` pairs a DEK with a volume id, the layer
// nonce carries it, and `commit` refuses a Fetch whose manifest names another volume. A
// clone reading its parent's layers builds an Encryption over the parent's id from the
// same key *bytes*; the wrap's AAD does not survive into the layer read, which is why
// re-wrapping a shared DEK under the child's id breaks no lineage.
func wrapAAD(keyID uint32, volumeID [16]byte) []byte {
	b := make([]byte, 4+len(volumeID))
	binary.LittleEndian.PutUint32(b[:4], keyID)
	copy(b[4:], volumeID[:])
	return b
}

// WrapDEK seals the DEK key material under the KEK: nonce || (ciphertext+tag). The
// keyID and the volume are bound as additional authenticated data (wrapAAD); the
// stored bytes are unchanged in shape, because AAD is never stored.
//
// volumeID is a required argument rather than a field on DEK so that no caller can
// forget it: every call site is a compile error until it says which volume this wrap
// is for.
func (k *DevKMS) WrapDEK(r io.Reader, dek DEK, volumeID [16]byte) ([]byte, error) {
	// KeyID 0 is the reserved marker for "this payload is cleartext" (ErrUnversionedKey),
	// so a DEK at version 0 is not a key version at all. Wrapping one succeeded and
	// produced sixty perfectly storable bytes that nothing downstream will ever accept:
	// NewEncryption refuses to bind it and metadata.CheckDEKKeyID refuses to store it. Key
	// material that is minted successfully and can never be used is worse than a refusal,
	// because the refusal arrives at the volume that needs it rather than at the operator
	// who created it.
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
// A wrapped DEK that belongs to another volume fails here, closed, rather than
// succeeding with the wrong key — which is the difference between an operator seeing a
// forged descriptor and a guest silently writing under a key somebody else chose. The
// error names the volume that was asked for, because the caller that gets it is holding
// a descriptor and needs to know which one disagreed.
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
