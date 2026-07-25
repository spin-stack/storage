package crypto

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// ErrUnwrap is returned when a wrapped DEK cannot be authenticated (wrong KEK or
// tampering).
var ErrUnwrap = errors.New("crypto: DEK unwrap failed")

// KMS wraps and unwraps DEKs with a KEK it holds. In production this is AWS
// KMS/Vault; on-prem the minimum acceptable is a KEK per host in a file (§15.1).
type KMS interface {
	// WrapDEK returns the DEK sealed under the KEK. r supplies wrap randomness.
	WrapDEK(r io.Reader, dek DEK) ([]byte, error)
	// UnwrapDEK recovers a DEK from its wrapped form, tagging it with keyID.
	UnwrapDEK(wrapped []byte, keyID uint32) (DEK, error)
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

func keyIDAAD(keyID uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, keyID)
	return b
}

// WrapDEK seals the DEK key material under the KEK: nonce || (ciphertext+tag). The
// keyID is bound as additional authenticated data.
func (k *DevKMS) WrapDEK(r io.Reader, dek DEK) ([]byte, error) {
	g, err := k.gcm()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(r, nonce); err != nil {
		return nil, fmt.Errorf("crypto: wrap nonce: %w", err)
	}
	sealed := g.Seal(nil, nonce, dek.Key[:], keyIDAAD(dek.KeyID))
	return append(nonce, sealed...), nil
}

// UnwrapDEK recovers the DEK from wrapped, authenticating against keyID.
func (k *DevKMS) UnwrapDEK(wrapped []byte, keyID uint32) (DEK, error) {
	if len(wrapped) < NonceSize+TagSize {
		return DEK{}, ErrUnwrap
	}
	g, err := k.gcm()
	if err != nil {
		return DEK{}, err
	}
	nonce := wrapped[:NonceSize]
	key, err := g.Open(nil, nonce, wrapped[NonceSize:], keyIDAAD(keyID))
	if err != nil || len(key) != DEKSize {
		return DEK{}, ErrUnwrap
	}
	var d DEK
	copy(d.Key[:], key)
	d.KeyID = keyID
	return d, nil
}
