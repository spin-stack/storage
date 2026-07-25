// Package crypto implements per-volume encryption at rest (§15, §5.10). Every VM
// data payload that leaves the host is AES-256-GCM sealed with the volume's DEK
// before any WAL append or S3 PUT. The DEK is wrapped by a KEK held in a KMS.
//
// The payload nonce is derived deterministically from (volume_id, epoch, sequence)
// and never stored (§15.2); because sequence is strictly monotonic per
// (volume, epoch), no nonce is reused within an epoch. This also keeps the data
// path deterministic under DST — only DEK generation and DEK wrapping consume
// randomness, and both take an injected io.Reader.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// DEKSize is the AES-256 key length.
	DEKSize = 32
	// NonceSize is the GCM nonce length.
	NonceSize = 12
	// TagSize is the GCM tag length (stored in RecordHeader.AuthTag).
	TagSize = 16
)

// ErrOpen is returned when authentication fails on Open (tamper or wrong key).
var ErrOpen = errors.New("crypto: authentication failed")

// DEK is a per-volume data-encryption key with a version (KeyID) for rotation
// without re-encrypting history (§15.1).
type DEK struct {
	Key   [DEKSize]byte
	KeyID uint32
}

// GenerateDEK reads a fresh DEK from r (crypto/rand.Reader in production; a
// deterministic reader under DST).
func GenerateDEK(r io.Reader, keyID uint32) (DEK, error) {
	var d DEK
	d.KeyID = keyID
	if _, err := io.ReadFull(r, d.Key[:]); err != nil {
		return DEK{}, fmt.Errorf("crypto: generate DEK: %w", err)
	}
	return d, nil
}

// deriveNonce returns the deterministic 12-byte nonce for (volumeID, epoch, seq).
func deriveNonce(volumeID [16]byte, epoch, seq uint64) [NonceSize]byte {
	var buf [16 + 8 + 8 + 6]byte
	copy(buf[0:16], volumeID[:])
	binary.LittleEndian.PutUint64(buf[16:24], epoch)
	binary.LittleEndian.PutUint64(buf[24:32], seq)
	copy(buf[32:], "nonce1")
	sum := sha256.Sum256(buf[:])
	var nonce [NonceSize]byte
	copy(nonce[:], sum[:NonceSize])
	return nonce
}

// aad binds the ciphertext to its identity so a record cannot be replayed under a
// different (volume, epoch, seq).
func aad(volumeID [16]byte, epoch, seq uint64) []byte {
	b := make([]byte, 16+8+8)
	copy(b[0:16], volumeID[:])
	binary.LittleEndian.PutUint64(b[16:24], epoch)
	binary.LittleEndian.PutUint64(b[24:32], seq)
	return b
}

func (d DEK) gcm() (cipher.AEAD, error) {
	block, err := aes.NewCipher(d.Key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Seal encrypts plaintext for (volumeID, epoch, seq), returning the ciphertext
// (same length as plaintext) and the GCM tag separately.
func (d DEK) Seal(volumeID [16]byte, epoch, seq uint64, plaintext []byte) (ciphertext []byte, tag [TagSize]byte, err error) {
	g, err := d.gcm()
	if err != nil {
		return nil, tag, err
	}
	nonce := deriveNonce(volumeID, epoch, seq)
	sealed := g.Seal(nil, nonce[:], plaintext, aad(volumeID, epoch, seq))
	// GCM appends the tag; split it out to store in the header.
	ct := sealed[:len(sealed)-TagSize]
	copy(tag[:], sealed[len(sealed)-TagSize:])
	return ct, tag, nil
}

// Open decrypts ciphertext+tag for (volumeID, epoch, seq). It fails closed on any
// tamper (ErrOpen); it never returns partially-authenticated plaintext.
func (d DEK) Open(volumeID [16]byte, epoch, seq uint64, ciphertext []byte, tag [TagSize]byte) ([]byte, error) {
	g, err := d.gcm()
	if err != nil {
		return nil, err
	}
	nonce := deriveNonce(volumeID, epoch, seq)
	sealed := make([]byte, 0, len(ciphertext)+TagSize)
	sealed = append(sealed, ciphertext...)
	sealed = append(sealed, tag[:]...)
	pt, err := g.Open(nil, nonce[:], sealed, aad(volumeID, epoch, seq))
	if err != nil {
		return nil, ErrOpen
	}
	return pt, nil
}
