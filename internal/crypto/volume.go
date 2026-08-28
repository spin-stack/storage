package crypto

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// ErrUnversionedKey is returned when a DEK carries KeyID 0. KeyID 0 is not a key
// version: it is the reserved marker for "this payload is cleartext", so a DEK with
// KeyID 0 would seal the payload and then label it plaintext — bytes that are
// ciphertext described as cleartext, discovered only when something tries to read
// them back. Refused at the binding instead.
var ErrUnversionedKey = errors.New("crypto: KeyID 0 is reserved for plaintext, it is not a DEK version")

// Encryption binds a volume's DEK to that volume's identity (§15). It is the unit the
// sealing paths take: a DEK on its own does not say which volume's bytes it may open,
// and every AEAD call in this package takes the volume id as additional authenticated
// data precisely so one volume's key cannot open another's object.
//
// The local write path is QEMU's qcow2 now, but the layers this system commits to the
// object store are still sealed with the volume's DEK on the way out, which is what keeps
// KMS wrapping, descriptor.DEKWrapped and crypto-shred working unchanged. It lives here
// rather than in a package of its own: a DEK plus the identity every DEK method already
// demands as AAD is this package's subject.
type Encryption struct {
	DEK      DEK
	VolumeID [16]byte
}

// NewEncryption binds a versioned DEK to a volume. It is the checked way to build an
// Encryption: KeyID 0 is refused here, at the binding, rather than at the read that
// discovers the bytes cannot be opened.
func NewEncryption(dek DEK, volumeID [16]byte) (*Encryption, error) {
	if dek.KeyID == 0 {
		return nil, fmt.Errorf("%w: volume %s", ErrUnversionedKey, uuid.UUID(volumeID))
	}
	return &Encryption{DEK: dek, VolumeID: volumeID}, nil
}
