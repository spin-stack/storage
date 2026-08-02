package agent

import (
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// ErrBadKEK is a key-encryption-key file that is not a KEK.
var ErrBadKEK = errors.New("agent: the KEK file does not hold a 32-byte key")

// LoadKEK reads this host's key-encryption key from name on d (§15.1: "mínimo
// aceptable on-prem: KEK por host en archivo con permisos estrictos").
//
// It goes through disk.Disk rather than os.ReadFile because INV-01 admits no
// exceptions for start-up code, and because a test must be able to hand it a bad file
// without writing one.
//
// Exactly 32 bytes, and nothing else. A shorter file is a truncated key and a longer
// one is usually a hex dump or a stray newline — either would produce a KEK that is
// *valid* (any 32 bytes are a valid AES-256 key) and *wrong*, so every volume on the
// host would fail to unwrap with an authentication error pointing at the DEK rather
// than at the file. Refusing the size names the actual problem.
func LoadKEK(d disk.Disk, name string) ([crypto.DEKSize]byte, error) {
	var kek [crypto.DEKSize]byte

	f, err := d.Open(name)
	if err != nil {
		return kek, fmt.Errorf("agent: opening the KEK file %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()

	size, err := f.Size()
	if err != nil {
		return kek, fmt.Errorf("agent: sizing the KEK file %s: %w", name, err)
	}
	if size != crypto.DEKSize {
		return kek, fmt.Errorf("%w: %s is %d bytes, want %d (generate one with `openssl rand -out %s %d`)",
			ErrBadKEK, name, size, crypto.DEKSize, name, crypto.DEKSize)
	}
	if _, err := f.ReadAt(kek[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return [crypto.DEKSize]byte{}, fmt.Errorf("agent: reading the KEK file %s: %w", name, err)
	}
	return kek, nil
}
