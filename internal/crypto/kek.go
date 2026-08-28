package crypto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// The KEK on disk (§15.1: "mínimo aceptable on-prem: KEK por host en archivo con
// permisos estrictos"). Both binaries read one — the Control Plane to wrap a volume's
// DEK, the Agent to unwrap it — and two readers with different rules meant a hex key file
// the Control Plane accepted and the Agent refused, on one deployment.

// ErrBadKEK is a key-encryption-key file that is not a KEK.
var ErrBadKEK = errors.New("crypto: the KEK file does not hold a 32-byte key")

// maxKEKFile bounds the read. A key file is small; anything large is not one, and
// reading it whole would otherwise be an unbounded allocation driven by a filename.
const maxKEKFile = 1024

// LoadKEK reads a key-encryption key from name on d.
//
// Two encodings, because both are what people actually produce: 32 raw bytes
// (`head -c 32 /dev/urandom > kek`) or 64 hex characters, whose surrounding whitespace is
// trimmed so a trailing newline is not a different key.
//
// Anything else is refused rather than truncated or padded: *any* 32 bytes are a valid
// AES-256 key, so a key built out of whatever fitted would be perfectly usable and
// completely wrong, and every volume would fail to unwrap with an error naming the DEK.
func LoadKEK(d disk.Disk, name string) ([DEKSize]byte, error) {
	var kek [DEKSize]byte

	f, err := d.Open(name)
	if err != nil {
		return kek, fmt.Errorf("crypto: opening the KEK file %s: %w", name, err)
	}
	defer func() { _ = f.Close() }()

	size, err := f.Size()
	if err != nil {
		return kek, fmt.Errorf("crypto: sizing the KEK file %s: %w", name, err)
	}
	if size > maxKEKFile {
		return kek, fmt.Errorf("%w: %s is %d bytes, which is not a key", ErrBadKEK, name, size)
	}
	raw := make([]byte, size)
	if _, err := f.ReadAt(raw, 0); err != nil && !errors.Is(err, io.EOF) {
		return kek, fmt.Errorf("crypto: reading the KEK file %s: %w", name, err)
	}
	// Whitespace is trimmed for the *hex* reading and never for the raw one. Trimming both
	// refused a raw 32-byte key whose first or last byte is 0x0a, 0x20, 0x09, 0x0d, 0x0b or
	// 0x0c — 1 - (250/256)^2, about one key in twenty-two — as malformed when it was
	// exactly right.
	//
	// The trim is there for `openssl rand -hex 32 > kek`, which writes a trailing newline.
	// A hex key is text; raw bytes are not, and every one of them is the key.
	if decoded, derr := hex.DecodeString(string(bytes.TrimSpace(raw))); derr == nil && len(decoded) == DEKSize {
		copy(kek[:], decoded)
		return kek, nil
	}
	if len(raw) != DEKSize {
		return [DEKSize]byte{}, fmt.Errorf("%w: %s holds %d bytes; want %d raw or %d hex characters "+
			"(generate one with `openssl rand -out %s %d`)",
			ErrBadKEK, name, len(raw), DEKSize, DEKSize*2, name, DEKSize)
	}
	copy(kek[:], raw)
	return kek, nil
}

// KEKID names a KEK by a hash of itself. `kek_id` is what a volume row records and what
// the Agent compares its own key against before unwrapping, so it is derived rather than
// configured: an operator-supplied id can be typed identically on two hosts holding
// different keys, and the mismatch would then surface as an AEAD failure with no hint that
// the files differ.
func KEKID(kek [DEKSize]byte) string {
	sum := sha256.Sum256(kek[:])
	return "kek-" + hex.EncodeToString(sum[:8])
}
