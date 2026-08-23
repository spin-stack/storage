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
// permisos estrictos"). Both binaries read one — the Control Plane to *wrap* a
// volume's DEK, the Agent to unwrap it — so both must read it the same way and must
// agree on what to call it. They did not: two copies of this logic existed with
// different rules, and a hex-encoded key file would have been accepted by the Control
// Plane and refused by the Agent, on the same deployment, with nothing to say why.

// ErrBadKEK is a key-encryption-key file that is not a KEK.
var ErrBadKEK = errors.New("crypto: the KEK file does not hold a 32-byte key")

// maxKEKFile bounds the read. A key file is small; anything large is not one, and
// reading it whole would otherwise be an unbounded allocation driven by a filename.
const maxKEKFile = 1024

// LoadKEK reads a key-encryption key from name on d.
//
// It goes through disk.Disk rather than os.ReadFile because INV-01 admits no
// exceptions for start-up code, and because a test must be able to hand it a bad file
// without writing one.
//
// Two encodings, because both are what people actually produce: 32 raw bytes
// (`head -c 32 /dev/urandom > kek`) or 64 hex characters (which survives a copy-paste
// through a terminal). Surrounding whitespace is trimmed, so a trailing newline — what
// every editor and `echo` adds — is not a different key.
//
// Anything else is refused rather than truncated or padded, and that is the point:
// *any* 32 bytes are a valid AES-256 key, so a key silently built out of whatever fitted
// would be perfectly usable and completely wrong. Every volume on the host would then
// fail to unwrap with an authentication error pointing at the DEK rather than at this
// file.
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
	// Whitespace is trimmed for the *hex* reading and never for the raw one.
	//
	// They were both trimmed until a soak found the consequence, in its first round: a
	// raw 32-byte key whose first or last byte happens to be 0x0a, 0x20, 0x09, 0x0d,
	// 0x0b or 0x0c comes back one byte short and is refused, with a message telling an
	// operator their key file is malformed when it is exactly right. Six whitespace
	// bytes at either end of a random key is 1 - (250/256)^2, about one key in
	// twenty-two — which is why it was not found by anyone generating a key once.
	//
	// The trim is there for a real case and keeps it: `openssl rand -hex 32 > kek`
	// writes a trailing newline, and a hex key is text, where trailing whitespace means
	// nothing. Raw bytes are not text and every one of them is the key.
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

// KEKID names a KEK by a hash of itself.
//
// Derived rather than configured, and that matters more than it looks: `kek_id` is
// what a volume row records and what the Agent compares its own key against before
// unwrapping. An operator-supplied id can be typed differently on two hosts holding
// the *same* key — or identically on two holding different ones, which is worse:
// the mismatch would then surface as an AEAD failure with no hint that the files
// differ. A hash cannot drift from the material, and two deployments do not both end
// up calling theirs "default".
func KEKID(kek [DEKSize]byte) string {
	sum := sha256.Sum256(kek[:])
	return "kek-" + hex.EncodeToString(sum[:8])
}
