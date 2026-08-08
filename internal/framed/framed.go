// Package framed stores a small structural object in the object store so that a single
// flipped bit is detected rather than decoded.
//
// It exists because two objects in this tree have the same problem and one of them had
// already solved it. `volumes/<vol>/descriptor.json` closed DEV-0015 by prefixing a
// digest line; `image/<vol>/manifest.json` and the snapshot manifests beside it were
// still bare JSON, which is DEV-0025. Duplicating fifteen lines of integrity primitive
// into a second package is how two copies of a rule drift, so it lives here once and
// both call it.
//
// # The stored shape, and why the digest is over the bytes as stored
//
// `<64 hex chars>\n<payload>`: a digest line, then the object. The digest covers the
// **bytes as stored**, and that is the whole design rather than a detail. The obvious
// alternative — a `digest` field inside the JSON, recomputed from the decoded struct —
// cannot work, and `internal/descriptor`'s property test proved it in one run: flipping
// one bit of the `v` in `"volume_id"` yields `"Volume_id"`, Go's decoder matches field
// names case-insensitively, the struct comes out identical, and re-marshalling it
// reproduces the original digest exactly. A hash over a re-encoding can only ever see
// what the decoder did not normalise away — and unknown fields, duplicate keys,
// whitespace and numeric spellings are all normalised away too.
//
// # What this is not
//
// Not authentication. The digest is unkeyed, so it catches corruption and not an author:
// anything that can write the object can write a matching digest. What protects a
// *chunk* is different and stronger — its key is the digest of its plaintext and its AAD
// binds the lineage — and what protects a manifest from another writer is the
// compare-and-set on its ETag (INV-10). This is the layer below both: bytes that came
// back changed are refused instead of parsed.
//
// Not a substitute for a chunk's own check either. A manifest names chunks that verify
// themselves, so the fields this protects are precisely the ones with no second
// opinion: the offsets and lengths that place data, and the tombstones that keep an
// ancestor's bytes buried.
package framed

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrCorrupt means the stored bytes disagree with their own digest. It is not repaired
// and not guessed at: an object nobody can state the true contents of is exactly what a
// read path must refuse to act on.
var ErrCorrupt = errors.New("framed: contents do not match the stored digest")

const digestLen = sha256.Size * 2

// Frame returns the bytes to store: the payload's digest, a newline, then the payload.
func Frame(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	out := make([]byte, 0, digestLen+1+len(payload))
	out = append(out, hex.EncodeToString(sum[:])...)
	out = append(out, '\n')
	return append(out, payload...)
}

// Unframe splits the digest line off and verifies it, returning the payload.
//
// An object written before its format carried a digest line has none at all, and it is
// refused with the same error as a corrupt one. Nothing is deployed, so there is no such
// object anywhere to be lenient for, and a lenient branch would leave the hole open
// permanently for the sake of an object that does not exist.
func Unframe(body []byte) ([]byte, error) {
	if len(body) < digestLen+1 || body[digestLen] != '\n' {
		return nil, fmt.Errorf("%w: no digest line", ErrCorrupt)
	}
	stored := string(body[:digestLen])
	payload := body[digestLen+1:]
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != stored {
		return nil, fmt.Errorf("%w: stored %s, contents hash to %s", ErrCorrupt, stored, got)
	}
	return payload, nil
}
