// Package framed stores a small structural object in the object store so that a single
// flipped bit is detected rather than decoded.
//
// It exists because `descriptor.json` had already solved this and the image and snapshot
// manifests beside it were still bare JSON (DEV-0025); two copies of an integrity rule drift.
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
// Not authentication: the digest is unkeyed, so anything that can write the object can write
// a matching digest. A chunk is protected by its own key and AAD, and a manifest from another
// writer by the compare-and-set on its ETag (INV-10). Nor a substitute for a chunk's own
// check: what this protects is exactly the fields with no second opinion — the offsets and
// lengths that place data, and the tombstones that keep an ancestor's bytes buried.
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

// FormatVersion is the generation of the structural on-S3 formats: the volume manifest,
// the snapshot manifests beside it, and the volume descriptor. Every one of them carries
// it as `format_version`, and every reader refuses an object that does not carry this
// exact number (CheckVersion).
//
// # Why one number, and why now
//
// In this tree the structural formats change together and in place (CLAUDE.md), so three
// version numbers would be a matrix nobody reasons about and an incident would open with
// which of them is authoritative. It is not read-old and not a compatibility layer — it is
// the ability to *detect* that compatibility was broken, and it has to exist before the
// first deployment, because adding a required field to objects already in a bucket is not
// a change you can make. Concretely: `json.Unmarshal` silently discards fields it does not
// know, so without a version an Agent meeting a newer manifest would drop them and serve
// the volume anyway. The binary WAL formats already had this; the S3 objects, the only
// ones that cross hosts, did not.
const FormatVersion = 1

// ErrFormatTooNew and ErrFormatTooOld are what a reader gets for an object of the wrong
// generation. They are two errors and not one because the operator's next move differs
// and is not guessable from the object: too-new means this host is behind the one that
// wrote it — roll this host forward, do not touch the object — while too-old means the
// object predates this binary's format, which is the case that becomes read-old work if
// it ever happens. Today nothing can produce too-old, since FormatVersion has only ever
// been 1; the branch exists so the first bump has somewhere to land and so the message
// is right the one time anybody sees it.
var (
	ErrFormatTooNew = errors.New("framed: object was written by a newer format than this binary understands")
	ErrFormatTooOld = errors.New("framed: object predates the format this binary understands")
)

// CheckVersion validates the `format_version` an object carried.
//
// A zero — the field absent — is refused as too old rather than read as "generation 1":
// nothing is deployed, so a lenient branch would permanently accept an unversioned object for
// the sake of one that does not exist.
func CheckVersion(got int) error {
	switch {
	case got == FormatVersion:
		return nil
	case got > FormatVersion:
		return fmt.Errorf("%w: object says version %d, this binary writes %d", ErrFormatTooNew, got, FormatVersion)
	default:
		return fmt.Errorf("%w: object says version %d, this binary writes %d", ErrFormatTooOld, got, FormatVersion)
	}
}

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
