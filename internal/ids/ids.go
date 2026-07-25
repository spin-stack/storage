// Package ids is the single sanctioned source of UUIDs in this codebase: every id
// is a UUIDv7 (time-ordered, §ADR-0007). Generation is centralized here so it can be
// enforced two ways (INV-22):
//
//  1. In code: the golangci `forbidigo` rules forbid the v4 generators
//     (uuid.New / uuid.NewString / uuid.NewRandom) everywhere except this package,
//     so no other code can mint a non-v7 id.
//  2. In Postgres: every uuid identity column carries a CHECK that the version
//     nibble is 7 (get_byte(uuid_send(col),6) >> 4 = 7), so the database rejects any
//     non-v7 id regardless of who inserts it.
//
// Deterministic generation (NewAt) exists so the DST harness can mint reproducible
// v7 ids from the simulated clock and PRNG.
package ids

import (
	"io"

	"github.com/google/uuid"
)

// New returns a fresh UUIDv7 using wall time + crypto randomness (production).
func New() uuid.UUID {
	// uuid.NewV7 only errors if the crypto rand source fails, which is fatal.
	return uuid.Must(uuid.NewV7())
}

// NewAt builds a deterministic UUIDv7 from a millisecond timestamp and a randomness
// reader. Used by the DST harness (sim clock + seeded PRNG) for reproducible ids.
func NewAt(unixMilli int64, r io.Reader) uuid.UUID {
	var u uuid.UUID
	// 48-bit big-endian millisecond timestamp in bytes 0..5.
	u[0] = byte(unixMilli >> 40)
	u[1] = byte(unixMilli >> 32)
	u[2] = byte(unixMilli >> 24)
	u[3] = byte(unixMilli >> 16)
	u[4] = byte(unixMilli >> 8)
	u[5] = byte(unixMilli)
	// Fill the rest with (deterministic) randomness.
	_, _ = io.ReadFull(r, u[6:])
	// Set version 7 (high nibble of byte 6) and the RFC 4122 variant (byte 8).
	u[6] = (u[6] & 0x0F) | 0x70
	u[8] = (u[8] & 0x3F) | 0x80
	return u
}

// Parse parses a UUID string (any version).
func Parse(s string) (uuid.UUID, error) { return uuid.Parse(s) }

// IsV7 reports whether u is a version-7 UUID (mirrors the DB CHECK).
func IsV7(u uuid.UUID) bool { return u.Version() == 7 }
