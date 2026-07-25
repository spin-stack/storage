package wal

import (
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/lifecycle"
)

// ErrSelfFenced is returned when a FLUSH/FUA cannot be ACKed because the host lease
// is no longer valid according to the Agent's monotonic clock (§12.2). The object
// may already be in S3, but it is NOT confirmed to the guest — the Agent self-fences.
var ErrSelfFenced = errors.New("wal: self-fenced (lease invalid at ACK)")

// ErrNoLease is returned when a volume in `remote` durability mode has no lease
// checker at all. §12.2 makes a valid lease a precondition of every durable ACK, so
// the absence of the check is not "no objection" — it is an unfenced writer, and the
// FLUSH fails closed (DEV-0004).
var ErrNoLease = errors.New("wal: remote durability requires a lease checker")

// ErrNoUploader is returned when a volume in `remote` durability mode has no remote
// path (no batcher, no uploader). `remote` is the default mode, so a log that was
// never given one would otherwise ACK a FLUSH and advance durable_sequence with an
// empty bucket — a durability claim S3 cannot back (INV-07). The absence of an
// uploader is not "nothing to upload".
var ErrNoUploader = errors.New("wal: remote durability requires a batcher and an uploader")

// DurabilityMode selects the FLUSH/FUA ACK contract per volume (§14.8).
type DurabilityMode int

const (
	// ModeRemote (default): ACK after fdatasync + verified S3 PUT + valid lease.
	// RPO 0 for covered writes.
	ModeRemote DurabilityMode = iota
	// ModeLocal: ACK after local fdatasync; S3 is asynchronous; the lease does not
	// gate the FLUSH ACK (§14.8 rule 3). RPO <= max_unflushed_age, measured.
	ModeLocal
)

// String renders the mode for traces and metrics.
func (m DurabilityMode) String() string { return string(m.Durability()) }

// Durability is the Control-Plane vocabulary for this mode (§14.8).
func (m DurabilityMode) Durability() lifecycle.Durability {
	if m == ModeLocal {
		return lifecycle.DurabilityLocal
	}
	return lifecycle.DurabilityRemote
}

// ModeFor maps the durability the Control Plane stores (§8) to the data path's ACK
// contract (§14.8). This is the single place the two vocabularies meet, so they
// cannot drift: an unknown value is an error, never a silent fallback to remote.
func ModeFor(d lifecycle.Durability) (DurabilityMode, error) {
	switch d {
	case lifecycle.DurabilityRemote:
		return ModeRemote, nil
	case lifecycle.DurabilityLocal:
		return ModeLocal, nil
	default:
		return ModeRemote, fmt.Errorf("wal: %w: durability %q", lifecycle.ErrUnknownState, d)
	}
}

// LeaseChecker reports whether the host lease is valid right now, on the monotonic
// clock. Defined here (the consumer) so wal does not depend on the lease package;
// *lease.Manager satisfies it structurally.
type LeaseChecker interface {
	Valid() bool
}
