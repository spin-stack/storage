package wal

import "errors"

// ErrSelfFenced is returned when a FLUSH/FUA cannot be ACKed because the host lease
// is no longer valid according to the Agent's monotonic clock (§12.2). The object
// may already be in S3, but it is NOT confirmed to the guest — the Agent self-fences.
var ErrSelfFenced = errors.New("wal: self-fenced (lease invalid at ACK)")

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

// LeaseChecker reports whether the host lease is valid right now, on the monotonic
// clock. Defined here (the consumer) so wal does not depend on the lease package;
// *lease.Manager satisfies it structurally.
type LeaseChecker interface {
	Valid() bool
}
