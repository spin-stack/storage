// Package controlplane holds the single-active Control Plane logic. This file is
// the writer-promotion protocol (§12.3): a suspected-dead primary is fenced and the
// volume's next epoch is granted to a new host, but only after FENCING_WAIT has
// elapsed so the old writer can no longer ACK durability on its monotonic clock
// (§12.2). Promotion increments the epoch in PostgreSQL (term-guarded) and CASes the
// S3 epoch object (§12.4), then grants the new lease.
package controlplane

import (
	"context"
	"errors"
	"time"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/clock"
)

// Promotion state names (§7 writer-failover state machine).
const (
	StateActive           = "ACTIVE"
	StatePrimarySuspected = "PRIMARY_SUSPECTED"
	StateFencingWait      = "FENCING_WAIT"
	StateRecoveryRequired = "RECOVERY_REQUIRED"
	StateRecovering       = "RECOVERING"
)

// ErrFencingWaitNotElapsed means promotion was attempted before
// last_renewal + lease_ttl + max_clock_skew (§12.3 step 3). The reconciler retries
// after the wait; it must never be bypassed.
var ErrFencingWaitNotElapsed = errors.New("controlplane: fencing wait not elapsed")

// Promoter runs the promotion protocol.
type Promoter struct {
	md           metadata.Store
	epochs       *epoch.Store
	clk          clock.Clock // wall clock; the promotion wait is the only wall-clock dependency (§12.1)
	leaseTTL     time.Duration
	maxClockSkew time.Duration
}

// NewPromoter builds a Promoter. leaseTTL and maxClockSkew are the §12 parameters.
func NewPromoter(md metadata.Store, epochs *epoch.Store, clk clock.Clock, leaseTTL, maxClockSkew time.Duration) *Promoter {
	return &Promoter{md: md, epochs: epochs, clk: clk, leaseTTL: leaseTTL, maxClockSkew: maxClockSkew}
}

// FencingDeadline is the earliest wall instant at which a primary whose lease was
// last renewed at renewedAt may be superseded (§12.3 step 3).
func (p *Promoter) FencingDeadline(renewedAt time.Time) time.Time {
	return renewedAt.Add(p.leaseTTL + p.maxClockSkew)
}

// Promote fences the suspected-dead primary of volumeID and grants epoch N+1 to
// newHost. It refuses (ErrFencingWaitNotElapsed) until FENCING_WAIT elapses on the
// CP wall clock; then it bumps the epoch in PG (term-guarded, §12.3 step 4), CASes
// the S3 epoch object (§12.4), and grants the new lease (step 5). Returns the new
// epoch.
func (p *Promoter) Promote(ctx context.Context, term int64, volumeID string, oldLeaseRenewedAt time.Time, newHost string) (uint64, error) {
	// Step 3: FENCING_WAIT. Adding max_clock_skew covers a CP wall clock running up
	// to that far ahead of true time; a CP behind simply waits longer (§12.1).
	if p.clk.Wall().Before(p.FencingDeadline(oldLeaseRenewedAt)) {
		return 0, ErrFencingWaitNotElapsed
	}

	// Read the epoch object's current ETag for the CAS.
	_, etag, err := p.epochs.Current(ctx, volumeID)
	if err != nil {
		return 0, err
	}

	// Step 4a: increment the epoch in PostgreSQL (term-guarded).
	newEpoch, err := p.md.BumpVolumeEpoch(ctx, term, volumeID, newHost)
	if err != nil {
		return 0, err
	}

	// Step 4b: CAS the S3 epoch object to N+1 (§12.4). A concurrent advance loses.
	if _, err := p.epochs.CompareAndAdvance(ctx, volumeID, etag, uint64(newEpoch)); err != nil {
		return 0, err
	}

	// Step 5: grant the lease to the new host.
	if err := p.md.RenewHostLease(ctx, term, newHost, int(p.leaseTTL/time.Second)); err != nil {
		return 0, err
	}
	return uint64(newEpoch), nil
}
