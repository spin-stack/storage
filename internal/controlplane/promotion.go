// Package controlplane holds the single-active Control Plane logic. This file is
// the writer-promotion protocol (§12.3): a suspected-dead primary is fenced and the
// volume's next epoch is granted to a new host, but only after FENCING_WAIT has
// elapsed so the old writer can no longer ACK durability on its monotonic clock
// (§12.2). Promotion increments the epoch in PostgreSQL (term-guarded) and CASes the
// S3 epoch object (§12.4), then grants the new lease.
//
// The §7 failover state names live in internal/lifecycle as lifecycle.VolumeState,
// with the transition table that forbids reaching recovery without passing through
// FENCING_WAIT.
package controlplane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/clock"
)

// ErrFencingWaitNotElapsed means promotion was attempted before
// last_renewal + lease_ttl + max_clock_skew (§12.3 step 3). The reconciler retries
// after the wait; it must never be bypassed.
var ErrFencingWaitNotElapsed = errors.New("controlplane: fencing wait not elapsed")

// ErrEpochConflict means the epoch object is ahead of what this promotion would
// grant: another Control Plane already promoted this volume further. Finishing our
// own steps would fence the writer that won, so we stop instead (§12.4).
var ErrEpochConflict = errors.New("controlplane: epoch object is ahead of this promotion")

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
// Promote is a *resumable* three-step write: the epoch in PostgreSQL, the epoch
// object in S3, and the lease. Any of them can be the last thing that happens before
// a crash, and the reconciler will call this again, so the target epoch is derived
// from the state that is already out there instead of being blindly incremented
// (DEV-0004):
//
//	pg == s3, primary is already newHost -> the promotion completed; return it
//	pg == s3                             -> nothing done yet; grant pg+1
//	pg == s3 + 1                         -> PG bumped, the CAS never landed; finish it
//	s3 >  pg                             -> another CP promoted further; refuse
//
// Every step is then idempotent: the PG bump only runs when PG is behind, the CAS is
// skipped when the object already carries the target, and the lease grant is an
// upsert. Two runs of the same promotion therefore grant one epoch — a second one
// would fence the writer the first one just installed.
func (p *Promoter) Promote(ctx context.Context, term int64, volumeID string, oldLeaseRenewedAt time.Time, newHost string) (uint64, error) {
	// Step 3: FENCING_WAIT. Adding max_clock_skew covers a CP wall clock running up
	// to that far ahead of true time; a CP behind simply waits longer (§12.1).
	if p.clk.Wall().Before(p.FencingDeadline(oldLeaseRenewedAt)) {
		return 0, ErrFencingWaitNotElapsed
	}

	v, err := p.md.GetVolume(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	stored, etag, err := p.epochs.Current(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	pgEpoch := uint64(v.CurrentEpoch)

	var target uint64
	switch {
	case stored > pgEpoch:
		return 0, fmt.Errorf("%w: object at %d, PostgreSQL at %d", ErrEpochConflict, stored, pgEpoch)
	case pgEpoch == stored && v.PrimaryHostID == newHost && pgEpoch > 0:
		// Already promoted to this host: finish any tail step and report the epoch.
		target = pgEpoch
	case pgEpoch == stored:
		target = pgEpoch + 1
		newEpoch, err := p.md.BumpVolumeEpoch(ctx, term, volumeID, newHost) // step 4a
		if err != nil {
			return 0, err
		}
		if uint64(newEpoch) != target {
			return 0, fmt.Errorf("%w: PostgreSQL granted %d, expected %d", ErrEpochConflict, newEpoch, target)
		}
	case pgEpoch == stored+1:
		// Resume: PostgreSQL was bumped, the CAS never landed.
		target = pgEpoch
	default:
		return 0, fmt.Errorf("%w: object at %d, PostgreSQL at %d", ErrEpochConflict, stored, pgEpoch)
	}

	// Step 4b: CAS the S3 epoch object to the target (§12.4), unless it is already
	// there from a previous attempt.
	if stored != target {
		if _, err := p.epochs.CompareAndAdvance(ctx, volumeID, etag, target); err != nil {
			return 0, err
		}
	}

	// Step 5: grant the lease to the new host (an upsert: safe to repeat).
	if err := p.md.RenewHostLease(ctx, term, newHost, int(p.leaseTTL/time.Second)); err != nil {
		return 0, err
	}
	return target, nil
}
