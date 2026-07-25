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
	"github.com/spin-stack/storage/internal/lifecycle"
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

// ErrSourceLeaseUnknown means the Control Plane cannot say when the volume's current
// primary last renewed its lease: there is no lease row and the caller supplied
// nothing either. That is "I know nothing", not "the lease expired long ago" — after
// a PITR restore or an operator cleanup of host_leases the source is very much alive
// and valid for up to lease_ttl on its own monotonic clock. Promotion refuses until
// a lease is observed or the host is recorded DEAD (§12.3, INV-11).
var ErrSourceLeaseUnknown = errors.New("controlplane: the source host's lease is unknown")

// ErrDestinationHostUnusable means the host being promoted to cannot take the
// volume: it is not registered, or its fleet state does not accept placement. The
// lease grant is the last of promotion's three writes and in PostgreSQL it has a
// foreign key, so discovering this at that point leaves the old writer fenced and
// nobody able to ACK (§8, §12.3).
var ErrDestinationHostUnusable = errors.New("controlplane: destination host cannot take the volume")

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
//
// oldLeaseRenewedAt is what the *caller* observed about the primary. It is a floor,
// not the authority: the promoter reads the lease of the host it is actually fencing
// (see fencingWaitElapsed), so a command that was overtaken — the volume moved to
// another host since it was issued — waits on the lease of the host now serving it
// instead of fencing a healthy writer with a stale instant.
func (p *Promoter) Promote(ctx context.Context, term int64, volumeID string, oldLeaseRenewedAt time.Time, newHost string) (uint64, error) {
	v, err := p.md.GetVolume(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	// The host has to be able to hold the volume before anything is advanced.
	// Starting a promotion also requires it to accept placement; finishing one that
	// already moved the volume there does not — a cordon stops new work, it does not
	// stop a host serving what it already holds.
	if err := p.checkDestination(ctx, newHost, v.PrimaryHostID != newHost); err != nil {
		return 0, err
	}
	// Step 3: FENCING_WAIT, measured against the host that is actually serving the
	// volume. If that is already newHost, this is our own promotion being resumed:
	// the wait was served the first time round, and re-waiting on the lease we just
	// granted would strand the volume (§12.3 steps 3-5).
	if v.PrimaryHostID != newHost {
		if err := p.fencingWaitElapsed(ctx, v.PrimaryHostID, oldLeaseRenewedAt); err != nil {
			return 0, err
		}
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

// checkDestination refuses a host that cannot hold the volume. It is deliberately
// the first thing Promote does: every later step is a durable write that fences
// somebody.
func (p *Promoter) checkDestination(ctx context.Context, hostID string, mustAcceptPlacement bool) error {
	h, err := p.md.GetHost(ctx, hostID)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrDestinationHostUnusable, hostID, err)
	}
	if mustAcceptPlacement && !h.State.AcceptsPlacement() {
		return fmt.Errorf("%w: %s is %s", ErrDestinationHostUnusable, hostID, h.State)
	}
	return nil
}

// fencingWaitElapsed reports whether the primary being fenced can still be ACKing
// durability (§12.3 step 3). The caller's instant is a hint, not the authority: it
// may have been read long ago, or from a host that is no longer the primary. The
// promoter reads the lease of the host it is actually fencing and takes the most
// conservative view of the two — including the TTL the lease was *granted* with,
// which is what the Agent is counting down, even when this CP is configured with a
// shorter one.
//
// With no lease row and nothing from the caller there is no conservative answer, so
// the promotion is refused (ErrSourceLeaseUnknown) unless the fleet has recorded the
// host as DEAD, which is the CP asserting that the writer is gone.
func (p *Promoter) fencingWaitElapsed(ctx context.Context, primary string, callerRenewedAt time.Time) error {
	if primary == "" {
		return nil // no writer to fence
	}
	renewedAt, ttl := callerRenewedAt, p.leaseTTL

	l, err := p.md.GetHostLease(ctx, primary)
	switch {
	case err == nil:
		if renewedAt.Before(l.LastRenewal) {
			renewedAt = l.LastRenewal
		}
		if granted := time.Duration(l.TTLSeconds) * time.Second; granted > ttl {
			ttl = granted
		}
	case errors.Is(err, metadata.ErrNotFound):
		if renewedAt.IsZero() && !p.observedDead(ctx, primary) {
			return fmt.Errorf("%w: %s", ErrSourceLeaseUnknown, primary)
		}
	default:
		return err
	}

	// Adding max_clock_skew covers a CP wall clock running up to that far ahead of
	// true time; a CP behind simply waits longer (§12.1).
	if p.clk.Wall().Before(renewedAt.Add(ttl + p.maxClockSkew)) {
		return ErrFencingWaitNotElapsed
	}
	return nil
}

// observedDead reports whether the fleet has recorded the host as DEAD. A read
// failure counts as "not observed": the fail-closed direction.
func (p *Promoter) observedDead(ctx context.Context, hostID string) bool {
	h, err := p.md.GetHost(ctx, hostID)
	return err == nil && h.State == lifecycle.HostDead
}
