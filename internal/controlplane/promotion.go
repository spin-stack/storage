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
	"sync"
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

// ErrVolumeNotPromotable means the volume's §7 state has no route to a new writer.
// A DETACHED volume has no guest and no writer to fence; granting it an epoch and a
// lease would arm a volume the Control Plane has already released.
var ErrVolumeNotPromotable = errors.New("controlplane: volume cannot be promoted from its current state")

// ErrClockOffsetTooLarge means the Control Plane's wall clock and the clock that
// stamped the lease row disagree by more than max_clock_skew, so no deadline derived
// from the two of them means anything (§12.1). Promotion refuses rather than compute
// a fencing wait it cannot justify; the fleet needs its clocks fixed.
var ErrClockOffsetTooLarge = errors.New("controlplane: control-plane and metadata clocks differ by more than max_clock_skew")

// Promoter runs the promotion protocol.
type Promoter struct {
	md           metadata.Store
	epochs       *epoch.Store
	clk          clock.Clock // the promoter's own clock: monotonic for the dwell, wall for the §12.1 offset check
	leaseTTL     time.Duration
	maxClockSkew time.Duration

	// marks is this promoter's monotonic view of the fences it is running, keyed by
	// volume. It is per-process on purpose: it is what a store clock cannot move
	// (ADR-0015). The durable half lives in volumes.fencing_started_at, which is
	// what a *replacement* promoter resumes from — a mark is seeded from it rather
	// than restarting the dwell.
	mu    sync.Mutex
	marks map[string]fenceMark
}

// fenceMark is one running fence as this promoter sees it: which durable observation
// it belongs to, and the monotonic instant at which this promoter's dwell is over.
type fenceMark struct {
	startedAt time.Time
	deadline  clock.Instant
}

// NewPromoter builds a Promoter. leaseTTL and maxClockSkew are the §12 parameters.
func NewPromoter(md metadata.Store, epochs *epoch.Store, clk clock.Clock, leaseTTL, maxClockSkew time.Duration) *Promoter {
	return &Promoter{
		md: md, epochs: epochs, clk: clk,
		leaseTTL: leaseTTL, maxClockSkew: maxClockSkew,
		marks: map[string]fenceMark{},
	}
}

// FencingDwell is how long a fence lasts for this Control Plane's parameters: one
// lease_ttl + max_clock_skew (§12.3). Promote may wait longer — the lease row it
// reads may have been granted with a longer TTL — so this is a lower bound.
func (p *Promoter) FencingDwell() time.Duration { return p.leaseTTL + p.maxClockSkew }

// FencingDeadline is the earliest instant at which a primary whose lease was last
// renewed at renewedAt may be superseded, on the clock that stamped that instant.
// Since ADR-0015 it is no longer what makes the wait long enough — the dwell is —
// but the timestamp keeps its second job, refusing a promotion of a writer that
// renewed after the fence began, so this remains a lower bound on the wait.
func (p *Promoter) FencingDeadline(renewedAt time.Time) time.Time {
	return renewedAt.Add(p.FencingDwell())
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
//	pg == s3 + 1, primary is newHost     -> PG bumped, the CAS never landed; finish it
//	anything else                        -> not our promotion to finish; refuse
//
// Every step is then idempotent: the PG bump only runs when PG is behind, the CAS is
// skipped when the object already carries the target, and the lease grant is an
// upsert. Two runs of the same promotion therefore grant one epoch — a second one
// would fence the writer the first one just installed.
//
// The resume branch checks the owner as well as the number, because "PostgreSQL is
// one ahead of the object" is *another* promoter's half-finished work just as often
// as it is ours. Finishing theirs would write our host into the epoch object while
// PostgreSQL records theirs, and both would believe they hold the same epoch.
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
	state := v.State
	// Step 3: FENCING_WAIT, measured against the host that is actually serving the
	// volume. If that is already newHost, this is our own promotion being resumed:
	// the wait was served the first time round, and re-waiting on the lease we just
	// granted would strand the volume (§12.3 steps 3-5).
	//
	// The §7 state is recorded *before* the wait, not after it: the window a second
	// Control Plane, a second reconciler pass or an operator could start a competing
	// promotion in is exactly the minutes the wait lasts, and a volume stored as
	// ACTIVE throughout it looks healthy to all of them.
	if v.PrimaryHostID != newHost {
		if state, err = p.recordState(ctx, term, v, state, lifecycle.VolumeFencingWait); err != nil {
			return 0, err
		}
		// Read the observation back: SetVolumeState stamped it with the store's own
		// clock, and the dwell is measured from it (ADR-0015). A read that does not
		// see the write yet reports nothing, and nothing means a full dwell.
		fenced, err := p.md.GetVolume(ctx, volumeID)
		if err != nil {
			return 0, err
		}
		if err := p.fencingWaitElapsed(ctx, volumeID, v.PrimaryHostID, oldLeaseRenewedAt, fenced.FencingStartedAt); err != nil {
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
	case pgEpoch == stored && v.PrimaryHostID == newHost && pgEpoch > 0:
		// Already promoted to this host: finish any tail step and report the epoch.
		target = pgEpoch
	case pgEpoch == stored:
		target = pgEpoch + 1
		newEpoch, err := p.md.BumpVolumeEpoch(ctx, term, volumeID, newHost, v.CurrentEpoch) // step 4a
		if err != nil {
			return 0, err
		}
		if uint64(newEpoch) != target {
			return 0, fmt.Errorf("%w: PostgreSQL granted %d, expected %d", ErrEpochConflict, newEpoch, target)
		}
	case pgEpoch == stored+1 && v.PrimaryHostID == newHost:
		// Resume: PostgreSQL was bumped for us, the CAS never landed.
		target = pgEpoch
	default:
		return 0, fmt.Errorf("%w: object at %d, PostgreSQL at %d on %q",
			ErrEpochConflict, stored, pgEpoch, v.PrimaryHostID)
	}

	// Step 4b: CAS the S3 epoch object to the target (§12.4), naming the host it is
	// granted to — unless it is already there from a previous attempt.
	if stored != target {
		if _, err := p.epochs.Grant(ctx, volumeID, etag, target, newHost); err != nil {
			return 0, err
		}
	}

	// Step 5: grant the lease to the new host (an upsert: safe to repeat).
	if err := p.md.RenewHostLease(ctx, term, newHost, int(p.leaseTTL/time.Second)); err != nil {
		return 0, err
	}
	// The epoch is granted, so §7 says the volume needs recovery before it serves
	// again — it never goes straight back to ACTIVE on a promotion.
	if _, err := p.recordState(ctx, term, v, state, lifecycle.VolumeRecoveryRequired); err != nil {
		return 0, err
	}
	// The fence is over: leaving FENCING_WAIT cleared the durable record, and this
	// drops the matching in-process mark so the map does not grow with every volume
	// this Control Plane ever fenced.
	p.forgetFence(volumeID)
	return target, nil
}

// promotionPath is §7's route from a serving volume to one that needs recovery. A
// promotion records every step it passes rather than jumping, so the stored state is
// never one the transition table says is unreachable from the last one.
var promotionPath = []lifecycle.VolumeState{
	lifecycle.VolumePrimarySuspected,
	lifecycle.VolumeFencingWait,
	lifecycle.VolumeRecoveryRequired,
}

// promotionStep is where a volume in state s sits on that path: the index of the
// next step it has to take. A DETACHED volume is not on it at all.
func promotionStep(s lifecycle.VolumeState) (int, bool) {
	switch s {
	case lifecycle.VolumeActive:
		return 0, true
	case lifecycle.VolumePrimarySuspected:
		return 1, true
	case lifecycle.VolumeFencingWait:
		return 2, true
	case lifecycle.VolumeRecoveryRequired, lifecycle.VolumeRecovering:
		// Already at (or past) the end: a re-promotion of a volume that is being
		// recovered puts it back to needing recovery, which the table allows.
		return 2, true
	}
	return 0, false
}

// pathIndex is where a state sits on promotionPath (-1 if it is not a step of it).
func pathIndex(s lifecycle.VolumeState) int {
	for i, step := range promotionPath {
		if step == s {
			return i
		}
	}
	return -1
}

// recordState walks the volume from `from` up to target along promotionPath,
// persisting each step (term-guarded, and transition-guarded inside the write), and
// returns the state now stored. Every step is idempotent — a state may always be
// re-written as itself — so a retried promotion re-affirms rather than fails.
func (p *Promoter) recordState(ctx context.Context, term int64, v metadata.Volume,
	from, target lifecycle.VolumeState) (lifecycle.VolumeState, error) {
	first, ok := promotionStep(from)
	if !ok {
		return from, fmt.Errorf("%w: %s is %s", ErrVolumeNotPromotable, v.VolumeID, from)
	}
	for i, last := first, pathIndex(target); i <= last; i++ {
		if err := p.md.SetVolumeState(ctx, term, v.VolumeID, promotionPath[i]); err != nil {
			return from, err
		}
		from = promotionPath[i]
	}
	return from, nil
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
// durability (§12.3 step 3). It is the whole of INV-11, and since ADR-0015 it is a
// *dwell* — elapsed time since this Control Plane observed the situation — rather
// than a comparison against a timestamp somebody else wrote.
//
// The distinction is not academic. The old form derived the deadline from
// host_leases.last_renewal, so a read served by a replica lagging by more than
// lease_ttl + max_clock_skew reported an instant old enough that the wait already
// looked over, and the epoch was granted while the old writer's monotonic lease was
// still valid. Nothing noticed: the §12.1 offset check compares *clocks*, and the
// clocks were fine. It was the data that was old.
//
// So the wait now has to clear three bars, all of which must agree:
//
//  1. This promoter's own monotonic clock, since it first observed the fence. A
//     mark it has not seen before is seeded from the durable record (see
//     dwellElapsed), so a Control Plane that restarts mid-fence resumes the wait its
//     predecessor started rather than beginning a new one — and once seeded, nothing
//     the database's clock does can shorten it.
//  2. The durable record itself, on the clock that stamped it. Missing — never
//     written, lost to a restore, or simply not visible to this read yet — is not
//     "long ago": it starts a full dwell. Fail slow, never short.
//  3. The lease timestamp, which keeps its second job: a writer that renewed *after*
//     the fence began is alive, whatever the dwell says, and its own TTL may be
//     longer than this Control Plane's.
//
// The caller's instant is a hint, not the authority: it may have been read long ago,
// or from a host that is no longer the primary. The promoter reads the lease of the
// host it is actually fencing and takes the most conservative view of the two.
//
// With no lease row and nothing from the caller there is no conservative answer, so
// the promotion is refused (ErrSourceLeaseUnknown) unless the fleet has recorded the
// host DEAD. That is the Control Plane asserting the writer is gone — an assertion,
// not a stale read — and §12.3 accepts it as a reason to skip the wait entirely.
func (p *Promoter) fencingWaitElapsed(ctx context.Context, volumeID, primary string,
	callerRenewedAt time.Time, fenceStartedAt time.Time,
) error {
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
		if renewedAt.IsZero() {
			if !p.observedDead(ctx, primary) {
				return fmt.Errorf("%w: %s", ErrSourceLeaseUnknown, primary)
			}
			return nil // §12.3's escape hatch: the fleet says the writer is gone
		}
	default:
		return err
	}
	dwell := ttl + p.maxClockSkew

	storeNow, err := p.md.Now(ctx)
	if err != nil {
		return err
	}
	cpNow := p.clk.Wall()
	if offset := cpNow.Sub(storeNow); offset > p.maxClockSkew || offset < -p.maxClockSkew {
		return fmt.Errorf("%w: %v", ErrClockOffsetTooLarge, offset)
	}

	// Bar 1: this promoter's own elapsed time. Seeding also happens here, so the
	// call that starts a fence always refuses — which is what makes a stale read of
	// the lease worth nothing.
	if !p.dwellElapsed(volumeID, fenceStartedAt, storeNow, dwell) {
		return ErrFencingWaitNotElapsed
	}
	// Bar 2: the durable observation, on the clock that stamped it.
	if fenceStartedAt.IsZero() || storeNow.Before(fenceStartedAt.Add(dwell)) {
		return ErrFencingWaitNotElapsed
	}
	// Bar 3: the writer's own last sign of life. max_clock_skew is added on top
	// because the Agent's clock may run that far ahead of the one that stamped the
	// row (§12.1), and both the store's clock and this one must agree — so a Control
	// Plane running behind waits longer and one running ahead cannot grant early.
	deadline := renewedAt.Add(dwell)
	if storeNow.Before(deadline) || cpNow.Before(deadline) {
		return ErrFencingWaitNotElapsed
	}
	return nil
}

// dwellElapsed reports whether dwell has passed on this promoter's monotonic clock
// since it first observed the fence that began at startedAt.
//
// A fence this promoter has not seen before — its first pass, or the first pass
// after a restart — is seeded rather than restarted: the time the durable record
// says has already gone is subtracted, so a replacement finishes the wait its
// predecessor started (ADR-0015's whole reason for making the record durable). Only
// the *remainder* is then counted on this clock, which is the part no store clock
// can move afterwards.
//
// A missing record (zero startedAt) seeds a full dwell, and a record from the future
// counts as no elapsed time at all: both are the fail-slow direction.
func (p *Promoter) dwellElapsed(volumeID string, startedAt, storeNow time.Time, dwell time.Duration) bool {
	now := p.clk.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.marks[volumeID]
	if !ok || !m.startedAt.Equal(startedAt) {
		remaining := dwell
		if !startedAt.IsZero() {
			if gone := storeNow.Sub(startedAt); gone > 0 {
				remaining = dwell - gone
			}
		}
		if remaining < 0 {
			remaining = 0
		}
		m = fenceMark{startedAt: startedAt, deadline: now.Add(remaining)}
		p.marks[volumeID] = m
	}
	return now >= m.deadline
}

// forgetFence drops the in-process mark for a fence that is over.
func (p *Promoter) forgetFence(volumeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.marks, volumeID)
}

// observedDead reports whether the fleet has recorded the host as DEAD. A read
// failure counts as "not observed": the fail-closed direction.
func (p *Promoter) observedDead(ctx context.Context, hostID string) bool {
	h, err := p.md.GetHost(ctx, hostID)
	return err == nil && h.State == lifecycle.HostDead
}
