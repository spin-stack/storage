package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// ADR-0015: the fencing wait is a monotonic dwell, not a timestamp comparison.
//
// Wave 2 removed the Control Plane's own wall clock from the deadline, which closed
// the case where the *container's* clock jumped. It did not close the case where the
// *data* is old: a read served by a replica lagging by more than
// lease_ttl + max_clock_skew reports a last_renewal old enough that the wait already
// looks over, and the epoch is granted while the old writer's monotonic lease is
// still valid. Both clocks agree, so the offset check sees nothing wrong — the clocks
// are fine, the row is not.
//
// The wait therefore has to be elapsed time on the promoter's own clock since the
// promoter itself observed the lease, with that observation recorded durably beside
// the §7 FENCING_WAIT state so a Control Plane that restarts mid-fence resumes the
// dwell instead of starting it again. A missing record starts a full dwell: fail
// slow, never short.

// laggingReplica is a metadata.Store whose lease reads are served by a replica so far
// behind that last_renewal predates everything. It is the maximally stale read: any
// deadline derived from that timestamp elapsed long ago.
type laggingReplica struct {
	metadata.Store
	renewedAt time.Time
}

func (s *laggingReplica) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	l, err := s.Store.GetHostLease(ctx, hostID)
	if err != nil {
		return l, err
	}
	l.LastRenewal = s.renewedAt
	return l, nil
}

// TestAStaleLeaseReadDoesNotShortenTheFence is the gap itself. The lease row says the
// source last renewed an hour before the process started; the source is in fact alive
// and counting its own lease down on a monotonic clock it never shares. The promoter
// must still sit through a full lease_ttl + max_clock_skew of its own time.
func TestAStaleLeaseReadDoesNotShortenTheFence(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	// Every lease read from here on is an hour stale.
	stale := w.dbClk.Wall().Add(-time.Hour)
	lagging := &laggingReplica{Store: w.md, renewedAt: stale}
	p := controlplane.NewPromoter(lagging, w.epochs, w.clk, fenceTTL, fenceSkew)

	// First look. Whatever the row says, nothing has elapsed for this promoter yet.
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("a stale lease read granted the epoch immediately: %v", err)
	}
	// The observation is durable, recorded with the §7 state the promoter writes.
	if got := w.state(t); got != lifecycle.VolumeFencingWait {
		t.Fatalf("volume state = %q, want FENCING_WAIT", got)
	}
	v, err := w.md.GetVolume(ctx, fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	if v.FencingStartedAt.IsZero() {
		t.Fatal("the promoter did not record when it started fencing")
	}

	// One tick short of the dwell, still refused.
	w.advance(fenceTTL + fenceSkew - time.Second)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("granted %v before the dwell elapsed: %v", time.Second, err)
	}
	w.unchanged(t, 1, fenceHostA)

	// Past it, and only there.
	w.advance(2 * time.Second)
	ep, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB)
	if err != nil {
		t.Fatalf("the dwell elapsed and the promotion was still refused: %v", err)
	}
	if ep != 2 {
		t.Fatalf("granted epoch %d, want 2", ep)
	}
}

// TestACrashMidFenceResumesTheDwellFromTheRecordedInstant: the promoter that started
// the fence is gone — the process restarted, or another Control Plane won the
// election. The dwell is not restarted from zero, because the instant it began is
// recorded beside FENCING_WAIT; the replacement finishes the wait the first one
// started.
func TestACrashMidFenceResumesTheDwellFromTheRecordedInstant(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}

	first := controlplane.NewPromoter(w.md, w.epochs, w.clk, fenceTTL, fenceSkew)
	if _, err := first.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	started, err := w.md.GetVolume(ctx, fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	if started.FencingStartedAt.IsZero() {
		t.Fatal("setup: the fence start was not recorded")
	}

	// Most of the dwell passes while nothing is running.
	w.advance(fenceTTL + fenceSkew - time.Second)

	// A replacement promoter, with no memory of anything.
	second := controlplane.NewPromoter(w.md, w.epochs, w.clk, fenceTTL, fenceSkew)
	if _, err := second.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("the resumed fence granted early: %v", err)
	}
	// The record it resumes from is the first promoter's, not a fresh one.
	resumed, err := w.md.GetVolume(ctx, fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.FencingStartedAt.Equal(started.FencingStartedAt) {
		t.Fatalf("the fence start moved from %v to %v — the dwell was restarted",
			started.FencingStartedAt, resumed.FencingStartedAt)
	}

	w.advance(2 * time.Second)
	if _, err := second.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); err != nil {
		t.Fatalf("the resumed dwell elapsed and the promotion was refused: %v", err)
	}
}

// TestAMissingFenceRecordStartsAFullDwell: nothing says when the fence began — the
// row was restored from a backup, an operator reset the state, the write never
// landed. There is no shorter answer that is safe, so a full dwell begins now.
func TestAMissingFenceRecordStartsAFullDwell(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	// The lease deadline is long past on every clock, so only the dwell can hold
	// the promotion back.
	w.advance(fenceTTL + fenceSkew + time.Minute)

	p := controlplane.NewPromoter(w.md, w.epochs, w.clk, fenceTTL, fenceSkew)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("a promotion with no fence record on file granted immediately: %v", err)
	}
	w.unchanged(t, 1, fenceHostA)

	w.advance(fenceTTL + fenceSkew - time.Second)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("the fresh dwell was cut short: %v", err)
	}
	w.advance(2 * time.Second)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); err != nil {
		t.Fatalf("the fresh dwell elapsed and the promotion was refused: %v", err)
	}
}

// TestTheFenceRecordIsClearedWhenTheVolumeLeavesFencingWait: the record belongs to
// one fence. A volume that has been promoted and later needs promoting again must
// wait a fresh dwell, not inherit the elapsed one — the old record would say the
// wait was over before the new writer was ever fenced.
func TestTheFenceRecordIsClearedWhenTheVolumeLeavesFencingWait(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	p := controlplane.NewPromoter(w.md, w.epochs, w.clk, fenceTTL, fenceSkew)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.advance(fenceTTL + fenceSkew + time.Second)
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB); err != nil {
		t.Fatalf("setup: %v", err)
	}
	v, err := w.md.GetVolume(ctx, fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	if !v.FencingStartedAt.IsZero() {
		t.Fatalf("a finished promotion left its fence record behind: %v", v.FencingStartedAt)
	}

	// A second promotion, of the writer the first one installed, waits its own dwell.
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostB, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostC); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("the second promotion inherited the first one's elapsed dwell: %v", err)
	}
}
