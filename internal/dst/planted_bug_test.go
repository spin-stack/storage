package dst

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// A checker that cannot fail is decoration — but a checker proven against a
// hand-written Emit is barely better. `s.Emit(Event{StalePublishOK: true})` proves the
// struct field is read; it says nothing about whether any real sequence of events
// could ever set it, which is the only question a regression cares about.
//
// So a planted bug here breaks *production behaviour* and the checker has to see it
// through a scenario driving real code: a bucket without versioning, a backend without
// conditional writes, a backend serving a stale read, a listing that never catches up,
// a clock that goes backwards, a lease checker that keeps saying yes, a volume created
// without encryption. Every one is an operational reality, and each is injected into
// the simulated I/O rather than edited into the code under test.
//
// Four checkers have no such proof yet, because inverting them needs a seam in
// production code that does not exist. They keep a literal-event proof, are grouped
// separately below with the missing seam named, and are counted by
// TestPlantedBugCoverageIsNotSilentlyWeakened so the gap is visible in the source
// rather than implied by its absence.

// Planted-bug outcomes. A behavioural planted bug also trips the scenario's own
// assertions; these name what went wrong for a reader of a failing run.
var (
	errNotYetExpired           = errors.New("planted: the lease should already have expired")
	errLeaseResurrected        = errors.New("planted: a monotonic regression revalidated an expired lease")
	errStaleWriterPublished    = errors.New("planted: a fenced writer verified its own epoch")
	errPromotedOverLiveLease   = errors.New("planted: an epoch was granted while the fenced writer's lease was still valid")
	errTruncatedAbovePublished = errors.New("planted: local WAL was reclaimed above the verified published point")
)

// plantedBug runs sc and requires checker to reject it, naming itself and printing the
// reproducing seed (without which a DST failure is not actionable).
//
// The scenario's own error is deliberately discarded: a checker that only fires when
// the scenario already caught the problem is not a checker. What is under test is
// whether the event stream carries the violation.
func plantedBug(t *testing.T, seed int64, checker Checker, wantName string, sc Scenario) {
	t.Helper()
	res := Run(seed, func(s *Sim) error { _ = sc(s); return nil }, checker)
	if res.Err == nil {
		t.Fatalf("%s did not catch the planted violation", wantName)
	}
	if !strings.Contains(res.Err.Error(), wantName) {
		t.Fatalf("failure must name the checker %q, got: %v", wantName, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "seed=") {
		t.Fatalf("failure must report the reproducing seed, got: %v", res.Err)
	}
}

// requirePasses is the control every planted bug needs: without the injected fault the
// same scenario and the same checker must be green, or the "bug" proved nothing.
func requirePasses(t *testing.T, seed int64, checker Checker, sc Scenario) {
	t.Helper()
	if res := Run(seed, sc, checker); res.Err != nil {
		t.Fatalf("the unplanted scenario must pass: %v\n--- trace ---\n%s", res.Err, res.TraceString())
	}
}

// ---------------------------------------------------------------------------
// Behavioural planted bugs: real code, real fault, checker sees the consequence.
// ---------------------------------------------------------------------------

// INV-14: the GC never permanently deletes. Planted by taking versioning off the
// bucket — the single configuration mistake that turns every delete marker the GC
// writes into an irreversible delete, with no error anywhere. The GC scenario is
// otherwise unchanged; it derives the event from whether the mark can be undone.
func TestPlantedBugPermanentDelete(t *testing.T) {
	requirePasses(t, 1234, NewNoPermanentDeleteChecker(), scenarioGCMarksOrphansNotLive)
	plantedBug(t, 1234, NewNoPermanentDeleteChecker(), "no-permanent-delete", func(s *Sim) error {
		s.Store.InjectPermanentDelete()
		return scenarioGCMarksOrphansNotLive(s)
	})
}

// INV-16: a published snapshot never changes. Planted by a backend that accepts a
// conditional write unconditionally — a real §6.1 conformance failure, and the one
// that silently makes every create-only publication in the system overwritable.
func TestPlantedBugSnapshotMutated(t *testing.T) {
	requirePasses(t, 17, NewImmutableSnapshotChecker(), scenarioSnapshotPauseFreeImmutable)
	plantedBug(t, 17, NewImmutableSnapshotChecker(), "immutable-snapshots", func(s *Sim) error {
		s.Store.InjectIgnorePreconditions()
		return scenarioSnapshotPauseFreeImmutable(s)
	})
}

// INV-09: the promoted writer recovers everything the fenced writer ACKed. Planted by
// a listing that never catches up: the durable prefix is computed from a LIST, and a
// backend that answers a short listing with no error hands recovery a prefix of zero
// while the objects sit right there.
func TestPlantedBugLostAckedWrite(t *testing.T) {
	requirePasses(t, 16, NewNoLostAckedWriteChecker(), scenarioFencedWriterNoLostAck)
	plantedBug(t, 16, NewNoLostAckedWriteChecker(), "no-lost-acked-write", func(s *Sim) error {
		s.Store.SetEventualList(true)
		return scenarioFencedWriterNoLostAck(s)
	})
}

// INV-10: a fenced writer never publishes. Planted by a backend that serves a stale
// read of the epoch object. Read-after-write on that one small object is the whole of
// §12.4's belt-and-suspenders fence: without it the superseded writer asks whether it
// is still current, is told yes, and publishes into a namespace another writer owns.
func TestPlantedBugStaleWriterPublished(t *testing.T) {
	const vid = "00000000-0000-7000-8000-0000000000a7"
	sc := func(staleAfterPromotion bool) Scenario {
		return func(s *Sim) error {
			ctx := context.Background()
			epochs := epoch.NewStore(s.Store)
			if _, err := epochs.Init(ctx, vid, 1); err != nil {
				return err
			}
			_, w1ETag, err := epochs.Current(ctx, vid)
			if err != nil {
				return err
			}
			// The CP promotes: the epoch object advances to 2 and W1 is fenced.
			if _, err := epochs.CompareAndAdvance(ctx, vid, w1ETag, 2); err != nil {
				return err
			}
			if staleAfterPromotion {
				s.Store.InjectStaleRead(epoch.Key(vid))
				s.Emit(Event{Kind: EventFault, Msg: "epoch object serves the pre-promotion version"})
			}
			// W1 returns and asks whether it may still publish (§18 "old writer
			// returns"): both answers must be no.
			verr := epochs.Verify(ctx, vid, 1)
			_, caserr := epochs.CompareAndAdvance(ctx, vid, w1ETag, 3)
			s.Emit(Event{Kind: EventStalePublsh, StalePublishOK: verr == nil || caserr == nil})
			if verr == nil || caserr == nil {
				return errStaleWriterPublished
			}
			return nil
		}
	}
	requirePasses(t, 15, NewSingleWriterChecker(), sc(false))
	plantedBug(t, 15, NewSingleWriterChecker(), "effective-single-writer", sc(true))
}

// INV-06: no FLUSH is ACKed while the lease is invalid. Planted by the lease checker
// the WAL consults answering "valid" for ever while the real lease manager has long
// expired — a stuck renewal, a heartbeat thread that died holding its last answer. The
// event carries the *real* manager's verdict, so the checker sees the ACK escape.
func TestPlantedBugDurableAckWithoutLease(t *testing.T) {
	requirePasses(t, 13, NewDurableAckLeaseChecker(), scenarioLeaseFencesDurableAck)
	plantedBug(t, 13, NewDurableAckLeaseChecker(), "durable-ack-requires-lease", func(s *Sim) error {
		return leaseFencesDurableAck(s, lyingLeaseChecker)
	})
}

// INV-15: nothing leaves the host in clear. Planted by creating the volume without
// encryption — no bug in the crypto, just a Log that was never handed a DEK, which is
// exactly how a plaintext volume reaches production.
func TestPlantedBugPlaintextLeavesHost(t *testing.T) {
	requirePasses(t, 12, NewNoPlaintextLeavesHostChecker(), scenarioEncryptedWALNoPlaintextLeak)
	plantedBug(t, 12, NewNoPlaintextLeavesHostChecker(), "no-plaintext-leaves-host", func(s *Sim) error {
		return walPlaintextScenario(s, plaintextWAL)
	})
}

// The monotonic clock is the basis of lease safety (§12.1). Planted by the clock
// source itself going backwards — a live migration, a broken CLOCK_MONOTONIC — and the
// scenario shows what that buys: a lease manager that had correctly expired reports
// itself valid again, un-fencing a writer the CP has already replaced.
func TestPlantedBugMonotonicClock(t *testing.T) {
	sc := func(regress time.Duration) Scenario {
		return func(s *Sim) error {
			lm := lease.NewManager(s.Clock, 10*time.Second)
			lm.Grant()
			s.Tick(11 * time.Second)
			if lm.Valid() {
				return errNotYetExpired
			}
			if regress > 0 {
				s.Clock.InjectMonotonicRegression(regress)
				s.Emit(Event{Kind: EventFault, Msg: "monotonic clock stepped backwards"})
			}
			s.Tick(time.Second)
			if lm.Valid() {
				return errLeaseResurrected
			}
			return nil
		}
	}
	requirePasses(t, 777, NewMonotonicClockChecker(), sc(0))
	plantedBug(t, 777, NewMonotonicClockChecker(), "monotonic-clock", sc(9*time.Second))
}

// ---------------------------------------------------------------------------
// Literal-event proofs: the checker is proven to read the field, but nothing in the
// simulation can produce the event, because inverting the behaviour needs a seam in
// production code that does not exist. Each names the missing seam.
// ---------------------------------------------------------------------------

// INV-03: published <= durable <= local. wal.Log enforces the ordering internally
// (ErrWatermarkOrder) and exposes no way to set the three independently, so no I/O
// fault can produce this event. Needs a seam in internal/wal.
func TestPlantedBugWatermarkOrder(t *testing.T) {
	plantedBug(t, 11, NewWatermarkOrderChecker(), "watermark-order", func(s *Sim) error {
		s.Emit(Event{Kind: EventWatermark, Published: 9, Durable: 3, Local: 5})
		return nil
	})
}

// ---------------------------------------------------------------------------
// Behavioural proofs under construction: the scenario below drives real production
// code and derives the event from what that code did, but the fault it injects does
// not (yet) invert the behaviour, so the checker sees nothing. Each states the mode.
// ---------------------------------------------------------------------------

// Ids for the fencing-wait plant, kept apart from the mandatory scenario's so the two
// never share an epoch object.
const (
	fencedVol   = "00000000-0000-7000-8000-0000000000c0"
	fencedHost1 = "00000000-0000-7000-8000-0000000000c1"
	fencedHost2 = "00000000-0000-7000-8000-0000000000c2"
)

// The §12 parameters this plant runs with.
const (
	fencingLeaseTTL = 10 * time.Second
	fencingSkew     = 2 * time.Second
)

// earlyPromotion drives a real controlplane.Promoter against a volume whose primary
// still holds a valid lease on its own monotonic clock, and reports whether the epoch
// was granted anyway. Both halves of that question are answered by production code:
// the grant is Promote's return, and the liveness of the writer being fenced is a real
// lease.Manager counting down the same TTL the Control Plane recorded.
//
// jump steps the wall clock forward before the attempt — an NTP correction, a VM
// restored from a snapshot, a bad RTC. It is the one fault the simulation can aim at
// FENCING_WAIT today, and it does not work: the promoter and the Agent's lease read
// the same clock, so a jump that carries the CP past the deadline carries the writer
// past the end of its lease too. Nobody is fenced early; there is no violation to
// catch. The seed is what the harness reproduces from, so the fault has to live
// somewhere a seed can reach it.
func earlyPromotion(jump time.Duration) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		md := metasim.New(s.Clock.Wall)
		epochs := epoch.NewStore(s.Store)
		p := controlplane.NewPromoter(md, epochs, s.Clock, fencingLeaseTTL, fencingSkew)

		term, err := md.AcquireLeadership(ctx, "cp")
		if err != nil {
			return err
		}
		for _, h := range []string{fencedHost1, fencedHost2} {
			if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
				return err
			}
		}
		if err := md.CreateVolume(ctx, term, metadata.Volume{
			VolumeID: fencedVol, State: lifecycle.VolumeActive,
			PrimaryHostID: fencedHost1, DEKWrapped: []byte{1}, KEKID: "k",
		}); err != nil {
			return err
		}
		if _, err := epochs.Init(ctx, fencedVol, 0); err != nil {
			return err
		}

		// W1 takes the lease the Control Plane records. The row and the Agent's own
		// monotonic countdown start at the same instant, which is the only condition
		// under which the CP's deadline says anything about the writer (§12.1).
		if err := md.RenewHostLease(ctx, term, fencedHost1, int(fencingLeaseTTL/time.Second)); err != nil {
			return err
		}
		w1 := lease.NewManager(s.Clock, fencingLeaseTTL)
		w1.Grant()
		renewedAt := s.Clock.Wall()

		// Half a TTL in: the reconciler has seen missed heartbeats and carries no
		// instant of its own, so the lease row is the whole authority (§12.3).
		s.Tick(fencingLeaseTTL / 2)
		if jump > 0 {
			s.Clock.Advance(jump)
			s.Emit(Event{Kind: EventFault, Msg: "the Control Plane's wall clock stepped forward"})
		}
		newEpoch, err := p.Promote(ctx, term, fencedVol, time.Time{}, fencedHost2)
		granted := err == nil
		s.Emit(Event{
			Kind: EventPromotion, EarlyGrant: granted && w1.Valid(),
			Msg: fmt.Sprintf("epoch=%d granted=%t w1_lease_valid=%t deadline=%s",
				newEpoch, granted, w1.Valid(), p.FencingDeadline(renewedAt).UTC()),
		})
		switch {
		case granted && w1.Valid():
			return errPromotedOverLiveLease
		case !granted && !errors.Is(err, controlplane.ErrFencingWaitNotElapsed):
			return fmt.Errorf("promotion before the wait: want ErrFencingWaitNotElapsed, got %w", err)
		}

		// Past the deadline the same promotion must succeed: a checker that fires on
		// every promotion would say nothing about the early ones.
		s.Tick(fencingLeaseTTL)
		newEpoch, err = p.Promote(ctx, term, fencedVol, time.Time{}, fencedHost2)
		if err != nil {
			return fmt.Errorf("promotion after the wait: %w", err)
		}
		s.Emit(Event{
			Kind: EventPromotion, EarlyGrant: w1.Valid(),
			Msg: fmt.Sprintf("epoch=%d after the wait w1_lease_valid=%t", newEpoch, w1.Valid()),
		})
		if newEpoch != 1 {
			return fmt.Errorf("new epoch = %d, want 1", newEpoch)
		}
		return nil
	}
}

// INV-11: no epoch is granted before FENCING_WAIT elapses.
//
// FAILS: the clock jump moves the observer and the observed together, so the promotion
// it lets through is not an early one — w1's lease has expired by then and the checker
// is right to stay quiet. The fault has to reach the *deadline* rather than the clock.
func TestPlantedBugEarlyPromotion(t *testing.T) {
	requirePasses(t, 14, NewPromotionWaitChecker(), earlyPromotion(0))
	plantedBug(t, 14, NewPromotionWaitChecker(), "promotion-fencing-wait", earlyPromotion(fencingLeaseTTL))
}

// truncateAfterListingRegresses is INV-13 end to end: a checkpoint publishes a durable
// point, local WAL is reclaimed up to it, and then the volume is checkpointed again.
//
// Both numbers in the event come from the log itself — what it reclaimed and what it
// has verified as published — so the only way to make them disagree is to make real
// code move one of them. checkpoint.Create recomputes the published point from what
// the object store can prove *now* (a LIST) and hands it to Log.AdvancePublished, which
// guards the ordering against durable but not against its own past: a listing that
// went backwards would walk the published point back under WAL that no longer exists.
//
// regress asks the store for that listing. SetEventualList is the closest it can do
// today, and it is the wrong shape: it delays keys that are not yet visible and never
// takes back one it has already served, so the second checkpoint proves exactly what
// the first did, adopts it, and nothing moves.
func truncateAfterListingRegresses(regress bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[15] = 0xd1

		f, err := s.Disk.Create("wal/active.wal")
		if err != nil {
			return err
		}
		l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
		l.EnableRemote(
			wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
			wal.NewUploader(s.Store, 5),
			alwaysValidLease{},
		)
		// Two objects, so a listing can lose the newer one and still answer.
		for i, payload := range []string{"a", "b", "c"} {
			if _, err := l.Write(uint64(i)*8, []byte(payload), 0); err != nil {
				return err
			}
			if i == 1 {
				if err := l.Flush(ctx); err != nil {
					return fmt.Errorf("flush %d: %w", i, err)
				}
			}
		}
		if err := l.Flush(ctx); err != nil {
			return err
		}

		cp := checkpoint.NewCheckpointer(s.Store)
		if _, err := cp.Create(ctx, l, vol, 1); err != nil {
			return fmt.Errorf("first checkpoint: %w", err)
		}
		published := l.Watermarks().Published
		if published != l.Watermarks().Durable {
			return fmt.Errorf("the checkpoint published %d of %d durable", published, l.Watermarks().Durable)
		}
		// §21.1: published, verified, and only then is the local copy disposable.
		if err := l.TruncateLocal(published); err != nil {
			return fmt.Errorf("truncate to the published point: %w", err)
		}
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: published})

		if regress {
			s.Store.SetEventualList(true)
			s.Emit(Event{Kind: EventFault, Msg: "the listing that proves the durable point goes backwards"})
		}
		// The next checkpoint recomputes the published point from the backend.
		if _, err := cp.Create(ctx, l, vol, 1); err != nil {
			return fmt.Errorf("second checkpoint: %w", err)
		}
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: l.Watermarks().Published})
		if l.TruncatedUpTo() > l.Watermarks().Published {
			return errTruncatedAbovePublished
		}
		return nil
	}
}

// INV-13: local WAL is never truncated above the verified published point.
//
// FAILS: nothing in the object store can un-list a key it has already served, so the
// second checkpoint proves the same durable point as the first, adopts the checkpoint
// that is already there, and the published point never moves. The listing needs to be
// able to go backwards.
func TestPlantedBugTruncateAbovePublished(t *testing.T) {
	requirePasses(t, 18, NewTruncateBelowPublishedChecker(), truncateAfterListingRegresses(false))
	plantedBug(t, 18, NewTruncateBelowPublishedChecker(), "no-truncate-above-published", truncateAfterListingRegresses(true))
}

// INV-17: background I/O yields.
//
// FAILS: ioclass.Scheduler is pure in-process arbitration — no clock, no disk, no
// network, no object store — so none of the faults the simulation can produce reaches
// the decision it makes. Throttling the backend and breaking the clock under a real
// background consumer changes what materialization *achieves* and nothing about
// whether it was allowed to run.
func TestPlantedBugBackgroundDidNotYield(t *testing.T) {
	requirePasses(t, 19, NewBackgroundYieldsChecker(), scenarioCrossHostMaterialization)
	plantedBug(t, 19, NewBackgroundYieldsChecker(), "background-yields", func(s *Sim) error {
		s.Store.InjectThrottle(3)
		s.Clock.InjectMonotonicRegression(time.Second)
		return scenarioCrossHostMaterialization(s)
	})
}

// ---------------------------------------------------------------------------

// proofKind records how strong a checker's planted-bug proof is.
type proofKind int

const (
	// proofBehavioural: a fault injected into the simulated I/O makes real production
	// code violate the invariant, and the checker catches it through a real scenario.
	proofBehavioural proofKind = iota
	// proofLiteral: only the event is planted. The checker is proven to read the
	// field; nothing proves a real run could ever set it.
	proofLiteral
)

// plantedProofs maps every checker to the strength of its proof.
var plantedProofs = map[string]proofKind{
	"no-permanent-delete":         proofBehavioural,
	"immutable-snapshots":         proofBehavioural,
	"no-lost-acked-write":         proofBehavioural,
	"effective-single-writer":     proofBehavioural,
	"durable-ack-requires-lease":  proofBehavioural,
	"no-plaintext-leaves-host":    proofBehavioural,
	"monotonic-clock":             proofBehavioural,
	"watermark-order":             proofLiteral,
	"promotion-fencing-wait":      proofLiteral,
	"no-truncate-above-published": proofLiteral,
	"background-yields":           proofLiteral,
	// Contributed by scenarios_recovery.go; proofs in planted_bug_recovery_test.go.
	"boundary-monotonic":      proofLiteral,
	"durable-point-monotonic": proofLiteral,
}

// TestEveryCheckerHasAPlantedBugProof fails when a checker is added without one, so
// this file cannot fall behind DefaultCheckers again.
func TestEveryCheckerHasAPlantedBugProof(t *testing.T) {
	for _, c := range DefaultCheckers() {
		if _, ok := plantedProofs[c.Name()]; !ok {
			t.Fatalf("checker %q has no planted-bug proof: add one in this file (PLAN.md §3 stop signals)", c.Name())
		}
	}
	if len(plantedProofs) != len(DefaultCheckers()) {
		t.Fatalf("plantedProofs has %d entries for %d checkers — it drifted", len(plantedProofs), len(DefaultCheckers()))
	}
}

// TestPlantedBugCoverageIsNotSilentlyWeakened pins the number of behavioural proofs.
// Converting a literal proof to a behavioural one is progress and raises this number;
// a checker quietly downgraded to a hand-written Emit is not, and fails here.
func TestPlantedBugCoverageIsNotSilentlyWeakened(t *testing.T) {
	const wantBehavioural = 7
	got := 0
	for _, kind := range plantedProofs {
		if kind == proofBehavioural {
			got++
		}
	}
	if got != wantBehavioural {
		t.Fatalf("%d checkers have a behavioural planted-bug proof, expected %d: "+
			"raise the constant when converting one, never lower it", got, wantBehavioural)
	}
}
