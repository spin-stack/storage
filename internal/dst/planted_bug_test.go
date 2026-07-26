package dst

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lease"
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
	errNotYetExpired        = errors.New("planted: the lease should already have expired")
	errLeaseResurrected     = errors.New("planted: a monotonic regression revalidated an expired lease")
	errStaleWriterPublished = errors.New("planted: a fenced writer verified its own epoch")
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

// INV-11: no epoch granted before FENCING_WAIT. The promoter and the scenario read the
// same wall clock, so skewing it moves the deadline along with the observation. Needs
// a fault double for metadata.Store (the lease row the promoter reads), which lives in
// internal/metadata/sim.
func TestPlantedBugEarlyPromotion(t *testing.T) {
	plantedBug(t, 14, NewPromotionWaitChecker(), "promotion-fencing-wait", func(s *Sim) error {
		s.Emit(Event{Kind: EventPromotion, EarlyGrant: true, Msg: "granted on a missed heartbeat"})
		return nil
	})
}

// INV-13: never truncate above the verified published point. Needs Log.TruncateLocal
// to be made to accept an upTo above published; no I/O fault reaches that decision.
func TestPlantedBugTruncateAbovePublished(t *testing.T) {
	plantedBug(t, 18, NewTruncateBelowPublishedChecker(), "no-truncate-above-published", func(s *Sim) error {
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: 50, Published: 20})
		return nil
	})
}

// INV-17: background I/O yields. The ioclass scheduler is pure in-process arbitration
// with no simulated I/O in it, so inverting it means a seam in internal/ioclass.
func TestPlantedBugBackgroundDidNotYield(t *testing.T) {
	plantedBug(t, 19, NewBackgroundYieldsChecker(), "background-yields", func(s *Sim) error {
		s.Emit(Event{Kind: EventIOClass, BgGranted: true, HighInFlight: true})
		return nil
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
