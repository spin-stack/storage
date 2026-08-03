package dst

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
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
// a listing that goes backwards, a clock that goes backwards, a lease row read from a
// replica, a lease checker that keeps saying yes, a volume created without encryption,
// a background consumer wired without a scheduler. Every one is an operational
// reality, and each is injected into the simulated I/O or into how the code under test
// is built — never into the code itself.
//
// Every checker now has one. TestPlantedBugCoverageIsNotSilentlyWeakened pins the
// count, so a checker quietly downgraded to a hand-written Emit fails there rather than
// disappearing into the absence of a test.
//
// Two of them are planted at a seam rather than at an I/O fault: INV-03's ordering
// rules and INV-17's arbitration are comparisons over numbers held in memory, and no
// disk, clock or object-store fault changes their answer. Those plants substitute one
// named policy (wal.OrderPolicy) or omit one constructor argument (the scheduler a
// background consumer is handed), which is how both invariants are actually lost —
// never by editing the code under test.

// Planted-bug outcomes. A behavioural planted bug also trips the scenario's own
// assertions; these name what went wrong for a reader of a failing run.
var (
	errNotYetExpired           = errors.New("planted: the lease should already have expired")
	errLeaseResurrected        = errors.New("planted: a monotonic regression revalidated an expired lease")
	errPublishedAheadOfDurable = errors.New("planted: a record no object store holds was reclaimed as published")
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

// publishAnything is the one ordering rule this plant removes: published may move
// anywhere, durable and truncation stay exactly as production has them (wal.StrictOrder
// is zero-size, so embedding it keeps the other two rules verbatim rather than
// reimplementing them here).
//
// One rule, not three. A Log with no rules at all would prove nothing about which
// check the checker depends on, and a checker that only fires when everything is off
// is not a regression test for anything.
type publishAnything struct{ wal.StrictOrder }

func (publishAnything) AllowPublished(uint64, wal.Watermarks) error { return nil }

// publishedAheadOfDurable is INV-03 where it costs something. A log holds three
// records and has ACKed two of them: local=3, durable=2, published=0. Advancing
// published to 3 claims that a verified checkpoint covers a record the object store
// has never seen — and INV-13, still strict, then *correctly* allows the local copy of
// that record to be reclaimed, because published is exactly what INV-13 trusts. The
// record exists nowhere afterwards.
//
// The move is the real one a checkpointer makes (Log.AdvancePublished, §21.1 step 2)
// and the trio is the log's own. relax substitutes the ordering policy behind that one
// move and leaves the other two rules strict, so what the checker sees is one rule
// missing rather than a Log with no rules at all.
func publishedAheadOfDurable(relax bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[15] = 0xd2

		l := wal.NewLog(s.Disk, "wal", s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
		l.EnableRemote(
			wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
			wal.NewUploader(s.Store, 5),
			alwaysValidLease{},
		)
		for i, payload := range []string{"acked", "acked-too", "never-uploaded"} {
			if _, err := l.Write(uint64(i)*8, []byte(payload), 0); err != nil {
				return err
			}
			if i == 1 {
				if err := l.Flush(ctx); err != nil {
					return fmt.Errorf("flush: %w", err)
				}
			}
		}
		// Record 3 has to sit in a segment that is not the newest, because the newest
		// is the one reclamation never takes. Sealing here is what an ordinary
		// checkpoint would have done a moment later; the fourth record opens the next
		// segment and is no more durable than the third.
		if err := l.Seal(); err != nil {
			return fmt.Errorf("seal: %w", err)
		}
		if _, err := l.Write(24, []byte("also-never-uploaded"), 0); err != nil {
			return err
		}
		emitWatermarks(s, l)
		w := l.Watermarks()
		if w.Local != 4 || w.Durable != 2 {
			return fmt.Errorf("setup: local=%d durable=%d, want 4 and 2", w.Local, w.Durable)
		}

		if relax {
			l.SetOrderPolicy(publishAnything{})
			s.Emit(Event{Kind: EventFault, Msg: "the published watermark stopped being checked against durable"})
		}
		// What a checkpoint's second step does, for a checkpoint that covers a record
		// S3 does not hold.
		switch err := l.AdvancePublished(w.Local); {
		case relax && err != nil:
			return fmt.Errorf("the relaxed policy still refused the move: %w", err)
		case !relax && !errors.Is(err, wal.ErrWatermarkOrder):
			return fmt.Errorf("publishing above durable: want ErrWatermarkOrder, got %v", err)
		}
		emitWatermarks(s, l)
		if !relax {
			return nil
		}

		// INV-13 is untouched and does its job against a number that is now a lie: the
		// only copy of record 3 is reclaimed.
		if err := l.TruncateLocal(l.Watermarks().Published); err != nil {
			return fmt.Errorf("truncate to the published point: %w", err)
		}
		left, err := wal.ReplaySegments(s.Disk, "wal", vol, 1)
		if err != nil {
			return err
		}
		for _, r := range left {
			if r.Sequence == 3 {
				return errors.New("the plant did not reach the disk: the record no object store " +
					"holds is still in the local WAL")
			}
		}
		return errPublishedAheadOfDurable
	}
}

// INV-03: published <= durable <= local at every observation.
//
// FAILS: the ordering rules are three comparisons over numbers held in memory and the
// Log applies them itself, so the trio it reports is ordered by construction whatever
// the disk and the object store do. Driving the real move only produces the error the
// Log is supposed to return.
func TestPlantedBugPublishedAheadOfDurable(t *testing.T) {
	requirePasses(t, 20, NewWatermarkOrderChecker(), publishedAheadOfDurable(false))
	plantedBug(t, 20, NewWatermarkOrderChecker(), "watermark-order", publishedAheadOfDurable(true))
}

// ---------------------------------------------------------------------------

// proofKind records how strong a checker's planted-bug proof is.
type proofKind int

const (
	// proofBehavioural: a fault injected into the simulated I/O makes real production
	// code violate the invariant, and the checker catches it through a real scenario.
	proofBehavioural proofKind = iota
	// proofLiteral: only the event is planted. The checker is proven to read the
	// field; nothing proves a real run could ever set it. No checker is here now; the
	// kind stays so a new checker can be added honestly before its proof exists.
	proofLiteral
)

// plantedProofs maps every checker to the strength of its proof.
var plantedProofs = map[string]proofKind{
	"effective-single-writer":    proofBehavioural,
	"durable-ack-requires-lease": proofBehavioural,
	"no-plaintext-leaves-host":   proofBehavioural,
	"monotonic-clock":            proofBehavioural,
	"background-yields":          proofBehavioural,
	"watermark-order":            proofBehavioural,
	// Contributed by scenarios_recovery.go; proofs in planted_bug_recovery_test.go.
	// Contributed by scenarios_agent.go; proof in planted_bug_agent_test.go.
	"fenced-volume-not-served":       proofBehavioural,
	"durable-range-survives-restart": proofBehavioural,
}

// TestEveryCheckerHasAPlantedBugProof fails when a checker is added without one, so
// this file cannot fall behind DefaultCheckers again.
func TestEveryCheckerHasAPlantedBugProof(t *testing.T) {
	for _, c := range DefaultCheckers() {
		if _, ok := plantedProofs[c.Name()]; !ok {
			t.Fatalf("checker %q has no planted-bug proof: add one in this file (a CLAUDE.md stop signal)", c.Name())
		}
	}
	if len(plantedProofs) != len(DefaultCheckers()) {
		t.Fatalf("plantedProofs has %d entries for %d checkers — it drifted", len(plantedProofs), len(DefaultCheckers()))
	}
}

// TestPlantedBugCoverageIsNotSilentlyWeakened pins the number of behavioural proofs.
// Converting a literal proof to a behavioural one is progress and raises this number;
// a checker quietly downgraded to a hand-written Emit is not, and fails here.
//
// It went 16 -> 15 on 2026-08-02, which is the one shape of decrease this test is not
// meant to stop: `no-permanent-delete` was removed *with its subject*. ADR-0026 deleted
// internal/gc, nothing issues a delete any more, and a checker that cannot fire proves
// nothing. INV-14 is `pending` rather than dropped — the number goes back up with the
// sweeper. A decrease for any other reason is the weakening this test exists to catch,
// and the comment is the difference between the two.
func TestPlantedBugCoverageIsNotSilentlyWeakened(t *testing.T) {
	// 15 -> 14 on 2026-08-02, and again the decrease is the shape this test allows: the
	// checkpoint-lease checker was removed *with its subject*. ADR-0026 withdrew the
	// durability scheduler, so nothing publishes a checkpoint and the checker could not
	// fire. A decrease for any other reason is the weakening this exists to catch.
	const wantBehavioural = 8
	got := 0
	var literal []string
	for name, kind := range plantedProofs {
		switch kind {
		case proofBehavioural:
			got++
		case proofLiteral:
			literal = append(literal, name)
		}
	}
	if got != wantBehavioural {
		sort.Strings(literal)
		t.Fatalf("%d checkers have a behavioural planted-bug proof, expected %d "+
			"(literal: %v): raise the constant when converting one, never lower it",
			got, wantBehavioural, literal)
	}
}
