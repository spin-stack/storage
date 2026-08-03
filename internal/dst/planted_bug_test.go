package dst

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

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
	errNotYetExpired    = errors.New("planted: the lease should already have expired")
	errLeaseResurrected = errors.New("planted: a monotonic regression revalidated an expired lease")
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

// proofKind says how a checker's planted bug reaches it.
type proofKind int

const (
	// proofBehavioural: a fault injected into the simulated I/O makes real production
	// code produce the violation.
	proofBehavioural proofKind = iota
	// proofLiteral: only the event is planted.
	proofLiteral
)

// plantedProofs is the registry TestEveryCheckerHasAPlantedBugProof holds to the
// checker list. It shrank with ADR-0026: every entry removed went with the checker it
// named, and every one of those checkers went with its subject.
var plantedProofs = map[string]proofKind{
	"effective-single-writer":  proofBehavioural,
	"no-plaintext-leaves-host": proofBehavioural,
	"monotonic-clock":          proofBehavioural,
	"watermark-order":          proofBehavioural,
	// Contributed by scenarios_agent.go; proofs in planted_bug_agent_test.go.
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
	// 8 -> 7 with ADR-0026 increment 4.5: durable-ack-requires-lease (INV-06) went with
	// the lease-gated ACK. Every decrease in this constant so far has been a checker
	// removed *with its subject*, which is the one shape this test allows — the comment
	// beside it is what separates that from the weakening it exists to catch.
	const wantBehavioural = 6
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
