package dst

import (
	"errors"
	"flag"
	"fmt"
	"os"
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
	proven[wantName] = true
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

// plantedProofs is the registry the checks below hold to the checker list. It shrank
// with ADR-0026: every entry removed went with the checker it named, and every one of
// those checkers went with its subject.
//
// **It is no longer only a declaration.** It used to be a map nothing cross-checked
// against the tests, and on 2026-08-03 two of its six entries were fiction:
// `effective-single-writer` claimed a behavioural proof while nothing emitted the event
// its checker reads — the scenario that did had gone with promotion — and
// `watermark-order` claimed one that was never written. A registry of claims about
// proofs is the same failure mode as a checker that cannot fire, which is this project's
// oldest lesson, and it survived because the test asserted the *map* matched the checker
// list rather than asserting the proofs exist. TestMain now closes that: plantedBug
// records what it actually proved, and a checker nobody exercised fails the package.
var plantedProofs = map[string]proofKind{
	"effective-single-writer":  proofBehavioural,
	"no-plaintext-leaves-host": proofBehavioural,
	"monotonic-clock":          proofBehavioural,
	// Literal, and honestly so: the ordering is enforced at the source
	// (Log.AdvanceDurable/AdvancePublished return ErrWatermarkOrder, unit-tested), so no
	// fault in the simulated disk, store or clock can make production emit an
	// out-of-order triple. The checker is a backstop against a *reporting* path that
	// computes them separately, and the only way to reach it is to plant the event.
	"watermark-order": proofLiteral,
	// Contributed by scenarios_agent.go; proofs in planted_bug_agent_test.go.
	"fenced-volume-not-served":       proofBehavioural,
	"durable-range-survives-restart": proofBehavioural,
	// Contributed by scenarios_carry.go; proof in planted_bug_carry_test.go.
	"acked-records-cross-epochs-intact": proofBehavioural,
	// Contributed by scenarios_refusal.go; proof in planted_bug_refusal_test.go.
	"refused-volume-has-no-device": proofBehavioural,
}

// proven records which checkers a plantedBug call actually exercised in this run.
var proven = map[string]bool{}

// TestMain asserts, after every test in the package has run, that each default checker
// was shown to catch a planted violation. Checking after the run rather than inside a
// test is what makes it independent of the order Go happens to execute them in.
func TestMain(m *testing.M) {
	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}
	// Only when the whole package ran. Under a -run filter the proofs are simply absent
	// rather than missing, and failing there would make every single-test invocation red
	// — which is how a check like this gets switched off.
	if f := flag.Lookup("test.run"); f == nil || f.Value.String() != "" {
		os.Exit(code)
	}
	var missing []string
	for _, c := range DefaultCheckers() {
		if !proven[c.Name()] {
			missing = append(missing, c.Name())
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		fmt.Fprintf(os.Stderr, "checkers with no planted-bug proof that actually ran: %v\n"+
			"A checker that has never been shown to catch a violation proves nothing (CLAUDE.md stop signal).\n",
			missing)
		os.Exit(1)
	}
	os.Exit(code)
}

// TestEveryCheckerHasAPlantedBugProof fails when a checker is added without a registry
// entry. TestMain is what proves the entry is true; this is what keeps the registry —
// and its proofKind, which nothing else records — from falling behind DefaultCheckers.
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

// The watermark backstop, planted literally — see the registry entry for why that is the
// only way in, and internal/wal's ErrWatermarkOrder tests for where it is really enforced.
func TestPlantedBugWatermarkOrder(t *testing.T) {
	ordered := func(s *Sim) error {
		s.Emit(Event{Kind: EventWatermark, Local: 30, Durable: 20, Published: 10})
		return nil
	}
	requirePasses(t, 41, NewWatermarkOrderChecker(), ordered)
	plantedBug(t, 41, NewWatermarkOrderChecker(), "watermark-order", func(s *Sim) error {
		s.Emit(Event{Kind: EventWatermark, Local: 10, Durable: 20, Published: 30})
		return nil
	})
}

// TestPlantedBugCoverageIsNotSilentlyWeakened pins the number of behavioural proofs.
// Converting a literal proof to a behavioural one is progress and raises this number;
// a checker quietly downgraded to a hand-written Emit is not, and fails here.
//
// The history of the decreases matters more than the number, because a decrease is the
// one thing this test cannot distinguish from the weakening it exists to catch:
//
//   - 16 -> 15 (2026-08-02): `no-permanent-delete` removed *with its subject* —
//     ADR-0026 deleted internal/gc, so nothing issues a delete.
//   - 15 -> 14 (2026-08-02): the checkpoint-lease checker, same shape — the durability
//     scheduler was withdrawn, so nothing publishes a checkpoint.
//   - 8 -> 7 (2026-08-02): `durable-ack-requires-lease` (INV-06), with the lease-gated ACK.
//   - 7 -> 6, then 6 -> 5 (2026-08-03): `watermark-order` was *reclassified*, not
//     removed. It had claimed a behavioural proof that did not exist; it is literal now
//     and has a proof that runs. The number went down because the record became true.
//   - 5 -> 6 (2026-08-09): `acked-records-cross-epochs-intact`, with the carry-forward
//     scenario. An increase, which is the only direction that needs no defence.
//   - 6 -> 7 (2026-08-09): `refused-volume-has-no-device`, with the refusal scenario.
func TestPlantedBugCoverageIsNotSilentlyWeakened(t *testing.T) {
	const wantBehavioural = 7
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
