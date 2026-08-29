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
// through a scenario driving real code: a clock that goes backwards, a lease that
// revalidates itself over it. The fault is injected into the simulated I/O or into how
// the code under test is built — never into the code itself.
//
// Every checker still has one: see coreCheckers for why three left with the engine that
// emitted their events, and what brings them back.
// TestPlantedBugCoverageIsNotSilentlyWeakened pins the count, so a checker quietly
// downgraded to a hand-written Emit fails there rather than disappearing into the
// absence of a test.

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

// plantedProofs is the registry the checks below hold to the checker list.
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
//
// That history is why this file shrank to one entry rather than keeping the other three
// against the day their subjects return. Three entries naming checkers nothing can emit
// for, proved by hand-written Emits, is precisely the fiction the paragraph above
// describes — the same shape, arrived at from the other direction.
var plantedProofs = map[string]proofKind{
	"monotonic-clock":                    proofBehavioural,
	"effective-single-writer":            proofBehavioural,
	"sealed-layers-publish-oldest-first": proofBehavioural,
	"no-live-layer-is-swept":             proofLiteral,
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

// TestPlantedBugCoverageIsNotSilentlyWeakened pins the number of behavioural proofs.
// Converting a literal proof to a behavioural one is progress and raises this number;
// a checker quietly downgraded to a hand-written Emit is not, and fails here.
//
// The history of the decreases matters more than the number, because a decrease is the
// one thing this test cannot distinguish from the weakening it exists to catch:
//
//   - 16 -> 15 (2026-08-02): `no-permanent-delete` removed *with its subject* —
//     ADR-0026 deleted internal/gc, so nothing issues a delete.
//
//   - 15 -> 14 (2026-08-02): the checkpoint-lease checker, same shape — the durability
//     scheduler was withdrawn, so nothing publishes a checkpoint.
//
//   - 8 -> 7 (2026-08-02): `durable-ack-requires-lease` (INV-06), with the lease-gated ACK.
//
//   - 7 -> 6, then 6 -> 5 (2026-08-03): `watermark-order` was *reclassified*, not
//     removed. It had claimed a behavioural proof that did not exist; it is literal now
//     and has a proof that runs. The number went down because the record became true.
//
//   - 5 -> 6 (2026-08-09): `acked-records-cross-epochs-intact`, with the carry-forward
//     scenario. An increase, which is the only direction that needs no defence.
//
//   - 6 -> 7 (2026-08-09): `refused-volume-has-no-device`, with the refusal scenario.
//
//   - 7 -> 1 (2026-08-22): the local block engine was withdrawn — QEMU owns the local
//     copy-on-write format through qcow2 from here on — and six checkers went with the
//     subjects they observed: `no-plaintext-leaves-host` and `watermark-order` (a WAL
//     that no longer exists to seal payloads or advance watermarks),
//     `effective-single-writer` and `durable-range-survives-restart` (an image nothing
//     publishes), `fenced-volume-not-served` and `refused-volume-has-no-device` (a
//     device nothing serves), and `acked-records-cross-epochs-intact` (a carry-forward
//     with no records to carry). This is the largest single decrease in this log and
//     every one of them is the "removed with its subject" case, not a weakening. The
//     commit protocol reinstates the subjects and this number climbs back.
//
//   - 2026-08-23, +1: `effective-single-writer` is back, and behaviourally. Its planted
//     bug is the one §6.1 names — an object store whose conditional writes are advisory
//     — injected into the simulated store and not into the protocol, and the scenario
//     that carries it is two hosts racing to publish onto one HEAD. This is the entry
//     that was fiction in 2026-08-03's audit; it is not fiction now.
//
//   - 2026-08-29, +1: `sealed-layers-publish-oldest-first`, the reconciler's derivation.
//     Its planted bug is a disk that acknowledges an fsync it does not honour, injected
//     into the simulated disk, and the power failure that then rolls state.json back a
//     version. Behavioural: the production derivation reads the rolled-back record, finds
//     the layer between the two lost writes nowhere in it, and publishes past it.
func TestPlantedBugCoverageIsNotSilentlyWeakened(t *testing.T) {
	const wantBehavioural = 3
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

// The published history is a chain, and the compare-and-set on HEAD is the only thing
// making it one (INV-10, v6 §9). The planted bug is a backend whose conditional writes
// are advisory — §6.1's own worked example, and the reason `task backend:conformance` is
// blocking per backend — injected into the simulated store, never into the protocol.
//
// What makes it a real proof rather than a demonstration: with preconditions ignored the
// losing host is handed a *success*. There is no error left anywhere for a scenario to
// assert on, and the only surviving evidence is the shape of what was published, which
// is exactly what this checker reads.
func TestSingleWriterCheckerCatchesAdvisoryPreconditions(t *testing.T) {
	const seed = 20260823
	requirePasses(t, seed, NewSingleWriterChecker(), scenarioTwoHostsCannotBothPublish)
	plantedBug(t, seed, NewSingleWriterChecker(), "effective-single-writer", func(s *Sim) error {
		s.Store.InjectIgnorePreconditions()
		s.Emit(Event{Kind: EventFault, Msg: "the object store's conditional writes are advisory"})
		return scenarioTwoHostsCannotBothPublish(s)
	})
}

// A layer that was sealed and never published is a hole in the history that nothing
// reports: every manifest on the chain is well formed, every digest matches, every object
// is there, and the guest writes that were in the skipped layer are gone. The derivation
// is what stands between the system and that — a layer is sealed because it is under the
// tip, not because a record says so.
//
// The planted bug is the disk telling the truth about everything except durability: a
// volatile write cache with no flush, so state records are acknowledged and not
// persisted, and then a power failure. It reaches the derivation the only way anything
// can, through what is left on disk, and it is a fault of the device and not an edit to
// the code under test.
//
// What makes it a real proof: the crash on its own is survivable and the unplanted run
// proves it — the tip is re-observed on the way back up and the layers under it are still
// owed. With the acknowledged writes gone, the layer between them is in no record, no
// error is returned anywhere, and the next commit is published straight past it. The only
// surviving evidence is the order the layers left the host in, which is what the checker
// reads.
// **A literal proof, and the reason is worth stating because this file is hostile to
// them.** Three behavioural routes were built and every one is closed by the rule as it
// stands, which is the finding rather than a shortcoming:
//
//   - the record acknowledged and not persisted, then a power failure: the pointer
//     survives, does not match the record, and readPointer refuses the whole sweep;
//   - the record *and* the pointer lost together, so the two are consistent with each
//     other and a power failure out of date: the layer the guest is on is missing from
//     both, and asking the running QEMU is what puts it back in the keep-set;
//   - both of those plus a QMP socket that stops answering, so there is nobody to ask:
//     an unknown live image stops the sweep instead of shortening it.
//
// Each was written, run, and watched go red before the line that closes it existed. The
// second is the sharp one and it is still checkable in one edit: delete the
// `keep[LayerIDOfImage(open.Path)]` line in internal/qcow/sweep.go and
// `sweepScenario(true)` removes the layer a guest is writing into, which is how that line
// came to be there.
//
// What is left for the checker itself is whether it can fire at all, and that is what a
// planted event settles.
func TestLiveLayerCheckerFires(t *testing.T) {
	const seed = 20260830
	requirePasses(t, seed, NewLiveLayerChecker(), sweepScenario(false))
	plantedBug(t, seed, NewLiveLayerChecker(), "no-live-layer-is-swept", func(s *Sim) error {
		s.Emit(Event{Kind: EventChain, VolumeID: "vol", Layers: []string{"layer-a", "layer-b"}})
		s.Emit(Event{Kind: EventSweep, LayerID: "layer-b"})
		return nil
	})
}

func TestSealOrderCheckerCatchesALostFsync(t *testing.T) {
	const seed = 20260829
	requirePasses(t, seed, NewSealOrderChecker(), reconcileScenario(false))
	plantedBug(t, seed, NewSealOrderChecker(), "sealed-layers-publish-oldest-first", reconcileScenario(true))
}
