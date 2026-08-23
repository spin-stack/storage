package dst_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/spin-stack/storage/internal/dst"
)

// seeds are the six the gate runs on every push, plus however many `task soak` asks for.
//
// The six are pinned and are not a sample: a scenario that fails on one of them fails on
// every machine, in every branch, for ever, which is what makes a DST failure a bug
// report rather than a rumour. SOAK_SEEDS widens the set for a long unattended run and
// cannot narrow it — a soak that could turn the gate down would be a way to make a
// failing seed disappear.
//
// The extra seeds are consecutive from a fixed origin rather than drawn, so a soak that
// finds something names a number anyone can re-run with `SOAK_SEEDS=<n> go test`.
var seeds = soakSeeds()

func soakSeeds() []int64 {
	pinned := []int64{1, 2, 42, 1337, 2024, 99999}
	extra, err := strconv.Atoi(os.Getenv("SOAK_SEEDS"))
	if err != nil || extra <= 0 {
		return pinned
	}
	for i := range int64(extra) {
		pinned = append(pinned, 1_000_000+i)
	}
	return pinned
}

// TestMandatoryScenarios runs the §25.1 mandatory set across several seeds with
// the default checkers; every run must pass.
func TestMandatoryScenarios(t *testing.T) {
	for _, sc := range dst.MandatoryScenarios() {
		for _, seed := range seeds {
			t.Run(sc.Name, func(t *testing.T) {
				res := dst.Run(seed, sc.Run, dst.DefaultCheckers()...)
				if res.Err != nil {
					t.Fatalf("seed=%d failed: %v\n--- trace ---\n%s", seed, res.Err, res.TraceString())
				}
			})
		}
	}
}

// TestDeterministicReplay is INV-02: the same seed and scenario yield an identical
// trace and identical outcome.
func TestDeterministicReplay(t *testing.T) {
	for _, sc := range dst.MandatoryScenarios() {
		for _, seed := range seeds {
			a := dst.Run(seed, sc.Run, dst.DefaultCheckers()...)
			b := dst.Run(seed, sc.Run, dst.DefaultCheckers()...)
			if (a.Err == nil) != (b.Err == nil) {
				t.Fatalf("%s seed=%d outcome differs: %v vs %v", sc.Name, seed, a.Err, b.Err)
			}
			if len(a.Trace) != len(b.Trace) {
				t.Fatalf("%s seed=%d trace length differs: %d vs %d", sc.Name, seed, len(a.Trace), len(b.Trace))
			}
			for i := range a.Trace {
				if a.Trace[i] != b.Trace[i] {
					t.Fatalf("%s seed=%d trace diverged at %d:\n a: %s\n b: %s", sc.Name, seed, i, a.Trace[i], b.Trace[i])
				}
			}
		}
	}
}

// TestCheckerNamesAndTrace exercises the checker metadata and the trace rendering.
func TestCheckerNamesAndTrace(t *testing.T) {
	names := map[string]bool{}
	for _, c := range dst.DefaultCheckers() {
		names[c.Name()] = true
		// Observing an unrelated event kind must be a harmless no-op.
		c.Observe(dst.Event{Kind: dst.EventNote, Msg: "ignored"})
	}
	// One name, and coreCheckers says why: every other checker read an event only the
	// local block engine emitted, and went with it on 2026-08-22. "no-permanent-delete"
	// left the same way on 2026-08-02 with internal/gc (ADR-0026).
	for _, want := range []string{"monotonic-clock"} {
		if !names[want] {
			t.Fatalf("DefaultCheckers missing %q", want)
		}
	}

	res := dst.Run(1, func(s *dst.Sim) error {
		s.Tick(1_000)
		s.Notef("hello")
		return nil
	})
	if res.TraceString() == "" {
		t.Fatal("TraceString should render the recorded events")
	}
}

// TestPassingRunHasNoError is a control: a benign scenario passes both checkers.
func TestPassingRunHasNoError(t *testing.T) {
	res := dst.Run(1, func(s *dst.Sim) error {
		s.Tick(1_000_000)
		s.Emit(dst.Event{Kind: dst.EventDelete, Key: "k", Permanent: false})
		return nil
	}, dst.DefaultCheckers()...)
	if res.Err != nil {
		t.Fatalf("benign run should pass, got: %v", res.Err)
	}
}
