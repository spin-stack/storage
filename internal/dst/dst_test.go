package dst_test

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/dst"
)

var seeds = []int64{1, 2, 42, 1337, 2024, 99999}

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

// TestPlantedBugMonotonicClock proves the MonotonicClockChecker actually catches a
// violation (not merely present) and that the failure reports the reproducing seed.
func TestPlantedBugMonotonicClock(t *testing.T) {
	buggy := func(s *dst.Sim) error {
		// Emit a regressing clock event directly (a broken component would).
		s.Emit(dst.Event{Kind: dst.EventClock, Mono: 100})
		s.Emit(dst.Event{Kind: dst.EventClock, Mono: 50}) // regression
		return nil
	}
	res := dst.Run(777, buggy, dst.NewMonotonicClockChecker())
	if res.Err == nil {
		t.Fatal("expected the monotonic-clock checker to catch the planted regression")
	}
	if !strings.Contains(res.Err.Error(), "seed=777") {
		t.Fatalf("failure must report the reproducing seed, got: %v", res.Err)
	}
	if !strings.Contains(res.Err.Error(), "monotonic-clock") {
		t.Fatalf("failure must name the checker, got: %v", res.Err)
	}
}

// TestPlantedBugPermanentDelete proves the NoPermanentDeleteChecker catches an
// irreversible delete (the INV-14 seed).
func TestPlantedBugPermanentDelete(t *testing.T) {
	buggy := func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventDelete, Key: "wal/live-object", Permanent: true})
		return nil
	}
	res := dst.Run(1234, buggy, dst.NewNoPermanentDeleteChecker())
	if res.Err == nil {
		t.Fatal("expected the no-permanent-delete checker to catch the planted delete")
	}
	if !strings.Contains(res.Err.Error(), "seed=1234") {
		t.Fatalf("failure must report the reproducing seed, got: %v", res.Err)
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
	for _, want := range []string{"monotonic-clock", "no-permanent-delete", "watermark-order", "no-plaintext-leaves-host"} {
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
