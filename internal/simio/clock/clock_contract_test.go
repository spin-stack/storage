package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// clockUnderTest adapts an implementation for the shared contract: advance moves
// time forward by d (real: wall sleep; sim: deterministic Advance). waitBlocked
// waits until a goroutine that is about to be woken has registered its timer
// (real: no-op, since real time elapses on its own; sim: poll PendingTimers).
type clockUnderTest struct {
	name        string
	clk         clock.Clock
	advance     func(d time.Duration)
	waitBlocked func()
}

func impls() []clockUnderTest {
	rc := real.NewClock()
	sc := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return []clockUnderTest{
		{
			name:        "real",
			clk:         rc,
			advance:     func(d time.Duration) { time.Sleep(d) },
			waitBlocked: func() {},
		},
		{
			name:    "sim",
			clk:     sc,
			advance: func(d time.Duration) { sc.Advance(d) },
			waitBlocked: func() {
				for sc.PendingTimers() == 0 {
					time.Sleep(time.Millisecond)
				}
			},
		},
	}
}

func TestSimTimerStopAfterFire(t *testing.T) {
	sc := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	timer := sc.NewTimer(10 * time.Millisecond)
	sc.Advance(20 * time.Millisecond) // fires
	<-timer.C()
	if timer.Stop() {
		t.Fatal("Stop after fire should report false")
	}
}

func TestInstantArithmetic(t *testing.T) {
	var base clock.Instant
	later := base.Add(5 * time.Second)
	if later.Sub(base) != 5*time.Second {
		t.Fatalf("Add/Sub mismatch: %v", later.Sub(base))
	}
	if base.Add(time.Second).Sub(base.Add(3*time.Second)) != -2*time.Second {
		t.Fatal("negative delta expected")
	}
}

func TestNowIsMonotonic(t *testing.T) {
	for _, ut := range impls() {
		t.Run(ut.name, func(t *testing.T) {
			prev := ut.clk.Now()
			for range 100 {
				ut.advance(time.Millisecond)
				cur := ut.clk.Now()
				if cur < prev {
					t.Fatalf("monotonic clock regressed: %d -> %d", prev, cur)
				}
				prev = cur
			}
		})
	}
}

func TestWallProgresses(t *testing.T) {
	for _, ut := range impls() {
		t.Run(ut.name, func(t *testing.T) {
			w0 := ut.clk.Wall()
			ut.advance(10 * time.Millisecond)
			w1 := ut.clk.Wall()
			if !w1.After(w0) && !w1.Equal(w0) {
				t.Fatalf("wall went backwards: %v -> %v", w0, w1)
			}
		})
	}
}

func TestTimerFiresAfterAdvance(t *testing.T) {
	for _, ut := range impls() {
		t.Run(ut.name, func(t *testing.T) {
			timer := ut.clk.NewTimer(20 * time.Millisecond)
			// Not yet due.
			select {
			case <-timer.C():
				t.Fatal("timer fired before its deadline")
			default:
			}
			ut.advance(25 * time.Millisecond)
			select {
			case fired := <-timer.C():
				if fired < ut.clk.Now()-clock.Instant(50*time.Millisecond) {
					t.Fatalf("fired instant implausible: %d", fired)
				}
			case <-time.After(time.Second):
				t.Fatal("timer did not fire after advancing past its deadline")
			}
		})
	}
}

func TestSleepReturns(t *testing.T) {
	for _, ut := range impls() {
		t.Run(ut.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- ut.clk.Sleep(context.Background(), 15*time.Millisecond) }()
			ut.waitBlocked()
			ut.advance(20 * time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Sleep returned error: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Sleep did not return after advancing past its duration")
			}
		})
	}
}

func TestSleepHonorsContext(t *testing.T) {
	for _, ut := range impls() {
		t.Run(ut.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- ut.clk.Sleep(ctx, time.Hour) }()
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("Sleep should return ctx error on cancel")
				}
			case <-time.After(time.Second):
				t.Fatal("Sleep did not observe context cancellation")
			}
		})
	}
}

// TestSimSkewDoesNotAffectMonotonic verifies the §12.1 property: injecting wall
// skew must not perturb monotonic time (lease safety depends only on monotonic).
func TestSimSkewDoesNotAffectMonotonic(t *testing.T) {
	sc := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	before := sc.Now()
	sc.SetSkew(5 * time.Second)
	if got := sc.Now(); got != before {
		t.Fatalf("skew changed monotonic time: %d -> %d", before, got)
	}
	// Wall reflects the skew.
	w := sc.Wall()
	if want := time.Unix(1_700_000_005, 0).UTC(); !w.Equal(want) {
		t.Fatalf("wall skew not applied: got %v want %v", w, want)
	}
}
