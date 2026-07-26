package sim_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestStopRacesAFiringTimer: a Sleep whose context is cancelled while another
// goroutine advances the clock has both paths touching the timer's fired flag — Stop
// reads it under the clock's lock, and firing used to write it outside. The flag
// decides whether Stop reports success, so the race was not benign, and -race only
// catches it when the two land together. Driven here rather than left to whichever
// scenario happens to interleave them.
func TestStopRacesAFiringTimer(t *testing.T) {
	for range 50 {
		clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
		ctx, cancel := context.WithCancel(t.Context())
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = clk.Sleep(ctx, time.Second)
		}()
		go func() {
			defer wg.Done()
			clk.Advance(time.Second)
		}()
		cancel()
		wg.Wait()
	}
}
