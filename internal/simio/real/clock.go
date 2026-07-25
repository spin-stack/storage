// Package real holds the thin passthrough implementations of the simio
// interfaces. It is the one place (with sim) allowed to touch real time, net,
// disk, and object-store primitives (ADR-0003); it lives under internal/simio so
// the simulable analyzer exempts it.
package real

import (
	"context"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
)

// Clock is the production clock backed by the runtime.
type Clock struct {
	origin time.Time
}

// NewClock returns a Clock whose monotonic origin is now.
func NewClock() *Clock {
	return &Clock{origin: time.Now()}
}

// Now returns the monotonic instant since origin.
func (c *Clock) Now() clock.Instant {
	return clock.Instant(time.Since(c.origin))
}

// Wall returns the current wall-clock time.
func (c *Clock) Wall() time.Time {
	return time.Now()
}

// NewTimer returns a runtime-backed one-shot timer.
func (c *Clock) NewTimer(d time.Duration) clock.Timer {
	rt := &realTimer{ch: make(chan clock.Instant, 1), clk: c}
	rt.t = time.AfterFunc(d, func() { rt.ch <- c.Now() })
	return rt
}

// Sleep blocks for d or until ctx is done.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type realTimer struct {
	t   *time.Timer
	ch  chan clock.Instant
	clk *Clock
}

func (rt *realTimer) C() <-chan clock.Instant { return rt.ch }
func (rt *realTimer) Stop() bool              { return rt.t.Stop() }
