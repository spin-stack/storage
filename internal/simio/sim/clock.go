// Package sim holds the deterministic simulated implementations of the simio
// interfaces, driven by the DST harness (internal/dst). It lives under
// internal/simio so the simulable analyzer exempts it. Nothing here reads real
// time, sockets, or disk: all state is explicit and advanced by the harness.
package sim

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
)

// Clock is a deterministic virtual clock. Monotonic time advances only via
// Advance; wall time is the monotonic base plus an injectable skew (§12 drift).
type Clock struct {
	mu         sync.Mutex
	mono       clock.Instant
	wallOrigin time.Time
	skew       time.Duration
	seq        uint64 // insertion counter for deterministic timer ordering
	timers     []*simTimer
}

// NewClock returns a virtual clock. wallOrigin fixes the wall base deterministically
// (passed by the harness); no real time is read.
func NewClock(wallOrigin time.Time) *Clock {
	return &Clock{wallOrigin: wallOrigin}
}

// Now returns the current monotonic instant.
func (c *Clock) Now() clock.Instant {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

// Wall returns the current wall time (monotonic base + skew).
func (c *Clock) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wallOrigin.Add(time.Duration(c.mono) + c.skew)
}

// SetSkew injects a wall-clock offset relative to monotonic time. Used to test
// clock-drift scenarios (§12.1, §23): monotonic safety must be unaffected.
func (c *Clock) SetSkew(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.skew = d
}

// Advance moves monotonic time forward by d and fires every timer whose deadline
// is now due, in deterministic (deadline, insertion) order.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.mono += clock.Instant(d)
	now := c.mono

	sort.SliceStable(c.timers, func(i, j int) bool {
		if c.timers[i].deadline != c.timers[j].deadline {
			return c.timers[i].deadline < c.timers[j].deadline
		}
		return c.timers[i].seq < c.timers[j].seq
	})

	var due, pending []*simTimer
	for _, t := range c.timers {
		if t.deadline <= now {
			due = append(due, t)
		} else {
			pending = append(pending, t)
		}
	}
	c.timers = pending
	c.mu.Unlock()

	// Fire outside the lock; channels are buffered so sends never block.
	for _, t := range due {
		t.fire(now)
	}
}

// PendingTimers reports how many timers are registered and not yet fired. The
// DST harness uses this to detect quiescence (all goroutines blocked on time)
// before advancing the clock.
func (c *Clock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// NewTimer registers a one-shot timer firing d from now.
func (c *Clock) NewTimer(d time.Duration) clock.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &simTimer{
		deadline: c.mono + clock.Instant(d),
		seq:      c.seq,
		ch:       make(chan clock.Instant, 1),
		clk:      c,
	}
	c.seq++
	c.timers = append(c.timers, t)
	return t
}

// Sleep blocks until d has elapsed on this clock (via Advance) or ctx is done.
func (c *Clock) Sleep(ctx context.Context, d time.Duration) error {
	t := c.NewTimer(d)
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	}
}

type simTimer struct {
	deadline clock.Instant
	seq      uint64
	ch       chan clock.Instant
	clk      *Clock
	fired    bool
}

func (t *simTimer) C() <-chan clock.Instant { return t.ch }

func (t *simTimer) fire(now clock.Instant) {
	t.fired = true
	t.ch <- now
}

// Stop removes the timer if it has not fired. Reports whether it did so in time.
func (t *simTimer) Stop() bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	if t.fired {
		return false
	}
	for i, other := range t.clk.timers {
		if other == t {
			t.clk.timers = append(t.clk.timers[:i], t.clk.timers[i+1:]...)
			return true
		}
	}
	return false
}
