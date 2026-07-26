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

// InjectMonotonicRegression steps monotonic time *backwards* by d. Nothing legitimate
// does this: it models the clock source itself breaking — a VM resumed from a
// snapshot, a live migration, a hypervisor serving a bad CLOCK_MONOTONIC — which is a
// fault the design has no defence against and every lease in the system trusts (§12.1).
//
// It exists so a checker can be proven to catch it. A lease that had correctly expired
// reports itself valid again after a regression, un-fencing a writer the Control Plane
// has already replaced; without an injector that outcome cannot be produced from a
// scenario, and the monotonic-clock checker can only be proven against a fabricated
// event. Timers are left where they are: a deadline already passed does not un-fire.
func (c *Clock) InjectMonotonicRegression(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if step := clock.Instant(d); step > c.mono {
		c.mono = 0
	} else {
		c.mono -= step
	}
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

// fire marks the timer fired and delivers the instant. The flag is set under the
// clock's lock because Stop reads it there: without that, a Sleep whose context is
// cancelled while another goroutine Advances is a data race, and the race is not
// theoretical — it decides whether Stop reports that it removed a timer that had
// already fired. The send happens outside the lock: the channel is buffered, and
// holding the clock while delivering would let a receiver's next clock call deadlock
// against the sender.
func (t *simTimer) fire(now clock.Instant) {
	t.clk.mu.Lock()
	t.fired = true
	t.clk.mu.Unlock()
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
