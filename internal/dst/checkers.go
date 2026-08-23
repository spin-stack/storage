package dst

import (
	"fmt"

	"github.com/spin-stack/storage/internal/simio/clock"
)

// Checker observes the event stream and, at the end of a run, reports whether an
// invariant held. Observe is called for every event in order; Check is called
// once after the scenario completes. A checker must be able to catch a violation
// (proven by the planted-bug test), not merely be present.
//
// This is the substrate every later data invariant plugs into (INV-03..INV-21):
// each phase adds a Checker here and flips its invariant to `active`.
type Checker interface {
	Name() string
	Observe(Event)
	Check() error
}

// MonotonicClockChecker enforces INV: the monotonic clock never regresses (§12.1,
// the basis of lease safety). It watches clock events and remembers the first
// regression it sees.
type MonotonicClockChecker struct {
	last      clock.Instant
	seen      bool
	violation error
}

// NewMonotonicClockChecker returns a fresh checker.
func NewMonotonicClockChecker() *MonotonicClockChecker { return &MonotonicClockChecker{} }

func (c *MonotonicClockChecker) Name() string { return "monotonic-clock" }

func (c *MonotonicClockChecker) Observe(e Event) {
	if e.Kind != EventClock {
		return
	}
	if c.seen && e.Mono < c.last {
		if c.violation == nil {
			c.violation = fmt.Errorf("monotonic clock regressed at step %d: %d -> %d", e.Step, c.last, e.Mono)
		}
		return
	}
	c.seen = true
	c.last = e.Mono
}

func (c *MonotonicClockChecker) Check() error { return c.violation }

// coreCheckers are the checkers something in this tree still emits events for.
//
// **One, and the count is the honest number rather than a loss.** A checker that cannot
// fire proves nothing — this file's own rule, and the rule TestMain enforces by
// demanding every checker here have a planted bug that actually ran. Watermark ordering,
// no-plaintext-leaves-host and effective-single-writer each read an event only the local
// block engine produced: a WAL advancing its watermarks, a payload on its way out of the
// host, a second incarnation racing to publish an image. With that engine withdrawn
// nothing emits any of the three, so they would have been three checkers observing an
// event stream that can no longer contain their subject, held up by planted bugs
// hand-writing the events they read. That is the exact shape this package refuses.
//
// They come back with the commit protocol, which reinstates every one of their subjects:
// a sealed layer leaving the host, a HEAD that moves only forward, and a compare-and-swap
// two hosts cannot both win. **One of them has** — see commitCheckers, which folds the
// second and third into a single invariant about the shape of the published history, and
// says why the plaintext one is asserted in internal/commit instead.
func coreCheckers() []Checker {
	return []Checker{
		NewMonotonicClockChecker(),
	}
}

// DefaultCheckers returns the checkers active so far. Later phases append.
func DefaultCheckers() []Checker {
	all := coreCheckers()
	all = append(all, commitCheckers()...)
	all = append(all, harnessCheckers()...)
	return all
}
