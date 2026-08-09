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

// WatermarkOrderChecker enforces INV-03 (§5.6): published <= durable <= local at
// every observation. It watches watermark events.
type WatermarkOrderChecker struct {
	violation error
}

// NewWatermarkOrderChecker returns a fresh checker.
func NewWatermarkOrderChecker() *WatermarkOrderChecker { return &WatermarkOrderChecker{} }

func (c *WatermarkOrderChecker) Name() string { return "watermark-order" }

func (c *WatermarkOrderChecker) Observe(e Event) {
	if e.Kind != EventWatermark || c.violation != nil {
		return
	}
	if e.Published > e.Durable || e.Durable > e.Local {
		c.violation = fmt.Errorf("watermark ordering violated at step %d: published=%d durable=%d local=%d",
			e.Step, e.Published, e.Durable, e.Local)
	}
}

func (c *WatermarkOrderChecker) Check() error { return c.violation }

// NoPlaintextLeavesHostChecker enforces INV-15 (§5.10): no cleartext guest data
// leaves the host. It watches leaves-host events for a detected cleartext leak.
type NoPlaintextLeavesHostChecker struct {
	violation error
}

// NewNoPlaintextLeavesHostChecker returns a fresh checker.
func NewNoPlaintextLeavesHostChecker() *NoPlaintextLeavesHostChecker {
	return &NoPlaintextLeavesHostChecker{}
}

func (c *NoPlaintextLeavesHostChecker) Name() string { return "no-plaintext-leaves-host" }

func (c *NoPlaintextLeavesHostChecker) Observe(e Event) {
	if e.Kind == EventLeavesHost && e.ClearLeak && c.violation == nil {
		c.violation = fmt.Errorf("cleartext guest data left the host at step %d (violates §5.10/INV-15): %s", e.Step, e.Msg)
	}
}

func (c *NoPlaintextLeavesHostChecker) Check() error { return c.violation }

// SingleWriterChecker enforces INV-10 (§12.4): a fenced/stale-epoch writer never
// publishes.
type SingleWriterChecker struct{ violation error }

// NewSingleWriterChecker returns a fresh checker.
func NewSingleWriterChecker() *SingleWriterChecker { return &SingleWriterChecker{} }

func (c *SingleWriterChecker) Name() string { return "effective-single-writer" }

func (c *SingleWriterChecker) Observe(e Event) {
	if e.Kind == EventStalePublsh && e.StalePublishOK && c.violation == nil {
		c.violation = fmt.Errorf("a stale-epoch writer published at step %d (violates §12.4/INV-10)", e.Step)
	}
}

func (c *SingleWriterChecker) Check() error { return c.violation }

// TruncateBelowPublishedChecker enforces INV-13 (§21.1): local WAL is never
// truncated above the verified published point.
type TruncateBelowPublishedChecker struct{ violation error }

// NewTruncateBelowPublishedChecker returns a fresh checker.
func NewTruncateBelowPublishedChecker() *TruncateBelowPublishedChecker {
	return &TruncateBelowPublishedChecker{}
}

func (c *TruncateBelowPublishedChecker) Name() string { return "no-truncate-above-published" }

func (c *TruncateBelowPublishedChecker) Observe(e Event) {
	if e.Kind == EventTruncate && e.TruncatedUpTo > e.Published && c.violation == nil {
		c.violation = fmt.Errorf("WAL truncated to %d above published %d at step %d (violates §21.1/INV-13)",
			e.TruncatedUpTo, e.Published, e.Step)
	}
}

func (c *TruncateBelowPublishedChecker) Check() error { return c.violation }

// DefaultCheckers returns the checkers active so far. Later phases append.
// coreCheckers are the checkers whose subjects survive V1 (ADR-0026). Four went with
// theirs in increment 4: promotion wait, no-lost-acked-write, truncate-below-published
// and immutable-snapshots; durable-ack-requires-lease and background-yields went in 4.5
// with the lease-gated ACK and the io-class scheduler. A checker that cannot fire proves
// nothing, which is this file's own rule.
func coreCheckers() []Checker {
	return []Checker{
		NewMonotonicClockChecker(),
		NewWatermarkOrderChecker(),
		NewNoPlaintextLeavesHostChecker(),
		NewSingleWriterChecker(),
	}
}

func DefaultCheckers() []Checker {
	all := coreCheckers()
	all = append(all, harnessCheckers()...)
	all = append(all, walCheckers()...)
	all = append(all, carryCheckers()...)
	all = append(all, agentCheckers()...)
	all = append(all, refusalCheckers()...)
	return all
}
