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

// NoPermanentDeleteChecker enforces INV-14 (in framework form for Phase 01): the
// GC/data path never performs a permanent (irreversible) delete. It watches
// delete events for the Permanent flag. The sim object store only ever performs
// reversible deletes, so this passes in real scenarios; the planted-bug test
// feeds a Permanent delete to prove the checker catches it.
type NoPermanentDeleteChecker struct {
	violation error
}

// NewNoPermanentDeleteChecker returns a fresh checker.
func NewNoPermanentDeleteChecker() *NoPermanentDeleteChecker { return &NoPermanentDeleteChecker{} }

func (c *NoPermanentDeleteChecker) Name() string { return "no-permanent-delete" }

func (c *NoPermanentDeleteChecker) Observe(e Event) {
	if e.Kind == EventDelete && e.Permanent && c.violation == nil {
		c.violation = fmt.Errorf("permanent delete of live object %q at step %d (violates §5.11/§21.3)", e.Key, e.Step)
	}
}

func (c *NoPermanentDeleteChecker) Check() error { return c.violation }

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

// DurableAckLeaseChecker enforces INV-06 (§12.2): no FLUSH/FUA is ACKed as durable
// while the host lease is invalid. It watches durable-ack events for an ACK that
// escaped with an invalid lease.
type DurableAckLeaseChecker struct {
	violation error
}

// NewDurableAckLeaseChecker returns a fresh checker.
func NewDurableAckLeaseChecker() *DurableAckLeaseChecker { return &DurableAckLeaseChecker{} }

func (c *DurableAckLeaseChecker) Name() string { return "durable-ack-requires-lease" }

func (c *DurableAckLeaseChecker) Observe(e Event) {
	if e.Kind == EventDurableAck && !e.LeaseValid && c.violation == nil {
		c.violation = fmt.Errorf("durable ACK of seq %d with an invalid lease at step %d (violates §12.2/INV-06)", e.Durable, e.Step)
	}
}

func (c *DurableAckLeaseChecker) Check() error { return c.violation }

// PromotionWaitChecker enforces INV-11 (§12.3): no epoch N+1 is granted before
// FENCING_WAIT elapses.
type PromotionWaitChecker struct{ violation error }

// NewPromotionWaitChecker returns a fresh checker.
func NewPromotionWaitChecker() *PromotionWaitChecker { return &PromotionWaitChecker{} }

func (c *PromotionWaitChecker) Name() string { return "promotion-fencing-wait" }

func (c *PromotionWaitChecker) Observe(e Event) {
	if e.Kind == EventPromotion && e.EarlyGrant && c.violation == nil {
		c.violation = fmt.Errorf("epoch granted before FENCING_WAIT elapsed at step %d (violates §12.3/INV-11)", e.Step)
	}
}

func (c *PromotionWaitChecker) Check() error { return c.violation }

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

// NoLostAckedWriteChecker enforces INV-09 (§12): across a partition + failover, the
// promoted writer's recovered prefix must cover everything the fenced writer ACKed
// as durable.
type NoLostAckedWriteChecker struct{ violation error }

// NewNoLostAckedWriteChecker returns a fresh checker.
func NewNoLostAckedWriteChecker() *NoLostAckedWriteChecker { return &NoLostAckedWriteChecker{} }

func (c *NoLostAckedWriteChecker) Name() string { return "no-lost-acked-write" }

func (c *NoLostAckedWriteChecker) Observe(e Event) {
	if e.Kind == EventFailover && e.Recovered < e.AckedDurable && c.violation == nil {
		c.violation = fmt.Errorf("recovered prefix %d < ACKed-durable %d at step %d (violates §12/INV-09)",
			e.Recovered, e.AckedDurable, e.Step)
	}
}

func (c *NoLostAckedWriteChecker) Check() error { return c.violation }

// ImmutableSnapshotChecker enforces INV-16 (§5.2): a published snapshot never
// changes.
type ImmutableSnapshotChecker struct{ violation error }

// NewImmutableSnapshotChecker returns a fresh checker.
func NewImmutableSnapshotChecker() *ImmutableSnapshotChecker { return &ImmutableSnapshotChecker{} }

func (c *ImmutableSnapshotChecker) Name() string { return "immutable-snapshots" }

func (c *ImmutableSnapshotChecker) Observe(e Event) {
	if e.Kind == EventSnapshot && e.SnapshotMutated && c.violation == nil {
		c.violation = fmt.Errorf("a published snapshot changed at step %d (violates §5.2/INV-16)", e.Step)
	}
}

func (c *ImmutableSnapshotChecker) Check() error { return c.violation }

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

// BackgroundYieldsChecker enforces INV-17 (§5.9): a background op is never granted
// while a foreground/flush op is in flight.
type BackgroundYieldsChecker struct{ violation error }

// NewBackgroundYieldsChecker returns a fresh checker.
func NewBackgroundYieldsChecker() *BackgroundYieldsChecker { return &BackgroundYieldsChecker{} }

func (c *BackgroundYieldsChecker) Name() string { return "background-yields" }

func (c *BackgroundYieldsChecker) Observe(e Event) {
	if e.Kind == EventIOClass && e.BgGranted && e.HighInFlight && c.violation == nil {
		c.violation = fmt.Errorf("background I/O granted while foreground/flush in flight at step %d (violates §5.9/INV-17)", e.Step)
	}
}

func (c *BackgroundYieldsChecker) Check() error { return c.violation }

// DefaultCheckers returns the checkers active so far. Later phases append.
func DefaultCheckers() []Checker {
	return []Checker{
		NewMonotonicClockChecker(),
		NewNoPermanentDeleteChecker(),
		NewWatermarkOrderChecker(),
		NewNoPlaintextLeavesHostChecker(),
		NewDurableAckLeaseChecker(),
		NewPromotionWaitChecker(),
		NewSingleWriterChecker(),
		NewNoLostAckedWriteChecker(),
		NewImmutableSnapshotChecker(),
		NewTruncateBelowPublishedChecker(),
		NewBackgroundYieldsChecker(),
	}
}
