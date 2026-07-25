package dst_test

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/dst"
)

// A checker that cannot fail is decoration. PLAN.md states the rule — "checkers must
// be able to *catch* a violation (prove it with a planted bug), not merely run" — but
// only two of the eleven had such a proof, so nine invariants were being asserted by
// code nobody had ever seen reject anything.
//
// Each case below plants the exact violation its invariant forbids and requires the
// checker to fail, name itself, and print the reproducing seed (without which a DST
// failure is not actionable).

func plantedBug(t *testing.T, seed int64, checker dst.Checker, wantName string, sc dst.Scenario) {
	t.Helper()
	res := dst.Run(seed, sc, checker)
	if res.Err == nil {
		t.Fatalf("%s did not catch the planted violation", wantName)
	}
	if !strings.Contains(res.Err.Error(), wantName) {
		t.Fatalf("failure must name the checker %q, got: %v", wantName, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "seed=") {
		t.Fatalf("failure must report the reproducing seed, got: %v", res.Err)
	}
}

// INV-03: published <= durable <= local, at every observation.
func TestPlantedBugWatermarkOrder(t *testing.T) {
	plantedBug(t, 11, dst.NewWatermarkOrderChecker(), "watermark-order", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventWatermark, Published: 9, Durable: 3, Local: 5})
		return nil
	})
}

// INV-15: no cleartext guest data leaves the host.
func TestPlantedBugPlaintextLeavesHost(t *testing.T) {
	plantedBug(t, 12, dst.NewNoPlaintextLeavesHostChecker(), "no-plaintext-leaves-host", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventLeavesHost, ClearLeak: true, Msg: "canary found in an object bound for S3"})
		return nil
	})
}

// INV-06: no durable ACK while the lease is invalid.
func TestPlantedBugDurableAckWithoutLease(t *testing.T) {
	plantedBug(t, 13, dst.NewDurableAckLeaseChecker(), "durable-ack-requires-lease", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventDurableAck, Durable: 42, LeaseValid: false})
		return nil
	})
}

// INV-11: no epoch granted before FENCING_WAIT elapses.
func TestPlantedBugEarlyPromotion(t *testing.T) {
	plantedBug(t, 14, dst.NewPromotionWaitChecker(), "promotion-fencing-wait", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventPromotion, EarlyGrant: true, Msg: "granted on a missed heartbeat"})
		return nil
	})
}

// INV-10: a fenced/stale-epoch writer never publishes.
func TestPlantedBugStaleWriterPublished(t *testing.T) {
	plantedBug(t, 15, dst.NewSingleWriterChecker(), "effective-single-writer", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventStalePublsh, StalePublishOK: true})
		return nil
	})
}

// INV-09: the promoted writer's recovered prefix covers everything the fenced writer
// ACKed as durable. This is the one that means "no ACKed write was lost".
func TestPlantedBugLostAckedWrite(t *testing.T) {
	plantedBug(t, 16, dst.NewNoLostAckedWriteChecker(), "no-lost-acked-write", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventFailover, AckedDurable: 100, Recovered: 97})
		return nil
	})
}

// INV-16: a published snapshot never changes.
func TestPlantedBugSnapshotMutated(t *testing.T) {
	plantedBug(t, 17, dst.NewImmutableSnapshotChecker(), "immutable-snapshots", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventSnapshot, SnapshotMutated: true, Msg: "manifest rewritten"})
		return nil
	})
}

// INV-13: local WAL is never truncated above the verified published point.
func TestPlantedBugTruncateAbovePublished(t *testing.T) {
	plantedBug(t, 18, dst.NewTruncateBelowPublishedChecker(), "no-truncate-above-published", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventTruncate, TruncatedUpTo: 50, Published: 20})
		return nil
	})
}

// INV-17: background I/O never runs while foreground/flush is in flight.
func TestPlantedBugBackgroundDidNotYield(t *testing.T) {
	plantedBug(t, 19, dst.NewBackgroundYieldsChecker(), "background-yields", func(s *dst.Sim) error {
		s.Emit(dst.Event{Kind: dst.EventIOClass, BgGranted: true, HighInFlight: true})
		return nil
	})
}

// TestEveryCheckerHasAPlantedBugProof fails when a checker is added without one, so
// this file cannot fall behind DefaultCheckers again.
func TestEveryCheckerHasAPlantedBugProof(t *testing.T) {
	proven := map[string]bool{
		"monotonic-clock":             true, // TestPlantedBugMonotonicClock
		"no-permanent-delete":         true, // TestPlantedBugPermanentDelete
		"watermark-order":             true,
		"no-plaintext-leaves-host":    true,
		"durable-ack-requires-lease":  true,
		"promotion-fencing-wait":      true,
		"effective-single-writer":     true,
		"no-lost-acked-write":         true,
		"immutable-snapshots":         true,
		"no-truncate-above-published": true,
		"background-yields":           true,
	}
	for _, c := range dst.DefaultCheckers() {
		if !proven[c.Name()] {
			t.Fatalf("checker %q has no planted-bug proof: add one in this file (PLAN.md §3 stop signals)", c.Name())
		}
	}
	if len(proven) != len(dst.DefaultCheckers()) {
		t.Fatalf("the proven list has %d entries for %d checkers — it drifted", len(proven), len(dst.DefaultCheckers()))
	}
}
