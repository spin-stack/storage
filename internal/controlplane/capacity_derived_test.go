package controlplane_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ADR-0017: committed capacity is a function of state, not a ledger.
//
//	committed(host) = Σ size_bytes of the volumes whose primary is host
//	                + Σ size_bytes reserved by in-flight operation plans targeting host
//
// The tests below state that identity and nothing else. They are deliberately
// written against the *interface* — ListHosts / ListVolumesByHost /
// ListOperationsByHost — and recompute the right-hand side with a decoder of their
// own, so they cannot agree with the implementation by sharing it.
//
// A ledger cannot satisfy this: its value is the sum of the deltas that happened to
// be applied, and a pass that dies between the promotion and its release leaves the
// two sides disagreeing by exactly one volume — the residual wave 3 named and could
// not close, because across a crash a stranger's change that nets to one volume size
// is indistinguishable from this operation's own.

// planEntry is the shape the drain records per volume in an operation's
// current_state. The test decodes it itself: a recomputation that imported the
// production type would stop being independent the day the production type changed.
type planEntry struct {
	VolumeID string `json:"volume_id"`
	Stage    string `json:"stage"`
	ToHost   string `json:"to_host"`
}

type planState struct {
	Volumes []planEntry `json:"volumes"`
}

// derivedCommitted recomputes ADR-0017's right-hand side for hostID from the rows
// the store exposes. A plan entry stops being a reservation once the volume it names
// is actually primary on the host — otherwise a volume in flight would be charged to
// its destination twice, once as a plan and once as a placement — and a settled entry
// (DONE, FOREIGN) reserves nothing at all.
func derivedCommitted(ctx context.Context, md metadata.Store, hostID string) (int64, error) {
	var total int64

	vols, err := md.ListVolumesByHost(ctx, hostID)
	if err != nil {
		return 0, fmt.Errorf("list volumes on %s: %w", hostID, err)
	}
	primary := map[string]bool{}
	for _, v := range vols {
		total += v.SizeBytes
		primary[v.VolumeID] = true
	}

	// A drain records its plan against the host it evacuates, so the reservations
	// aimed at this host are spread across every host's operations.
	hosts, err := md.ListHosts(ctx)
	if err != nil {
		return 0, fmt.Errorf("list hosts: %w", err)
	}
	seen := map[string]bool{}
	for _, h := range hosts {
		ops, err := md.ListOperationsByHost(ctx, h.HostID)
		if err != nil {
			return 0, fmt.Errorf("list operations on %s: %w", h.HostID, err)
		}
		for _, op := range ops {
			if seen[op.OperationID] || op.Phase.Terminal() {
				continue
			}
			seen[op.OperationID] = true
			var st planState
			if err := json.Unmarshal(op.CurrentState, &st); err != nil {
				continue // a plan nobody can read reserves nothing
			}
			for _, e := range st.Volumes {
				if e.ToHost != hostID || e.Stage == "DONE" || e.Stage == "FOREIGN" || primary[e.VolumeID] {
					continue
				}
				v, verr := md.GetVolume(ctx, e.VolumeID)
				if verr != nil {
					continue
				}
				total += v.SizeBytes
			}
		}
	}
	return total, nil
}

// capacityIsDerived checks the identity on every host of the fleet, returning the
// first host it does not hold for.
func capacityIsDerived(ctx context.Context, md metadata.Store) error {
	hosts, err := md.ListHosts(ctx)
	if err != nil {
		return err
	}
	for _, h := range hosts {
		want, err := derivedCommitted(ctx, md, h.HostID)
		if err != nil {
			return err
		}
		if h.NVMeCommittedBytes != want {
			return fmt.Errorf("host %s reports %d committed bytes; its volumes and in-flight plans sum to %d",
				h.HostID, h.NVMeCommittedBytes, want)
		}
	}
	return nil
}

// TestCommittedCapacityIsDerivedFromState kills a drain in the window a ledger
// cannot survive: the volume has been promoted to the destination and the source's
// bytes have not been given back. A stored number is then a statement about a world
// that no longer exists — the source is charged for a volume it does not hold — and
// every later placement decision is taken against it.
func TestCommittedCapacityIsDerivedFromState(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if err := capacityIsDerived(ctx, w.base); err != nil {
		t.Fatalf("before the drain: %v", err)
	}

	// Stop the pass exactly where the source's bytes would stop being charged.
	w.killAfterThePromotion(t)
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err == nil {
		t.Fatal("setup: the pass was supposed to die after the promotion")
	}
	first, err := w.base.GetVolume(ctx, format.UUIDString(w.vols[0]))
	if err != nil {
		t.Fatal(err)
	}
	if first.PrimaryHostID != destHost {
		t.Fatalf("setup: the volume never moved: %+v", first)
	}
	if err := capacityIsDerived(ctx, w.base); err != nil {
		t.Fatalf("after a pass died between the promotion and the release: %v", err)
	}

	w.clearFaults()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := capacityIsDerived(ctx, w.base); err != nil {
		t.Fatalf("after the resumed drain finished: %v", err)
	}
}

// TestCommittedCapacityHoldsUnderAnyInterleaving is the property a ledger cannot
// express. For any interleaving of drain passes, faults and elapsed time, the
// identity holds on every host after every pass. There is no number to reconcile, so
// there is no interleaving that can break it.
func TestCommittedCapacityHoldsUnderAnyInterleaving(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := context.Background()
		w := newDrainWorld(t, 10*volSize)

		for i := range rapid.IntRange(1, 6).Draw(rt, "passes") {
			// Time and faults move independently: the fence elapsing is what lets a
			// pass reach the promotion at all, and the fault decides where it dies.
			if rapid.Bool().Draw(rt, fmt.Sprintf("elapse%d", i)) {
				w.pastFencingWait()
			}
			switch rapid.IntRange(0, 2).Draw(rt, fmt.Sprintf("fault%d", i)) {
			case 0:
				w.clearFaults()
			case 1:
				w.killAfterThePromotion(t)
			case 2:
				w.killAtTheProgressWrite(t)
			}
			// Every outcome is admissible here: a completed pass, a refusal (the
			// fencing wait, no capacity), or a fault. The identity must hold after
			// each of them, which is the whole claim.
			_, _ = w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
			if err := capacityIsDerived(ctx, w.base); err != nil {
				rt.Fatalf("after %d passes: %v", i+1, err)
			}
		}
	})
}

// TestVolumeInFlightIsChargedToItsDestinationOnce: the destination must be charged
// before the volume is primary there — two placements racing for the same host would
// both see room otherwise — and it must not be charged twice once it is.
func TestVolumeInFlightIsChargedToItsDestinationOnce(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// A first pass plans the move and stops at the fencing wait: the volume is still
	// on the source, and the destination already carries the reservation.
	_, _ = w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	first, err := w.base.GetVolume(ctx, format.UUIDString(w.vols[0]))
	if err != nil {
		t.Fatal(err)
	}
	if first.PrimaryHostID != cloneHostA {
		t.Fatalf("setup: the volume moved before the fence elapsed: %+v", first)
	}
	if w.committed(t, destHost) < volSize {
		t.Fatalf("the destination is not charged for the volume in flight to it: %d",
			w.committed(t, destHost))
	}
	if err := capacityIsDerived(ctx, w.base); err != nil {
		t.Fatalf("with a move in flight: %v", err)
	}

	w.pastFencingWait()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatalf("drain: %v", err)
	}
	// Both volumes are primary on the destination and charged exactly once.
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d — a volume in flight was charged twice", got, 2*volSize)
	}
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("source committed = %d, want 0 — the move itself is the release", got)
	}
}
