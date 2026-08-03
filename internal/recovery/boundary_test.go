package recovery_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The recovery point is the only create-only, immutable number in the system: it is
// the floor below which the next epoch will never look again. Nothing checked that a
// new boundary is at least as high as the one before it, and nothing checked that it
// is at least as high as what the previous epoch's writer already ACKed.
//
// One bad pass — a stale LIST, a swallowed floor error, a GC-shortened prefix — is
// enough to write a boundary below an earlier one. There is no in-band repair: the
// object cannot be rewritten, so every ACKed write below the new floor is gone for
// good. This is the guard that catches the whole family at the moment of the write
// instead of long after it.

// TestBoundaryMustNotRegress: epoch 3's boundary may not be lower than epoch 2's.
func TestBoundaryMustNotRegress(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 10); err != nil {
		t.Fatal(err)
	}
	// Epoch 3 claims the volume was only recovered up to 4 — below a boundary that is
	// already immutable. Sequences 5..10 would sit under the new floor for ever.
	err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 2, 4)
	if err == nil {
		t.Fatal("a boundary below its predecessor was accepted: sequences 5..10 are now " +
			"permanently below the floor, and the object cannot be rewritten")
	}
	if !errors.Is(err, recovery.ErrBoundaryRegression) {
		t.Fatalf("want ErrBoundaryRegression, got %v", err)
	}

	// The legitimate move — the same boundary or higher — still works.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 2, 10); err != nil {
		t.Fatalf("a boundary equal to its predecessor is legal: %v", err)
	}
}

// TestEpochChainRefusesANonMonotonicChain: the guard above cannot catch a pair
// written in an order that hides the regression (epoch 3's boundary first, then
// epoch 2's). Recovery must still refuse to build a volume out of it rather than
// hand back spans whose ranges run backwards.
func TestEpochChainRefusesANonMonotonicChain(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	// Written newest-first, so neither write sees a predecessor to compare against.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 2, 4); err != nil {
		t.Fatal(err)
	}
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 10); err != nil {
		t.Fatal(err)
	}

	spans, err := recovery.EpochChain(ctx, store, vol, 3)
	if err == nil {
		t.Fatalf("a chain whose boundaries run backwards was accepted: %+v — epoch 2 is "+
			"reported as covering sequences 11..4", spans)
	}
	if !errors.Is(err, recovery.ErrBrokenEpochChain) {
		t.Fatalf("want ErrBrokenEpochChain, got %v", err)
	}
}

// TestBoundaryWriteRefusesWhatItCannotCheck: the guard is only worth having if it
// fails closed. A backend that cannot answer for the previous epoch leaves the floor
// unknown, and an unknown floor may not be written over.
func TestBoundaryWriteRefusesWhatItCannotCheck(t *testing.T) {
	ctx := t.Context()
	vol := vol7()

	tests := []struct {
		name  string
		store func(*sim.ObjectStore) objectstore.Store
	}{
		{"the previous boundary cannot be read", func(s *sim.ObjectStore) objectstore.Store {
			return rpFaultStore{Store: s, err: sim.ErrThrottled}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := sim.NewObjectStore()
			// prevEpoch is 1, which is the epoch both fault stores answer for.
			if err := recovery.WriteRecoveryPoint(ctx, tc.store(base), vol, 2, 1, 5); err == nil {
				t.Fatal("a boundary was recorded while the state it must not go below was unreadable")
			}
			if _, err := recovery.ReadRecoveryPoint(ctx, base, vol, 2); err == nil {
				t.Fatal("the boundary object was written anyway")
			}
		})
	}
}

// TestChainedBoundariesAreMonotonicAndLinked walks a real three-epoch volume and
// asserts the two properties the chain rests on: each boundary links to the epoch
// before it, and the recovered points never go down.
func TestChainedBoundariesAreMonotonicAndLinked(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := vol7()

	var seq uint64
	for epoch := uint64(1); epoch <= 3; epoch++ {
		if epoch > 1 {
			if err := recovery.WriteRecoveryPoint(ctx, store, vol, epoch, epoch-1, seq); err != nil {
				t.Fatalf("boundary for epoch %d: %v", epoch, err)
			}
		}
		l := epochWriter(t, store, clk, vol, epoch, seq)
		if _, err := l.Write(epoch*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		seq = l.Watermarks().Durable
	}

	var prev uint64
	for epoch := uint64(2); epoch <= 3; epoch++ {
		rp, err := recovery.ReadRecoveryPoint(ctx, store, vol, epoch)
		if err != nil {
			t.Fatalf("epoch %d has no boundary: %v", epoch, err)
		}
		if rp.PrevEpoch != epoch-1 {
			t.Fatalf("epoch %d chains to %d, want %d", epoch, rp.PrevEpoch, epoch-1)
		}
		if rp.RecoveredUpTo < prev {
			t.Fatalf("epoch %d recovered up to %d, below epoch %d's %d",
				epoch, rp.RecoveredUpTo, epoch-1, prev)
		}
		prev = rp.RecoveredUpTo
	}
}
