package gc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// DEV-0006. INV-14 claims the GC "only marks, never deletes" and that the interface
// does not expose permanent deletion. Neither was true: Collect computed a list and
// did nothing with it, and Delete removed the object outright in both store
// implementations. These tests pin the behaviour the invariant asserts.

func seed(t *testing.T, s *sim.ObjectStore, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, err := s.Put(context.Background(), k, []byte(k), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestMarkIsReversible: a marked object is still readable by anyone who asks for the
// marked version — that is what makes a GC mistake recoverable (§21.3).
func TestMarkIsReversible(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	seed(t, store, "wal/v/1/1-1-a.wal", "volumes/v/descriptor.json")

	marked, err := gc.Mark(ctx, store, clk, map[string]bool{"volumes/v/descriptor.json": true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 1 || marked[0] != "wal/v/1/1-1-a.wal" {
		t.Fatalf("marked = %v, want the unreachable WAL object", marked)
	}
	// A normal read no longer sees it...
	if _, err := store.Get(ctx, "wal/v/1/1-1-a.wal"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("a marked object must not be returned by a plain GET: %v", err)
	}
	// ...but the bytes are still there and can be restored.
	if err := store.Restore(ctx, "wal/v/1/1-1-a.wal"); err != nil {
		t.Fatalf("a mark must be reversible: %v", err)
	}
	body, err := store.Get(ctx, "wal/v/1/1-1-a.wal")
	if err != nil || string(body) != "wal/v/1/1-1-a.wal" {
		t.Fatalf("restored body = %q err=%v", body, err)
	}
}

// TestMarkNeverTouchesAReachableObject is the property that makes the GC safe to run
// at all: reachability decides, and a live object is never marked.
func TestMarkNeverTouchesAReachableObject(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	seed(t, store, "wal/v/1/live.wal", "wal/v/1/orphan.wal")

	if _, err := gc.Mark(ctx, store, clk, map[string]bool{"wal/v/1/live.wal": true}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "wal/v/1/live.wal"); err != nil {
		t.Fatalf("a reachable object was marked: %v", err)
	}
	if _, err := store.Get(ctx, "wal/v/1/orphan.wal"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatal("the orphan was not marked")
	}
}

// TestGracePeriodProtectsFreshObjects: an object written seconds ago may belong to a
// manifest that is still being published (§21.1 order). The grace period is what
// keeps the GC from racing a publication.
func TestGracePeriodProtectsFreshObjects(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	seed(t, store, "wal/v/1/fresh.wal")

	marked, err := gc.Mark(ctx, store, clk, map[string]bool{}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 0 {
		t.Fatalf("marked %v inside the grace period", marked)
	}
	if _, err := store.Get(ctx, "wal/v/1/fresh.wal"); err != nil {
		t.Fatalf("a fresh object was marked: %v", err)
	}

	// Once it ages past the grace period it becomes eligible.
	clk.Advance(2 * time.Hour)
	marked, err = gc.Mark(ctx, store, clk, map[string]bool{}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 1 {
		t.Fatalf("marked = %v, want the aged orphan", marked)
	}
}

// TestMarkIsIdempotent: the GC runs on a schedule; marking what is already marked
// must not error or double-count.
func TestMarkIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	seed(t, store, "wal/v/1/orphan.wal")

	first, err := gc.Mark(ctx, store, clk, map[string]bool{}, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("first pass: %v err=%v", first, err)
	}
	second, err := gc.Mark(ctx, store, clk, map[string]bool{}, 0)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second pass re-marked %v", second)
	}
}

// TestStoreHasNoPermanentDelete is the structural half of INV-14: the interface must
// not offer a way to destroy data, so no amount of GC bugs can.
func TestStoreHasNoPermanentDelete(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	seed(t, store, "k")

	var s objectstore.Store = store
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	// Delete is a mark: the object is gone from reads but recoverable.
	if _, err := s.Get(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("delete must hide the object: %v", err)
	}
	if err := store.Restore(ctx, "k"); err != nil {
		t.Fatalf("delete must be reversible: %v", err)
	}
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Fatalf("restore did not bring the object back: %v", err)
	}
}
