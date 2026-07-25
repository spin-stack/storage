package gc_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
)

// DEV-0006. INV-14 claims the GC "only marks, never deletes" and that the interface
// does not expose permanent deletion. Neither was true: Collect computed a list and
// did nothing with it, and Delete removed the object outright in both store
// implementations. These tests pin the behaviour the invariant asserts.

// newStore returns a store whose object timestamps come from clk, so the grace
// period is exercised deterministically rather than against wall time.
func newStore(clk *sim.Clock) *sim.ObjectStore {
	s := sim.NewObjectStore()
	s.SetClock(clk)
	return s
}

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
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
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
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
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
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
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
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
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

// TestMarkStopsAtTheFirstFailure: the GC reports what it managed to mark before an
// error rather than claiming the whole sweep succeeded — an operator reading
// gc_marked_bytes_total needs that number to be true.
func TestMarkStopsAtTheFirstFailure(t *testing.T) {
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	seed(t, store, "a.wal", "b.wal")

	store.InjectThrottle(2) // the LIST succeeds, the first Delete does not
	if _, err := gc.Mark(ctx, store, clk, map[string]bool{}, 0); err == nil {
		t.Fatal("a failing mark must be reported")
	}
}

// TestReachableAnchorsWALObjectsFromManifests is the safety half of the sweep: an
// object referenced by a published manifest is live even though nothing else points
// at it.
func TestReachableAnchorsWALObjectsFromManifests(t *testing.T) {
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	seed(t, store, "wal/v/1/anchored.wal", "wal/v/1/orphan.wal")
	// Published the way snapshot.Publish does it: with the root digest that says
	// "these objects, this sequence". A manifest without one is not an anchor the
	// sweep may act on (finding 2).
	m := snapshot.Manifest{
		SnapshotID: "s1", VolumeID: "v", Epoch: 1, TargetSequence: 1,
		Objects: []string{"wal/v/1/anchored.wal"},
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "snapshots/v/s1/manifest.json", body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	marked, err := gc.Mark(ctx, store, clk, reachable, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 1 || marked[0] != "wal/v/1/orphan.wal" {
		t.Fatalf("marked = %v, want only the orphan", marked)
	}
	if _, err := store.Get(ctx, "wal/v/1/anchored.wal"); err != nil {
		t.Fatalf("a manifest-anchored object was marked: %v", err)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
