package sim_test

import (
	"context"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// SetEventualList models a listing that never catches up until someone calls Settle.
// That is one end of the spectrum, and it is the only one the sim could express: a
// scenario cannot say "the listing lags by N operations and then catches up on its
// own", which is what a real eventually consistent LIST does and what a seed-driven
// scenario needs in order to explore *when* the catch-up lands relative to the
// promotion, the boundary write, or the GC's second listing.
func TestSetListLagMakesAKeyVisibleAfterNOperations(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		lag  int
		// opsAfterPut is how many further store operations happen before the LIST
		// under test; wantVisible is whether the key should appear by then.
		opsAfterPut int
		wantVisible bool
	}{
		{name: "no lag is strongly consistent", lag: 0, opsAfterPut: 0, wantVisible: true},
		{name: "lag 1 hides the key from the immediate LIST", lag: 1, opsAfterPut: 0, wantVisible: false},
		{name: "lag 1 catches up after one operation", lag: 1, opsAfterPut: 1, wantVisible: true},
		{name: "lag 3 still lags after two operations", lag: 3, opsAfterPut: 2, wantVisible: false},
		{name: "lag 3 catches up after three", lag: 3, opsAfterPut: 3, wantVisible: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := sim.NewObjectStore()
			s.SetListLag(tc.lag)
			if _, err := s.Put(ctx, "wal/k", []byte("v"), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			for range tc.opsAfterPut {
				if _, err := s.Head(ctx, "wal/k"); err != nil {
					t.Fatal(err)
				}
			}
			objs, err := s.List(ctx, "wal/")
			if err != nil {
				t.Fatal(err)
			}
			if got := len(objs) == 1; got != tc.wantVisible {
				t.Fatalf("List saw the key = %t, want %t (lag=%d, ops after put=%d)",
					got, tc.wantVisible, tc.lag, tc.opsAfterPut)
			}
			// A lagging LIST is a LIST fault only: GET and HEAD are read-after-write
			// consistent in every backend the design accepts (§6.1), and recovery
			// depends on that asymmetry.
			if _, err := s.Get(ctx, "wal/k"); err != nil {
				t.Fatalf("GET must be strongly consistent regardless of the LIST lag: %v", err)
			}
		})
	}
}

// Settle is the escape hatch every scenario needs at the end: whatever the lag, the
// listing eventually reflects reality.
func TestSettleCatchesUpAnyLag(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	s.SetListLag(100)
	if _, err := s.Put(ctx, "wal/k", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if objs, _ := s.List(ctx, "wal/"); len(objs) != 0 {
		t.Fatalf("a lag of 100 should hide the key, got %d objects", len(objs))
	}
	s.Settle()
	if objs, _ := s.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("after Settle the key must be listed, got %d objects", len(objs))
	}
}

// The lag must not resurrect a delete marker: an object marked while it was still
// invisible to List stays out of the listing forever, not "until it catches up".
func TestListLagDoesNotResurrectAMarkedObject(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	s.SetListLag(2)
	if _, err := s.Put(ctx, "wal/k", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "wal/k"); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		_, _ = s.List(ctx, "wal/")
	}
	if objs, _ := s.List(ctx, "wal/"); len(objs) != 0 {
		t.Fatalf("a marked object became listable once its write caught up: %v", objs)
	}
}
