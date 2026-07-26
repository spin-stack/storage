package sim_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// put writes n keys in order, so the store has a write order to be behind on.
func put(t *testing.T, s *sim.ObjectStore, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, err := s.Put(context.Background(), k, []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
}

func listed(t *testing.T, s *sim.ObjectStore, prefix string) []string {
	t.Helper()
	objs, err := s.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

// A listing that goes backwards is the fault SetEventualList/SetListLag cannot
// express: they withhold keys nobody has seen yet, this one takes back keys a previous
// listing already served. Everything the system derives from a LIST is a number, and
// only this direction can make that number smaller.
func TestInjectStaleListingHidesTheNewestWrites(t *testing.T) {
	tests := []struct {
		name string
		// inject is the budget asked for; nilRand asks for the no-op guard.
		inject   int
		nilRand  bool
		listings int
		// wantShort is whether each listing in order should be shorter than the full
		// three keys.
		wantShort []bool
	}{
		{name: "no injection lists everything", inject: 0, listings: 2, wantShort: []bool{false, false}},
		{name: "a negative budget is a no-op", inject: -1, listings: 1, wantShort: []bool{false}},
		{name: "a nil source is a no-op", inject: 3, nilRand: true, listings: 1, wantShort: []bool{false}},
		{name: "one stale listing, then the replica catches up", inject: 1, listings: 2, wantShort: []bool{true, false}},
		{name: "the budget covers consecutive listings", inject: 2, listings: 3, wantShort: []bool{true, true, false}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := sim.NewObjectStore()
			put(t, s, "wal/1", "wal/2", "wal/3")
			r := rand.New(rand.NewSource(7))
			if tc.nilRand {
				s.InjectStaleListing(nil, tc.inject)
			} else {
				s.InjectStaleListing(r, tc.inject)
			}
			for i := range tc.listings {
				keys := listed(t, s, "wal/")
				if short := len(keys) < 3; short != tc.wantShort[i] {
					t.Fatalf("listing %d returned %v (short=%t), want short=%t", i, keys, short, tc.wantShort[i])
				}
			}
		})
	}
}

// The keys that go missing are the ones written last. A replica that is behind has the
// old objects and not the new ones; which way the keys happen to sort is irrelevant,
// and a WAL object key sorts by an unpadded sequence number anyway.
func TestInjectStaleListingLosesTheNewestNotTheLast(t *testing.T) {
	s := sim.NewObjectStore()
	// Written newest-first in key order: a listing behind the data must lose "wal/a".
	put(t, s, "wal/c", "wal/b", "wal/a")
	s.InjectStaleListing(rand.New(rand.NewSource(3)), 1)

	keys := listed(t, s, "wal/")
	for _, k := range keys {
		if k == "wal/a" {
			t.Fatalf("the most recent write survived a stale listing: %v", keys)
		}
	}
	if len(keys) == 0 {
		t.Fatal("a stale listing must still answer with the older keys, got nothing")
	}
	// GET stays strongly consistent: the asymmetry recovery depends on (§6.1).
	if _, err := s.Get(context.Background(), "wal/a"); err != nil {
		t.Fatalf("GET must be unaffected by a lagging listing: %v", err)
	}
}

// INV-02: the fault is seeded, so two stores driven identically answer identically.
// Nothing may depend on map iteration order.
func TestInjectStaleListingIsDeterministic(t *testing.T) {
	run := func() [][]string {
		s := sim.NewObjectStore()
		put(t, s, "wal/1", "wal/2", "wal/3", "wal/4", "wal/5")
		s.InjectStaleListing(rand.New(rand.NewSource(99)), 3)
		var got [][]string
		for range 4 {
			got = append(got, listed(t, s, "wal/"))
		}
		return got
	}
	a, b := run(), run()
	for i := range a {
		if len(a[i]) != len(b[i]) {
			t.Fatalf("listing %d differs in length: %v vs %v", i, a[i], b[i])
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				t.Fatalf("listing %d differs at %d: %v vs %v", i, j, a[i], b[i])
			}
		}
	}
}

// A listing with nothing to return does not spend the budget: a fault nobody can
// observe is a fault that lands somewhere else than the scenario aimed it.
func TestInjectStaleListingSkipsEmptyListings(t *testing.T) {
	s := sim.NewObjectStore()
	put(t, s, "wal/1", "wal/2")
	s.InjectStaleListing(rand.New(rand.NewSource(5)), 1)

	if keys := listed(t, s, "other/"); len(keys) != 0 {
		t.Fatalf("unexpected keys under another prefix: %v", keys)
	}
	if keys := listed(t, s, "wal/"); len(keys) >= 2 {
		t.Fatalf("the empty listing spent the fault: %v", keys)
	}
}

// A stale listing hides keys; it never resurrects a marked one. The two mechanisms are
// independent and INV-14's marker outranks them both.
func TestInjectStaleListingDoesNotResurrectAMarkedObject(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	put(t, s, "wal/1", "wal/2")
	if err := s.Delete(ctx, "wal/2"); err != nil {
		t.Fatal(err)
	}
	s.InjectStaleListing(rand.New(rand.NewSource(11)), 4)
	for range 4 {
		for _, k := range listed(t, s, "wal/") {
			if k == "wal/2" {
				t.Fatal("a marked object appeared in a stale listing")
			}
		}
	}
}
