package gc_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
)

const grace = time.Hour

// Finding 5 (medium). Reachability and marking are two independent, non-atomic LIST
// calls. A bucket-wide sweep that GETs every manifest takes minutes; a snapshot
// manifest published inside that window anchors objects that are *already* older
// than the grace period, so the reachability set Mark is handed is stale by the time
// it lists again — and the newly anchored, live objects are marked. Correct code on
// both sides of the race, data loss anyway.
func TestAManifestPublishedDuringTheSweepIsHonoured(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	seed(t, store, anchoredWAL, otherWAL)
	clk.Advance(48 * time.Hour) // §21.1 writes the objects first; they are long past grace

	// Phase one of the sweep: nothing anchors these objects yet.
	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if reachable[anchoredWAL] {
		t.Fatal("setup: the object must be an orphan when reachability is computed")
	}

	// The publication that was in flight completes: the manifest lands.
	m := goodManifest("s1", anchoredWAL)
	if _, err := store.Put(ctx, snapshot.ManifestKey(anchorVol, "s1"), mustJSON(t, m), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	marked, err := gc.Mark(ctx, store, clk, reachable, grace)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(marked, anchoredWAL) {
		t.Fatalf("an object anchored between LIST and LIST was marked: %v", marked)
	}
	if _, err := store.Get(ctx, anchoredWAL); err != nil {
		t.Fatalf("the freshly published snapshot's data was marked: %v", err)
	}
	// The genuine orphan is still collected — the fix must not disable the GC.
	if !slices.Contains(marked, otherWAL) {
		t.Fatalf("the real orphan was not marked: %v", marked)
	}
}

// Finding 4 (high), (b): a publication can legitimately outlast the grace period —
// throttling, a retried multipart upload, a large snapshot. §21.1 writes the objects
// first, so by the time the manifest lands its objects are already older than grace.
// Combined with a GC host whose clock runs ahead of the backend's, the window
// collapses entirely: every object of the in-flight publication looks ancient.
func TestAPublicationOutlastingGraceUnderClockSkewIsHonoured(t *testing.T) {
	ctx := t.Context()
	storeClk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	gcClk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC().Add(4 * grace)) // GC host runs ahead
	store := newStore(storeClk)
	seed(t, store, anchoredWAL)

	// The GC starts its sweep while the publication is still writing objects.
	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// The publication finishes: the manifest is published (§21.1 order).
	m := goodManifest("s1", anchoredWAL)
	if _, err := store.Put(ctx, snapshot.ManifestKey(anchorVol, "s1"), mustJSON(t, m), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	marked, err := gc.Mark(ctx, store, gcClk, reachable, grace)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 0 {
		t.Fatalf("a completed publication was marked under clock skew: %v", marked)
	}
}

// Finding 4 (high), (a) and (c): the object timestamps and the GC's clock come from
// two different machines. If the store's are *ahead* — the backend's clock is ahead,
// or the GC host's clock jumped backwards — every age is negative, every object looks
// protected forever, and the bucket grows without bound with no signal at all. The
// sweep must say so rather than quietly do nothing.
func TestSweepRefusesWhenTheStoreClockIsAheadOfTheGCClock(t *testing.T) {
	ctx := t.Context()
	base := time.Unix(1_700_000_000, 0).UTC()

	tests := []struct {
		name  string
		drive func(t *testing.T) (*sim.ObjectStore, *sim.Clock)
	}{
		{
			name: "backend clock ahead of the GC host",
			drive: func(t *testing.T) (*sim.ObjectStore, *sim.Clock) {
				storeClk := sim.NewClock(base.Add(24 * time.Hour))
				gcClk := sim.NewClock(base)
				s := newStore(storeClk)
				seed(t, s, anchoredWAL)
				return s, gcClk
			},
		},
		{
			name: "GC host clock jumps backwards between sweeps",
			drive: func(t *testing.T) (*sim.ObjectStore, *sim.Clock) {
				clk := sim.NewClock(base)
				s := newStore(clk)
				seed(t, s, anchoredWAL)
				// A first sweep at the correct time, then NTP walks the clock back.
				if _, err := gc.Mark(ctx, s, clk, map[string]bool{anchoredWAL: true}, grace); err != nil {
					t.Fatal(err)
				}
				return s, sim.NewClock(base.Add(-24 * time.Hour))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, gcClk := tc.drive(t)
			marked, err := gc.Mark(ctx, store, gcClk, map[string]bool{}, grace)
			if !errors.Is(err, gc.ErrClockSkew) {
				t.Fatalf("err = %v, want ErrClockSkew — a silent no-op is not a signal", err)
			}
			if len(marked) != 0 {
				t.Fatalf("a skewed sweep marked %v", marked)
			}
			if _, err := store.Get(ctx, anchoredWAL); err != nil {
				t.Fatalf("a skewed sweep marked an object: %v", err)
			}
		})
	}
}

// Finding 8 (low). A cron sweep that overruns into the next one — or an operator
// running the sweep by hand alongside it — makes one pass hit a key the other has
// already marked. That is an ErrNotFound from Delete, which killed the pass at the
// first collision and left the rest of the bucket unswept, with an opaque error.
func TestTwoOverlappingSweepsBothComplete(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	inner := newStore(clk)
	orphans := []string{"wal/av/1/1-1-a.wal", "wal/av/1/2-2-b.wal", "wal/av/1/3-3-c.wal", "wal/av/1/4-4-d.wal"}
	seed(t, inner, orphans...)
	clk.Advance(48 * time.Hour)

	// The other sweep runs to completion inside this one's first Delete.
	store := hooked(inner)
	var other []string
	store.onDelete = func(string) error {
		if other != nil {
			return nil
		}
		var err error
		other, err = gc.Mark(ctx, inner, clk, map[string]bool{}, grace)
		return err
	}

	mine, err := gc.Mark(ctx, store, clk, map[string]bool{}, grace)
	if err != nil {
		t.Fatalf("an overlapping sweep must not kill this one: %v", err)
	}
	if len(other) == 0 {
		t.Fatal("setup: the overlapping sweep marked nothing")
	}

	union := append(append([]string{}, mine...), other...)
	slices.Sort(union)
	if deduped := slices.Compact(slices.Clone(union)); len(deduped) != len(union) {
		t.Fatalf("a key was reported as marked by both sweeps: %v", union)
	}
	want := slices.Clone(orphans)
	slices.Sort(want)
	if !slices.Equal(union, want) {
		t.Fatalf("union of the two sweeps = %v, want every orphan exactly once %v", union, want)
	}
	for _, k := range orphans {
		if !inner.Marked(k) {
			t.Fatalf("%q survived both sweeps", k)
		}
	}
}

// TestASweepResumesAfterAFailureAtEveryPosition: whatever object the backend refuses
// on, the next pass must pick up exactly the remainder — no gap, no double count.
func TestASweepResumesAfterAFailureAtEveryPosition(t *testing.T) {
	ctx := t.Context()
	orphans := []string{"wal/av/1/1-1-a.wal", "wal/av/1/2-2-b.wal", "wal/av/1/3-3-c.wal", "wal/av/1/4-4-d.wal"}

	for k := 1; k <= len(orphans); k++ {
		t.Run(fmt.Sprintf("throttled at delete %d", k), func(t *testing.T) {
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			inner := newStore(clk)
			seed(t, inner, orphans...)
			clk.Advance(48 * time.Hour)

			store := hooked(inner)
			var n int
			store.onDelete = func(string) error {
				n++
				if n == k {
					return sim.ErrThrottled
				}
				return nil
			}

			first, err := gc.Mark(ctx, store, clk, map[string]bool{}, grace)
			if err == nil {
				t.Fatal("a throttled Delete must be reported, not swallowed")
			}
			if len(first) != k-1 {
				t.Fatalf("first pass reported %d marks (%v), want the %d it actually made", len(first), first, k-1)
			}

			store.onDelete = nil
			second, err := gc.Mark(ctx, store, clk, map[string]bool{}, grace)
			if err != nil {
				t.Fatalf("the resumed pass failed: %v", err)
			}
			union := append(append([]string{}, first...), second...)
			slices.Sort(union)
			want := slices.Clone(orphans)
			slices.Sort(want)
			if !slices.Equal(union, want) {
				t.Fatalf("first=%v second=%v: union %v, want %v", first, second, union, want)
			}
		})
	}
}

// TestOrphanGaugeIsRecordedEvenWhenTheSweepFails: orphan_objects_total was set only
// on the success path, so a sweep that died at its first Delete left the operator
// looking at the previous pass's number — and a stuck GC is exactly the condition the
// gauge exists to show (§26.2, DEV-0010).
func TestOrphanGaugeIsRecordedEvenWhenTheSweepFails(t *testing.T) {
	ctx := t.Context()
	p, err := obs.NewTestProvider("gc-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	inner := newStore(clk)
	seed(t, inner, "wal/av/1/1-1-a.wal", "wal/av/1/2-2-b.wal")
	clk.Advance(48 * time.Hour)

	store := hooked(inner)
	store.onDelete = func(string) error { return sim.ErrThrottled }

	rec := obs.NewRecorder(p.Metrics)
	if _, err := gc.MarkWithRecorder(ctx, store, clk, map[string]bool{}, grace, rec); err == nil {
		t.Fatal("setup: the sweep was supposed to fail")
	}
	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got["orphan_objects_total"] {
		t.Fatalf("orphan_objects_total was not recorded by a failing sweep; collected: %v", got)
	}
}
