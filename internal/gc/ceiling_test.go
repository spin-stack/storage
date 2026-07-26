package gc_test

import (
	"slices"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ADR-0012. The sweep discovers anchors by listing, so a manifest published moments
// ago is GET-visible and LIST-invisible while the objects it names — older by
// construction (§21.1) — are already listed and already past grace. That only costs
// data when those objects are outside every durable prefix, and the way an ordinary
// volume gets there is the *ceiling*: a promotion records the epoch's boundary
// (§12.5), the GC clamps the epoch's durable point to it, and everything the epoch
// wrote above it stops being covered by a root.
//
// A published snapshot targeting a sequence above that boundary then hangs off its
// manifest alone — and a listing that has not caught up marks its data. The boundary
// is a create-only, permanent number that was itself computed from a listing; using it
// as a licence to destroy is the coupling this fixes. For the GC, a superseded epoch's
// objects are reachable: "what is the volume's state?" and "what may I destroy?" are
// not the same question.

// closeEpoch records the promotion boundary that supersedes epoch 1 at `upTo`.
func closeEpoch(t *testing.T, store *sim.ObjectStore, vol [16]byte, upTo uint64) {
	t.Helper()
	if err := recovery.WriteRecoveryPoint(t.Context(), store, vol, 2, 1, upTo); err != nil {
		t.Fatal(err)
	}
}

// publishSnapshot publishes a manifest anchoring every object of epoch 1 up to target.
func publishSnapshot(t *testing.T, store *sim.ObjectStore, vol [16]byte, target uint64) []string {
	t.Helper()
	ctx := t.Context()
	objects, err := recovery.ObjectKeysUpTo(ctx, store, vol, 1, target)
	if err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{
		SnapshotID: "00000000-0000-7000-8000-0000000000f1", VolumeID: format.UUIDString(vol),
		Epoch: 1, TargetSequence: target, Objects: objects,
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	return objects
}

// TestASupersededEpochsSnapshotSurvivesASweep: the snapshot's objects must survive
// whether or not the sweep's listing has caught up with the manifest that anchors
// them.
func TestASupersededEpochsSnapshotSurvivesASweep(t *testing.T) {
	tests := []struct {
		name string
		// close records the promotion boundary below the snapshot's target, which is
		// what takes the snapshot's last object out of the durable prefix.
		close bool
		// listLags hides the manifest from LIST while GET still returns it — the
		// eventually consistent listing of §6.1.
		listLags bool
	}{
		{name: "superseded epoch, listing has not caught up with the manifest", close: true, listLags: true},
		{name: "superseded epoch, listing sees the manifest", close: true},
		{name: "open epoch, listing has not caught up with the manifest", listLags: true},
		{name: "open epoch, listing sees the manifest"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			store := newStore(clk)
			vol := vol9()
			writeDurableWAL(t, store, clk, vol) // sequences 1 and 2, both ACKed

			if tc.close {
				closeEpoch(t, store, vol, 1) // the boundary lands below the snapshot's target
			}
			clk.Advance(48 * time.Hour) // the objects are long past any grace period
			if tc.listLags {
				store.SetListLag(1_000)
			}
			objects := publishSnapshot(t, store, vol, 2)
			if len(objects) != 2 {
				t.Fatalf("setup: the snapshot should anchor both objects, got %v", objects)
			}

			reachable, err := gc.Reachable(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			marked, err := gc.Mark(ctx, store, clk, reachable, grace)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range objects {
				if slices.Contains(marked, key) {
					t.Errorf("the published snapshot's object %s was marked (marked = %v)", key, marked)
				}
				if _, err := store.Get(ctx, key); err != nil {
					t.Errorf("the published snapshot's data is gone: %s: %v", key, err)
				}
			}
		})
	}
}

// TestSweepStopsWhenAnEpochBoundaryCannotBeRead: whether an epoch has been closed
// decides whether its objects are roots, and the answer comes from a GET at a
// deterministic key. A backend that will not answer it (§24 throttling, a damaged
// object) leaves the sweep unable to tell a superseded epoch from an open one, and
// that is a reason to stop — the same discipline as an unreadable anchor.
func TestSweepStopsWhenAnEpochBoundaryCannotBeRead(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol)
	closeEpoch(t, store, vol, 2)
	clk.Advance(48 * time.Hour)

	// Every read of the boundary object fails, however many times the sweep asks.
	store.InjectThrottleKey("wal/"+format.UUIDString(vol)+"/2/recovery-point.json", 100)

	if _, err := gc.Reachable(ctx, store); err == nil {
		t.Fatal("a boundary the sweep cannot read must abort it, not leave it guessing")
	}
	objects, err := recovery.ObjectKeysUpTo(ctx, store, vol, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range objects {
		if _, err := store.Get(ctx, key); err != nil {
			t.Fatalf("the aborted sweep marked %s: %v", key, err)
		}
	}
}

// TestASupersededEpochsLatePutIsNoLongerCollected is the cost of the rule above, pinned
// deliberately: an object a fenced writer landed past the boundary is real bytes the
// sweep can no longer tell apart from a snapshot's object under a lagging listing, so
// it stays. It is a handful of objects per promotion, against an unbounded loss —
// INV-14's own trade. Collecting them is an operator action on a named epoch, never an
// inference the sweep makes for itself.
func TestASupersededEpochsLatePutIsNoLongerCollected(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol)
	closeEpoch(t, store, vol, 2)

	late := "wal/" + format.UUIDString(vol) + "/1/90-91-deadbeef.wal"
	if _, err := store.Put(ctx, late, []byte("late write from a fenced writer"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour)

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	marked, err := gc.Mark(ctx, store, clk, reachable, grace)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(marked, late) {
		t.Fatalf("a superseded epoch's object was marked: %v", marked)
	}
}
