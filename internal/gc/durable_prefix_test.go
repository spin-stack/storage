package gc_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// INV-14 says the GC cannot cause the worst incident. Reachability was computed from
// structural objects only — descriptors, epoch objects, manifests, checkpoints — so
// every WAL object that no manifest or checkpoint happens to enumerate was an orphan.
// That is *every ACKed write since the last checkpoint*, and for a volume that has
// never checkpointed it is the entire log: a scheduled sweep would delete-marker the
// durable prefix under a live writer and recovery would then report a durable point
// of zero. The GC would be the incident.
//
// The durable prefix is a root, by definition: it is what §5.8 calls the authority.

func vol9() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	v[15] = 9
	return v
}

// writeDurableWAL puts two ACKed WAL objects in the store, the way a volume that has
// never been checkpointed looks.
func writeDurableWAL(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, vol [16]byte) {
	t.Helper()
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("acked-write"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDurablePrefixIsAGCRoot: the objects that make up the durable point must survive
// a sweep even when nothing enumerates them.
func TestDurablePrefixIsAGCRoot(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol)

	before, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil || before == 0 {
		t.Fatalf("setup: durable prefix = %d err=%v", before, err)
	}

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour) // well past any grace period
	marked, err := gc.Mark(ctx, store, clk, reachable, time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	after, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a GC sweep moved the durable point from %d to %d (marked %v) — the GC destroyed ACKed data",
			before, after, marked)
	}
}

// TestOrphansPastTheDurablePrefixAreStillCollected: making the prefix a root must not
// turn the GC off. An object beyond a gap is not durable and is exactly what §22.1
// says will be GC'd as an orphan.
func TestOrphansPastTheDurablePrefixAreStillCollected(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol)

	// A late PUT from a fenced writer, past a gap: sequences 90..91 with 3..89 missing.
	orphan := "wal/" + format.UUIDString(vol) + "/1/90-91-deadbeef.wal"
	if _, err := store.Put(ctx, orphan, []byte("late write from a fenced writer"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour)
	marked, err := gc.Mark(ctx, store, clk, reachable, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	var sawOrphan bool
	for _, m := range marked {
		if m == orphan {
			sawOrphan = true
		}
	}
	if !sawOrphan {
		t.Fatalf("the object past the gap was not collected; marked = %v", marked)
	}
	if got, _ := recovery.DurablePrefix(ctx, store, vol, 1); got != 2 {
		t.Fatalf("durable prefix = %d after collecting the orphan, want 2", got)
	}
}

// TestSweepStopsWhenTheDurablePointCannotBeEstablished: if a prefix cannot be read,
// the GC must not proceed to mark anything under it. An unreadable prefix is a reason
// to stop and page someone, not a licence to sweep.
func TestSweepStopsWhenTheDurablePointCannotBeEstablished(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol)

	// A summary that claims more than the prefix provides: recovery refuses to name a
	// durable point at all (§22.1).
	lie := []byte(`{"volume_id":"` + format.UUIDString(vol) + `","epoch":1,"durable_sequence":9999}`)
	if _, err := store.Put(ctx, wal.SummaryKey(vol, 1), lie, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	if _, err := gc.Reachable(ctx, store); err == nil {
		t.Fatal("reachability must fail when a durable point cannot be established")
	}
}

// TestReachableCoversEveryEpochOfAVolume: a volume that was promoted has objects
// under more than one epoch, and the older epoch's prefix is still what a recovery
// point points back to.
func TestReachableCoversEveryEpochOfAVolume(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	vol := vol9()
	writeDurableWAL(t, store, clk, vol) // epoch 1

	// Epoch 2 starts after the boundary the promotion recorded.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 2); err != nil {
		t.Fatal(err)
	}
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 2, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 2, 2, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	if _, err := l.Write(0, []byte("after the move"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour)
	if _, err := gc.Mark(ctx, store, clk, reachable, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []uint64{1, 2} {
		got, err := recovery.DurablePrefix(ctx, store, vol, epoch)
		if err != nil {
			t.Fatalf("epoch %d: %v", epoch, err)
		}
		if got == 0 {
			t.Fatalf("the sweep emptied epoch %d's durable prefix", epoch)
		}
	}
}
