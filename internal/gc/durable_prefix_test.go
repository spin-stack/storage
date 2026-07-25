package gc_test

import (
	"context"
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
	f, err := d.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("acked-write"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDurablePrefixIsAGCRoot: the objects that make up the durable point must survive
// a sweep even when nothing enumerates them.
func TestDurablePrefixIsAGCRoot(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
