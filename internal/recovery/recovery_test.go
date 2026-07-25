package recovery_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

const rvol = "00000000-0000-7000-8000-000000000030"

// putBatch uploads a WAL object covering [first,last] for the volume/epoch.
func putBatch(t *testing.T, store *sim.ObjectStore, volID [16]byte, epoch, first, last uint64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	b := wal.NewBatcher(clk, volID, epoch, 0, wal.DefaultBatchConfig())
	for seq := first; seq <= last; seq++ {
		enc, _ := wal.Record{Sequence: seq, Epoch: epoch, Payload: []byte("x")}.Encode()
		b.Append(seq, enc, false)
	}
	b.Flush()
	if _, err := wal.NewUploader(store, 3).Upload(context.Background(), b.Pending()[0]); err != nil {
		t.Fatal(err)
	}
}

func TestDurablePrefixContiguous(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	var volID [16]byte
	volID[6], volID[8] = 0x70, 0x80 // v7 shape; UUIDString computed below

	// Objects [1-2] and [3-4] are contiguous → durable prefix ends at 4.
	putBatch(t, store, volID, 1, 1, 2)
	putBatch(t, store, volID, 1, 3, 4)

	vs := format.UUIDString(volID)
	last, err := recovery.DurablePrefix(ctx, store, vs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 {
		t.Fatalf("durable prefix = %d, want 4", last)
	}
}

func TestDurablePrefixStopsAtGap(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	var volID [16]byte
	volID[6], volID[8] = 0x70, 0x80

	// [1-2] present, [3-4] MISSING, [5-6] present → contiguous prefix ends at 2.
	putBatch(t, store, volID, 1, 1, 2)
	putBatch(t, store, volID, 1, 5, 6)

	last, err := recovery.DurablePrefix(ctx, store, format.UUIDString(volID), 1)
	if err != nil {
		t.Fatal(err)
	}
	if last != 2 {
		t.Fatalf("durable prefix past a gap = %d, want 2", last)
	}
}

func TestDurablePrefixEmpty(t *testing.T) {
	last, err := recovery.DurablePrefix(context.Background(), sim.NewObjectStore(), rvol, 1)
	if err != nil || last != 0 {
		t.Fatalf("empty epoch: last=%d err=%v", last, err)
	}
}

func TestRecoveryPointRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	if err := recovery.WriteRecoveryPoint(ctx, store, rvol, 2, 1, 42); err != nil {
		t.Fatal(err)
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, store, rvol, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rp.PrevEpoch != 1 || rp.RecoveredUpTo != 42 {
		t.Fatalf("recovery point = %+v", rp)
	}
}
