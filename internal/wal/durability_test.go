package wal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

func remoteLeasedLog(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, lm *lease.Manager) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{7}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 5),
	)
	l.SetLease(lm)
	return l
}

// TestFlushAcksWhileLeaseValid: the happy path — a valid lease lets the FLUSH
// advance durable and ACK.
func TestFlushAcksWhileLeaseValid(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	_, _ = l.Write(0, []byte("data"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush with valid lease: %v", err)
	}
	if l.Watermarks().Durable != 1 {
		t.Fatalf("durable = %d, want 1", l.Watermarks().Durable)
	}
	if l.Fenced() {
		t.Fatal("should not be fenced")
	}
}

// TestFlushSelfFencesWhenLeaseExpired is INV-06 / §12.2: the object reaches S3, but
// because the lease is invalid at the instant of ACK, the write is NOT confirmed —
// durable does not advance and the log self-fences.
func TestFlushSelfFencesWhenLeaseExpired(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	_, _ = l.Write(0, []byte("data"), 0)

	clk.Advance(11 * time.Second) // lease expires, no renewal

	err := l.Flush(ctx)
	if !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("expired lease flush: want ErrSelfFenced, got %v", err)
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable must NOT advance with an invalid lease, got %d", l.Watermarks().Durable)
	}
	if !l.Fenced() {
		t.Fatal("log should have self-fenced")
	}
	// The object nonetheless landed in S3 (the §12.2 case: PUT succeeded, ACK did not).
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("object should be in S3 despite no ACK, got %d", len(objs))
	}
	// Once fenced, further flushes refuse.
	if err := l.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("fenced log should keep refusing, got %v", err)
	}
}

// TestLocalModeIgnoresLeaseForFlush is §14.8 rule 3 / §23: a `local` volume ACKs a
// FLUSH after local fdatasync even with an expired lease (PG-down does not stop it).
func TestLocalModeIgnoresLeaseForFlush(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	l.SetDurabilityMode(wal.ModeLocal)
	_, _ = l.Write(0, []byte("data"), 0)

	clk.Advance(30 * time.Second) // lease long expired
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("local-mode flush must ACK regardless of the lease: %v", err)
	}
	if l.Fenced() {
		t.Fatal("local mode must not self-fence on FLUSH")
	}
}

// TestNoLeaseConfiguredStillAcks: dev without a CP/lease.
func TestNoLeaseConfiguredStillAcks(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	l := remoteLog(t, store) // no lease set
	_ = clk
	_, _ = l.Write(0, []byte("data"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush without a lease gate should ACK: %v", err)
	}
	if l.Watermarks().Durable != 1 {
		t.Fatalf("durable = %d, want 1", l.Watermarks().Durable)
	}
}
