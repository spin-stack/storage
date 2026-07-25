package wal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
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
		lm,
	)
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

// (Removed) TestNoLeaseConfiguredStillAcks asserted that a remote-durability log
// with no lease checker "should ACK". That was the specification of DEV-0004: it
// made an unfenced writer legal, and every other fencing proof was conditional on
// somebody remembering to call SetLease. TestRemoteModeWithoutALeaseFailsClosed
// below asserts the opposite, which is what §12.2 requires.

// TestModeForCoversEveryDurability: the data path and the Control Plane must agree on
// the §14.8 vocabulary. If a mode is added to lifecycle and not mapped here, this
// fails — the two ends cannot drift apart silently (ADR-0009).
func TestModeForCoversEveryDurability(t *testing.T) {
	for _, d := range lifecycle.Durabilities() {
		mode, err := wal.ModeFor(d)
		if err != nil {
			t.Fatalf("durability %q has no data-path mode: %v", d, err)
		}
		if got := mode.Durability(); got != d {
			t.Fatalf("round trip %q -> %v -> %q", d, mode, got)
		}
		if mode.String() != d.String() {
			t.Fatalf("String() = %q, want %q", mode.String(), d)
		}
	}
}

// TestModeForRejectsUnknown: an unmapped value fails closed instead of quietly
// promising remote durability the volume never asked for.
func TestModeForRejectsUnknown(t *testing.T) {
	if _, err := wal.ModeFor(lifecycle.Durability("eventual")); !errors.Is(err, lifecycle.ErrUnknownState) {
		t.Fatalf("want ErrUnknownState, got %v", err)
	}
}

// TestRemoteModeWithoutALeaseFailsClosed is DEV-0004: the durable-ACK rule of §12.2
// was opt-in — a remote-durability log built without a lease checker ACKed FLUSH with
// no lease at all. A missing fence must fail closed, not open.
func TestRemoteModeWithoutALeaseFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{7}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), nil)

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); !errors.Is(err, wal.ErrNoLease) {
		t.Fatalf("remote FLUSH without a lease checker must fail closed, got %v", err)
	}
	if w := l.Watermarks(); w.Durable != 0 {
		t.Fatalf("durable advanced to %d on a fenceless flush", w.Durable)
	}
}

// TestLocalModeWithoutALeaseStillAcks is the other half of §14.8 rule 3: in local
// mode the lease does not gate the FLUSH ACK, so no lease is required.
func TestLocalModeWithoutALeaseStillAcks(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	d := sim.NewDisk()
	f, _ := d.Create("wal/local.wal")
	vol := [16]byte{8}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), nil)
	l.SetDurabilityMode(wal.ModeLocal)

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("local-mode FLUSH must ACK without a lease: %v", err)
	}
	if l.Fenced() {
		t.Fatal("local mode must not self-fence for a missing lease")
	}
	// The remote durable watermark stays put on purpose: in local mode the ACK is on
	// fdatasync and S3 catches up asynchronously (§14.8), which is what the RPO
	// metric measures.
}
