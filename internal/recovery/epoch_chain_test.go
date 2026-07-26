package recovery_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// A volume's sequence space is continuous across epochs: epoch N+1 starts at the
// sequence after the boundary the promotion recorded (§12.5). Recovery, however,
// scanned a single epoch's prefix — so a volume that has ever been promoted rebuilt
// with only the writes made since its last promotion, and reported that as complete.
// Every failover, drain, or evacuation after the first one lost everything written
// before it, with nothing to signal it.

// epochWriter appends records to a volume's epoch and uploads them.
func epochWriter(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, vol [16]byte, epoch, startSeq uint64) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	l := wal.NewLogAfter(d, "wal", clk, vol, epoch, startSeq, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, epoch, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	return l
}

// TestRecoverChainsAcrossEpochs: the writes from before a promotion must be in the
// rebuilt view.
func TestRecoverChainsAcrossEpochs(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	vol := vol7()

	// Epoch 1: two writes, both ACKed durable.
	e1 := epochWriter(t, store, clk, vol, 1, 0)
	if _, err := e1.Write(0, []byte("before-the-move"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	boundary := e1.Watermarks().Durable

	// The promotion records the epoch boundary, then epoch 2 continues the sequence.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, boundary); err != nil {
		t.Fatal(err)
	}
	e2 := epochWriter(t, store, clk, vol, 2, boundary)
	if _, err := e2.Write(4096, []byte("after-the-move"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e2.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	view, durable, err := recovery.Recover(ctx, store, nil, vol, 2)
	if err != nil {
		t.Fatalf("recover at epoch 2: %v", err)
	}
	if durable != boundary+1 {
		t.Fatalf("durable = %d, want %d (the sequence space is continuous across epochs)", durable, boundary+1)
	}
	buf := make([]byte, 15)
	view.Read(0, buf)
	if string(buf) != "before-the-move" {
		t.Fatalf("the pre-promotion write is missing from the rebuilt view: %q", buf)
	}
	after := make([]byte, 14)
	view.Read(4096, after)
	if string(after) != "after-the-move" {
		t.Fatalf("the post-promotion write is missing: %q", after)
	}
}

// TestRecoverChainsAcrossThreeEpochs: a volume moved twice (rolling maintenance) must
// still hold everything.
func TestRecoverChainsAcrossThreeEpochs(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	vol := vol7()

	var seq uint64
	for epoch := uint64(1); epoch <= 3; epoch++ {
		if epoch > 1 {
			if err := recovery.WriteRecoveryPoint(ctx, store, vol, epoch, epoch-1, seq); err != nil {
				t.Fatal(err)
			}
		}
		l := epochWriter(t, store, clk, vol, epoch, seq)
		if _, err := l.Write(uint64(epoch)*4096, []byte("epoch-"+string(rune('0'+epoch))), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		seq = l.Watermarks().Durable
	}

	view, durable, err := recovery.Recover(ctx, store, nil, vol, 3)
	if err != nil {
		t.Fatal(err)
	}
	if durable != seq {
		t.Fatalf("durable = %d, want %d", durable, seq)
	}
	for epoch := uint64(1); epoch <= 3; epoch++ {
		buf := make([]byte, 7)
		view.Read(epoch*4096, buf)
		if want := "epoch-" + string(rune('0'+epoch)); string(buf) != want {
			t.Fatalf("epoch %d's write is missing from the rebuilt view: %q", epoch, buf)
		}
	}
}

// TestRecoverRefusesABrokenChain: if the boundary that links an epoch to its
// predecessor is missing while the predecessor holds data, recovery must not quietly
// return the newest epoch alone — that is the silent-loss shape.
func TestRecoverRefusesABrokenChain(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	vol := vol7()

	e1 := epochWriter(t, store, clk, vol, 1, 0)
	if _, err := e1.Write(0, []byte("orphaned-by-a-missing-boundary"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	boundary := e1.Watermarks().Durable

	// Epoch 2 exists and continues the sequence, but its recovery point was never
	// written (a crash between the promotion and the boundary).
	e2 := epochWriter(t, store, clk, vol, 2, boundary)
	if _, err := e2.Write(4096, []byte("after"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e2.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if _, _, err := recovery.Recover(ctx, store, nil, vol, 2); err == nil {
		t.Fatal("recovery must refuse an epoch whose predecessor holds data it cannot chain to")
	}
}

// TestEpochChainReportsTheSpans: the chain is the contract other packages build on
// (materialization fetches per span), so its shape is asserted directly.
func TestEpochChainReportsTheSpans(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 5); err != nil {
		t.Fatal(err)
	}
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 2, 9); err != nil {
		t.Fatal(err)
	}

	spans, err := recovery.EpochChain(ctx, store, vol, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 3 {
		t.Fatalf("chain has %d spans, want 3", len(spans))
	}
	want := []recovery.EpochSpan{
		{Epoch: 1, From: 1, Upto: 5},  // ends where epoch 2's boundary says
		{Epoch: 2, From: 6, Upto: 9},  // ends where epoch 3's boundary says
		{Epoch: 3, From: 10, Upto: 0}, // open: the durable point decides
	}
	for i, w := range want {
		if spans[i] != w {
			t.Fatalf("span %d = %+v, want %+v", i, spans[i], w)
		}
	}
}

// TestEpochChainRefusesASelfReference: a boundary pointing at itself or forward is
// corrupt, and following it would loop or invent history.
func TestEpochChainRefusesASelfReference(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 2, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.EpochChain(ctx, store, vol, 2); err == nil {
		t.Fatal("a boundary pointing at its own epoch must be refused")
	}
}

// TestFirstEpochNeedsNoBoundary: the common case must stay simple.
func TestFirstEpochNeedsNoBoundary(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	spans, err := recovery.EpochChain(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0] != (recovery.EpochSpan{Epoch: 1, From: 1}) {
		t.Fatalf("chain = %+v", spans)
	}
}

// TestChainStopsWhenAnEarlierEpochIsUnreadable: walking back must not paper over an
// epoch whose objects cannot be listed — the volume's history would silently start
// later than it does.
func TestChainStopsWhenAnEarlierEpochIsUnreadable(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	vol := vol7()

	e1 := epochWriter(t, store, clk, vol, 1, 0)
	if _, err := e1.Write(0, []byte("epoch-one"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e1.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Epoch 3 claims to follow epoch 2 — which never existed — while epoch 1 holds
	// data. Recovering epoch 3 alone would drop it.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.EpochChain(ctx, store, vol, 3); err == nil {
		t.Fatal("a chain whose middle epoch has no boundary while an earlier one holds data must be refused")
	}
}
