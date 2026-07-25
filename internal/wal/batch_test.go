package wal_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func newBatcher(t *testing.T, cfg wal.BatchConfig) (*wal.Batcher, *sim.Clock) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return wal.NewBatcher(clk, [16]byte{1}, 1, 0, cfg), clk
}

func rec(seq uint64, payload []byte) []byte {
	b, _ := wal.Record{Type: format.RecordWrite, Epoch: 1, Sequence: seq, Payload: payload}.Encode()
	return b
}

func TestBatchClosesOnFUA(t *testing.T) {
	b, _ := newBatcher(t, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("a")), false)
	if len(b.Pending()) != 0 {
		t.Fatal("no close expected before FUA")
	}
	b.Append(2, rec(2, []byte("b")), true) // FUA
	if len(b.Pending()) != 1 {
		t.Fatalf("FUA should close the batch, pending=%d", len(b.Pending()))
	}
	if b.Pending()[0].Reason != wal.CloseFUA || b.Pending()[0].Count != 2 {
		t.Fatalf("unexpected closed batch: %+v", b.Pending()[0])
	}
}

func TestBatchClosesOnFlush(t *testing.T) {
	b, _ := newBatcher(t, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("x")), false)
	b.Flush()
	if len(b.Pending()) != 1 || b.Pending()[0].Reason != wal.CloseFlush {
		t.Fatalf("flush should close batch: %+v", b.Pending())
	}
	// Flush again with nothing open is a no-op.
	b.Flush()
	if len(b.Pending()) != 1 {
		t.Fatal("empty flush must not create a batch")
	}
}

func TestBatchClosesOnTargetAndMax(t *testing.T) {
	cfg := wal.BatchConfig{TargetBytes: 500, MaxBytes: 2000, MaxAge: time.Hour}
	b, _ := newBatcher(t, cfg)
	// One ~154-byte record (104 header + 50 payload) at a time until target.
	payload := make([]byte, 50)
	seq := uint64(0)
	for b.OpenBytes() < cfg.TargetBytes && len(b.Pending()) == 0 {
		seq++
		b.Append(seq, rec(seq, payload), false)
	}
	if len(b.Pending()) != 1 || b.Pending()[0].Reason != wal.CloseTarget {
		t.Fatalf("should close on target: %+v", b.Pending())
	}
}

func TestBatchClosesOnAge(t *testing.T) {
	cfg := wal.BatchConfig{TargetBytes: 1 << 20, MaxBytes: 1 << 20, MaxAge: 20 * time.Second}
	b, clk := newBatcher(t, cfg)
	b.Append(1, rec(1, []byte("x")), false)
	b.MaybeCloseForAge()
	if len(b.Pending()) != 0 {
		t.Fatal("not old enough yet")
	}
	clk.Advance(21 * time.Second)
	b.MaybeCloseForAge()
	if len(b.Pending()) != 1 || b.Pending()[0].Reason != wal.CloseAge {
		t.Fatalf("should close on age: %+v", b.Pending())
	}
}

func TestObjectAssemblyRoundTrip(t *testing.T) {
	b, _ := newBatcher(t, wal.DefaultBatchConfig())
	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for i, p := range payloads {
		b.Append(uint64(i+1), rec(uint64(i+1), p), false)
	}
	b.Flush()
	cb := b.TakePending()[0]

	key, data, sha := cb.Object()

	// Header parses and matches.
	h, err := format.UnmarshalObjectHeader(data[:format.ObjectHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if h.FirstSequence != 1 || h.LastSequence != 3 || h.RecordCount != 3 {
		t.Fatalf("object header wrong: %+v", h)
	}
	if h.PayloadSHA256 != sha {
		t.Fatal("header SHA mismatch")
	}
	if h.PayloadLength != uint64(len(data)-format.ObjectHeaderSize) {
		t.Fatalf("payload length mismatch: %d vs %d", h.PayloadLength, len(data)-format.ObjectHeaderSize)
	}

	// Records replay from the object payload.
	recs, err := wal.Replay(data[format.ObjectHeaderSize:])
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 records in the object, got %d", len(recs))
	}

	// Key is deterministic and embeds the sequence range + sha prefix.
	if key != format.WALObjectKey([16]byte{1}, 1, 1, 3, sha) {
		t.Fatalf("unexpected key %q", key)
	}
}
