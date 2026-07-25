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

func TestBatchCloseRules(t *testing.T) {
	def := wal.DefaultBatchConfig()
	tests := []struct {
		name   string
		cfg    wal.BatchConfig
		drive  func(b *wal.Batcher, clk *sim.Clock)
		reason wal.CloseReason
	}{
		{
			name: "FUA closes immediately",
			cfg:  def,
			drive: func(b *wal.Batcher, _ *sim.Clock) {
				b.Append(1, rec(1, []byte("a")), false)
				b.Append(2, rec(2, []byte("b")), true) // FUA
			},
			reason: wal.CloseFUA,
		},
		{
			name: "FLUSH closes",
			cfg:  def,
			drive: func(b *wal.Batcher, _ *sim.Clock) {
				b.Append(1, rec(1, []byte("x")), false)
				b.Flush()
			},
			reason: wal.CloseFlush,
		},
		{
			name: "target size closes",
			cfg:  wal.BatchConfig{TargetBytes: 500, MaxBytes: 2000, MaxAge: time.Hour},
			drive: func(b *wal.Batcher, _ *sim.Clock) {
				payload := make([]byte, 50)
				for seq := uint64(1); b.OpenBytes() < 500 && len(b.Pending()) == 0; seq++ {
					b.Append(seq, rec(seq, payload), false)
				}
			},
			reason: wal.CloseTarget,
		},
		{
			name: "age closes",
			cfg:  wal.BatchConfig{TargetBytes: 1 << 20, MaxBytes: 1 << 20, MaxAge: 20 * time.Second},
			drive: func(b *wal.Batcher, clk *sim.Clock) {
				b.Append(1, rec(1, []byte("x")), false)
				clk.Advance(21 * time.Second)
				b.MaybeCloseForAge()
			},
			reason: wal.CloseAge,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, clk := newBatcher(t, tc.cfg)
			tc.drive(b, clk)
			if len(b.Pending()) != 1 {
				t.Fatalf("expected exactly 1 closed batch, got %d", len(b.Pending()))
			}
			if got := b.Pending()[0].Reason; got != tc.reason {
				t.Fatalf("close reason = %s, want %s", got, tc.reason)
			}
		})
	}
}

func TestEmptyFlushIsNoop(t *testing.T) {
	b, _ := newBatcher(t, wal.DefaultBatchConfig())
	b.Flush()
	if len(b.Pending()) != 0 {
		t.Fatal("empty flush must not create a batch")
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
