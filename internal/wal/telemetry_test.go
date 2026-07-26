package wal_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// DEV-0010: the metric catalog was registered and nothing ever wrote to it. These
// assert the numbers an operator actually reads — the watermarks that are the
// volume's RPO, and the self-fencing counter — are recorded by the path that owns
// them, not merely declared.
func TestFlushRecordsTheWatermarkMetrics(t *testing.T) {
	ctx := t.Context()
	p, err := obs.NewTestProvider("wal-telemetry")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	d := sim.NewDisk()
	vol := [16]byte{7}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), lm)
	l.SetRecorder(obs.NewRecorder(p.Metrics), "vol-7")

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"wal_local_sequence", "wal_durable_sequence", "wal_published_sequence",
		"wal_unflushed_bytes", "wal_durable_gap_bytes",
	} {
		if !got[name] {
			t.Fatalf("%s is declared but never recorded; collected: %v", name, got)
		}
	}
}

func TestSelfFencingIsCounted(t *testing.T) {
	ctx := t.Context()
	p, err := obs.NewTestProvider("wal-telemetry")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	d := sim.NewDisk()
	vol := [16]byte{7}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), lm)
	l.SetRecorder(obs.NewRecorder(p.Metrics), "vol-7")

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // the lease expires
	if err := l.Flush(ctx); err == nil {
		t.Fatal("expected the flush to self-fence")
	}
	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got["self_fenced_total"] {
		t.Fatalf("a self-fencing event must be counted; collected: %v", got)
	}
}
