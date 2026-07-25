package wal_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// metricSink is a manual-reader metric pipeline whose *values* a test can read.
// obs.Provider only reports which names carry data, and "wal_durable_gap_bytes was
// recorded" is not the property an operator depends on — the number is. A gauge that
// is published as 0 while the backlog grows is worse than one that is missing.
type metricSink struct {
	rec    *obs.Recorder
	reader *sdkmetric.ManualReader
}

func newMetricSink(t *testing.T) *metricSink {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := obs.NewMetrics(mp.Meter("wal-rpo"))
	if err != nil {
		t.Fatal(err)
	}
	return &metricSink{rec: obs.NewRecorder(m), reader: reader}
}

// gauge returns the last recorded value of a gauge and whether it was recorded at all.
func (s *metricSink) gauge(t *testing.T, name string) (float64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := s.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok || len(g.DataPoints) == 0 {
				return 0, false
			}
			return g.DataPoints[len(g.DataPoints)-1].Value, true
		}
	}
	return 0, false
}

// localModeLog builds a `local` durability log with no remote path at all — the
// §14.8 rule-3 volume, and the shape an operator watches through the RPO gauges.
func localModeLog(t *testing.T, sink *metricSink) (*wal.Log, *sim.Clock) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, err := d.Create("wal/local.wal")
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, [16]byte{5}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.SetDurabilityMode(wal.ModeLocal)
	l.SetRecorder(sink.rec, "vol-5")
	return l, clk
}

// TestLocalModeFlushPublishesTheRPOGauges: in `local` mode the ACK is on fdatasync
// and S3 catches up later, so wal_durable_gap_bytes is the *only* RPO signal there
// is — §26.2 calls it critical in exactly this mode. Today the local-mode FLUSH
// returns before any gauge is published, so the volume with the real RPO exposure is
// the one that reports nothing.
func TestLocalModeFlushPublishesTheRPOGauges(t *testing.T) {
	ctx := context.Background()
	sink := newMetricSink(t)
	l, _ := localModeLog(t, sink)

	for i := range 3 {
		if _, err := l.Write(uint64(i)*64, []byte("guest-data"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"wal_local_sequence", "wal_durable_sequence", "wal_durable_gap_bytes"} {
		if _, ok := sink.gauge(t, name); !ok {
			t.Fatalf("%s is never recorded in local mode, where §26.2 calls it critical", name)
		}
	}
}

// TestDurableGapBytesCountsWhatS3DoesNotHave: the gauge means "bytes not yet durable
// in S3" (§26.2). It is currently wired to the *unsynced* byte counter, which a
// local-mode FLUSH (and a bare Sync) resets — so the backlog that a host loss would
// destroy grows without bound while the gauge reads 0.
func TestDurableGapBytesCountsWhatS3DoesNotHave(t *testing.T) {
	ctx := context.Background()
	sink := newMetricSink(t)
	l, _ := localModeLog(t, sink)

	for i := range 4 {
		if _, err := l.Write(uint64(i)*64, []byte("guest-data"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(ctx); err != nil { // ACKs on fdatasync; nothing reached S3
		t.Fatal(err)
	}

	gap, ok := sink.gauge(t, "wal_durable_gap_bytes")
	if !ok {
		t.Fatal("wal_durable_gap_bytes was never recorded")
	}
	if gap <= 0 {
		t.Fatalf("wal_durable_gap_bytes = %v after 4 ACKed FLUSHed records that no S3 object covers", gap)
	}
}

// TestSyncDoesNotClearTheRemoteDurabilityAccounting: fdatasync makes bytes durable on
// the host, which is precisely the durability a host loss destroys. Clearing the
// remote-gap accounting there reports RPO 0 for data no other machine has.
func TestSyncDoesNotClearTheRemoteDurabilityAccounting(t *testing.T) {
	ctx := context.Background()
	sink := newMetricSink(t)
	l, _ := localModeLog(t, sink)

	if _, err := l.Write(0, []byte("only-on-this-host"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	gap, ok := sink.gauge(t, "wal_durable_gap_bytes")
	if !ok {
		t.Fatal("wal_durable_gap_bytes was never recorded")
	}
	if gap <= 0 {
		t.Fatalf("Sync() cleared the remote-durability accounting: gap = %v with an empty bucket", gap)
	}
}

// TestVerifiedUploadIsWhatClosesTheGap: the other direction — the gap must fall only
// when an object is verified in the store, and reach 0 when everything is covered.
func TestVerifiedUploadIsWhatClosesTheGap(t *testing.T) {
	ctx := context.Background()
	sink := newMetricSink(t)
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{6}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	l.SetRecorder(sink.rec, "vol-6")

	if _, err := l.Write(0, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}

	store.InjectThrottle(5) // the upload budget is exhausted: nothing lands in S3
	if err := l.Flush(ctx); err == nil {
		t.Fatal("flush should fail while the store is throttled")
	}
	// A failed upload is exactly when an operator needs the number, so the gauge must
	// be published there too — and it must show the whole backlog.
	gap, ok := sink.gauge(t, "wal_durable_gap_bytes")
	if !ok {
		t.Fatal("wal_durable_gap_bytes is not published when the upload fails")
	}
	if gap <= 0 {
		t.Fatalf("gap = %v after a failed upload; no object exists", gap)
	}

	if err := l.Flush(ctx); err != nil { // the throttle cleared; the batch was retained
		t.Fatal(err)
	}
	gap, ok = sink.gauge(t, "wal_durable_gap_bytes")
	if !ok {
		t.Fatal("wal_durable_gap_bytes was never recorded")
	}
	if gap != 0 {
		t.Fatalf("gap = %v once every record is in a verified object, want 0", gap)
	}
}
