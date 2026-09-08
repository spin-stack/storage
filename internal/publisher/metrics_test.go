package publisher_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The §28 layer numbers, asserted against the object that actually reached the bucket.
// "A metric was recorded" would pass on a publisher reporting the plaintext length, the
// virtual size, or zero — and the number an operator sizes a bucket and a network with is
// the sealed one.
func TestPublishRecordsWhatLeftTheHost(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := obs.NewTestProvider("publisher-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	w := newWorld(t)
	w.pub.WithTelemetry(sim.NewClock(time.Unix(0, 0)), p.Recorder())
	if err := w.pub.Publish(ctx, w.layer); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	m, err := commit.ReadManifest(ctx, w.store, w.vol, w.layer.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := w.store.Head(ctx, m.Layer.ObjectKey)
	if err != nil {
		t.Fatal(err)
	}
	labels := `{volume="` + w.vol + `"}`

	size, err := p.HistogramSeries(ctx, "layer_size_bytes")
	if err != nil {
		t.Fatal(err)
	}
	if got := size[labels]; got.Count != 1 || got.Sum != float64(stored.Size) {
		t.Errorf("layer_size_bytes = %+v, want one sample of %d (the object in the bucket)", got, stored.Size)
	}
	sent, err := p.CounterSeries(ctx, "layer_upload_bytes_total")
	if err != nil {
		t.Fatal(err)
	}
	if sent[labels] != stored.Size {
		t.Errorf("layer_upload_bytes_total = %d, want %d", sent[labels], stored.Size)
	}
	// The plaintext is the number this must *not* be: it is what qcow measures
	// locally, and reporting it here would understate the bucket by a tag per frame.
	if stored.Size == int64(len(w.plain)) {
		t.Fatal("the sealed object is the same length as the plaintext; this test proves nothing")
	}

	// A second publish of a second layer adds to the counter and leaves one more sample.
	next := w.layer
	next.CommitID, next.LayerID = ids.New().String(), ids.New().String()
	next.Path = "/data/volumes/" + w.vol + "/layers/y.qcow2"
	w.files.files[next.Path] = w.plain
	if err := w.pub.Publish(ctx, next); err != nil {
		t.Fatalf("the second publish: %v", err)
	}
	sent, err = p.CounterSeries(ctx, "layer_upload_bytes_total")
	if err != nil {
		t.Fatal(err)
	}
	if sent[labels] != 2*stored.Size {
		t.Errorf("after two publishes layer_upload_bytes_total = %d, want %d", sent[labels], 2*stored.Size)
	}
}
