package recovery_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The §28 recovery numbers. Both are asserted against what the bucket says the chain
// weighs and against the clock the restore ran on, because a counter that is merely
// present tells an operator nothing about how long a host is unavailable after a
// placement, or what that placement cost the object store.
func TestARestoreRecordsWhatItCostAndHowLongItTook(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := obs.NewTestProvider("recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	w := newWorld(t, 3)
	clk := sim.NewClock(time.Unix(0, 0))
	// A restore's time is spent downloading layers and running qemu-img over them; the
	// simulated clock moves where the second of those happens.
	w.run.onRun = func() { clk.Advance(time.Second) }
	w.rec.WithTelemetry(clk, p.Recorder())

	if _, err := w.rec.Restore(ctx, w.vol, virtualSize); err != nil {
		t.Fatalf("restoring: %v", err)
	}

	// What the chain weighs in the bucket, from the manifests rather than from this
	// test's own arithmetic.
	var want int64
	for _, id := range w.commits {
		m, err := commit.ReadManifest(ctx, w.store, w.vol, id)
		if err != nil {
			t.Fatal(err)
		}
		want += m.Layer.SizeBytes
	}
	labels := `{volume="` + w.vol + `"}`
	got, err := p.CounterSeries(ctx, "recovery_download_bytes_total")
	if err != nil {
		t.Fatal(err)
	}
	if got[labels] != want {
		t.Errorf("recovery_download_bytes_total = %d, want %d (the three sealed layers)", got[labels], want)
	}
	took, err := p.HistogramSeries(ctx, "recovery_duration_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if s := took[labels]; s.Count != 1 || s.Sum <= 0 {
		t.Fatalf("recovery_duration_seconds = %+v, want one sample of the time the restore took", s)
	}

	// A second restore on the same host downloads nothing: every layer is already here
	// and recorded in state.json, so the counter must not move. A recovery that recorded
	// bytes it did not fetch would read as a fleet re-downloading its chains on every
	// placement.
	if _, err := w.rec.Restore(ctx, w.vol, virtualSize); err != nil {
		t.Fatalf("restoring again: %v", err)
	}
	got, err = p.CounterSeries(ctx, "recovery_download_bytes_total")
	if err != nil {
		t.Fatal(err)
	}
	if got[labels] != want {
		t.Errorf("after a restore that downloaded nothing the counter is %d, want %d", got[labels], want)
	}
	took, err = p.HistogramSeries(ctx, "recovery_duration_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if s := took[labels]; s.Count != 2 {
		t.Errorf("recovery_duration_seconds has %d samples, want one per restore", s.Count)
	}
}

// A same-host clone downloads nothing: every layer it needs is already on this disk under
// its parent, and copyLocal puts it where the clone needs it. The bytes counter must stay
// silent for the clone, or the series an operator watches to size object-store egress
// reports a local file copy as a download — the cost §19 exists to avoid, billed as if it
// had been paid.
//
// The layer objects are removed from the bucket first, so the only way this restore can
// succeed at all is off the local disk. Held-and-present and copied-from-a-parent are two
// different branches of materialize; the test above only reaches the first.
func TestASameHostCloneDownloadsNothingAndSaysSo(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := obs.NewTestProvider("recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	w := newWorld(t, 3)
	clk := sim.NewClock(time.Unix(0, 0))
	w.run.onRun = func() { clk.Advance(time.Second) }
	w.rec.WithTelemetry(clk, p.Recorder())

	if _, err := w.rec.Restore(ctx, w.vol, virtualSize); err != nil {
		t.Fatalf("restoring the parent: %v", err)
	}
	objs, err := w.store.List(ctx, "layers/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		t.Fatal("the fixture published no layer objects")
	}
	for _, o := range objs {
		if err := w.store.Delete(ctx, o.Key); err != nil {
			t.Fatal(err)
		}
	}

	clone := ids.New().String()
	w.keys.keys[clone] = cloneKeys(t, w, clone)
	if _, err := w.rec.RestoreFrom(ctx, qcow.Lineage{
		VolumeID: clone, ParentVolumeID: w.vol, ParentCommitID: w.commits[len(w.commits)-1],
	}, virtualSize); err != nil {
		t.Fatalf("the clone went to the object store for layers this host already holds: %v", err)
	}

	got, err := p.CounterSeries(ctx, "recovery_download_bytes_total")
	if err != nil {
		t.Fatal(err)
	}
	if n := got[`{volume="`+clone+`"}`]; n != 0 {
		t.Errorf("a same-host clone recorded %d downloaded bytes; it copied every layer off the local disk", n)
	}
	// And the restore is still timed, so the assertion above cannot be satisfied by a
	// clone path that records nothing at all.
	took, err := p.HistogramSeries(ctx, "recovery_duration_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if s := took[`{volume="`+clone+`"}`]; s.Count != 1 {
		t.Fatalf("recovery_duration_seconds for the clone = %+v, want one sample", s)
	}
}
