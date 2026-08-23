package obs_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

// DEV-0010. The §26.2 catalog was registered at startup and nothing ever recorded to
// it: no counter or gauge was touched by any production path, while the
// phase documents said metrics were flowing. A Recorder makes recording possible
// without every package importing OTel, and the nil Recorder has to be safe — a
// component built without observability must still run.

func TestNilRecorderIsSafe(t *testing.T) {
	var r *obs.Recorder // never assigned
	ctx := t.Context()
	// None of these may panic: a data path is not allowed to fail because telemetry
	// was not wired.
	r.Count(ctx, "clone_same_host_total", 10)
	r.Gauge(ctx, "chain_depth", 7)
	r.Count(ctx, "not_in_the_catalog_total", 1)
}

func TestRecorderWritesToTheRegisteredInstruments(t *testing.T) {
	p, err := obs.NewTestProvider("recorder-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(t.Context()) }()
	ctx := t.Context()

	r := obs.NewRecorder(p.Metrics)
	r.Count(ctx, "clone_same_host_total", 42)
	r.Gauge(ctx, "chain_depth", 9)

	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"clone_same_host_total", "chain_depth"} {
		if !got[name] {
			t.Fatalf("%s was never recorded; collected: %v", name, got)
		}
	}
}

// TestRecorderIgnoresUnknownNames: a typo must not create a new series (that is what
// the fixed catalog is for, §26.2) and must not panic either.
func TestRecorderIgnoresUnknownNames(t *testing.T) {
	p, err := obs.NewTestProvider("recorder-test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(t.Context()) }()
	ctx := t.Context()

	r := obs.NewRecorder(p.Metrics)
	r.Count(ctx, "wal_typo_total", 1)

	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got["wal_typo_total"] {
		t.Fatal("an unregistered metric name must not create a series")
	}
}
