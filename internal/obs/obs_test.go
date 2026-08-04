package obs_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

func newProvider(t *testing.T) *obs.Provider {
	t.Helper()
	p, err := obs.NewTestProvider("test")
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

// TestMetricsCatalogWellFormed guards the stable-cardinality contract: no
// duplicate names, every entry documented, and the critical §26.2 series present.
func TestMetricsCatalogWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, d := range obs.Catalog() {
		if d.Name == "" {
			t.Fatal("metric with empty name")
		}
		if d.Help == "" {
			t.Fatalf("metric %q has no help text", d.Name)
		}
		if seen[d.Name] {
			t.Fatalf("duplicate metric name %q", d.Name)
		}
		seen[d.Name] = true
	}
	// A few load-bearing names must exist (typo/regression guard).
	for _, want := range []string{
		"wal_out_of_space",                // the device is refusing writes (§5.7)
		"image_publish_duration_seconds",  // the cost of a session leaving the host (ADR-0026)
		"snapshot_pause_duration_seconds", // ~0 invariant (§19)
		"s3_request_latency_seconds",      // tail latency (§24)
		"clock_offset_seconds",            // drift alert (§23)
	} {
		if !seen[want] {
			t.Fatalf("required metric %q missing from catalog", want)
		}
	}
}

// TestMetricsRegistration asserts every catalog entry becomes a live instrument.
func TestMetricsRegistration(t *testing.T) {
	p := newProvider(t)
	if got, want := p.Metrics.Len(), len(obs.Catalog()); got != want {
		t.Fatalf("registered %d instruments, catalog has %d", got, want)
	}
	// Spot-check retrieval by the correct kind.
	if _, ok := p.Metrics.Counter("lease_renewal_failures_total"); !ok {
		t.Fatal("lease_renewal_failures_total should be a counter")
	}
	if _, ok := p.Metrics.Histogram("image_publish_duration_seconds"); !ok {
		t.Fatal("image_publish_duration_seconds should be a histogram")
	}
	if _, ok := p.Metrics.Gauge("wal_unflushed_bytes"); !ok {
		t.Fatal("wal_unflushed_bytes should be a gauge")
	}
}
