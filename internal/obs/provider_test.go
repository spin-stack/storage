package obs_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

// The no-telemetry deployment is the default one, and it has to be indistinguishable
// from today's `Recorder: nil`: an Agent started without an endpoint records into
// nothing, shuts down clean, and never fails a data-path call because observability was
// not configured. If this needed a branch at the call site, every recording site in the
// tree would grow one.
func TestAProviderWithoutAnExporterRecordsIntoNothing(t *testing.T) {
	ctx := t.Context()

	provider, err := obs.NewProvider("volume-agent", nil)
	if err != nil {
		t.Fatalf("NewProvider without an exporter: %v", err)
	}

	r := provider.Recorder()
	if r == nil {
		t.Fatal("a provider with no exporter handed out no Recorder, so every caller would need a nil check")
	}
	r.Count(ctx, "lease_renewal_failures_total", 1, obs.String("host", "host-a"))
	r.Gauge(ctx, "wal_out_of_space", 1, obs.String("volume", "vol-1"))
	r.Observe(ctx, "image_publish_duration_seconds", 0.25, obs.String("volume", "vol-1"))

	if err := provider.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown of a provider with no exporter: %v", err)
	}
}

// CollectedMetrics and GaugeValues read the test provider's manual reader. A production
// provider has no such reader, and the useful failure is a sentence saying so rather
// than a nil dereference inside a test that was pointed at the wrong constructor.
func TestCollectingFromAProductionProviderIsRefused(t *testing.T) {
	provider, err := obs.NewProvider("volume-agent", nil)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if _, err := provider.CollectedMetrics(t.Context()); err == nil {
		t.Fatal("CollectedMetrics answered for a provider that has no manual reader")
	}
	if _, err := provider.GaugeValues(t.Context()); err == nil {
		t.Fatal("GaugeValues answered for a provider that has no manual reader")
	}
}

// The Recorder a test provider hands out is the same one production code takes, so the
// accessor has to work there too — otherwise the wiring the binaries use is a path no
// test exercises.
func TestTestProviderHandsOutAWorkingRecorder(t *testing.T) {
	ctx := t.Context()

	provider, err := obs.NewTestProvider("recorder-accessor")
	if err != nil {
		t.Fatalf("NewTestProvider: %v", err)
	}
	provider.Recorder().Count(ctx, "lease_renewal_failures_total", 1, obs.String("host", "host-a"))

	collected, err := provider.CollectedMetrics(ctx)
	if err != nil {
		t.Fatalf("CollectedMetrics: %v", err)
	}
	if !collected["lease_renewal_failures_total"] {
		t.Fatalf("the Recorder from Provider.Recorder() recorded nothing: %v", collected)
	}
}
