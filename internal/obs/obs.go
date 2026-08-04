// Package obs is the metric substrate: the §26.2 name catalog, the instruments built
// from it, and the Recorder production code writes through without importing
// OpenTelemetry.
//
// **It used to also carry tracing and correlation-keyed logging, and on 2026-08-03 that
// half was deleted.** §26.1 describes a trace context propagating CP → Agent → object
// store → KMS, and the code for it existed — a W3C propagator, InjectContext /
// ExtractContext over a JSON header, five context keys (request_id, operation_id,
// volume_id, epoch, host_id), NewLogger and LoggerFrom to stamp them onto a slog record.
// None of it had a single caller outside this package's own tests. No RPC injected a
// header, no handler extracted one, no binary built a Tracer, and no line anywhere in the
// tree was logged through LoggerFrom. What the tests proved was that the OpenTelemetry
// propagator propagates, which is OTel's test to write.
//
// The alternative was to wire it: one span around a Connect handler and one around
// `Volume.publish`. That was rejected because it is not one line and it is not this
// package's to make. The injection point is `api/`+`internal/cpserver`'s interceptor and
// the extraction point is the Agent's loop; until those two exist, keeping the machinery
// here means "registered and unused" — the exact shape this repository has spent three
// passes removing (DEV-0010 was the same finding about the metric catalog). Git holds the
// deleted code; when the CP grows an interceptor, that increment brings back the six
// functions it actually calls, and not the five context keys nothing will carry.
//
// Metrics stayed because they have real callers: the WAL's watermarks and out-of-space
// gauge, the Agent's lease counters, and §19's snapshot histograms.
//
// obs depends on the OTel SDK, whose internal timestamping is not our concern for
// §25.1/INV-01: our own code never calls the time package (the simulable analyzer
// enforces this — obs takes timestamps from OTel, not from time.Now).
package obs

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Provider bundles the registered metric set with the SDK meter provider behind it.
// Two constructors: NewProvider is what a binary builds (its samples leave the process
// through an exporter), NewTestProvider is what a test builds (its samples stay in a
// manual reader it can collect from).
//
// There is deliberately no Meter accessor. One existed, "for ad-hoc instrument creation
// in tests", used by exactly one test that created a counter and added 1 to it. It was a
// hole in the property the Recorder exists to hold — an unregistered name is dropped, not
// created (§26.2) — handed out to anyone who asked. A metric worth recording goes in
// Catalog().
type Provider struct {
	Metrics *Metrics

	mp *sdkmetric.MeterProvider
	// reader is nil on a production Provider: it is the test constructor's collection
	// point, and CollectedMetrics/GaugeValues say so rather than dereferencing it.
	reader *sdkmetric.ManualReader
}

// NewProvider builds the Provider a binary runs with: every §26.2 instrument, registered
// on a meter named name, exported through exp. name is the service identity a collector
// separates one process's series by (`service.name`), so pass the binary's name.
//
// **A nil exp is a supported, working Provider that exports nothing**, and it is the
// default deployment: no collector configured means no exporter, and the Agent must then
// start and run exactly as it did before this existed. It is not a stub or a fallback —
// the SDK simply has no reader attached, so a recorded sample is dropped at the
// instrument. The alternative, refusing a nil exporter, would make every caller carry a
// branch and would make telemetry a startup dependency of the data path; the alternative
// of substituting NewTestProvider would export the metrics to memory and look like
// observability from the outside, which is the trap `cmd/volume-agent` avoided by
// passing `Recorder: nil` for as long as this constructor did not exist.
//
// Construct exp with real.NewOTLPMetricExporter, which returns exactly this nil for an
// unset endpoint — so a binary needs no conditional of its own. Nothing is exported
// until the periodic reader's interval elapses (the SDK's default is 60s) or Shutdown
// flushes, which is why Shutdown is not optional for a process that exits.
func NewProvider(name string, exp sdkmetric.Exporter) (*Provider, error) {
	opts := []sdkmetric.Option{
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName(name))),
	}
	if exp != nil {
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	mp := sdkmetric.NewMeterProvider(opts...)

	metrics, err := NewMetrics(mp.Meter(name))
	if err != nil {
		return nil, err
	}
	return &Provider{Metrics: metrics, mp: mp}, nil
}

// Recorder returns the handle production code records through. It exists so wiring a
// binary is one expression (`Recorder: provider.Recorder()`) instead of reaching into
// the Provider's fields — the friction that kept the metrics unwired is the thing this
// package has to remove.
func (p *Provider) Recorder() *Recorder { return NewRecorder(p.Metrics) }

// NewTestProvider builds a Provider over a manual reader, so a test can collect and
// assert on what a code path actually recorded. name labels the meter.
func NewTestProvider(name string) (*Provider, error) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := NewMetrics(mp.Meter(name))
	if err != nil {
		return nil, err
	}

	return &Provider{Metrics: metrics, mp: mp, reader: reader}, nil
}

// Shutdown flushes and stops the SDK provider.
func (p *Provider) Shutdown(ctx context.Context) error { return p.mp.Shutdown(ctx) }

// errNoManualReader is what the collection helpers answer on a production Provider: its
// samples go to an exporter, and there is nothing here to read them back from.
var errNoManualReader = errors.New("this Provider has no manual reader: it exports, so collect from the collector (NewTestProvider is the one to read back from)")

// CollectedMetrics returns the set of metric names that actually carry data, which
// is what a test asserting "this path records telemetry" needs: the registry always
// knows the name, so only collection can tell recorded from merely declared.
func (p *Provider) CollectedMetrics(ctx context.Context) (map[string]bool, error) {
	if p.reader == nil {
		return nil, errNoManualReader
	}
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			out[m.Name] = true
		}
	}
	return out, nil
}

// GaugeValues returns the latest value of every collected Float64 gauge. A state
// gauge (`wal_out_of_space`) is only useful to an operator if it reads 1 while the
// condition holds and 0 once it clears, so a test that asserts merely "the name was
// recorded" would pass on a gauge wired backwards. The last data point wins, which
// is what a gauge means.
func (p *Provider) GaugeValues(ctx context.Context) (map[string]float64, error) {
	if p.reader == nil {
		return nil, errNoManualReader
	}
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				out[m.Name] = dp.Value
			}
		}
	}
	return out, nil
}
