// Package obs is the metric substrate: the §26.2 name catalog, the instruments built
// from it, and the Recorder production code writes through without importing
// OpenTelemetry.
//
// Tracing and correlation-keyed logging were deleted on 2026-08-03: a W3C propagator,
// Inject/ExtractContext and five context keys, with no caller outside this package's own
// tests. They come back in the increment that wires a CP interceptor and an Agent
// extraction point, carrying only the functions it calls.
//
// obs depends on the OTel SDK, whose internal timestamping is not our concern for
// §25.1/INV-01: our own code never calls the time package (the simulable analyzer
// enforces this — obs takes timestamps from OTel, not from time.Now).
package obs

import (
	"context"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Provider bundles the registered metric set with the SDK meter provider behind it.
//
// There is deliberately no Meter accessor. One existed, "for ad-hoc instrument creation
// in tests", used by exactly one test that created a counter and added 1 to it. It was a
// hole in the property the Recorder exists to hold — an unregistered name is dropped, not
// created (§26.2) — handed out to anyone who asked. A metric worth recording goes in
// Catalog().
type Provider struct {
	Metrics *Metrics

	mp *sdkmetric.MeterProvider
	// reader is the collection point every Provider keeps, production or test: it is
	// what Scrape reads, and therefore what makes the Agent's /metrics endpoint
	// possible at all. It used to exist only in NewTestProvider, which left a
	// production process whose samples only a collector nobody deploys could read.
	reader *sdkmetric.ManualReader
}

// NewProvider builds the Provider a binary runs with: every §26.2 instrument on a meter named
// name, exported through exp. name is the `service.name` a collector separates processes by.
//
// **A nil exp is a supported Provider that exports nothing**, and it is the default
// deployment: the SDK simply has no reader attached, so a sample is dropped at the
// instrument. Refusing nil would put a branch in every caller and make telemetry a startup
// dependency of the data path; substituting NewTestProvider would export to memory and look
// like observability from the outside.
//
// real.NewOTLPMetricExporter returns exactly this nil for an unset endpoint. Nothing is
// exported until the periodic reader's interval elapses (SDK default 60s) or Shutdown
// flushes, which is why Shutdown is not optional for a process that exits.
func NewProvider(name string, exp sdkmetric.Exporter) (*Provider, error) {
	reader := sdkmetric.NewManualReader()
	opts := []sdkmetric.Option{
		sdkmetric.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName(name))),
		sdkmetric.WithReader(reader),
	}
	if exp != nil {
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	mp := sdkmetric.NewMeterProvider(opts...)

	metrics, err := NewMetrics(mp.Meter(name))
	if err != nil {
		return nil, err
	}
	return &Provider{Metrics: metrics, mp: mp, reader: reader}, nil
}

// Recorder returns the handle production code records through. It exists so wiring a
// binary is one expression (`Recorder: provider.Recorder()`) instead of reaching into
// the Provider's fields — the friction that kept the metrics unwired is the thing this
// package has to remove.
func (p *Provider) Recorder() *Recorder { return NewRecorder(p.Metrics) }

// NewTestProvider builds the Provider a test asserts against. It is NewProvider with no
// exporter, and it is a separate name only because that is what a test means: the two
// stopped differing when the manual reader became unconditional, and a test that built
// something a binary never builds would be proving the wrong thing.
func NewTestProvider(name string) (*Provider, error) { return NewProvider(name, nil) }

// Shutdown flushes and stops the SDK provider.
func (p *Provider) Shutdown(ctx context.Context) error { return p.mp.Shutdown(ctx) }

// CollectedMetrics returns the set of metric names that actually carry data, which
// is what a test asserting "this path records telemetry" needs: the registry always
// knows the name, so only collection can tell recorded from merely declared.
func (p *Provider) CollectedMetrics(ctx context.Context) (map[string]bool, error) {
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

// GaugeSeries returns every collected data point of one Float64 gauge family, keyed by
// its label set rendered the way a scrape renders it (`{k="v",…}`, keys sorted).
//
// GaugeValues cannot answer for a family with more than one label set: it keys by metric
// name, so the last data point the SDK happens to hand back wins and which one that is is
// not defined anywhere. That is harmless only for a gauge with one series per process,
// and neither gauge in the catalogue is one: `chain_depth` has a series per volume and
// `lease_remaining_seconds` one per host, so reading either by name alone would be
// asserting on a coin flip and calling it evidence.
func (p *Provider) GaugeSeries(ctx context.Context, name string) (map[string]float64, error) {
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				out[labelsOf(dp.Attributes)] = dp.Value
			}
		}
	}
	return out, nil
}

// Distribution is what a test asserting on a recorded histogram needs: how many samples
// landed in one series and what they added up to. The buckets themselves are the
// scrape's business, not an assertion's — a test that pinned them would fail on a
// boundary change that measured nothing differently.
type Distribution struct {
	Count uint64
	Sum   float64
}

// HistogramSeries returns every collected data point of one Float64 histogram family,
// keyed by its label set rendered the way a scrape renders it.
func (p *Provider) HistogramSeries(ctx context.Context, name string) (map[string]Distribution, error) {
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := map[string]Distribution{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				out[labelsOf(dp.Attributes)] = Distribution{Count: dp.Count, Sum: dp.Sum}
			}
		}
	}
	return out, nil
}

// CounterSeries returns every collected data point of one Int64 counter family, keyed
// the same way. A counter read by name alone has the coin-flip problem GaugeSeries
// exists for: every counter in the catalogue that is worth asserting on is labelled.
func (p *Provider) CounterSeries(ctx context.Context, name string) (map[string]int64, error) {
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				out[labelsOf(dp.Attributes)] = dp.Value
			}
		}
	}
	return out, nil
}

// GaugeValues returns the latest value of every collected Float64 gauge. A state gauge
// is only useful to an operator if it reads 1 while the condition holds and 0 once it
// clears, so a test that asserts merely "the name was recorded" would pass on a gauge
// wired backwards. The last data point wins, which is what a gauge means.
//
// One data point per name, so it answers only for gauges with a single series. Use
// GaugeSeries for a family whose label set varies.
func (p *Provider) GaugeValues(ctx context.Context) (map[string]float64, error) {
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
