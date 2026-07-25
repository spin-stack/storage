// Package obs provides the observability substrate mandated from day 1 (§26):
// OpenTelemetry tracing, structured logging keyed on request_id/operation_id, and
// the full §26.2 metric-name registry. Phase 01 wires the plumbing and registers
// every metric name as a no-op/zero instrument; later phases attach real values,
// so cardinality is fixed and stable up front.
//
// obs depends on the OTel SDK, whose internal timestamping is not our concern for
// §25.1/INV-01: our own code never calls the time package (the simulable analyzer
// enforces this — obs takes timestamps from OTel, not from time.Now).
package obs

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Provider bundles the tracer and metric registry with their SDK providers. Use
// NewTestProvider in tests to inspect recorded spans and metrics; production
// wiring (OTLP exporters) is a deploy concern for a later phase.
type Provider struct {
	Tracer  *Tracer
	Metrics *Metrics

	tp     *sdktrace.TracerProvider
	mp     *sdkmetric.MeterProvider
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
}

// NewTestProvider builds a Provider that records spans in memory and exposes a
// manual metric reader, so tests can assert on both. name labels the tracer/meter.
func NewTestProvider(name string) (*Provider, error) {
	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	metrics, err := NewMetrics(mp.Meter(name))
	if err != nil {
		return nil, err
	}

	return &Provider{
		Tracer:  NewTracer(tp, name),
		Metrics: metrics,
		tp:      tp,
		mp:      mp,
		spans:   spans,
		reader:  reader,
	}, nil
}

// RecordedSpans returns the spans captured so far (test provider only).
func (p *Provider) RecordedSpans() []sdktrace.ReadOnlySpan { return p.spans.Ended() }

// Meter exposes a meter for ad-hoc instrument creation in tests.
func (p *Provider) Meter(name string) metric.Meter { return p.mp.Meter(name) }

// Shutdown flushes and stops the SDK providers.
func (p *Provider) Shutdown(ctx context.Context) error {
	if err := p.tp.Shutdown(ctx); err != nil {
		return err
	}
	return p.mp.Shutdown(ctx)
}
