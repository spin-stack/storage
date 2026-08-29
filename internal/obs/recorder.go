package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Recorder is how production code writes to the §26.2 catalog without importing
// OpenTelemetry: the packages that own a metric take a *Recorder and call Count,
// Gauge, or Observe by name.
//
// Two deliberate properties:
//
//   - the nil Recorder is a no-op. A data path must never fail, or panic, because
//     telemetry was not wired — components built for a unit test or the DST harness
//     simply pass nil;
//   - an unregistered name is dropped rather than created. The catalog is fixed
//     up front so cardinality cannot drift (§26.2), and a typo must not quietly
//     become a new series that nobody alerts on.
type Recorder struct {
	m *Metrics
}

// NewRecorder returns a Recorder over a registered metric set.
func NewRecorder(m *Metrics) *Recorder { return &Recorder{m: m} }

// Attr is a label to record a sample with (volume, host, class...).
type Attr = attribute.KeyValue

// String builds a string-valued label without importing OTel at the call site.
func String(key, value string) Attr { return attribute.String(key, value) }

// Count adds delta to a registered counter.
func (r *Recorder) Count(ctx context.Context, name string, delta int64, attrs ...Attr) {
	if r == nil || r.m == nil {
		return
	}
	if c, ok := r.m.Counter(name); ok {
		c.Add(ctx, delta, metric.WithAttributes(attrs...))
	}
}

// Observe adds one sample to a registered histogram.
func (r *Recorder) Observe(ctx context.Context, name string, value float64, attrs ...Attr) {
	if r == nil || r.m == nil {
		return
	}
	if h, ok := r.m.Histogram(name); ok {
		h.Record(ctx, value, metric.WithAttributes(attrs...))
	}
}

// Gauge records the current value of a registered gauge.
func (r *Recorder) Gauge(ctx context.Context, name string, value float64, attrs ...Attr) {
	if r == nil || r.m == nil {
		return
	}
	if g, ok := r.m.Gauge(name); ok {
		g.Record(ctx, value, metric.WithAttributes(attrs...))
	}
}
