package obs

import (
	"context"
	"encoding/json"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Tracer wraps an OTel tracer plus the propagator used to carry trace context
// across the simio.network boundary (CP → Agent → object store → KMS, §26.1).
type Tracer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// NewTracer returns a Tracer backed by tp using W3C trace-context propagation.
func NewTracer(tp trace.TracerProvider, name string) *Tracer {
	return &Tracer{
		tracer:     tp.Tracer(name),
		propagator: propagation.TraceContext{},
	}
}

// Start begins a span. The returned context carries the span; call End on it.
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, name)
}

// InjectContext serializes the trace context (and the correlation fields, §26.1)
// in ctx into a byte header suitable for sending as part of a simio.network
// message. It never fails; an empty context yields an empty-ish header.
func (t *Tracer) InjectContext(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	t.propagator.Inject(ctx, carrier)
	// Correlation fields travel alongside the W3C headers.
	for k, key := range correlationKeys {
		if v := valueFromContext(ctx, key); v != "" {
			carrier[k] = v
		}
	}
	data, _ := json.Marshal(map[string]string(carrier))
	return data
}

// ExtractContext rebuilds a context from a header produced by InjectContext,
// restoring the remote span context and correlation fields.
func (t *Tracer) ExtractContext(ctx context.Context, header []byte) context.Context {
	var m map[string]string
	if err := json.Unmarshal(header, &m); err != nil || m == nil {
		return ctx
	}
	carrier := propagation.MapCarrier(m)
	ctx = t.propagator.Extract(ctx, carrier)
	for k, key := range correlationKeys {
		if v := carrier.Get(k); v != "" {
			ctx = context.WithValue(ctx, key, v)
		}
	}
	return ctx
}
