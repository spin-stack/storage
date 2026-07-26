package obs_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
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
		"wal_durable_gap_seconds",         // RPO in local mode (§14.8)
		"self_fenced_total",               // fencing (§26.2)
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
	if _, ok := p.Metrics.Counter("self_fenced_total"); !ok {
		t.Fatal("self_fenced_total should be a counter")
	}
	if _, ok := p.Metrics.Histogram("wal_put_latency_seconds"); !ok {
		t.Fatal("wal_put_latency_seconds should be a histogram")
	}
	if _, ok := p.Metrics.Gauge("wal_durable_gap_seconds"); !ok {
		t.Fatal("wal_durable_gap_seconds should be a gauge")
	}
}

// TestTracePropagationAcrossBoundary models CP → Agent over simio.network: the
// context is injected into a message header on one side and extracted on the
// other, and the child span shares the trace and carries the request_id (§26.1).
func TestTracePropagationAcrossBoundary(t *testing.T) {
	p := newProvider(t)

	// CP side: start a root span with a request_id.
	cpCtx := obs.WithRequestID(t.Context(), "req-123")
	cpCtx, cpSpan := p.Tracer.Start(cpCtx, "cp.Attach")
	wantTrace := cpSpan.SpanContext().TraceID()

	header := p.Tracer.InjectContext(cpCtx)
	cpSpan.End()

	// Agent side: fresh context, extract, start child span.
	agentCtx := p.Tracer.ExtractContext(t.Context(), header)
	_, agentSpan := p.Tracer.Start(agentCtx, "agent.attach")
	gotTrace := agentSpan.SpanContext().TraceID()
	agentSpan.End()

	if gotTrace != wantTrace {
		t.Fatalf("trace id not propagated: cp=%s agent=%s", wantTrace, gotTrace)
	}
	// The correlation field survived the boundary (observed via the logger, since
	// the context keys are private to the package).
	buf := &bytes.Buffer{}
	obs.LoggerFrom(agentCtx, obs.NewLogger(buf, slog.LevelInfo)).Info("agent")
	if !bytes.Contains(buf.Bytes(), []byte(`"request_id":"req-123"`)) {
		t.Fatalf("request_id did not propagate across the boundary: %s", buf.String())
	}
}

// TestStructuredLogHasCorrelationFields asserts LoggerFrom emits the correlation
// fields and the active trace/span ids.
func TestStructuredLogHasCorrelationFields(t *testing.T) {
	p := newProvider(t)

	ctx := t.Context()
	ctx = obs.WithRequestID(ctx, "req-1")
	ctx = obs.WithOperationID(ctx, "op-1")
	ctx = obs.WithVolumeID(ctx, "vol-1")
	ctx = obs.WithHostID(ctx, "host-1")
	ctx, span := p.Tracer.Start(ctx, "op")
	defer span.End()

	buf := &bytes.Buffer{}
	obs.LoggerFrom(ctx, obs.NewLogger(buf, slog.LevelInfo)).Info("did a thing")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log is not JSON: %v (%s)", err, buf.String())
	}
	for _, field := range []string{"request_id", "operation_id", "volume_id", "host_id", "trace_id", "span_id"} {
		if _, ok := rec[field]; !ok {
			t.Fatalf("log record missing field %q: %s", field, buf.String())
		}
	}
	if rec["request_id"] != "req-1" {
		t.Fatalf("request_id wrong: %v", rec["request_id"])
	}
}

func TestMeterRecords(t *testing.T) {
	p := newProvider(t)
	ctr, err := p.Meter("adhoc").Int64Counter("adhoc_total")
	if err != nil {
		t.Fatal(err)
	}
	ctr.Add(t.Context(), 1) // must not panic; exercises Meter()
}

// TestNestedSpansShareTrace models CP → Agent → object store: nested spans share
// one trace, which is what makes a slow FLUSH one trace to open, not a grep.
func TestNestedSpansShareTrace(t *testing.T) {
	p := newProvider(t)
	ctx := t.Context()
	ctx, root := p.Tracer.Start(ctx, "cp")
	ctx, mid := p.Tracer.Start(ctx, "agent")
	_, leaf := p.Tracer.Start(ctx, "objectstore.put")
	leaf.End()
	mid.End()
	root.End()

	spans := p.RecordedSpans()
	if len(spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(spans))
	}
	tid := spans[0].SpanContext().TraceID()
	for _, s := range spans {
		if s.SpanContext().TraceID() != tid {
			t.Fatalf("span %q not in the shared trace", s.Name())
		}
	}
}
