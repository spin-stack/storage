package obs

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// correlationKey is a private context-key type for the correlation fields that
// propagate through every component (§26.1).
type correlationKey string

const (
	keyRequestID   correlationKey = "request_id"
	keyOperationID correlationKey = "operation_id"
	keyVolumeID    correlationKey = "volume_id"
	keyEpoch       correlationKey = "epoch"
	keyHostID      correlationKey = "host_id"
)

// correlationKeys maps the wire header name to its context key, for propagation.
var correlationKeys = map[string]correlationKey{
	"request_id":   keyRequestID,
	"operation_id": keyOperationID,
	"volume_id":    keyVolumeID,
	"epoch":        keyEpoch,
	"host_id":      keyHostID,
}

// WithRequestID annotates ctx with a request_id (admin idempotency / tracing, §18).
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyRequestID, id)
}

// WithOperationID annotates ctx with an operation_id (reconciliation, §7).
func WithOperationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyOperationID, id)
}

// WithVolumeID annotates ctx with a volume_id.
func WithVolumeID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyVolumeID, id)
}

// WithHostID annotates ctx with a host_id.
func WithHostID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyHostID, id)
}

func valueFromContext(ctx context.Context, key correlationKey) string {
	if v, ok := ctx.Value(key).(string); ok {
		return v
	}
	return ""
}

// NewLogger returns a structured JSON logger writing to w. Fields are attached
// per-record from context via LoggerFrom.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// LoggerFrom returns base decorated with the correlation fields present in ctx
// plus the current trace_id/span_id if a span is active. The day a FLUSH takes
// 4 s, these fields are what tie the log line to the trace (§26.1).
func LoggerFrom(ctx context.Context, base *slog.Logger) *slog.Logger {
	l := base
	for header, key := range correlationKeys {
		if v := valueFromContext(ctx, key); v != "" {
			l = l.With(slog.String(header, v))
		}
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		l = l.With(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return l
}
