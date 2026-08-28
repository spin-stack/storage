package real

import (
	"context"
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// NewOTLPMetricExporter builds the one thing in the telemetry path that touches the
// network: an OTLP/HTTP metric exporter pointed at endpoint (e.g.
// "http://collector:4318"). An empty endpoint returns (nil, nil). The result is handed
// to obs.NewProvider, which owns everything else.
//
// It lives here because opening a socket outside internal/simio is what INV-01 (§25.1)
// forbids, and obs keeps no knowledge of transports. Rejected: a /metrics scrape
// endpoint — the project already pins the OTel SDK, and a pull model loses whatever
// happened between the last scrape and an Agent's exit, which is precisely the publish
// it would be watching for. HTTP over gRPC: no gRPC dependency of our own, and every
// collector accepts 4318.
func NewOTLPMetricExporter(ctx context.Context, endpoint string) (sdkmetric.Exporter, error) {
	// No endpoint configured is a working deployment: returning an error would make
	// telemetry a startup dependency of the data path, and the SDK's default
	// ("localhost:4318") would have every host retrying against a collector nobody
	// deployed. Silent, not a warning: the operator chose it, and warning on every start
	// of the normal case trains them to ignore the log that carries the real ones.
	if endpoint == "" {
		return nil, nil
	}

	// Parsed and refused here rather than left to WithEndpointURL, which logs a parse
	// failure to OTel's global handler and then silently keeps its defaults — so a
	// typo'd flag would produce a healthy-looking Agent exporting to localhost. An
	// operator's typo must fail the start, where they are already looking.
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parsing the OTLP endpoint %q: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("the OTLP endpoint %q needs an http:// or https:// scheme (the scheme is what selects TLS)", endpoint)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("the OTLP endpoint %q names no host", endpoint)
	}

	exporter, err := otlpmetrichttp.New(ctx,
		// The URL carries the scheme, so TLS is chosen by the endpoint rather than by a
		// second flag that can disagree with it. A path is optional: without one the
		// exporter appends OTLP's standard /v1/metrics.
		otlpmetrichttp.WithEndpointURL(endpoint),
		// Retries off, deliberately. The exporter's default is to retry a failed export
		// for up to a minute, and the export that matters most is the one Shutdown
		// flushes while a volume is stopping — so a collector that is down would add up
		// to a minute to every Agent's shutdown, delaying the publish that carries V1's
		// whole RPO. Nothing is lost by dropping it: OTLP metrics are cumulative, so the
		// next successful export restates the totals the failed one carried.
		otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}),
	)
	if err != nil {
		return nil, fmt.Errorf("building the OTLP metric exporter for %q: %w", endpoint, err)
	}
	return exporter, nil
}
