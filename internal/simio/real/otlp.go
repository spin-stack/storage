package real

import (
	"context"
	"fmt"
	"net/url"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// NewOTLPMetricExporter builds the one thing in the telemetry path that touches the
// network: an OTLP/HTTP metric exporter pointed at endpoint (e.g. "http://collector:4318").
// An empty endpoint returns (nil, nil) — see below, it is a supported answer and not a
// degenerate one. The result is handed to obs.NewProvider, which owns everything else.
//
// **Why it lives here.** Opening a socket outside internal/simio is exactly what INV-01
// (§25.1) forbids, and the rule is enforced twice — by the custom `simulable` analyzer
// and by depguard. Constructing the exporter next to the metric catalog in internal/obs
// would have meant adding an exemption to both, which is the widening the invariant
// exists to prevent; internal/simio/real is where a real implementation belongs, and it
// is where every other one already is (the clock, the disk, the S3 store). obs keeps no
// knowledge of transports: it takes an sdkmetric.Exporter, so the DST harness and the
// unit tests can pass anything, including nothing.
//
// **Why OTLP and not a /metrics scrape endpoint.** The project already pins the
// OpenTelemetry SDK and §26.1 assumes OTLP, so this adds one exporter module rather than
// a second protocol, a second port to open on every host, and a second thing to firewall.
// The Agent is also short-lived by design under ADR-0026 — a session ends when the volume
// stops — and a pull model loses whatever happened between the last scrape and the exit,
// which is precisely the publish that a scrape would have been watching for.
//
// **Why HTTP and not gRPC.** They are the same protocol over different transports; HTTP
// needs no gRPC dependency of our own, is the easier of the two to put a receiver in
// front of in a test, and is what every collector accepts on 4318.
func NewOTLPMetricExporter(ctx context.Context, endpoint string) (sdkmetric.Exporter, error) {
	// No endpoint configured is a working deployment, not a misconfiguration: an Agent
	// with no collector in front of it must start and serve exactly as it does today.
	// Returning an error here would make telemetry a startup dependency of the data
	// path, and defaulting to OTLP's "localhost:4318" — which is what the SDK does if
	// this function is called at all — would have every host in the fleet retrying
	// against a collector nobody deployed, logging failures forever.
	//
	// Silence is right *only because the caller chose it*: the operator either passed
	// the flag or did not. Being loud here would mean warning on every start of the
	// normal case, which trains an operator to ignore the log that carries the real
	// warnings (this Agent already prints one that matters — running without a KEK).
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
