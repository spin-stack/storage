package real_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/real"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// collector is an OTLP/HTTP receiver: the smallest thing that can answer the only
// question worth asking of an exporter — did the bytes leave this process and arrive
// somewhere shaped like a collector.
//
// It is a real HTTP server on a real port rather than a stubbed transport because the
// stub would be a second implementation of the thing under test: an exporter that
// serialises correctly and never dials would satisfy any assertion made against an
// injected round-tripper. Here, nothing but an actual request on an actual socket puts
// anything in `requests`.
type collector struct {
	mu       sync.Mutex
	paths    []string
	requests []*collectormetrics.ExportMetricsServiceRequest
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := &collectormetrics.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	c.paths = append(c.paths, r.URL.Path)
	c.requests = append(c.requests, req)
	c.mu.Unlock()

	// An empty ExportMetricsServiceResponse is the "everything accepted" answer.
	resp, err := proto.Marshal(&collectormetrics.ExportMetricsServiceResponse{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	_, _ = w.Write(resp)
}

// sums returns every Sum data point the collector received, keyed by metric name.
func (c *collector) sums() map[string][]*metricspb.NumberDataPoint {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := map[string][]*metricspb.NumberDataPoint{}
	for _, req := range c.requests {
		for _, rm := range req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					sum := m.GetSum()
					if sum == nil {
						continue
					}
					out[m.GetName()] = append(out[m.GetName()], sum.GetDataPoints()...)
				}
			}
		}
	}
	return out
}

// resourceAttr returns the first value of key across every received resource.
func (c *collector) resourceAttr(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, req := range c.requests {
		for _, rm := range req.GetResourceMetrics() {
			for _, kv := range rm.GetResource().GetAttributes() {
				if kv.GetKey() == key {
					return kv.GetValue().GetStringValue()
				}
			}
		}
	}
	return ""
}

// This is the whole point of the increment, and it is asserted from outside the
// process's own bookkeeping: a metric recorded through the Recorder that production
// code holds has to arrive at a collector, by name, with its value and its label. Every
// previous test in this tree asserted through obs.NewTestProvider's manual reader, which
// proves the SDK aggregates — it cannot distinguish an exporter that works from one that
// was never built, and for the life of the project it was the latter.
func TestOTLPExporterDeliversARecordedMetricToACollector(t *testing.T) {
	ctx := t.Context()

	c := &collector{}
	srv := httptest.NewServer(c)
	defer srv.Close()

	exporter, err := real.NewOTLPMetricExporter(ctx, srv.URL)
	if err != nil {
		t.Fatalf("NewOTLPMetricExporter: %v", err)
	}
	if exporter == nil {
		t.Fatal("a configured endpoint produced no exporter")
	}

	provider, err := obs.NewProvider("volume-agent", exporter)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	// A registered §26.2 counter, recorded exactly the way internal/agent records it.
	provider.Recorder().Count(ctx, "lease_renewal_failures_total", 3, obs.String("host", "host-a"))

	// Shutdown is what flushes: the periodic reader would otherwise sit on the sample
	// for its whole interval, and a process that exits without this loses whatever it
	// recorded since the last tick. Asserting after it is asserting the contract the
	// caller has to honour.
	if err := provider.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	dps := c.sums()["lease_renewal_failures_total"]
	if len(dps) == 0 {
		t.Fatalf("the collector received no lease_renewal_failures_total; it saw %v", c.sums())
	}
	if got := dps[0].GetAsInt(); got != 3 {
		t.Fatalf("the collector received lease_renewal_failures_total = %d, want 3", got)
	}

	// The label is half of what makes the series useful — a lease-failure count nobody
	// can attribute to a host is an alert with no next step.
	var host string
	for _, kv := range dps[0].GetAttributes() {
		if kv.GetKey() == "host" {
			host = kv.GetValue().GetStringValue()
		}
	}
	if host != "host-a" {
		t.Fatalf("the data point carried host=%q, want host-a", host)
	}

	// service.name is how a collector tells one process's series from another's; without
	// it every Agent in the fleet reports as the same nameless producer.
	if got := c.resourceAttr("service.name"); got != "volume-agent" {
		t.Fatalf("the resource carried service.name=%q, want volume-agent", got)
	}

	// OTLP/HTTP is a wire contract, not just "some POST": a collector routes on the
	// path, and an endpoint given without one has to grow the standard suffix.
	c.mu.Lock()
	paths := append([]string(nil), c.paths...)
	c.mu.Unlock()
	if len(paths) == 0 || paths[0] != "/v1/metrics" {
		t.Fatalf("the exporter POSTed to %v, want /v1/metrics", paths)
	}
}

// An Agent with no telemetry configured must start and run exactly as it does today, so
// "no endpoint" has to be a first-class answer rather than an error the caller branches
// on. This is the half of the contract that keeps the caller's wiring to a few lines.
func TestNoEndpointYieldsNoExporter(t *testing.T) {
	exporter, err := real.NewOTLPMetricExporter(t.Context(), "")
	if err != nil {
		t.Fatalf("an unset endpoint must not be an error: %v", err)
	}
	if exporter != nil {
		t.Fatal("an unset endpoint produced an exporter, which would dial the OTLP default endpoint")
	}
}

// A malformed endpoint is refused at startup rather than at the first export: an
// operator who typo'd the flag should see it in the exit, where they are already
// looking, not in a metric that silently never arrives. The refusal has to name what to
// change, because the exporter's own behaviour on a bad URL is to keep its defaults and
// export to localhost — a healthy-looking Agent talking to nobody.
func TestAMalformedEndpointIsRefused(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		says     string
	}{
		{
			// The shape an operator most likely types, copying an S3 endpoint's habit
			// or a collector's host:port from a deployment manifest.
			name:     "no scheme",
			endpoint: "collector:4318",
			says:     "scheme",
		},
		{
			name:     "no host",
			endpoint: "http://",
			says:     "host",
		},
		{
			name:     "unparseable",
			endpoint: "http://[::1",
			says:     "parsing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exporter, err := real.NewOTLPMetricExporter(t.Context(), tc.endpoint)
			if err == nil {
				t.Fatalf("the endpoint %q was accepted", tc.endpoint)
			}
			if exporter != nil {
				t.Fatal("a refused endpoint still returned an exporter")
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal must tell an operator what to change: %v", err)
			}
		})
	}
}
