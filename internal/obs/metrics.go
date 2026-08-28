package obs

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// MetricKind is the instrument kind.
type MetricKind int

// There is no KindHistogram. Every histogram in this catalogue measured the local block
// engine and went with it, leaving Recorder.Observe with no caller. It comes back with the
// increment that declares the first duration again: a Float64Histogram and eight lines.
const (
	// KindCounter is a monotonic Int64 counter (…_total).
	KindCounter MetricKind = iota
	// KindGauge is a Float64 level that can go up and down.
	KindGauge
)

// MetricDesc declares one metric: its name, kind, help text, and the label keys
// it is recorded with. The catalog below is the single source of truth for the
// §26.2 taxonomy; instruments are built from it, and the stable-cardinality
// contract test asserts the catalog is well-formed. Labels are documentation and
// the contract the later phases record against (OTel attaches them at record time).
type MetricDesc struct {
	Name   string
	Kind   MetricKind
	Help   string
	Labels []string
}

// Catalog returns the §26.2 metric taxonomy. Every entry is registered up front so a
// later increment increments an existing series rather than inventing a name — no
// cardinality drift, no typos discovered during an incident.
//
// Trimmed three times (2026-08-03, -08-08, -08-22) down to these four. A series declared
// for a mechanism that does not exist reads as a plan, and a declared series nothing
// records is worse than an absent one: permanently empty reads as "the thing being
// measured is not happening", so a dashboard on `s3_errors_total` showed a healthy object
// store nobody was talking to. The commit protocol declares its own in the increment that
// records them.
func Catalog() []MetricDesc {
	return []MetricDesc{
		// --- Leases (liveness, no longer durability — §26.2) ---
		{"lease_remaining_seconds", KindGauge, "Remaining lease time per host", []string{"host"}},
		{"lease_renewal_failures_total", KindCounter, "Lease renewal failures", []string{"host"}},

		// --- Fleet (§26.2) ---
		{"clone_same_host_total", KindCounter, "Same-host clones", nil},
		{"chain_depth", KindGauge, "Snapshot chain depth", []string{"volume"}},
	}
}

// Metrics holds the instantiated instruments, keyed by name. In Phase 01 they are
// registered but unused; later phases fetch and record against them.
type Metrics struct {
	counters map[string]metric.Int64Counter
	gauges   map[string]metric.Float64Gauge
}

// NewMetrics builds every catalog instrument on the given meter. It fails if any
// name is duplicated or any instrument cannot be created.
func NewMetrics(m metric.Meter) (*Metrics, error) {
	out := &Metrics{
		counters: map[string]metric.Int64Counter{},
		gauges:   map[string]metric.Float64Gauge{},
	}
	seen := map[string]bool{}
	for _, d := range Catalog() {
		if seen[d.Name] {
			return nil, fmt.Errorf("duplicate metric name %q", d.Name)
		}
		seen[d.Name] = true

		switch d.Kind {
		case KindCounter:
			inst, err := m.Int64Counter(d.Name, metric.WithDescription(d.Help))
			if err != nil {
				return nil, fmt.Errorf("counter %q: %w", d.Name, err)
			}
			out.counters[d.Name] = inst
		case KindGauge:
			inst, err := m.Float64Gauge(d.Name, metric.WithDescription(d.Help))
			if err != nil {
				return nil, fmt.Errorf("gauge %q: %w", d.Name, err)
			}
			out.gauges[d.Name] = inst
		default:
			return nil, fmt.Errorf("metric %q: unknown kind %d", d.Name, d.Kind)
		}
	}
	return out, nil
}

// Counter returns the named counter, or false if it is not a registered counter.
func (m *Metrics) Counter(name string) (metric.Int64Counter, bool) {
	c, ok := m.counters[name]
	return c, ok
}

// Gauge returns the named gauge.
func (m *Metrics) Gauge(name string) (metric.Float64Gauge, bool) {
	g, ok := m.gauges[name]
	return g, ok
}

// Len reports the total number of registered instruments.
func (m *Metrics) Len() int {
	return len(m.counters) + len(m.gauges)
}
