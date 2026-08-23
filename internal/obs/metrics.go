package obs

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// MetricKind is the instrument kind.
type MetricKind int

// There is no KindHistogram. Every histogram in this catalogue measured the local block
// engine — append and fdatasync latency, the duration of an image or snapshot publish —
// and went with it, so the kind had no declaration, `Recorder.Observe` had no caller and
// `Metrics.Histogram` had nothing to return. Keeping the machinery against the day a
// duration is measured again is exactly the "reads as a plan" failure the catalogue
// above was trimmed twice for; it comes back in the increment that declares the first
// one, which is a Float64Histogram and eight lines.
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
// later increment starts incrementing an existing series rather than inventing a name —
// no cardinality drift, no typos discovered during an incident.
//
// **Trimmed 2026-08-03 with §26.2 itself (DEV-0022).** It had grown to about forty
// entries and three quarters of them named mechanisms ADR-0026 withdrew: the WAL-remote
// block, checkpoints, objectization and compaction, GC, the fencing wait, mid-session
// recovery, the warm standby and the io-class scheduler. Declaring a series for a
// mechanism that does not exist is not free — it reads as a plan, which is exactly how
// this file came to describe a system nobody had. Each goes back
// in with the thing it measures.
//
// `wal_published_sequence` went for a smaller reason worth writing down: nothing
// publishes in V1, so it would be a series permanently at 0.
//
// # The eleven that went on 2026-08-08, and the rule that took them
//
// A doc-vs-code audit found that thirteen series in this catalogue were declared and
// recorded by nothing: instantiated by NewMetrics, exported on every scrape, and
// permanently empty. That is worse than an absent series, because an empty series
// reads as "the thing being measured is not happening" rather than "nothing is
// measuring". A dashboard built on `s3_errors_total` shows a healthy object store.
//
// The rule applied, and it is CLAUDE.md's: a component with no caller is a liability.
// So the ones whose *mechanism* does not exist went with it —
// `inflight_recovered_total` and `vhost_reconnects_total` (increment 3.3 is not
// started), `clone_cross_host_total` (ADR-0026 removed the cross-host path),
// `agent_memory_bytes` (§10.1's memory budget was never built; `agent.Budget` is a
// device budget), `clock_offset_seconds` (nothing reads chrony),
// `host_nvme_committed_ratio` (derived at placement, never recorded),
// `wal_oldest_unflushed_age_seconds` (the Log tracks *whether* there are unflushed
// records, not the age of the oldest, and no Agent sets the age bound it belongs to),
// and the four `s3_*` series (§24's subsystem does not exist; the client is one file
// with no hedging, no circuit breaker and no classes).
//
// The ones whose mechanism *does* exist were wired instead of deleted, in the same
// change, which is the other half of the rule.
//
// # The eleven that went on 2026-08-22, and it is the same rule again
//
// The local block engine was withdrawn — QEMU manages the local copy-on-write format
// through qcow2 now, and this system keeps immutable commits, publication and recovery —
// and every series that measured it went with it in the same commit: the five `wal_*`
// series and `wal_out_of_space` (there is no write-ahead log), `volume_backpressure` (no
// device refusing a guest), the three `read_view_*` series (no interval map),
// `discarded_bytes_total` (no DISCARD reaching a backend), and the image and snapshot
// publish durations (nothing publishes).
//
// Four are left, and that is the whole catalogue: two lease series the Agent's loop
// records, and the two the clone path records. The commit protocol declares its own in
// the increment that records them, which is this file's rule stated from the other end.
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
