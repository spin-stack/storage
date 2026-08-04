package obs

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

// MetricKind is the instrument kind.
type MetricKind int

const (
	// KindCounter is a monotonic Int64 counter (…_total).
	KindCounter MetricKind = iota
	// KindHistogram is a Float64 distribution (…_seconds, sizes).
	KindHistogram
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
// this file and INVARIANTS.md both came to describe a system nobody had. Each goes back
// in with the thing it measures.
//
// `wal_published_sequence` went for a smaller reason worth writing down: nothing
// publishes in V1, so it would be a series permanently at 0.
func Catalog() []MetricDesc {
	return []MetricDesc{
		// --- WAL local (§26.2) ---
		{"wal_append_latency_seconds", KindHistogram, "WAL local append latency", []string{"volume"}},
		{"wal_fdatasync_latency_seconds", KindHistogram, "WAL local fdatasync latency", []string{"volume"}},
		{"wal_unflushed_bytes", KindGauge, "Unflushed WAL bytes", []string{"volume"}},
		{"wal_oldest_unflushed_age_seconds", KindGauge, "Age of the oldest unflushed record", []string{"volume"}},
		{"wal_local_sequence", KindGauge, "Local sequence watermark (informative)", []string{"volume"}},
		{"wal_durable_sequence", KindGauge, "Durable sequence watermark (informative)", []string{"volume"}},
		{"wal_out_of_space", KindGauge, "1 while the WAL device is refusing appends for want of space (§5.7)", []string{"volume"}},

		// --- Image and snapshots (§26.2) ---
		//
		// Publishing at stop is the only moment anything leaves the host under ADR-0026,
		// so its duration is the cost of a session rather than one step among many.
		{"image_publish_duration_seconds", KindHistogram, "Time to publish a volume's image at stop", []string{"volume"}},
		{"snapshot_publish_duration_seconds", KindHistogram, "Snapshot publish duration", []string{"volume"}},
		{"snapshot_pause_duration_seconds", KindHistogram, "Guest I/O pause during snapshot (~0 expected; measured around the freeze, not the upload)", []string{"volume"}},

		// --- Leases (liveness, no longer durability — §26.2) ---
		{"lease_remaining_seconds", KindGauge, "Remaining lease time per host", []string{"host"}},
		{"lease_renewal_failures_total", KindCounter, "Lease renewal failures", []string{"host"}},
		{"clock_offset_seconds", KindGauge, "chrony-reported wall-clock offset", []string{"host"}},

		// --- Fleet (§26.2) ---
		{"host_nvme_committed_ratio", KindGauge, "NVMe committed/total ratio", []string{"host"}},
		{"clone_same_host_total", KindCounter, "Same-host clones", nil},
		{"clone_cross_host_total", KindCounter, "Cross-host clones", nil},
		{"chain_depth", KindGauge, "Snapshot chain depth", []string{"volume"}},
		{"discarded_bytes_total", KindCounter, "Bytes reclaimed via DISCARD/WRITE_ZEROES", []string{"volume"}},

		// --- Agent (§10.1, §26.2) ---
		{"agent_memory_bytes", KindGauge, "Agent memory by component", []string{"component"}},
		{"vhost_reconnects_total", KindCounter, "vhost-user reconnections", []string{"host"}},
		{"inflight_recovered_total", KindCounter, "Inflight requests recovered via shmfd", []string{"host"}},

		// --- S3 client subsystem (§24, §26.2) ---
		{"s3_errors_total", KindCounter, "Object-store errors", []string{"type"}},
		{"s3_request_latency_seconds", KindHistogram, "Object-store request latency", []string{"op", "class", "hedged"}},
		{"s3_retry_budget_exhausted_total", KindCounter, "Retry-budget exhaustions", []string{"backend"}},
		{"s3_circuit_open_seconds", KindGauge, "Time the backend circuit breaker was open", []string{"backend"}},
	}
}

// Metrics holds the instantiated instruments, keyed by name. In Phase 01 they are
// registered but unused; later phases fetch and record against them.
type Metrics struct {
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64Gauge
}

// NewMetrics builds every catalog instrument on the given meter. It fails if any
// name is duplicated or any instrument cannot be created.
func NewMetrics(m metric.Meter) (*Metrics, error) {
	out := &Metrics{
		counters:   map[string]metric.Int64Counter{},
		histograms: map[string]metric.Float64Histogram{},
		gauges:     map[string]metric.Float64Gauge{},
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
		case KindHistogram:
			inst, err := m.Float64Histogram(d.Name, metric.WithDescription(d.Help))
			if err != nil {
				return nil, fmt.Errorf("histogram %q: %w", d.Name, err)
			}
			out.histograms[d.Name] = inst
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

// Histogram returns the named histogram.
func (m *Metrics) Histogram(name string) (metric.Float64Histogram, bool) {
	h, ok := m.histograms[name]
	return h, ok
}

// Gauge returns the named gauge.
func (m *Metrics) Gauge(name string) (metric.Float64Gauge, bool) {
	g, ok := m.gauges[name]
	return g, ok
}

// Len reports the total number of registered instruments.
func (m *Metrics) Len() int {
	return len(m.counters) + len(m.histograms) + len(m.gauges)
}
