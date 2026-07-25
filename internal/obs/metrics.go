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

// Catalog returns the full §26.2 metric taxonomy. Phase 01 registers every entry
// as a no-op/zero instrument so later phases only start incrementing existing
// series (no cardinality drift, no typos introduced later).
func Catalog() []MetricDesc {
	return []MetricDesc{
		// --- WAL local (§26.2) ---
		{"wal_append_latency_seconds", KindHistogram, "WAL local append latency", []string{"volume"}},
		{"wal_fdatasync_latency_seconds", KindHistogram, "WAL local fdatasync latency", []string{"volume"}},
		{"wal_unflushed_bytes", KindGauge, "Unflushed WAL bytes", []string{"volume"}},
		{"wal_oldest_unflushed_age_seconds", KindGauge, "Age of the oldest unflushed record", []string{"volume"}},
		{"wal_local_sequence", KindGauge, "Local sequence watermark (informative)", []string{"volume"}},
		{"wal_durable_sequence", KindGauge, "Durable sequence watermark (informative)", []string{"volume"}},
		{"wal_published_sequence", KindGauge, "Published sequence watermark (informative)", []string{"volume"}},

		// --- WAL remote (§26.2) ---
		{"wal_batch_size_bytes", KindHistogram, "Closed WAL batch size", []string{"volume"}},
		{"wal_batch_age_seconds", KindHistogram, "Age of a WAL batch at close", []string{"volume"}},
		{"wal_put_latency_seconds", KindHistogram, "WAL object PUT latency", []string{"volume"}},
		{"wal_put_retries_total", KindCounter, "WAL object PUT retries", []string{"volume"}},
		{"wal_small_batch_ratio", KindGauge, "Ratio of small batches (fsync-heavy signal)", []string{"volume"}},
		{"wal_durable_gap_bytes", KindGauge, "Bytes not yet durable in S3 (RPO, §14.8)", []string{"volume"}},
		{"wal_durable_gap_seconds", KindGauge, "Effective RPO in seconds (§14.8, critical in local mode)", []string{"volume"}},
		{"wal_objects_total", KindGauge, "Count of WAL objects for a volume", []string{"volume"}},

		// --- Fencing and leases (§26.2) ---
		{"lease_remaining_seconds", KindGauge, "Remaining lease time per host", []string{"host"}},
		{"lease_renewal_failures_total", KindCounter, "Lease renewal failures", []string{"host"}},
		{"self_fenced_total", KindCounter, "SELF_FENCED transitions", []string{"host", "volume"}},
		{"fencing_wait_duration_seconds", KindHistogram, "FENCING_WAIT duration before promotion", []string{"volume"}},
		{"clock_offset_seconds", KindGauge, "chrony-reported wall-clock offset", []string{"host"}},

		// --- Snapshots (§26.2) ---
		{"snapshot_publish_duration_seconds", KindHistogram, "Snapshot publish duration", []string{"volume"}},
		{"snapshot_pause_duration_seconds", KindHistogram, "Guest I/O pause during snapshot (~0 expected)", []string{"volume"}},

		// --- Objectization / compaction / GC (§21, §26.2) ---
		{"checkpoint_duration_seconds", KindHistogram, "Checkpoint duration", []string{"volume"}},
		{"objectization_pending_bytes", KindGauge, "Bytes pending objectization", []string{"volume"}},
		{"compaction_bytes_total", KindCounter, "Bytes rewritten by compaction", []string{"volume"}},
		{"compaction_objects_merged_total", KindCounter, "WAL objects merged by compaction", []string{"volume"}},
		{"orphan_objects_total", KindGauge, "Orphan objects awaiting GC", []string{"volume"}},
		{"gc_marked_bytes_total", KindCounter, "Bytes marked by GC (never permanently deleted)", []string{"volume"}},
		{"gc_reclaimed_bytes_total", KindCounter, "Bytes reclaimed by lifecycle after grace", []string{"volume"}},
		{"chain_depth", KindGauge, "Snapshot chain depth", []string{"volume"}},
		{"flatten_operations_total", KindCounter, "Chain-flatten operations", []string{"volume"}},
		{"discarded_bytes_total", KindCounter, "Bytes reclaimed via DISCARD/WRITE_ZEROES", []string{"volume"}},

		// --- Recovery / standby (§26.2) ---
		{"recovery_duration_seconds", KindHistogram, "Recovery duration", []string{"volume"}},
		{"standby_checkpoint_lag_bytes", KindGauge, "Warm-standby checkpoint lag", []string{"volume"}},
		{"bytes_downloaded_before_boot", KindGauge, "Bytes materialized before boot (cross-host)", []string{"volume"}},
		{"s3_errors_total", KindCounter, "Object-store errors", []string{"type"}},

		// --- Agent (§10.1, §26.2) ---
		{"agent_memory_bytes", KindGauge, "Agent memory by component", []string{"component"}},
		{"active_map_bytes", KindGauge, "Active-map memory per volume", []string{"volume"}},
		{"io_class_bytes_total", KindCounter, "Bytes per I/O class and resource", []string{"class", "resource"}},
		{"io_class_throttled_seconds", KindGauge, "Time an I/O class spent throttled", []string{"class"}},
		{"vhost_reconnects_total", KindCounter, "vhost-user reconnections", []string{"host"}},
		{"inflight_recovered_total", KindCounter, "Inflight requests recovered via shmfd", []string{"host"}},

		// --- Fleet (§26.2) ---
		{"host_nvme_committed_ratio", KindGauge, "NVMe committed/total ratio", []string{"host"}},
		{"clone_same_host_total", KindCounter, "Same-host clones", nil},
		{"clone_cross_host_total", KindCounter, "Cross-host clones", nil},

		// --- S3 client subsystem (§24, §26.2) ---
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
