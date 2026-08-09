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
// change, which is the other half of the rule: `wal_append_latency_seconds`,
// `wal_fdatasync_latency_seconds`, `discarded_bytes_total` (DISCARD reached the wire
// in this same increment) and `clone_same_host_total`.
func Catalog() []MetricDesc {
	return []MetricDesc{
		// --- WAL local (§26.2) ---
		{"wal_append_latency_seconds", KindHistogram, "WAL local append latency", []string{"volume"}},
		{"wal_fdatasync_latency_seconds", KindHistogram, "WAL local fdatasync latency", []string{"volume"}},
		{"wal_unflushed_bytes", KindGauge, "Unflushed WAL bytes", []string{"volume"}},
		{"wal_local_sequence", KindGauge, "Local sequence watermark (informative)", []string{"volume"}},
		{"wal_durable_sequence", KindGauge, "Durable sequence watermark (informative)", []string{"volume"}},
		{"wal_out_of_space", KindGauge, "1 while the WAL device is refusing appends for want of space (§5.7)", []string{"volume"}},
		// Distinct from wal_out_of_space, and the distinction is the whole reason it
		// exists: that gauge is the *device* reaching ENOSPC, which under ADR-0013 §1
		// is supposed never to happen because every volume is bounded well below it.
		// The bound that actually stops a guest is the volume's share, and crossing it
		// moved nothing anywhere — the guest took EIO and a failed fsync while the
		// host recorded not one sample and printed not one line. Recorded by
		// cmd/volume-agent, which is the only place that holds both the devices and
		// the budget the share was divided out of.
		{"volume_backpressure", KindGauge, "1 once this volume's device has refused a guest write for want of its share of the local device (ADR-0013 §1); it does not clear while the volume runs", []string{"volume"}},

		// --- Image and snapshots (§26.2) ---
		//
		// Publishing at stop is the only moment anything leaves the host under ADR-0026,
		// so its duration is the cost of a session rather than one step among many.
		{"image_publish_duration_seconds", KindHistogram, "Time to publish a volume's image at stop", []string{"volume"}},
		{"snapshot_publish_duration_seconds", KindHistogram, "Snapshot publish duration", []string{"volume"}},
		{"snapshot_pause_duration_seconds", KindHistogram, "Guest I/O pause during snapshot (~0 expected; measured around the freeze, not the upload)", []string{"volume"}},

		// --- The read view (§13.2, §19) ---
		//
		// `cow.IntervalMap` is the only per-volume structure on an Agent whose size is
		// decided by the guest rather than by configuration, and until these three
		// existed nothing measured it: the first evidence of a host holding too many
		// read views would have been the OOM killer. Three series and not one, because
		// they answer different questions and a snapshotted volume moves them apart —
		// `cow.Cost`'s doc comment carries the reasoning and the alternative rejected.
		//
		// Recorded by the owner of the map, under the lock that serializes it, at the
		// cadence the watermarks already use (a flush). Not sampled by a poller: the
		// structure is not safe to read concurrently, and a poller would be a second
		// thing needing the volume's lock on the data path.
		{"read_view_bytes", KindGauge, "Live extent bytes held by a volume's read view, across its whole layer chain", []string{"volume"}},
		{"read_view_extents", KindGauge, "Live extent records in a volume's read view; Read scans them all, per layer", []string{"volume"}},
		{"read_view_layers", KindGauge, "Layers a read traverses (1 = no snapshot; §19's Freeze adds one and no bytes)", []string{"volume"}},

		// --- Leases (liveness, no longer durability — §26.2) ---
		{"lease_remaining_seconds", KindGauge, "Remaining lease time per host", []string{"host"}},
		{"lease_renewal_failures_total", KindCounter, "Lease renewal failures", []string{"host"}},

		// --- Fleet (§26.2) ---
		{"clone_same_host_total", KindCounter, "Same-host clones", nil},
		{"chain_depth", KindGauge, "Snapshot chain depth", []string{"volume"}},
		{"discarded_bytes_total", KindCounter, "Bytes reclaimed via DISCARD/WRITE_ZEROES", []string{"volume"}},
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
