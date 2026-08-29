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
	// KindGauge is a Float64 level that can go up and down.
	KindGauge
	// KindHistogram is a Float64 distribution. Every entry carries its own boundaries:
	// the SDK's default set ends at 10000, so a layer size in tens of millions of bytes
	// would land in the overflow bucket and the histogram would say only "large".
	KindHistogram
)

// Bucket boundaries. Two sets, because everything measured here is either a duration or
// a size, and a histogram whose boundaries do not straddle the values it receives
// answers no question an operator has.
var (
	// secondsBuckets spans a publish that took milliseconds and a recovery that took
	// minutes.
	secondsBuckets = []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
	// layerBuckets straddles the rotation threshold (32 MiB measured), which is a floor
	// rather than a bound: the buckets above it are where an unbounded layer shows up.
	layerBuckets = []float64{1 << 20, 8 << 20, 32 << 20, 64 << 20, 128 << 20, 256 << 20, 1 << 30, 4 << 30}
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
	// Buckets are the explicit boundaries of a KindHistogram, ignored by the others.
	Buckets []float64
}

// Catalog returns the §26.2 metric taxonomy. Every entry is registered up front so a
// later increment increments an existing series rather than inventing a name — no
// cardinality drift, no typos discovered during an incident.
//
// A series declared for a mechanism that does not exist reads as a plan, and a declared
// series nothing records is worse than an absent one: permanently empty reads as "the
// thing being measured is not happening", so a dashboard on `s3_errors_total` showed a
// healthy object store nobody was talking to. Every entry below is recorded by
// non-test code, which TestEveryDeclaredMetricHasANonTestProducer holds to.
func Catalog() []MetricDesc {
	return []MetricDesc{
		// --- Leases (liveness, no longer durability — §26.2) ---
		{"lease_remaining_seconds", KindGauge, "Remaining lease time per host", []string{"host"}, nil},
		{"lease_renewal_failures_total", KindCounter, "Lease renewal failures", []string{"host"}, nil},

		// --- Fleet (§26.2) ---
		{"clone_same_host_total", KindCounter, "Same-host clones", nil, nil},
		{"chain_depth", KindGauge, "Lineage depth as the catalog holds it: how many clones deep the volume was created", []string{"volume"}, nil},

		// --- The publish path (§28) ---
		//
		// §28 lists these without units; the suffix is here because a scrape is read by
		// a human and `lease_remaining_seconds` already set the rule. Labelled by
		// volume, as §28 asks ("por volumen"): the cardinality is the number of volumes
		// one Agent holds.
		{"layer_size_bytes", KindHistogram, "Sealed layer size as published", []string{"volume"}, layerBuckets},
		{"layer_upload_duration_seconds", KindHistogram, "Time to seal and upload one layer", []string{"volume"}, secondsBuckets},
		{"layer_upload_bytes_total", KindCounter, "Sealed bytes uploaded to the object store", []string{"volume"}, nil},
		{"commit_publish_latency_seconds", KindHistogram, "Time from the first HEAD read to the CAS that publishes a commit", []string{"volume"}, secondsBuckets},
		{"cas_failures_total", KindCounter, "HEAD compare-and-set failures: this host is no longer the volume's writer", []string{"volume"}, nil},
		{"recovery_duration_seconds", KindHistogram, "Time to rebuild a volume's published chain on this host", []string{"volume"}, secondsBuckets},
		{"recovery_download_bytes_total", KindCounter, "Sealed bytes downloaded to rebuild a chain", []string{"volume"}, nil},

		// --- Where each volume stands, recorded on every report (§28) ---
		//
		// Gauges and not histograms: each is a level with one current value per volume,
		// and what an alert asks of them is "is this one above the line right now".
		{"last_successful_commit_age_seconds", KindGauge, "Time since this host last published a commit for the volume — the RPO if the host is lost now", []string{"volume"}, nil},
		{"unpublished_local_bytes", KindGauge, "Local bytes the volume has not published yet: what those seconds cost", []string{"volume"}, nil},
		{"local_disk_bytes", KindGauge, "What the volume's layer files occupy on this host", []string{"volume"}, nil},
		// Not `chain_depth`, which is already taken by a different number under the same
		// `volume` label: the Control Plane records the catalog's lineage depth — the one
		// MaxChainDepth refuses a clone on — while this is the host counting
		// the qcow2 layers a guest reads through, which is routinely tens. One name for
		// both makes every query over it a coin flip on which process last exported.
		{"local_chain_depth", KindGauge, "Layers of this volume's chain on this host — what a guest reads through", []string{"volume"}, nil},
	}
}

// Metrics holds the instantiated instruments, keyed by name. In Phase 01 they are
// registered but unused; later phases fetch and record against them.
type Metrics struct {
	counters   map[string]metric.Int64Counter
	gauges     map[string]metric.Float64Gauge
	histograms map[string]metric.Float64Histogram
}

// NewMetrics builds every catalog instrument on the given meter. It fails if any
// name is duplicated or any instrument cannot be created.
func NewMetrics(m metric.Meter) (*Metrics, error) {
	out := &Metrics{
		counters:   map[string]metric.Int64Counter{},
		gauges:     map[string]metric.Float64Gauge{},
		histograms: map[string]metric.Float64Histogram{},
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
		case KindHistogram:
			if len(d.Buckets) == 0 {
				return nil, fmt.Errorf("histogram %q has no bucket boundaries", d.Name)
			}
			inst, err := m.Float64Histogram(d.Name,
				metric.WithDescription(d.Help),
				metric.WithExplicitBucketBoundaries(d.Buckets...))
			if err != nil {
				return nil, fmt.Errorf("histogram %q: %w", d.Name, err)
			}
			out.histograms[d.Name] = inst
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

// Histogram returns the named histogram.
func (m *Metrics) Histogram(name string) (metric.Float64Histogram, bool) {
	h, ok := m.histograms[name]
	return h, ok
}

// Len reports the total number of registered instruments.
func (m *Metrics) Len() int {
	return len(m.counters) + len(m.gauges) + len(m.histograms)
}
