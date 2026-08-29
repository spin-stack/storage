package obs

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ContentType is what a handler serving Scrape's output must set. The version belongs
// in it: a scraper that is told nothing assumes 0.0.4 anyway, but naming it is what
// keeps the header honest if a different format is ever served from here.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Scrape renders everything this Provider has collected in the Prometheus text exposition
// format — what `curl` prints readably and every scraper already parses. Until it existed
// every series left this process over OTLP or not at all, and nothing here stands a
// collector up.
//
// Rendered here rather than by otel's prometheus exporter: that brings prometheus/
// client_golang and its global registry into a binary whose only use for either is one
// read-only handler, in place of the sixty lines below.
//
// Only series that carry data are emitted — a permanently empty line under every registered
// name reads as "this is not happening". The output is deterministic (families by name,
// samples by label set) because a scrape is most often read as a diff against the last one.
func (p *Provider) Scrape(ctx context.Context) ([]byte, error) {
	var rm metricdata.ResourceMetrics
	if err := p.reader.Collect(ctx, &rm); err != nil {
		return nil, err
	}

	var all []metricdata.Metrics
	for _, scope := range rm.ScopeMetrics {
		all = append(all, scope.Metrics...)
	}
	slices.SortFunc(all, func(a, b metricdata.Metrics) int { return cmp.Compare(a.Name, b.Name) })

	var b bytes.Buffer
	for _, m := range all {
		switch data := m.Data.(type) {
		case metricdata.Gauge[float64]:
			writeFamily(&b, m, "gauge")
			writeSamples(&b, m.Name, numbers(data.DataPoints))
		case metricdata.Sum[int64]:
			// Every counter in Catalog() is monotonic; a non-monotonic Int64 sum is
			// still a valid thing for the SDK to hand back, and calling it a counter
			// would tell a scraper it may compute a rate over something that can drop.
			kind := "counter"
			if !data.IsMonotonic {
				kind = "gauge"
			}
			writeFamily(&b, m, kind)
			writeSamples(&b, m.Name, numbers(data.DataPoints))
		case metricdata.Histogram[float64]:
			writeFamily(&b, m, "histogram")
			writeHistogram(&b, m.Name, data)
		default:
			// A kind Catalog() cannot produce. Named rather than dropped: a scrape that
			// silently omits a series is the failure this endpoint exists to end, and a
			// comment line is valid in the format.
			fmt.Fprintf(&b, "# UNSUPPORTED %s\n", m.Name)
		}
	}
	return b.Bytes(), nil
}

// sample is one rendered data point: its label set, already formatted and escaped, and
// its value.
type sample struct {
	labels string
	value  float64
}

// numbers flattens the SDK's generic numeric data points into samples.
func numbers[N int64 | float64](dps []metricdata.DataPoint[N]) []sample {
	out := make([]sample, 0, len(dps))
	for _, dp := range dps {
		out = append(out, sample{labels: labelsOf(dp.Attributes), value: float64(dp.Value)})
	}
	return out
}

// writeHistogram renders one histogram family the way the exposition format defines it:
// cumulative `_bucket` counts with an `le` label, then `_sum` and `_count`. The buckets
// are cumulative and the SDK's are not, so they are added up here; the +Inf bucket is
// mandatory and equals the total count.
func writeHistogram(b *bytes.Buffer, name string, data metricdata.Histogram[float64]) {
	type series struct {
		labels string
		dp     metricdata.HistogramDataPoint[float64]
	}
	all := make([]series, 0, len(data.DataPoints))
	for _, dp := range data.DataPoints {
		all = append(all, series{labels: labelsOf(dp.Attributes), dp: dp})
	}
	slices.SortFunc(all, func(x, y series) int { return cmp.Compare(x.labels, y.labels) })
	for _, s := range all {
		cumulative := uint64(0)
		for i, count := range s.dp.BucketCounts {
			cumulative += count
			le := "+Inf"
			if i < len(s.dp.Bounds) {
				le = formatValue(s.dp.Bounds[i])
			}
			fmt.Fprintf(b, "%s_bucket%s %d\n", name, withLabel(s.labels, "le", le), cumulative)
		}
		fmt.Fprintf(b, "%s_sum%s %s\n", name, s.labels, formatValue(s.dp.Sum))
		fmt.Fprintf(b, "%s_count%s %d\n", name, s.labels, s.dp.Count)
	}
}

// withLabel adds one label to an already-rendered label set. `le` sorts after nothing in
// particular — the exposition format does not require label order, and a bucket line is
// only ever read with its family.
func withLabel(labels, key, value string) string {
	pair := key + `="` + escapeLabel(value) + `"`
	if labels == "" {
		return "{" + pair + "}"
	}
	return labels[:len(labels)-1] + "," + pair + "}"
}

func writeFamily(b *bytes.Buffer, m metricdata.Metrics, kind string) {
	if m.Description != "" {
		// A HELP line is one line: a description carrying a newline would end the line
		// early and the remainder would be parsed as a sample.
		fmt.Fprintf(b, "# HELP %s %s\n", m.Name, escapeHelp(m.Description))
	}
	fmt.Fprintf(b, "# TYPE %s %s\n", m.Name, kind)
}

func writeSamples(b *bytes.Buffer, name string, samples []sample) {
	slices.SortFunc(samples, func(x, y sample) int { return cmp.Compare(x.labels, y.labels) })
	for _, s := range samples {
		fmt.Fprintf(b, "%s%s %s\n", name, s.labels, formatValue(s.value))
	}
}

// labelsOf renders an attribute set as `{k="v",…}`, or "" when there are none.
func labelsOf(set attribute.Set) string {
	if set.Len() == 0 {
		return ""
	}
	parts := make([]string, 0, set.Len())
	for iter := set.Iter(); iter.Next(); {
		kv := iter.Attribute()
		parts = append(parts, string(kv.Key)+`="`+escapeLabel(kv.Value.Emit())+`"`)
	}
	slices.Sort(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

// escapeLabel escapes the three characters the exposition format reserves inside a
// quoted label value. An unescaped one does not corrupt its own line only: a scraper
// that fails to parse a line discards the whole scrape, so one bad label value takes
// every other series with it.
var escapeLabel = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace

// escapeHelp escapes what a HELP line reserves: it is not quoted, so only the backslash
// and the newline matter.
var escapeHelp = strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace

// formatValue renders a float the way the exposition format requires. The three
// non-finite values have their own spellings and Go's default formatting of them
// ("+Inf" aside) is not what a scraper accepts.
func formatValue(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
