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

// Scrape renders everything this Provider has collected in the Prometheus text
// exposition format — the thing `curl` prints readably and every scraper in a pilot's
// toolbox already parses.
//
// It exists because until it did there was nothing an operator could look at. Every
// series this process produced left it over OTLP or not at all, and no documented step
// stood a collector up, so the honest description of the Agent's observability was
// "none". A pilot needs an answer from the process itself, on demand, with no
// infrastructure in front of it.
//
// Rendered here rather than by go.opentelemetry.io/otel/exporters/prometheus, and that
// is a dependency judgement rather than a preference: that exporter brings
// prometheus/client_golang and its global registry into a binary whose only use for
// either is one read-only handler, and what it would do for us is the sixty lines
// below — `_bucket`/`_sum`/`_count` for a histogram, labels quoted and escaped, one
// `# TYPE` per family. The dependency is the larger thing to review.
//
// Only series that carry data are emitted. The catalogue registers every instrument up
// front so cardinality cannot drift, and exporting all of them would put a permanently
// empty line under every name — which reads as "this is not happening" rather than
// "nothing has recorded this yet", the exact confusion the catalogue was trimmed for.
//
// The output is deterministic: families sorted by name, samples sorted by label set. A
// scrape is most often read by diffing it against the last one, and map iteration order
// would make every diff claim everything moved.
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
		default:
			// A kind Catalog() cannot produce — every histogram left with the local block
			// engine, so this is the branch a returning duration lands in until its
			// renderer comes back with it. Named rather than dropped: a scrape that
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
