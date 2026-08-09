package obs_test

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

// The Provider a binary builds is the one that has to be scrapable. This is the shape
// `cmd/volume-agent` runs in a pilot — no OTLP endpoint configured, so no exporter —
// and until this existed that Provider was a black hole: every series it collected
// could reach a collector or nothing, and no documented step stood a collector up.
func TestAProductionProviderCanBeScraped(t *testing.T) {
	ctx := t.Context()

	p, err := obs.NewProvider("volume-agent", nil)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	r := p.Recorder()
	r.Gauge(ctx, "wal_local_sequence", 7, obs.String("volume", "vol-1"))
	r.Count(ctx, "discarded_bytes_total", 4096, obs.String("volume", "vol-1"))
	r.Observe(ctx, "wal_append_latency_seconds", 0.5, obs.String("volume", "vol-1"))

	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	got := string(body)

	for _, want := range []string{
		"# TYPE wal_local_sequence gauge",
		`wal_local_sequence{volume="vol-1"} 7`,
		"# TYPE discarded_bytes_total counter",
		`discarded_bytes_total{volume="vol-1"} 4096`,
		"# TYPE wal_append_latency_seconds histogram",
		`wal_append_latency_seconds_sum{volume="vol-1"} 0.5`,
		`wal_append_latency_seconds_count{volume="vol-1"} 1`,
		`wal_append_latency_seconds_bucket{le="+Inf",volume="vol-1"} 1`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("the scrape does not contain %q\n---\n%s", want, got)
		}
	}

	// A metric nobody recorded must not be exported at all. An empty series reads as
	// "the thing being measured is not happening", which is the failure the catalogue
	// was trimmed for; the reader only reports what carries data.
	if strings.Contains(got, "snapshot_pause_duration_seconds") {
		t.Errorf("a series nothing recorded was exported:\n%s", got)
	}
}

// A label value is guest-influenced in principle and quoted in the exposition format
// unconditionally, so an unescaped quote produces a line a scraper rejects — and the
// series next to it disappear with it, silently.
func TestScrapeEscapesLabelValues(t *testing.T) {
	ctx := t.Context()

	p, err := obs.NewProvider("volume-agent", nil)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	p.Recorder().Gauge(ctx, "wal_out_of_space", 1, obs.String("volume", `a"b\c`+"\n"))

	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	want := `wal_out_of_space{volume="a\"b\\c\n"} 1`
	if !strings.Contains(string(body), want+"\n") {
		t.Fatalf("the scrape does not escape the label value; want %q in\n%s", want, body)
	}
}

// Two scrapes of an unchanged Provider must be byte-identical: a diff between two
// scrapes is how an operator sees what moved, and map iteration order would make every
// diff say everything moved.
func TestScrapeIsStable(t *testing.T) {
	ctx := t.Context()

	p, err := obs.NewProvider("volume-agent", nil)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	r := p.Recorder()
	for _, vol := range []string{"vol-c", "vol-a", "vol-b"} {
		r.Gauge(ctx, "wal_local_sequence", 1, obs.String("volume", vol))
		r.Gauge(ctx, "wal_unflushed_bytes", 2, obs.String("volume", vol))
	}

	first, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	second, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("two scrapes of an unchanged provider differ:\n%s\n---\n%s", first, second)
	}
	if a, b := strings.Index(string(first), `volume="vol-a"`), strings.Index(string(first), `volume="vol-c"`); a > b {
		t.Fatalf("the volumes are not in a stable order:\n%s", first)
	}
}
