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
	r.Gauge(ctx, "chain_depth", 7, obs.String("volume", "vol-1"))
	r.Count(ctx, "clone_same_host_total", 4096)

	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	got := string(body)

	for _, want := range []string{
		"# TYPE chain_depth gauge",
		`chain_depth{volume="vol-1"} 7`,
		"# TYPE clone_same_host_total counter",
		"clone_same_host_total 4096",
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("the scrape does not contain %q\n---\n%s", want, got)
		}
	}

	// A metric nobody recorded must not be exported at all. An empty series reads as
	// "the thing being measured is not happening", which is the failure the catalogue
	// was trimmed for; the reader only reports what carries data.
	if strings.Contains(got, "lease_remaining_seconds") {
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

	p.Recorder().Gauge(ctx, "chain_depth", 1, obs.String("volume", `a"b\c`+"\n"))

	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	want := `chain_depth{volume="a\"b\\c\n"} 1`
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
		r.Gauge(ctx, "chain_depth", 1, obs.String("volume", vol))
	}
	for _, host := range []string{"host-c", "host-a", "host-b"} {
		r.Gauge(ctx, "lease_remaining_seconds", 2, obs.String("host", host))
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
