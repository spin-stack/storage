package obs_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

func newProvider(t *testing.T) *obs.Provider {
	t.Helper()
	p, err := obs.NewTestProvider("test")
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(t.Context()) })
	return p
}

// TestMetricsCatalogWellFormed guards the stable-cardinality contract: no
// duplicate names, every entry documented, and the critical §26.2 series present.
func TestMetricsCatalogWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, d := range obs.Catalog() {
		if d.Name == "" {
			t.Fatal("metric with empty name")
		}
		if d.Help == "" {
			t.Fatalf("metric %q has no help text", d.Name)
		}
		if seen[d.Name] {
			t.Fatalf("duplicate metric name %q", d.Name)
		}
		seen[d.Name] = true
	}
	// A few load-bearing names must exist (typo/regression guard). The list is short
	// because the catalogue is: every entry that measured the local block engine went
	// with it, and what is left is the Agent's liveness and the clone path.
	for _, want := range []string{
		"lease_remaining_seconds",      // the host is alive and its window is this wide (§12.2)
		"lease_renewal_failures_total", // and it stopped being able to renew
		"chain_depth",                  // §20.1's ceiling, recorded where the clone is placed
	} {
		if !seen[want] {
			t.Fatalf("required metric %q missing from catalog", want)
		}
	}
}

// TestMetricsRegistration asserts every catalog entry becomes a live instrument.
func TestMetricsRegistration(t *testing.T) {
	p := newProvider(t)
	if got, want := p.Metrics.Len(), len(obs.Catalog()); got != want {
		t.Fatalf("registered %d instruments, catalog has %d", got, want)
	}
	// Spot-check retrieval by the correct kind.
	if _, ok := p.Metrics.Counter("lease_renewal_failures_total"); !ok {
		t.Fatal("lease_renewal_failures_total should be a counter")
	}
	if _, ok := p.Metrics.Gauge("lease_remaining_seconds"); !ok {
		t.Fatal("lease_remaining_seconds should be a gauge")
	}
}

// Every declared series must have a producer somewhere outside a test, and this is the
// check that would have caught the thirteen that did not.
//
// It is a source scan rather than a runtime assertion on purpose. A runtime check would
// have to drive every producer to see the series move, which means a test that boots an
// Agent, takes a snapshot, clones a volume and fills a device — and the failure mode it
// is guarding against is precisely that nobody drives the producer. Grepping the tree
// for the name answers the actual question: does any non-test file mention it at all.
//
// A series that legitimately has no producer yet does not get an exemption list here.
// That was considered and rejected: an allow-list is how the catalogue drifted in the
// first place — every one of the thirteen would have been on it, added by whoever
// declared the series, and the list would read as a plan the same way the catalogue did.
func TestEveryDeclaredMetricHasANonTestProducer(t *testing.T) {
	root := repoRoot(t)
	for _, d := range obs.Catalog() {
		if d.Name == "obs_build_info" {
			continue
		}
		t.Run(d.Name, func(t *testing.T) {
			out, err := exec.Command("grep", "-rl", "--include=*.go", d.Name, filepath.Join(root, "internal"), filepath.Join(root, "cmd")).Output()
			if err != nil && len(out) == 0 {
				t.Fatalf("%q appears in no Go file at all outside this catalogue", d.Name)
			}
			for _, f := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				if f == "" || strings.HasSuffix(f, "_test.go") || strings.HasSuffix(f, "internal/obs/metrics.go") {
					continue
				}
				return // a non-test, non-catalogue file names it
			}
			t.Fatalf("%q is declared and recorded by nothing: it would be exported on every "+
				"scrape and permanently empty, which reads as 'this is not happening' rather "+
				"than 'nothing is measuring'. Wire a producer or delete the entry.", d.Name)
		})
	}
}

// repoRoot walks up from the test's working directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("no go.mod above the working directory")
	return ""
}

// GaugeValues keys by name and the last data point wins, so a family with several label
// sets collapses to whichever one the SDK handed back last — unordered, and not part of
// any contract. Any gauge recorded for more than one subject is such a family, and a
// test reading it through GaugeValues would pass or fail on map iteration order.
func TestGaugeSeriesSeparatesOneFamilysLabelSets(t *testing.T) {
	ctx := t.Context()
	p := newProvider(t)
	rec := p.Recorder()

	rec.Gauge(ctx, "chain_depth", 1, obs.String("volume", "vol-a"))
	rec.Gauge(ctx, "chain_depth", 3, obs.String("volume", "vol-b"))
	rec.Gauge(ctx, "chain_depth", 0, obs.String("volume", "vol-c"))

	got, err := p.GaugeSeries(ctx, "chain_depth")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		`{volume="vol-a"}`: 1,
		`{volume="vol-b"}`: 3,
		`{volume="vol-c"}`: 0,
	}
	if len(got) != len(want) {
		t.Fatalf("GaugeSeries returned %d series, want %d: %v", len(got), len(want), got)
	}
	for labels, v := range want {
		if got[labels] != v {
			t.Errorf("%s = %v, want %v (whole family: %v)", labels, got[labels], v, got)
		}
	}

	// And the scrape an operator curls carries the same three lines, which is where this
	// actually gets read from.
	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `chain_depth{volume="vol-b"} 3`) {
		t.Fatalf("the scrape does not separate one gauge family's series:\n%s", body)
	}
}
