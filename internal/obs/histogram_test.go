package obs_test

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/obs"
)

// A histogram has to survive the trip an operator actually makes: recorded through the
// Recorder, read back per label set, and rendered into the scrape. Until this kind
// existed the scrape printed `# UNSUPPORTED <name>` for it, which is a series nobody sees.
func TestAHistogramIsRecordedReadBackAndScraped(t *testing.T) {
	ctx := t.Context()
	p := newProvider(t)
	rec := p.Recorder()

	rec.Observe(ctx, "commit_publish_latency_seconds", 0.2, obs.String("volume", "vol-a"))
	rec.Observe(ctx, "commit_publish_latency_seconds", 0.8, obs.String("volume", "vol-a"))
	rec.Observe(ctx, "commit_publish_latency_seconds", 4, obs.String("volume", "vol-b"))

	got, err := p.HistogramSeries(ctx, "commit_publish_latency_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if s := got[`{volume="vol-a"}`]; s.Count != 2 || s.Sum != 1 {
		t.Fatalf(`vol-a = %+v, want 2 samples summing to 1`, s)
	}
	if s := got[`{volume="vol-b"}`]; s.Count != 1 || s.Sum != 4 {
		t.Fatalf(`vol-b = %+v, want 1 sample of 4`, s)
	}

	body, err := p.Scrape(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE commit_publish_latency_seconds histogram",
		// Cumulative: both of vol-a's samples are at or below 1s.
		`commit_publish_latency_seconds_bucket{volume="vol-a",le="1"} 2`,
		`commit_publish_latency_seconds_bucket{volume="vol-a",le="+Inf"} 2`,
		`commit_publish_latency_seconds_sum{volume="vol-a"} 1`,
		`commit_publish_latency_seconds_count{volume="vol-a"} 2`,
		`commit_publish_latency_seconds_count{volume="vol-b"} 1`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("the scrape is missing %q:\n%s", want, body)
		}
	}
	// A byte histogram gets boundaries that straddle a layer; the SDK's default set ends
	// at 10000, which would put every layer in the overflow bucket.
	rec.Observe(ctx, "layer_size_bytes", 33<<20, obs.String("volume", "vol-a"))
	body, err = p.Scrape(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `layer_size_bytes_bucket{volume="vol-a",le="6.7108864e+07"} 1`) {
		t.Errorf("a 33 MiB layer did not land in the 64 MiB bucket:\n%s", body)
	}
}
