package sim_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// runScenario drives the sim clock, disk, and object store through a
// seed-derived operation sequence and returns an observable trace. It is
// single-goroutine, so the only source of nondeterminism would be the sim data
// structures themselves — which this test exists to rule out (the seed of
// INV-02; full harness-level deterministic replay activates in Increment 1.3).
func runScenario(seed int64) []string {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	dsk := sim.NewDisk()
	store := sim.NewObjectStore()
	f, _ := dsk.Create("wal")

	var trace []string
	record := func(format string, args ...any) { trace = append(trace, fmt.Sprintf(format, args...)) }

	for range 500 {
		switch rng.Intn(6) {
		case 0:
			clk.Advance(time.Duration(rng.Intn(1000)) * time.Millisecond)
			record("clock now=%d wall=%s", clk.Now(), clk.Wall().Format(time.RFC3339Nano))
		case 1:
			payload := fmt.Appendf(nil, "rec-%d", rng.Intn(100))
			n, err := f.Append(payload)
			record("append n=%d err=%v", n, err)
		case 2:
			err := f.Sync()
			sz, _ := f.Size()
			record("sync err=%v size=%d", err, sz)
		case 3:
			key := fmt.Sprintf("wal/v/%d", rng.Intn(20))
			data := fmt.Appendf(nil, "obj-%d", rng.Intn(100))
			res, err := store.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: rng.Intn(2) == 0})
			record("put key=%s etag=%s err=%v", key, res.ETag, err)
		case 4:
			key := fmt.Sprintf("wal/v/%d", rng.Intn(20))
			data, err := store.Get(ctx, key)
			record("get key=%s data=%q err=%v", key, data, err)
		case 5:
			infos, _ := store.List(ctx, "wal/")
			record("list count=%d", len(infos))
			for _, in := range infos {
				record("  %s size=%d etag=%s", in.Key, in.Size, in.ETag)
			}
		}
	}
	return trace
}

func TestSimDeterministicForSameSeed(t *testing.T) {
	for _, seed := range []int64{1, 42, 1337, 2024} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			a := runScenario(seed)
			b := runScenario(seed)
			if len(a) != len(b) {
				t.Fatalf("trace length differs for same seed: %d vs %d", len(a), len(b))
			}
			for i := range a {
				if a[i] != b[i] {
					t.Fatalf("trace diverged at step %d:\n  a: %s\n  b: %s", i, a[i], b[i])
				}
			}
		})
	}
}

func TestSimDifferentSeedsDiverge(t *testing.T) {
	// Not a correctness requirement, but a sanity check that the trace actually
	// depends on the seed (otherwise the determinism test above is vacuous).
	a := runScenario(1)
	b := runScenario(2)
	same := len(a) == len(b)
	if same {
		for i := range a {
			if a[i] != b[i] {
				same = false
				break
			}
		}
	}
	if same {
		t.Fatal("expected different seeds to produce different traces")
	}
}
