package snapshot_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func v7Vol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

func remoteLog(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, vol [16]byte) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	return l
}

// TestSnapshotIsPauseFreeAndCapturesSequence: the capture does not advance the clock
// (pause ≈ 0) and the manifest's target is the captured sequence.
func TestSnapshotIsPauseFreeAndCapturesSequence(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)

	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)

	m, pause, err := snapshot.NewSnapshotter(store, clk).Create(ctx, l, vol, 1, "snap-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if pause != 0 {
		t.Fatalf("snapshot pause = %v, want ~0", pause)
	}
	if m.TargetSequence != 2 {
		t.Fatalf("target sequence = %d, want 2", m.TargetSequence)
	}
	if len(m.Objects) == 0 || m.RootDigest == "" {
		t.Fatalf("manifest incomplete: %+v", m)
	}
}

// TestSnapshotExcludesLaterWrites: writes after the capture are not in the snapshot.
func TestSnapshotExcludesLaterWrites(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	snap := snapshot.NewSnapshotter(store, clk)

	_, _ = l.Write(0, []byte("a"), 0)
	m, _, err := snap.Create(ctx, l, vol, 1, "snap-1", "")
	if err != nil {
		t.Fatal(err)
	}
	before := len(m.Objects)

	// More writes + a flush → a new object at a higher sequence.
	_, _ = l.Write(8, []byte("b"), 0)
	_, _ = l.Write(16, []byte("c"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// The published manifest is unchanged (immutable) and still excludes the new writes.
	got, err := snapshot.Read(ctx, store, m.VolumeID, "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetSequence != 1 || len(got.Objects) != before {
		t.Fatalf("snapshot changed after later writes: %+v", got)
	}
}

func TestReadMissingManifest(t *testing.T) {
	if _, err := snapshot.Read(t.Context(), sim.NewObjectStore(), "vol", "nope"); err == nil {
		t.Fatal("reading a missing manifest should error")
	}
}

// TestSnapshotManifestIsImmutable is INV-16: republishing a manifest fails.
func TestSnapshotManifestIsImmutable(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	m := snapshot.Manifest{SnapshotID: "s1", VolumeID: format.UUIDString(v7Vol()), Epoch: 1, TargetSequence: 5}
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	m.TargetSequence = 99 // attempt to change it
	if err := snapshot.Publish(ctx, store, m); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("a published manifest must be immutable, got %v", err)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }

// TestCaptureIsTheWholePause is §19 step 1 on its own: "capturar atómicamente N (µs)".
//
// The existing pause test measures Create, which captures *and* seals, and it passes
// because the simulated clock only advances when something does work — so it proves the
// pause is zero without ever proving where the work went. This one holds the two apart:
// after a Capture, nothing is durable, nothing has been listed, and no manifest exists.
// That is what makes the sealing safe to run in the background later, and it is the
// property a caller that splits the two depends on.
func TestCaptureIsTheWholePause(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	// A bound generous enough to hold what is written below: the point is a large
	// unflushed WAL, and backpressure (§5.7) refusing the writes would prove nothing.
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 16 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 5), leaseOK{})

	// Megabytes of unflushed WAL: whatever Capture costs, it must not be proportional
	// to this.
	payload := make([]byte, 64<<10)
	for i := range 32 {
		if _, err := l.Write(uint64(i)*uint64(len(payload)), payload, 0); err != nil {
			t.Fatal(err)
		}
	}

	before := clk.Now()
	c := snapshot.NewSnapshotter(store, clk).Capture(ctx, l, vol, 1, "snap-1", "")
	switch {
	case c.Pause != 0:
		t.Fatalf("capture pause = %v, want 0: it did I/O", c.Pause)
	case clk.Now() != before:
		t.Fatal("the clock advanced during a capture, so something blocked")
	case c.TargetSequence != 32:
		t.Fatalf("captured sequence %d, want 32", c.TargetSequence)
	case c.State != lifecycle.SnapshotCreating:
		t.Fatalf("a captured snapshot is %q, want CREATING", c.State)
	}

	// Nothing durable, and no manifest: everything expensive is still ahead.
	if objs, err := store.List(ctx, ""); err != nil || len(objs) != 0 {
		t.Fatalf("a capture put %d object(s) in the store (err=%v)", len(objs), err)
	}
	if w := l.Watermarks(); w.Durable != 0 {
		t.Fatalf("a capture advanced durable to %d", w.Durable)
	}
}

// TestSealIsRetryableAfterACrash. Sealing is meant to run in the background, so the
// process running it can die halfway; §19's answer is that the manifest is published
// create-only, and this is that answer checked.
//
// The second Seal is given the *same* Captured value, which is what a caller that
// recorded CREATING and resumed would have. One manifest, same digest — not an error,
// because a snapshot that exists is not a failure to report.
func TestSealIsRetryableAfterACrash(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	_, _ = l.Write(0, []byte("a"), 0)

	s := snapshot.NewSnapshotter(store, clk)
	c := s.Capture(ctx, l, vol, 1, "snap-1", "")

	first, err := s.Seal(ctx, l, c)
	if err != nil {
		t.Fatalf("first seal: %v", err)
	}
	second, err := s.Seal(ctx, l, c)
	if err != nil {
		t.Fatalf("a retried seal must not fail: %v", err)
	}
	if first.RootDigest != second.RootDigest {
		t.Fatalf("a retried seal produced a different digest: %s vs %s", first.RootDigest, second.RootDigest)
	}
	manifests := 0
	objs, err := store.List(ctx, "snapshots/")
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if strings.HasSuffix(o.Key, "manifest.json") {
			manifests++
		}
	}
	if manifests != 1 {
		t.Fatalf("%d manifests after two seals, want 1", manifests)
	}
}

// TestTheMandatoryMetricsAreRecorded. §19 names both and internal/obs has registered
// them since Phase 01 with nothing observing either — a metric nobody records is a
// metric that will be missing during the first incident that needs it.
func TestTheMandatoryMetricsAreRecorded(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	_, _ = l.Write(0, []byte("a"), 0)

	p, err := obs.NewTestProvider("snapshot-telemetry")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	s := snapshot.NewSnapshotter(store, clk)
	s.SetRecorder(obs.NewRecorder(p.Metrics))
	if _, _, err := s.Create(ctx, l, vol, 1, "snap-1", ""); err != nil {
		t.Fatal(err)
	}
	got, err := p.CollectedMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"snapshot_pause_duration_seconds",
		"snapshot_publish_duration_seconds",
	} {
		if !got[name] {
			t.Fatalf("%s was never recorded (§19 makes it mandatory)", name)
		}
	}
}

// TestSealRefusesADifferentManifestAtTheSameID is the other half of the retry rule.
// Converging on our own manifest is right; converging on somebody else's would let two
// snapshots claim one id and hand the second one the first one's data.
func TestSealRefusesADifferentManifestAtTheSameID(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	_, _ = l.Write(0, []byte("a"), 0)

	s := snapshot.NewSnapshotter(store, clk)
	c := s.Capture(ctx, l, vol, 1, "snap-1", "")

	// Somebody else's manifest, under our id, describing a different state.
	other := snapshot.Manifest{
		SnapshotID: "snap-1", VolumeID: format.UUIDString(vol), Epoch: 1,
		TargetSequence: 99, Objects: []string{"wal/other.wal"},
	}
	other.RootDigest = snapshot.Digest(other.TargetSequence, other.Objects)
	if err := snapshot.Publish(ctx, store, other); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Seal(ctx, l, c); !errors.Is(err, snapshot.ErrSnapshotConflict) {
		t.Fatalf("want ErrSnapshotConflict, got %v", err)
	}
}
