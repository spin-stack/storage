package agent_test

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// waitForSnapshotReport reads the status the Agent would report until it says something
// about a snapshot. The upload runs on its own goroutine — the reconcile loop must not
// block on it, or an Agent misses the heartbeat that renews its lease — so the outcome
// arrives on a later cycle, exactly as it does in production.
//
// It yields rather than sleeps, and counts iterations rather than seconds: INV-01 keeps
// the wall clock out of this tree, and a bound on scheduler turns is a bound that does
// not change with the machine the test runs on.
func waitForSnapshotReport(t *testing.T, m *agent.VolumeManager, volumeID string) agent.VolumeStatus {
	t.Helper()
	for range 1_000_000 {
		vols, err := m.Volumes(t.Context())
		if err != nil {
			t.Fatalf("Volumes: %v", err)
		}
		for _, v := range vols {
			if v.VolumeID == volumeID && v.SnapshotID != "" {
				return v
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("volume %s never reported a snapshot", volumeID)
	return agent.VolumeStatus{}
}

// The whole trigger: a snapshot id in the desired state is taken, and the answer comes
// back on the volume's own report. Nothing calls the Agent — it is told (ADR-0021).
func TestAPendingSnapshotInTheDesiredStateIsTaken(t *testing.T) {
	r := newPublishRig(t, sim.NewObjectStore())
	defer func() { _ = r.m.Close() }()
	d := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, r.m, d.GetVolumeId())

	snapID := ids.New().String()
	d.PendingSnapshotId = snapID
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply with a pending snapshot: %v", err)
	}

	st := waitForSnapshotReport(t, r.m, d.GetVolumeId())
	if st.SnapshotID != snapID || st.SnapshotError != "" {
		t.Fatalf("report = %+v, want snapshot %s with no error", st, snapID)
	}
	if st.SnapshotSequence == 0 {
		t.Fatalf("the snapshot was reported at sequence 0, so it names no point at all: %+v", st)
	}

	// The artefact, not the report: an Agent that set the fields and wrote nothing
	// would satisfy every assertion above.
	key := "image/" + d.GetVolumeId() + "/snapshots/" + snapID + ".json"
	if _, err := r.store.Get(t.Context(), key); err != nil {
		t.Fatalf("no manifest at %s: %v", key, err)
	}
}

// countingStore counts the writes to one key. Asserting "the snapshot was taken once"
// against the manifest's *contents* proves nothing — the second attempt would find it
// there, read its sequence and report the same answer — so the only honest evidence is
// how many times the write was attempted.
type countingStore struct {
	objectstore.Store
	key string
	mu  sync.Mutex
	n   int
}

func (c *countingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if key == c.key {
		c.mu.Lock()
		c.n++
		c.mu.Unlock()
	}
	return c.Store.Put(ctx, key, data, opts)
}

func (c *countingStore) puts() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// The request repeats every few seconds until the catalog row leaves CREATING, so the
// second, third and hundredth arrival must be free.
func TestARepeatedSnapshotRequestIsTakenOnce(t *testing.T) {
	snapID := ids.New().String()
	counting := &countingStore{Store: sim.NewObjectStore()}
	r := newPublishRig(t, counting)
	defer func() { _ = r.m.Close() }()
	d := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, r.m, d.GetVolumeId())

	counting.key = "image/" + d.GetVolumeId() + "/snapshots/" + snapID + ".json"
	d.PendingSnapshotId = snapID
	for range 5 {
		if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	st := waitForSnapshotReport(t, r.m, d.GetVolumeId())
	if st.SnapshotError != "" {
		t.Fatalf("a repeated request produced an error: %s", st.SnapshotError)
	}

	// And it applies again after the answer is in, which is what the Control Plane does
	// until it has recorded the report.
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply after the answer: %v", err)
	}
	again := waitForSnapshotReport(t, r.m, d.GetVolumeId())
	if again.SnapshotSequence != st.SnapshotSequence || again.SnapshotError != "" {
		t.Fatalf("the answer changed on a repeat: %+v then %+v", st, again)
	}
	if n := counting.puts(); n != 1 {
		t.Fatalf("the manifest was written %d times for %d requests, want 1", n, 6)
	}
}

// A request that stops arriving stops being answered. Without it the Agent reports a
// snapshot the Control Plane has already recorded, forever.
//
// Two things enforce it and this covers the pair: Status answers only about the pending
// id, and ensureSnapshot prunes what is no longer asked for (which is also what keeps the
// map from growing for the life of the process). Either alone satisfies this test —
// breaking both is what turns it red, which is the honest description of what it proves.
func TestASnapshotNoLongerAskedForIsNotReported(t *testing.T) {
	r := newPublishRig(t, sim.NewObjectStore())
	defer func() { _ = r.m.Close() }()
	d := desiredVolume(t, 1)
	d.PendingSnapshotId = ids.New().String()
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	waitForSnapshotReport(t, r.m, d.GetVolumeId())

	d.PendingSnapshotId = ""
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	vols, err := r.m.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vols {
		if v.SnapshotID != "" {
			t.Fatalf("still reporting snapshot %s after it stopped being asked for", v.SnapshotID)
		}
	}
}

// An Agent with no object store cannot take a snapshot, and saying so is the difference
// between an operator seeing FAILED and a row that sits in CREATING forever.
func TestASnapshotWithNowhereToGoIsReportedAsFailed(t *testing.T) {
	m, _, _ := newTestManager(t) // no Store in its deps
	d := desiredVolume(t, 1)
	d.PendingSnapshotId = ids.New().String()
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	st := waitForSnapshotReport(t, m, d.GetVolumeId())
	if !strings.Contains(st.SnapshotError, "object store") {
		t.Fatalf("error = %q, want it to name the missing object store", st.SnapshotError)
	}
}
