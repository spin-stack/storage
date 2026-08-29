package controlplane_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// gcWorld is a bucket plus the catalog that describes it, with a clock both share so
// that an object's age — the only thing the grace period is decided on — is a number the
// test sets rather than one it waits for.
type gcWorld struct {
	t     *testing.T
	clk   *sim.Clock
	md    *metasim.Store
	store *sim.ObjectStore
	term  int64
}

func newGCWorld(t *testing.T) *gcWorld {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	store.SetClock(clk)
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	return &gcWorld{t: t, clk: clk, md: md, store: store, term: term}
}

// volume records a volume in the catalog and returns the id its objects are written
// under. No descriptor and no placement: reachability is a question about commits.
func (w *gcWorld) volume() string {
	w.t.Helper()
	id := ids.New().String()
	err := w.md.CreateVolume(w.t.Context(), w.term, metadata.Volume{
		VolumeID: id, SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, State: lifecycle.VolumeActive,
		DEKWrapped: []byte("wrapped"), KEKID: "kek-test", DEKKeyID: 1,
	}, nil)
	if err != nil {
		w.t.Fatalf("CreateVolume: %v", err)
	}
	return id
}

// publish appends one commit to a volume through the real writer — PUT layer, PUT
// manifest, CAS HEAD — so the objects the report classifies are the objects production
// produces, byte for byte.
func (w *gcWorld) publish(volumeID string, payload string) commit.Manifest {
	w.t.Helper()
	u, err := uuid.Parse(volumeID)
	if err != nil {
		w.t.Fatal(err)
	}
	enc, err := crypto.NewEncryption(crypto.DEK{Key: [crypto.DEKSize]byte{7}, KeyID: 1}, [16]byte(u))
	if err != nil {
		w.t.Fatal(err)
	}
	m, err := commit.Publish(w.t.Context(), w.store, enc, strings.NewReader(payload), commit.Request{
		VolumeID: volumeID, CommitID: ids.New().String(), LayerID: ids.New().String(),
		Epoch: 1, VirtualSize: 1 << 30,
	})
	if err != nil {
		w.t.Fatalf("Publish: %v", err)
	}
	// Every commit is written at a distinct instant, so an age in the report is a fact
	// about one object rather than about the whole fixture.
	w.clk.Advance(time.Minute)
	return m
}

// snapshotAt publishes a catalog snapshot naming a commit — the second kind of root.
func (w *gcWorld) snapshotAt(volumeID, commitID string) string {
	w.t.Helper()
	id := ids.New().String()
	ctx := w.t.Context()
	err := w.md.CreateSnapshot(ctx, w.term, metadata.Snapshot{
		SnapshotID: id, VolumeID: volumeID, Epoch: 1,
		State: lifecycle.SnapshotCreating, RequestID: ids.New().String(),
	})
	if err != nil {
		w.t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := w.md.PublishSnapshot(ctx, w.term, id, commitID, ""); err != nil {
		w.t.Fatalf("PublishSnapshot: %v", err)
	}
	return id
}

// orphan writes an object under the layer prefix that no manifest names — what an
// interrupted publish leaves behind (PUT layer, then nothing).
func (w *gcWorld) orphanLayer(body string) string {
	w.t.Helper()
	key := commit.LayerKey(commit.Digest([]byte(body)))
	if _, err := w.store.Put(w.t.Context(), key, []byte(body), objectstore.PutOptions{}); err != nil {
		w.t.Fatalf("putting an orphan layer: %v", err)
	}
	return key
}

func (w *gcWorld) plan(grace time.Duration) (controlplane.GCPlan, string) {
	w.t.Helper()
	plan, err := controlplane.PlanGC(w.t.Context(), w.md, w.store, w.clk.Wall(), grace)
	if err != nil {
		w.t.Fatalf("PlanGC: %v", err)
	}
	var buf bytes.Buffer
	if err := plan.Print(&buf); err != nil {
		w.t.Fatalf("printing the plan: %v", err)
	}
	return plan, buf.String()
}

// keys lists every object the bucket still answers for.
func (w *gcWorld) keys() []string {
	w.t.Helper()
	var out []string
	for _, prefix := range []string{commit.LayerPrefix, "volumes/"} {
		objs, err := w.store.List(w.t.Context(), prefix)
		if err != nil {
			w.t.Fatalf("List %s: %v", prefix, err)
		}
		for _, o := range objs {
			out = append(out, o.Key)
		}
	}
	return out
}

const gcGrace = time.Hour

// THE invariant: an object any root reaches is never a candidate.
//
// The last shape is the one that makes it hard, and it is the whole reason §20's roots
// are not just "the current HEADs": a snapshot names a commit, and the volume's published
// history has since been restarted, so nothing but the snapshot names those objects. A
// walk from HEAD alone finds a bucket full of orphans that are a tenant's snapshot.
func TestGCNeverProposesAnObjectAnyRootReaches(t *testing.T) {
	cases := []struct {
		name string
		// build returns the keys that must never be proposed.
		build func(w *gcWorld) []string
	}{
		{
			name: "the current HEAD and every ancestor of it",
			build: func(w *gcWorld) []string {
				v := w.volume()
				var want []string
				for _, payload := range []string{"one", "two", "three", "four", "five"} {
					want = append(want, keysOf(w.t, v, w.publish(v, payload))...)
				}
				return want
			},
		},
		{
			name: "a commit a snapshot names, with HEAD far past it",
			build: func(w *gcWorld) []string {
				v := w.volume()
				snapped := w.publish(v, "snapshot me")
				w.snapshotAt(v, snapped.CommitID)
				want := keysOf(w.t, v, snapped)
				for i := range 8 {
					want = append(want, keysOf(w.t, v, w.publish(v, string(rune('a'+i))))...)
				}
				return want
			},
		},
		{
			name: "a commit reachable only through a snapshot, the volume's history restarted",
			build: func(w *gcWorld) []string {
				v := w.volume()
				first := w.publish(v, "old one")
				second := w.publish(v, "old two")
				snapped := w.publish(v, "old three")
				w.snapshotAt(v, snapped.CommitID)
				// HEAD is lost — a restored bucket, an operator's mistake — and the
				// volume publishes again from nothing. Its new chain reaches none of
				// the three commits above; the snapshot is all that holds them.
				if err := w.store.Delete(w.t.Context(), commit.HeadKey(v)); err != nil {
					w.t.Fatalf("dropping HEAD: %v", err)
				}
				restarted := w.publish(v, "new one")
				if restarted.ParentCommitID != "" {
					w.t.Fatalf("the fixture did not restart the history: %s has parent %s",
						restarted.CommitID, restarted.ParentCommitID)
				}
				var want []string
				for _, m := range []commit.Manifest{first, second, snapped, restarted} {
					want = append(want, keysOf(w.t, v, m)...)
				}
				return want
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newGCWorld(t)
			// A second volume with a history of its own, and one true orphan, so the
			// report is never trivially empty: a planner that proposed nothing at all
			// would pass every assertion below.
			other := w.volume()
			w.publish(other, "elsewhere")
			protected := tc.build(w)
			w.clk.Advance(2 * gcGrace)
			orphan := w.orphanLayer("nothing names me")
			w.clk.Advance(2 * gcGrace)

			plan, report := w.plan(gcGrace)
			for _, key := range protected {
				for _, c := range plan.Candidates {
					if c.Key == key {
						t.Fatalf("%s is reachable and was proposed for deletion: %s", key, c.Why)
					}
				}
				if strings.Contains(report, key) {
					t.Fatalf("the report names the reachable object %s:\n%s", key, report)
				}
			}
			if !strings.Contains(report, orphan) {
				t.Fatalf("the orphan %s is not in the report, so nothing here was tested:\n%s", orphan, report)
			}
		})
	}
}

// The command deletes nothing. Asserted on the bucket, not on the absence of a call: a
// planner that removed an object and reported it as a candidate satisfies every
// assertion about the report.
func TestGCDryRunRemovesNothing(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	w.publish(v, "kept")
	w.orphanLayer("condemned")
	w.clk.Advance(2 * gcGrace)
	before := w.keys()

	plan, report := w.plan(gcGrace)
	if len(plan.Candidates) == 0 {
		t.Fatalf("no candidate, so this test proves nothing:\n%s", report)
	}
	after := w.keys()
	if len(before) != len(after) {
		t.Fatalf("the bucket held %d objects before the dry run and %d after: %v -> %v",
			len(before), len(after), before, after)
	}
	for _, c := range plan.Candidates {
		if _, err := w.store.Get(t.Context(), c.Key); err != nil {
			t.Fatalf("candidate %s cannot be read back: %v", c.Key, err)
		}
	}
	if !strings.Contains(report, "deletes nothing") {
		t.Fatalf("the report does not say it deletes nothing:\n%s", report)
	}
}

// The grace period, which is the whole of §20's defence against mistaking a publish in
// flight for an orphan: the layer of a commit whose manifest has not been written yet is
// indistinguishable from garbage, and it is young.
func TestGCHoldsAnObjectYoungerThanTheGracePeriod(t *testing.T) {
	w := newGCWorld(t)
	w.publish(w.volume(), "settled")
	w.clk.Advance(2 * gcGrace)
	inFlight := w.orphanLayer("a layer whose manifest is still being written")

	plan, report := w.plan(gcGrace)
	for _, c := range plan.Candidates {
		if c.Key == inFlight {
			t.Fatalf("%s was written this instant and is already a candidate under a %s grace period", c.Key, gcGrace)
		}
	}
	if !containsKey(plan.Held, inFlight) {
		t.Fatalf("the in-flight layer is neither a candidate nor held:\n%s", report)
	}

	// Past the grace period it is a candidate, and the age says how long it has been
	// there. Without this half, a planner that held everything for ever would pass.
	w.clk.Advance(gcGrace + time.Minute)
	plan, report = w.plan(gcGrace)
	if !containsKey(plan.Candidates, inFlight) {
		t.Fatalf("the orphan never became a candidate:\n%s", report)
	}
	for _, c := range plan.Candidates {
		if c.Key == inFlight && c.Age < gcGrace {
			t.Fatalf("candidate %s is reported as %s old, which is inside the %s grace period",
				c.Key, c.Age, gcGrace)
		}
	}
}

// A commit a root names that cannot be read stops the whole report. Half a walk cannot
// tell an orphan from an object whose only reference is the one that would not load, and
// the section §20 opens with is "never delete an object simply because it does not
// appear".
func TestGCRefusesToReportWhenAWalkCannotFinish(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	first := w.publish(v, "one")
	w.publish(v, "two")
	w.orphanLayer("would have been a candidate")
	w.clk.Advance(2 * gcGrace)
	if err := w.store.Delete(t.Context(), commit.ManifestKey(v, first.CommitID)); err != nil {
		t.Fatalf("dropping a manifest: %v", err)
	}

	_, err := controlplane.PlanGC(t.Context(), w.md, w.store, w.clk.Wall(), gcGrace)
	if err == nil {
		t.Fatal("a history with a hole in it produced a candidate list")
	}
	if !strings.Contains(err.Error(), first.CommitID) {
		t.Fatalf("the error does not name the commit that could not be read: %v", err)
	}
}

// A volume whose objects are in the bucket and whose row is not in the catalog is the
// shape a lost PostgreSQL leaves (§22.5 exists for it). Its HEAD is still a root: this
// command must not turn a catalog outage into a proposal to delete the fleet.
func TestGCRootsAVolumeTheCatalogHasNeverHeardOf(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	m := w.publish(v, "the catalog will forget me")
	w.clk.Advance(2 * gcGrace)
	if err := w.md.DeleteVolume(t.Context(), w.term, v); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	plan, report := w.plan(gcGrace)
	for _, key := range keysOf(t, v, m) {
		if containsKey(plan.Candidates, key) {
			t.Fatalf("%s belongs to a volume the catalog lost and was proposed for deletion:\n%s", key, report)
		}
	}
}

// keysOf is the pair of objects one commit adds: its manifest and its layer.
func keysOf(t *testing.T, volumeID string, m commit.Manifest) []string {
	t.Helper()
	return []string{commit.ManifestKey(volumeID, m.CommitID), m.Layer.ObjectKey}
}

func containsKey(cs []controlplane.GCCandidate, key string) bool {
	for _, c := range cs {
		if c.Key == key {
			return true
		}
	}
	return false
}
