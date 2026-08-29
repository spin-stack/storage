package controlplane_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// apply plans and then deletes, which is the only order the command offers: a plan is
// what names the objects, and one taken in another process is one nothing re-checked.
func (w *gcWorld) apply() (controlplane.GCResult, error) {
	w.t.Helper()
	plan, _ := w.plan(gcGrace)
	return controlplane.ApplyGC(w.t.Context(), w.store, plan)
}

// publishWithoutMovingHead leaves the bucket in the state a kill between the manifest and
// the CAS leaves it: both objects written, HEAD still naming the commit before them.
func (w *gcWorld) publishWithoutMovingHead(volumeID, payload string) commit.Manifest {
	w.t.Helper()
	before, _, err := commit.ReadHead(w.t.Context(), w.store, volumeID)
	if err != nil {
		w.t.Fatalf("reading HEAD: %v", err)
	}
	m := w.publish(volumeID, payload)
	_, etag, err := commit.ReadHead(w.t.Context(), w.store, volumeID)
	if err != nil {
		w.t.Fatalf("reading HEAD: %v", err)
	}
	if err := commit.CASHead(w.t.Context(), w.store, volumeID, before.CommitID, etag); err != nil {
		w.t.Fatalf("winding HEAD back: %v", err)
	}
	return m
}

// republish is the Agent restarting and finishing the commit it owed, through the same
// writer and with the same ids — which is what makes it a retry and not a second commit.
func (w *gcWorld) republish(volumeID string, m commit.Manifest, payload string) {
	w.t.Helper()
	u, err := uuid.Parse(volumeID)
	if err != nil {
		w.t.Fatal(err)
	}
	enc, err := crypto.NewEncryption(crypto.DEK{Key: [crypto.DEKSize]byte{7}, KeyID: 1}, [16]byte(u))
	if err != nil {
		w.t.Fatal(err)
	}
	_, err = commit.Publish(w.t.Context(), w.store, enc, strings.NewReader(payload), commit.Request{
		VolumeID: volumeID, CommitID: m.CommitID, LayerID: m.Layer.LayerID,
		Epoch: 1, VirtualSize: 1 << 30,
	})
	if err != nil {
		w.t.Fatalf("republishing: %v", err)
	}
}

// The point of the whole command: what the report proposed stops being in the bucket, and
// nothing else moves.
func TestGCDeletesTheCandidatesAndNothingElse(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	kept := w.publish(v, "reachable")
	snapped := w.publish(v, "snapshotted")
	w.snapshotAt(v, snapped.CommitID)
	head := w.publish(v, "the head")
	orphan := w.orphanLayer("nothing names me")
	w.clk.Advance(2 * gcGrace)

	res, err := w.apply()
	if err != nil {
		t.Fatalf("ApplyGC: %v", err)
	}
	if len(res.Deleted) == 0 {
		t.Fatal("nothing was deleted, so this test proves nothing")
	}
	if _, err := w.store.Get(t.Context(), orphan); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("the orphan layer still answers: %v", err)
	}
	// Everything a root reaches, plus the three objects that are a volume's identity and
	// that no walk can ever mark reachable.
	var reachable []string
	for _, m := range []commit.Manifest{kept, snapped, head} {
		reachable = append(reachable, keysOf(t, v, m)...)
	}
	reachable = append(reachable, commit.HeadKey(v))
	for _, key := range reachable {
		if _, err := w.store.Get(t.Context(), key); err != nil {
			t.Fatalf("%s was reachable and is gone after the delete: %v", key, err)
		}
	}
	if res.Bytes <= 0 {
		t.Fatalf("%d objects were deleted and the reclaimed size is %d", len(res.Deleted), res.Bytes)
	}
}

// The grace period is the plan's, and the delete inherits it rather than re-deciding: an
// object the report held is an object this must not touch.
func TestGCDeletesNothingItHeld(t *testing.T) {
	w := newGCWorld(t)
	w.publish(w.volume(), "settled")
	w.clk.Advance(2 * gcGrace)
	inFlight := w.orphanLayer("a layer whose manifest is still being written")

	plan, _ := w.plan(gcGrace)
	if !containsKey(plan.Held, inFlight) {
		t.Fatal("the fixture did not produce a held object")
	}
	if _, err := controlplane.ApplyGC(t.Context(), w.store, plan); err != nil {
		t.Fatalf("ApplyGC: %v", err)
	}
	if _, err := w.store.Get(t.Context(), inFlight); err != nil {
		t.Fatalf("a held object was deleted: %v", err)
	}
}

// A mistake costs a restore and not the data (INV-14). Asserted here rather than trusted
// from the interface's documentation, because it is the property that decides how much
// this command is allowed to be wrong.
func TestGCDeletesReversibly(t *testing.T) {
	w := newGCWorld(t)
	w.publish(w.volume(), "reachable")
	orphan := w.orphanLayer("condemned")
	w.clk.Advance(2 * gcGrace)

	if _, err := w.apply(); err != nil {
		t.Fatalf("ApplyGC: %v", err)
	}
	if err := w.store.Restore(t.Context(), orphan); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	body, err := w.store.Get(t.Context(), orphan)
	if err != nil {
		t.Fatalf("the restored object does not answer: %v", err)
	}
	if string(body) != "condemned" {
		t.Fatalf("the restored object came back as %q", body)
	}
}

// THE adversary, and the reason this command re-reads anything at all.
//
// An Agent that was killed between its manifest and its CAS holds the commit id in
// state.json and republishes *that* id. Both objects are already in the bucket and
// neither is rewritten — the layer is content-addressed and verified in place, the
// manifest is byte-identical and create-only — so their age never changes. Let the Agent
// stay down longer than the grace period and the report is right: nothing reaches them.
// Then the Agent comes back between the report and the delete, the CAS lands, and
// Commit() returns SUCCESS for a commit whose objects this command is holding a list of.
//
// Nothing about the objects themselves can be re-checked to see this: what changed is
// HEAD. So HEAD is what is re-read.
func TestAdversaryTheAgentRepublishesBetweenTheReportAndTheDelete(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	w.publish(v, "the last commit that landed")

	// The interrupted publish: the real writer, up to and including the manifest, with
	// HEAD left where it was.
	interrupted := w.publishWithoutMovingHead(v, "written, never pointed at")
	w.clk.Advance(2 * gcGrace)

	plan, _ := w.plan(gcGrace)
	for _, key := range keysOf(t, v, interrupted) {
		if !containsKey(plan.Candidates, key) {
			t.Fatalf("the fixture did not produce the shape under test: %s is not a candidate", key)
		}
	}

	// The Agent comes back and finishes the commit it owed.
	w.republish(v, interrupted, "written, never pointed at")

	res, err := controlplane.ApplyGC(t.Context(), w.store, plan)
	if !errors.Is(err, controlplane.ErrGCPlanStale) {
		t.Fatalf("the delete went ahead over a HEAD that moved: %v (deleted %d)", err, len(res.Deleted))
	}
	if len(res.Deleted) != 0 {
		t.Fatalf("%d objects were deleted before the plan was found stale: %v", len(res.Deleted), res.Deleted)
	}
	// And the commit that returned SUCCESS is whole.
	for _, key := range keysOf(t, v, interrupted) {
		if _, err := w.store.Get(t.Context(), key); err != nil {
			t.Fatalf("%s belongs to a commit HEAD names and is gone: %v", key, err)
		}
	}
	if !strings.Contains(err.Error(), v) {
		t.Fatalf("the refusal does not name the volume whose HEAD moved: %v", err)
	}
}

// A volume that publishes for the first time between the report and the delete moves no
// HEAD that the report read — it creates one. Its objects are young, so they were held,
// but the run must not be refused by a HEAD that simply appeared either: with a volume
// per tenant, an appearing HEAD is the ordinary case and a command that refuses on it
// never runs.
func TestGCProceedsWhenAVolumePublishesItsFirstCommit(t *testing.T) {
	w := newGCWorld(t)
	w.publish(w.volume(), "reachable")
	orphan := w.orphanLayer("condemned")
	w.clk.Advance(2 * gcGrace)
	plan, _ := w.plan(gcGrace)

	w.publish(w.volume(), "a volume that did not exist when the report was taken")

	res, err := controlplane.ApplyGC(t.Context(), w.store, plan)
	if err != nil {
		t.Fatalf("ApplyGC refused a run because a new volume appeared: %v", err)
	}
	if !containsDeleted(res, orphan) {
		t.Fatalf("the orphan was not deleted: %+v", res.Deleted)
	}
}

func containsDeleted(r controlplane.GCResult, key string) bool {
	for _, c := range r.Deleted {
		if c.Key == key {
			return true
		}
	}
	return false
}

// Manifests go before layers, so that an interruption never leaves a manifest naming a
// layer that is not there — a commit that reads as present and cannot be opened. The
// other order leaves an orphan layer, which is the thing this command collects.
//
// Driven by failing the manifest's delete: with manifests first, nothing else has gone
// yet; with layers first, the layer would already be marked.
func TestGCDeletesAManifestBeforeItsLayer(t *testing.T) {
	w := newGCWorld(t)
	v := w.volume()
	w.publish(v, "the head")
	condemned := w.publishWithoutMovingHead(v, "unreachable, both objects")
	w.clk.Advance(2 * gcGrace)

	plan, _ := w.plan(gcGrace)
	manifest := commit.ManifestKey(v, condemned.CommitID)
	if !containsKey(plan.Candidates, manifest) || !containsKey(plan.Candidates, condemned.Layer.ObjectKey) {
		t.Fatal("the fixture did not condemn both objects of one commit")
	}
	w.store.InjectThrottleKey(manifest, 1)

	res, err := controlplane.ApplyGC(t.Context(), w.store, plan)
	if err == nil {
		t.Fatal("the manifest's delete was made to fail and the pass reported success")
	}
	if _, err := w.store.Get(t.Context(), condemned.Layer.ObjectKey); err != nil {
		t.Fatalf("the layer was deleted before the manifest that names it: %v", err)
	}
	if len(res.Deleted) != 0 {
		t.Fatalf("the pass reported deleting %v", res.Deleted)
	}
}
