package gc_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
)

// Finding 2 (high). Reachable decoded every anchor with
//
//	if err := json.Unmarshal(body, &r); err == nil { ... }
//
// so an anchor it could not parse anchored *nothing* — silently. A half-written
// manifest, an interrupted multipart upload, or a future schema change turns every
// WAL object that manifest protected into a mark candidate, and the snapshot's live
// data is delete-markered because its index was unreadable. The correct behaviour
// for an unreadable anchor is to abort the sweep, never to widen it.
//
// The same applies to an anchor that parses but does not describe itself: a manifest
// whose RootDigest does not match its own contents is not a manifest we may act on
// (INV-16 — the digest is exactly what says "these objects, this sequence").

const (
	anchorVol = "av"
	// anchoredWAL is only reachable through the anchor: "av" is not a UUID, so the
	// durable-prefix root skips it (§22.5 layout parsing) and nothing else points at it.
	anchoredWAL = "wal/av/1/1-1-aaaa.wal"
	otherWAL    = "wal/av/1/2-2-bbbb.wal"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func goodManifest(snapID string, objects ...string) snapshot.Manifest {
	m := snapshot.Manifest{
		SnapshotID: snapID, VolumeID: anchorVol, Epoch: 1,
		TargetSequence: 1, Objects: objects,
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	return m
}

func goodCheckpoint(objects ...string) checkpoint.Checkpoint {
	cp := checkpoint.Checkpoint{
		VolumeID: anchorVol, Epoch: 1, DurableSequence: 1, Objects: objects,
	}
	cp.RootDigest = checkpoint.Digest(cp.DurableSequence, cp.Objects)
	return cp
}

// TestReachableRefusesAnUnreadableAnchor: every way an anchor can be unreadable must
// stop the sweep, and the objects it protected must survive.
func TestReachableRefusesAnUnreadableAnchor(t *testing.T) {
	ctx := t.Context()
	manifestKey := snapshot.ManifestKey(anchorVol, "s1")
	cpKey := checkpoint.Key(anchorVol, 1, 1)

	manifestBody := mustJSON(t, goodManifest("s1", anchoredWAL))
	cpBody := mustJSON(t, goodCheckpoint(anchoredWAL))

	tamperedManifest := goodManifest("s1", anchoredWAL)
	tamperedManifest.Objects = nil // digest still claims the object; contents no longer do

	tamperedCP := goodCheckpoint(anchoredWAL)
	tamperedCP.DurableSequence = 99 // digest no longer describes the sequence

	tests := []struct {
		name string
		key  string
		body []byte
	}{
		{"manifest truncated mid-write", manifestKey, manifestBody[:len(manifestBody)/2]},
		{"manifest is zero bytes", manifestKey, nil},
		{"manifest is not JSON at all", manifestKey, []byte("\x00\x01\x02 not json")},
		{"manifest is JSON but not an object", manifestKey, []byte(`["objects"]`)},
		{"manifest has no objects list", manifestKey, []byte(`{"snapshot_id":"s1","volume_id":"av","target_sequence":1}`)},
		{"manifest root digest does not match", manifestKey, mustJSON(t, tamperedManifest)},
		{"checkpoint truncated mid-write", cpKey, cpBody[:len(cpBody)/2]},
		{"checkpoint is not JSON at all", cpKey, []byte("nope")},
		{"checkpoint has no objects list", cpKey, []byte(`{"volume_id":"av","epoch":1,"durable_sequence":1}`)},
		{"checkpoint root digest does not match", cpKey, mustJSON(t, tamperedCP)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			store := newStore(clk)
			seed(t, store, anchoredWAL)
			if _, err := store.Put(ctx, tc.key, tc.body, objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			clk.Advance(48 * time.Hour) // everything is past any grace period

			if _, err := gc.Reachable(ctx, store); !errors.Is(err, gc.ErrUnreadableAnchor) {
				t.Fatalf("Reachable over an unreadable anchor: err = %v, want ErrUnreadableAnchor", err)
			}
			// The sweep itself must refuse too — a caller that computed reachability
			// earlier (or passed an empty set) must not be allowed to mark.
			if _, err := gc.Mark(ctx, store, clk, map[string]bool{}, time.Hour); !errors.Is(err, gc.ErrUnreadableAnchor) {
				t.Fatalf("Mark over an unreadable anchor: err = %v, want ErrUnreadableAnchor", err)
			}
			if _, err := store.Get(ctx, anchoredWAL); err != nil {
				t.Fatalf("the object the unreadable anchor protected was marked: %v", err)
			}
		})
	}
}

// TestReachableAcceptsAWellFormedAnchor is the control: the digest check must not be
// so eager that it refuses a real anchor, including a snapshot that legitimately
// references nothing.
func TestReachableAcceptsAWellFormedAnchor(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name string
		key  string
		body func(t *testing.T) []byte
		want []string // keys that must be reachable
	}{
		{
			name: "manifest anchoring one object",
			key:  snapshot.ManifestKey(anchorVol, "s1"),
			body: func(t *testing.T) []byte { return mustJSON(t, goodManifest("s1", anchoredWAL)) },
			want: []string{anchoredWAL},
		},
		{
			name: "manifest anchoring nothing",
			key:  snapshot.ManifestKey(anchorVol, "s1"),
			body: func(t *testing.T) []byte { return mustJSON(t, goodManifest("s1")) },
		},
		{
			name: "checkpoint anchoring one object",
			key:  checkpoint.Key(anchorVol, 1, 1),
			body: func(t *testing.T) []byte { return mustJSON(t, goodCheckpoint(anchoredWAL)) },
			want: []string{anchoredWAL},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			store := newStore(clk)
			seed(t, store, anchoredWAL)
			if _, err := store.Put(ctx, tc.key, tc.body(t), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			reachable, err := gc.Reachable(ctx, store)
			if err != nil {
				t.Fatalf("a well-formed anchor must be accepted: %v", err)
			}
			for _, k := range tc.want {
				if !reachable[k] {
					t.Fatalf("%q should be reachable through the anchor; reachable=%v", k, reachable)
				}
			}
			if !reachable[tc.key] {
				t.Fatalf("the anchor itself must be reachable; reachable=%v", reachable)
			}
		})
	}
}

// TestAnchorThatDisappearsBetweenListAndGetAbortsTheSweep: a lifecycle rule or an
// operator can retire an anchor in the microseconds between the sweep's LIST and its
// GET. Reading that as "anchors nothing" would mark the objects it protected.
func TestAnchorThatDisappearsBetweenListAndGetAbortsTheSweep(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	inner := newStore(clk)
	seed(t, inner, anchoredWAL)
	manifestKey := snapshot.ManifestKey(anchorVol, "s1")
	if _, err := inner.Put(ctx, manifestKey, mustJSON(t, goodManifest("s1", anchoredWAL)), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour)

	store := hooked(inner)
	store.onGet = func(key string) error {
		if key == manifestKey {
			store.onGet = nil
			return inner.Delete(ctx, manifestKey) // retired underneath us
		}
		return nil
	}

	if _, err := gc.Reachable(ctx, store); err == nil {
		t.Fatal("an anchor that vanished mid-sweep must abort the sweep, not widen it")
	}
	if _, err := inner.Get(ctx, anchoredWAL); err != nil {
		t.Fatalf("the vanished anchor's objects were marked: %v", err)
	}
}

// Finding 9 (medium). Reachability follows an anchor exactly one level: a manifest
// enumerates WAL objects and that is the end of it. A clone chains to its parent
// through ParentSnapshotID, and the parent manifest is what enumerates the parent's
// objects — so retiring the parent (a cost-control lifecycle rule on old snapshots,
// an operator expiring a manifest) silently turns the parent's objects into
// orphans, and the *child* snapshot is the thing that gets destroyed. It is
// invisible until a recovery reads a short prefix.

// TestChildManifestKeepsItsParentsObjectsReachable: with the whole lineage present,
// both generations survive a sweep.
func TestChildManifestKeepsItsParentsObjectsReachable(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	seed(t, store, anchoredWAL, otherWAL)

	parent := goodManifest("parent", anchoredWAL)
	child := goodManifest("child", otherWAL)
	child.ParentSnapshotID = "parent"
	for key, m := range map[string]snapshot.Manifest{
		snapshot.ManifestKey(anchorVol, "parent"): parent,
		snapshot.ManifestKey(anchorVol, "child"):  child,
	} {
		if _, err := store.Put(ctx, key, mustJSON(t, m), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(48 * time.Hour)

	marked, err := sweep(ctx, store, clk, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(marked) != 0 {
		t.Fatalf("a complete snapshot lineage must survive a sweep; marked %v", marked)
	}
}

// TestRetiringAParentManifestDoesNotWidenTheMarkSet: the parent is gone, so the
// child's lineage is dangling and the sweep can no longer know what the parent
// anchored. Abort — do not mark the parent's objects.
func TestRetiringAParentManifestDoesNotWidenTheMarkSet(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newStore(clk)
	seed(t, store, anchoredWAL, otherWAL)

	child := goodManifest("child", otherWAL)
	child.ParentSnapshotID = "parent" // whose manifest a lifecycle rule already expired
	if _, err := store.Put(ctx, snapshot.ManifestKey(anchorVol, "child"), mustJSON(t, child), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(48 * time.Hour)

	marked, err := sweep(ctx, store, clk, time.Hour)
	if err == nil {
		t.Fatalf("a dangling snapshot lineage must abort the sweep; it marked %v instead", marked)
	}
	if slices.Contains(marked, anchoredWAL) {
		t.Fatalf("the retired parent's object was marked: %v", marked)
	}
	if _, err := store.Get(ctx, anchoredWAL); err != nil {
		t.Fatalf("the retired parent's object was marked: %v", err)
	}
}

// sweep is Reachable followed by Mark, the way an operator or a cron runs the GC.
func sweep(ctx context.Context, store objectstore.Store, clk *sim.Clock, grace time.Duration) ([]string, error) {
	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		return nil, err
	}
	return gc.Mark(ctx, store, clk, reachable, grace)
}
