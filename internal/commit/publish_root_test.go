package commit_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestPublishARootThatReplacesTheHistoryUnderIt is v6 §19's compaction arriving at the
// object store: the layer is a whole image, so the commit carries no parent and a recovery
// stops there instead of walking and downloading the chain it flattens.
//
// The rest of the protocol is unchanged, and the assertions at the end are what says so:
// HEAD moves by the same compare-and-set, and everything under it is still in the bucket
// for the snapshots that name it.
func TestPublishARootThatReplacesTheHistoryUnderIt(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	first, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("the first commit: %v", err)
	}
	second, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("the second commit: %v", err)
	}

	req := request(volumeID)
	req.ReplacesCommitID = second.CommitID
	root, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), req)
	if err != nil {
		t.Fatalf("publishing the compacted root: %v", err)
	}
	if root.ParentCommitID != "" {
		t.Errorf("the root claims parent %q, so a recovery walks the history it was supposed to replace", root.ParentCommitID)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != root.CommitID {
		t.Errorf("HEAD is %q, want the root %q", h.CommitID, root.CommitID)
	}
	for _, id := range []string{first.CommitID, second.CommitID} {
		if _, err := commit.ReadManifest(t.Context(), store, volumeID, id); err != nil {
			t.Errorf("the manifest of %s is gone from the bucket: %v", id, err)
		}
	}
}

// TestARootIsRefusedWhenHeadIsNotTheCommitItFlattens. The layer holds one state of the
// volume and says so by carrying no parent, so landing it on a HEAD that has moved past
// that commit takes every commit in between out of the history — each of which returned
// SUCCESS. It is not a fence: nobody else took the volume, and stopping a guest for this
// host's own bookkeeping is a worse answer than collapsing again.
func TestARootIsRefusedWhenHeadIsNotTheCommitItFlattens(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	flattened, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("the commit the root would flatten: %v", err)
	}
	later, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("the commit that lands while the collapse is under way: %v", err)
	}

	req := request(volumeID)
	req.ReplacesCommitID = flattened.CommitID
	_, err = commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), req)
	if !errors.Is(err, commit.ErrRootSuperseded) {
		t.Fatalf("a root that flattens %s was published over a HEAD at %s: %v", flattened.CommitID, later.CommitID, err)
	}
	if errors.Is(err, commit.ErrHeadMoved) {
		t.Error("a superseded root reads as a lost CAS, which fences the volume and stops the guest")
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != later.CommitID {
		t.Errorf("HEAD is %q and the newest commit is %q", h.CommitID, later.CommitID)
	}
}

// TestARootIsRefusedOnAVolumeWithNoHead. A root replaces a commit, so a volume that has
// none is not the history these bytes came from — whatever this host is holding, it is not
// in this bucket.
func TestARootIsRefusedOnAVolumeWithNoHead(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	req := request(volumeID)
	req.ReplacesCommitID = newID()
	if _, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), req); !errors.Is(err, commit.ErrRootSuperseded) {
		t.Fatalf("a root was published as the first commit of a volume: %v", err)
	}
	if _, _, err := commit.ReadHead(t.Context(), store, volumeID); !errors.Is(err, commit.ErrNoHead) {
		t.Errorf("the volume has a HEAD after a refused root: %v", err)
	}
}

// TestPublishARootIsStillFencedByTheEpoch. A root carries no parent, and the epoch of the
// commit it moves HEAD off is still the fence it has to clear: without that check a host
// that had already been fenced could republish the whole volume as a root of its own, which
// is the one thing the epoch exists to stop.
func TestPublishARootIsStillFencedByTheEpoch(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	current := request(volumeID)
	current.Epoch = 9
	if _, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), current); err != nil {
		t.Fatalf("the commit the fenced host is about to publish over: %v", err)
	}

	stale := request(volumeID)
	stale.Epoch, stale.ReplacesCommitID = 8, current.CommitID
	_, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), stale)
	if !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("a host at epoch 8 published a root over a history written at epoch 9: %v", err)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != current.CommitID {
		t.Errorf("HEAD is %q, and the volume's writer published %q", h.CommitID, current.CommitID)
	}
}
