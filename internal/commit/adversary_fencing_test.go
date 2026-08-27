package commit_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// advPublish is one host publishing one layer at one epoch. The epoch is the fencing
// token the writer held, which is the whole subject of this file.
func advPublish(t *testing.T, store objectstore.Store, enc *crypto.Encryption, volumeID string, epoch int64, plain []byte) (commit.Manifest, error) {
	t.Helper()
	return commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), commit.Request{
		VolumeID: volumeID, CommitID: newID(), LayerID: newID(),
		Epoch: epoch, VirtualSize: 1 << 30, PlainBytes: int64(len(plain)), FrameBytes: 4096,
	})
}

// TestAdversaryAFencedHostAppendsOntoItsSuccessorsHead is the split brain the epoch is
// supposed to stop, arranged so that the compare-and-set cannot see it.
//
// host-a writes the volume at epoch 5 and seals a layer. The fleet decides host-a is
// gone, raises the epoch, and hands the volume to host-b, which rebuilds the chain and
// publishes its own commit at epoch 6. host-a is not gone: it still has the sealed layer,
// the DEK it cached, and a route to the bucket. It publishes.
//
// The CAS does not stop it. host-a reads HEAD *after* host-b moved it, so the etag it
// compares against is the current one and the write lands: the published history now ends
// in a commit written by a host that was fenced two commits ago, whose layer is an overlay
// of a chain that ended at host-a's own commit and not at host-b's. A recovery that walks
// this history stacks host-a's clusters over host-b's and hands the result to a guest,
// and host-b — the volume's actual writer — is refused on its next publish and stops.
//
// The manifest records the epoch and nothing reads it. Publish has every fact it needs at
// the moment it decides: the epoch it was handed and the epoch of the commit it is about
// to name as parent.
func TestAdversaryAFencedHostAppendsOntoItsSuccessorsHead(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, enc := sim.NewObjectStore(), dek(t, volumeID)

	if _, err := advPublish(t, store, enc, volumeID, 5, layerBytes(t, 4096)); err != nil {
		t.Fatalf("host-a's commit at epoch 5: %v", err)
	}
	promoted, err := advPublish(t, store, enc, volumeID, 6, layerBytes(t, 4096))
	if err != nil {
		t.Fatalf("host-b's commit at epoch 6, after the volume was granted to it: %v", err)
	}

	// host-a, fenced and alive, with the layer it sealed before it lost the volume.
	zombie, err := advPublish(t, store, enc, volumeID, 5, layerBytes(t, 4096))
	if err == nil {
		t.Errorf("a host at epoch 5 published commit %s onto commit %s, which was written at epoch %d",
			zombie.CommitID, zombie.ParentCommitID, promoted.Epoch)
	}

	// The observable, independent of the error: what the bucket now says the volume is.
	// Walking back from HEAD, no commit may have been written at a lower epoch than the
	// one it is layered on — that is a writer the fleet had already moved past.
	head, _, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	for id := head.CommitID; id != ""; {
		m, err := commit.ReadManifest(t.Context(), store, volumeID, id)
		if err != nil {
			t.Fatalf("reading commit %s: %v", id, err)
		}
		if m.ParentCommitID == "" {
			break
		}
		parent, err := commit.ReadManifest(t.Context(), store, volumeID, m.ParentCommitID)
		if err != nil {
			t.Fatalf("reading commit %s: %v", m.ParentCommitID, err)
		}
		if m.Epoch < parent.Epoch {
			t.Fatalf("the published history has commit %s at epoch %d layered on commit %s at epoch %d: HEAD names a commit written by a fenced host",
				m.CommitID, m.Epoch, parent.CommitID, parent.Epoch)
		}
		id = m.ParentCommitID
	}
}

// advFailHead is a store whose CAS on HEAD is lost: the host that ran it does not learn
// whether it took effect. Here it did not — the write is dropped — which is v6 §15's
// interrupted commit, the case the commit id is reused across retries for.
type advFailHead struct {
	objectstore.Store
	volumeID string
	fail     bool
}

func (s *advFailHead) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if s.fail && key == commit.HeadKey(s.volumeID) {
		s.fail = false
		return objectstore.PutResult{}, errors.New("i/o timeout")
	}
	return s.Store.Put(ctx, key, data, opts)
}

// TestAdversaryALostRaceReportedAsSomethingOtherThanAFence is the same split brain seen
// from the losing side, and the point is which sentinel comes back.
//
// host-a uploads its layer and its manifest and is interrupted before the CAS. host-b
// takes the volume and publishes. host-a retries the commit it owes — the same commit id,
// which is what makes a retry a retry — and now builds a manifest whose parent is host-b's
// commit, which collides with the manifest it already wrote against the older parent.
//
// It comes back as ErrManifestConflict, and ErrManifestConflict is not what anything
// fences on: qcow.Manager.publish stops a volume on commit.ErrHeadMoved alone and treats
// every other publishing failure as an upload that will succeed later, keeping the guest's
// disk attached. So the one host in the fleet that has proof it lost the volume goes on
// serving it, retrying for ever, and the fleet reads it as healthy.
func TestAdversaryALostRaceReportedAsSomethingOtherThanAFence(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	inner, enc := sim.NewObjectStore(), dek(t, volumeID)
	store := &advFailHead{Store: inner, volumeID: volumeID}

	first, err := advPublish(t, store, enc, volumeID, 5, layerBytes(t, 4096))
	if err != nil {
		t.Fatalf("the commit both hosts share: %v", err)
	}

	// host-a's interrupted commit: layer and manifest are in the bucket, HEAD never moved.
	plain := layerBytes(t, 4096)
	req := commit.Request{
		VolumeID: volumeID, CommitID: newID(), LayerID: newID(),
		Epoch: 5, VirtualSize: 1 << 30, PlainBytes: int64(len(plain)), FrameBytes: 4096,
	}
	store.fail = true
	if _, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req); err == nil {
		t.Fatal("the interrupted commit reported success")
	}
	if head, _, err := commit.ReadHead(t.Context(), store, volumeID); err != nil || head.CommitID != first.CommitID {
		t.Fatalf("HEAD is %v (%v), want the shared commit %s", head, err, first.CommitID)
	}

	// host-b takes the volume at epoch 6 and publishes onto the shared commit.
	if _, err := advPublish(t, store, enc, volumeID, 6, layerBytes(t, 4096)); err != nil {
		t.Fatalf("host-b's commit: %v", err)
	}

	// host-a retries the layer it still owes, from the record on its own disk.
	_, err = commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req)
	if !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("host-a lost the volume and was told %v; only commit.ErrHeadMoved makes it stop serving the guest", err)
	}
}

// TestAdversaryTheEpochWalkAcceptsALegitimateSuccession is the control for the walk the
// first test asserts on: a volume that moves between hosts the way it is supposed to —
// each grant raising the epoch, each host publishing only while it holds one — produces a
// history whose epochs never go backwards. The walk is a property a correct fleet already
// satisfies, not a rule invented to fail.
func TestAdversaryTheEpochWalkAcceptsALegitimateSuccession(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, enc := sim.NewObjectStore(), dek(t, volumeID)

	for _, epoch := range []int64{5, 6, 6, 9} {
		if _, err := advPublish(t, store, enc, volumeID, epoch, layerBytes(t, 4096)); err != nil {
			t.Fatalf("publishing at epoch %d: %v", epoch, err)
		}
	}

	head, _, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	for id := head.CommitID; id != ""; {
		m, err := commit.ReadManifest(t.Context(), store, volumeID, id)
		if err != nil {
			t.Fatalf("reading commit %s: %v", id, err)
		}
		if m.ParentCommitID == "" {
			break
		}
		parent, err := commit.ReadManifest(t.Context(), store, volumeID, m.ParentCommitID)
		if err != nil {
			t.Fatalf("reading commit %s: %v", m.ParentCommitID, err)
		}
		if m.Epoch < parent.Epoch {
			t.Fatalf("a legitimate succession was read as a fenced write: commit %s at epoch %d on commit %s at epoch %d",
				m.CommitID, m.Epoch, parent.CommitID, parent.Epoch)
		}
		id = m.ParentCommitID
	}
}
