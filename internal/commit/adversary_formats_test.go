package commit_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestAdversarySameSizeObjectAtALayerKey is the object-under-the-wrong-key attack aimed
// at the one check that stands between "this layer was already uploaded" and "a manifest
// of ours names something somebody else put in the bucket": putLayer compares the length
// of the object that is already there and nothing else.
//
// The layer key is content-addressed, so an object of the same length that is not this
// layer cannot come from this code — it comes from anything else with write access to a
// shared bucket, or from a restore that put an old object back under a key. Publish then
// reports SUCCESS for a commit whose layer bytes are not in the bucket at all, which is
// exactly what `Commit() → SUCCESS` promises cannot happen.
func TestAdversarySameSizeObjectAtALayerKey(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096*2)
	req := request(volumeID, len(plain))

	// Sealing is deterministic — that is what makes a retry the same object — so a
	// rehearsal against a scratch bucket says where this layer will land and how long it
	// will be, before the real publish.
	rehearsal, err := commit.Publish(t.Context(), sim.NewObjectStore(), d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("working out the key: %v", err)
	}
	impostor := bytes.Repeat([]byte{0xA5}, int(rehearsal.Layer.SizeBytes))
	if _, err := store.Put(t.Context(), rehearsal.Layer.ObjectKey, impostor, objectstore.PutOptions{}); err != nil {
		t.Fatalf("planting an object of the same length: %v", err)
	}

	m, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if err != nil {
		t.Skipf("publish refused it, which is the right answer: %v", err)
	}

	// It said SUCCESS. The promise is that this commit is reconstructible without the
	// host that wrote it, so read it back the way a recovery on another host would.
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, d, m, &out); err != nil {
		t.Fatalf("commit %s was published successfully and cannot be reconstructed: %v", m.CommitID, err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatalf("commit %s reconstructs %d bytes that are not the layer's", m.CommitID, out.Len())
	}
}

// TestAdversaryPublishingACommitWithNoID is the other end of the same hole: nothing on
// the way out refuses an empty commit id either.
//
// It matters because the id is not always minted a line above the call. A restart
// republishes the id recorded in `state.json`'s `pending` record, and a `pending` record
// whose `commit_id` is absent — a field an older or a newer writer did not set — decodes
// to the empty string with every framing check intact. What lands in the bucket is a
// manifest at `volumes/<vol>/commits/.json` and a HEAD naming no commit, which is the
// object recovery.Restore indexes past the end of.
func TestAdversaryPublishingACommitWithNoID(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096)
	req := request(volumeID, len(plain))
	req.CommitID = ""

	if _, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req); err == nil {
		h, _, readErr := commit.ReadHead(t.Context(), store, volumeID)
		t.Fatalf("a commit with no id was published: HEAD now names %q (%v)", h.CommitID, readErr)
	}
}
