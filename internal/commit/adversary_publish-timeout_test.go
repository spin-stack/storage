package commit_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestAdversaryATimeoutMidPublishLosesNoCommitAndPublishesNoSecondOne is the object
// store timing out in the middle of a publish, at each of the three writes v6 §9
// orders — and it is the half of that boundary the existing crash tests cannot reach.
// TestAPublishedCommitIsAlwaysReconstructible cuts the publish with a store that
// *refuses* the write, so the object is not there afterwards; a timeout is the other
// shape and the dangerous one: the request landed and only the answer was lost, so the
// retry meets its own writes and has to recognise them as its own. sim.ErrLostResponse
// is exactly that PUT, and §15's row "CAS exitoso, respuesta perdida" is the case this
// protocol is most easily got wrong at — a retry that reads HEAD, finds it naming a
// commit it did not know it had made, and reports a lost race.
//
// The five questions v6 §22 asks of a boundary, answered by the assertions below:
//
//   - Which commit is visible? Before the retry, whatever HEAD named when the timeout
//     hit — the parent, except for the lost CAS, where it is already this commit. After
//     the retry, this commit, in every case.
//   - What local state is left? Nothing here: the sealed layer stays on the host and the
//     Agent retries from the same SealedLayer (qcow's pending record covers that).
//   - What remote objects are left? The objects the timed-out writes landed. They are
//     the ones the retry would have written, byte for byte — the layer's key is the
//     digest of its content and the manifest is immutable — so the bucket gains no
//     orphan and no second commit for this layer.
//   - Does it recover by itself? Yes: the next cycle's retry of the same Request lands
//     the commit, with no operator and no fresh identifier.
//   - Could a confirmed commit be lost? No. The parent commit stays readable and
//     fetchable through every timeout, and once HEAD names this commit a retry never
//     moves it back.
func TestAdversaryATimeoutMidPublishLosesNoCommitAndPublishesNoSecondOne(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	d := dek(t, volumeID)
	parentPlain := layerBytes(t, 4096*2)
	plain := layerBytes(t, 4096*3+11)
	req := request(volumeID, len(plain))

	// The layer's key is the digest of the sealed bytes, which nothing outside can
	// predict, so it is learned by publishing this very Request into a store that is
	// then thrown away. The sealing is deterministic — the nonce is derived from the
	// volume, the layer id and the frame index — so the key learned here is the key the
	// run below will write to. Nothing rests on that being true: an injection that did
	// not fire cannot produce the ErrLostResponse the first attempt is asserted to
	// return.
	twin, err := commit.Publish(t.Context(), sim.NewObjectStore(), d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("learning the layer's key: %v", err)
	}

	steps := []struct {
		name string
		key  string
	}{
		{"the layer", twin.Layer.ObjectKey},
		{"the manifest", commit.ManifestKey(volumeID, req.CommitID)},
		{"the CAS on HEAD", commit.HeadKey(volumeID)},
	}
	for _, step := range steps {
		t.Run("the answer to "+step.name+" is lost", func(t *testing.T) {
			t.Parallel()
			store := sim.NewObjectStore()
			// A commit that completed, so the timeout below cuts into a volume with a
			// history — the case where a mistake costs somebody's data rather than an
			// empty bucket.
			parentReq := request(volumeID, len(parentPlain))
			parent, err := commit.Publish(t.Context(), store, d, bytes.NewReader(parentPlain), parentReq)
			if err != nil {
				t.Fatalf("the commit before the timeout: %v", err)
			}

			store.InjectLostResponse(step.key)
			if _, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req); !errors.Is(err, sim.ErrLostResponse) {
				t.Fatalf("the attempt that timed out returned %v, want ErrLostResponse — the fault did not land where this case is about", err)
			}

			// Whatever the timeout left behind, the commit that was already confirmed is
			// still there and still readable. This is the invariant above every other
			// one in §22.
			if err := fetchable(t, store, d, volumeID, parent.CommitID); err != nil {
				t.Fatalf("the commit that had returned SUCCESS is gone after a timeout on %s: %v", step.name, err)
			}

			// The retry: the same Request, which is what makes it a retry and not a
			// second commit.
			again, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
			if err != nil {
				t.Fatalf("the retry after the lost answer to %s was refused: %v", step.name, err)
			}
			switch {
			case again.CommitID != req.CommitID:
				t.Errorf("the retry published commit %s, want %s", again.CommitID, req.CommitID)
			case again.Layer.ObjectKey != twin.Layer.ObjectKey:
				t.Errorf("the retry uploaded the layer at %s, want %s", again.Layer.ObjectKey, twin.Layer.ObjectKey)
			}

			// What the outside sees: HEAD names this commit, the whole chain behind it
			// can be rebuilt, and the bytes come back.
			head, _, err := commit.ReadHead(t.Context(), store, volumeID)
			if err != nil {
				t.Fatalf("reading HEAD after the retry: %v", err)
			}
			if head.CommitID != req.CommitID {
				t.Fatalf("HEAD names %s after the retry, want %s", head.CommitID, req.CommitID)
			}
			m, err := commit.ReadManifest(t.Context(), store, volumeID, head.CommitID)
			if err != nil {
				t.Fatalf("HEAD names a manifest that is not readable: %v", err)
			}
			if m.ParentCommitID != parent.CommitID {
				t.Errorf("the commit's parent is %q, want the commit that was already confirmed, %s", m.ParentCommitID, parent.CommitID)
			}
			var out bytes.Buffer
			if err := commit.Fetch(t.Context(), store, d, m, &out); err != nil {
				t.Fatalf("the layer HEAD names cannot be fetched: %v", err)
			}
			if !bytes.Equal(out.Bytes(), plain) {
				t.Errorf("the layer came back as %d bytes, want the %d that went in", out.Len(), len(plain))
			}

			// And the timeout cost the volume no extra entry in its history: two
			// commits went in, two manifests are in the bucket. A retry that minted or
			// wrote anything of its own would show up here as a third.
			objs, err := store.List(t.Context(), "volumes/"+volumeID+"/commits/")
			if err != nil {
				t.Fatalf("listing the volume's commits: %v", err)
			}
			if len(objs) != 2 {
				t.Errorf("the bucket holds %d manifests for two commits, one of which timed out", len(objs))
			}
		})
	}
}

// fetchable is the whole of what "this commit is still there" means: its manifest reads
// and its layer comes back and opens.
func fetchable(t *testing.T, store *sim.ObjectStore, d *crypto.Encryption, volumeID, commitID string) error {
	t.Helper()
	m, err := commit.ReadManifest(t.Context(), store, volumeID, commitID)
	if err != nil {
		return err
	}
	var discard bytes.Buffer
	return commit.Fetch(t.Context(), store, d, m, &discard)
}
