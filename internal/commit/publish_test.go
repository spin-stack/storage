package commit_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func dek(t *testing.T, volumeID string) *crypto.Encryption {
	t.Helper()
	d, err := crypto.GenerateDEK(rand.Reader, 1)
	if err != nil {
		t.Fatalf("generating a DEK: %v", err)
	}
	enc, err := crypto.NewEncryption(d, uuid.MustParse(volumeID))
	if err != nil {
		t.Fatalf("binding it to a volume: %v", err)
	}
	return enc
}

// layerBytes is a stand-in for a sealed qcow2: what it contains does not matter, only
// that it is long enough to span several frames.
func layerBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("drawing a layer: %v", err)
	}
	return b
}

func request(volumeID string) commit.Request {
	return commit.Request{
		VolumeID: volumeID, CommitID: newID(), LayerID: newID(),
		Epoch: 3, VirtualSize: 1 << 30, FrameBytes: 4096,
	}
}

// TestPublishRoundTripsALayer is the whole of what a commit promises, in one test: the
// bytes go up sealed, and they come back as the bytes that went in — through the
// manifest, without the host that wrote them.
func TestPublishRoundTripsALayer(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096*3+17)
	req := request(volumeID)

	m, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}
	// What is in the bucket at the layer's key is *not* the plaintext. A commit that
	// uploaded the guest's disk in the clear would pass every other assertion here.
	body, err := store.Get(t.Context(), m.Layer.ObjectKey)
	if err != nil {
		t.Fatalf("getting the layer: %v", err)
	}
	if bytes.Contains(body, plain[:64]) {
		t.Fatal("the layer went up in cleartext")
	}
	if m.Layer.ObjectKey != commit.LayerKey(commit.Digest(body)) {
		t.Errorf("the layer is not at the key its own digest names")
	}

	// And the way back, from nothing but the store and the key.
	head, _, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	got, err := commit.ReadManifest(t.Context(), store, volumeID, head.CommitID)
	if err != nil {
		t.Fatalf("reading the manifest HEAD names: %v", err)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, d, got, &out); err != nil {
		t.Fatalf("fetching: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer did not survive the round trip")
	}
}

// TestPublishChainsOntoTheCommitBefore: a second commit names the first as its parent,
// which is the whole of how a recovery finds every layer it must download (v6 §14).
func TestPublishChainsOntoTheCommitBefore(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	first, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if first.ParentCommitID != "" {
		t.Errorf("the first commit claims a parent: %q", first.ParentCommitID)
	}
	second, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID))
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if second.ParentCommitID != first.CommitID {
		t.Errorf("the second commit's parent is %q, want %q", second.ParentCommitID, first.CommitID)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != second.CommitID {
		t.Errorf("HEAD is %q, want the newest commit %q", h.CommitID, second.CommitID)
	}
}

// TestPublishIsIdempotent is the recovery from any interruption: do the whole commit
// again. Every step has to accept its own second attempt — the layer's key is its digest,
// the manifest is byte-identical, and HEAD already names this commit.
func TestPublishIsIdempotent(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096*2)
	req := request(volumeID)

	first, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	again, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("the same commit again was refused: %v", err)
	}
	if again.CommitID != first.CommitID || again.Layer.ObjectKey != first.Layer.ObjectKey {
		t.Errorf("the retry published something else: %+v", again)
	}
	// One commit, not two: the history a person reads must not gain an entry because a
	// network call was retried.
	objs, err := store.List(t.Context(), "volumes/"+volumeID+"/commits/")
	if err != nil {
		t.Fatalf("listing commits: %v", err)
	}
	if len(objs) != 1 {
		t.Errorf("the bucket holds %d manifests after one commit published twice", len(objs))
	}
}

// TestPublishStopsAtAHeadThatMoved. Everything before the CAS has happened — the layer is
// uploaded, the manifest is published — and none of it counts, because a commit exists
// only when HEAD names it. What must not happen is HEAD being taken anyway.
func TestPublishStopsAtAHeadThatMoved(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)

	if _, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID)); err != nil {
		t.Fatalf("the first commit: %v", err)
	}
	// A second writer that publishes between this commit's HEAD read and its CAS is not
	// reachable from here, so the same thing is arranged the other way round: HEAD is
	// moved by somebody else, and this commit is published against what it read.
	theirs := newID()
	_, etag, _ := commit.ReadHead(t.Context(), store, volumeID)
	if err := commit.CASHead(t.Context(), store, volumeID, theirs, etag); err != nil {
		t.Fatalf("their publish: %v", err)
	}
	// Re-reading HEAD inside Publish will now find theirs, so this commit chains onto it
	// and succeeds — which is correct. The refusal being tested is one attempt further
	// on: a CAS against an ETag that is no longer current.
	stale := etag
	if err := commit.CASHead(t.Context(), store, volumeID, newID(), stale); !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("want ErrHeadMoved, got %v", err)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != theirs {
		t.Errorf("HEAD is %q; the loser overwrote the winner", h.CommitID)
	}
}

// TestPublishRefusesALayerKeyHoldingSomethingElse: the key is the digest of the content,
// so this cannot arise from this code. It can arise from a bucket somebody else writes
// into, and a manifest that named it would point a recovery at an object no Agent wrote.
func TestPublishRefusesALayerKeyHoldingSomethingElse(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096)
	req := request(volumeID)

	// Work out where it will land, and put something else there first.
	var sealed bytes.Buffer
	m, err := commit.Publish(t.Context(), sim.NewObjectStore(), d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("working out the key: %v", err)
	}
	sealed.Reset()
	if _, err := store.Put(t.Context(), m.Layer.ObjectKey, []byte("not a layer"), objectstore.PutOptions{}); err != nil {
		t.Fatalf("planting an object: %v", err)
	}
	_, err = commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if !errors.Is(err, commit.ErrLayerKeyTaken) {
		t.Fatalf("want ErrLayerKeyTaken, got %v", err)
	}
	// And nothing after the upload happened: no manifest, no HEAD.
	if _, _, err := commit.ReadHead(t.Context(), store, volumeID); !errors.Is(err, commit.ErrNoHead) {
		t.Errorf("HEAD was written for a commit whose layer was refused")
	}
}

// TestFetchRefusesALayerThatCameBackWrong: the digest is checked before a byte is
// unsealed, so a recovery is told "this object came back wrong" and not "authentication
// failed", which are different problems with different next moves.
func TestFetchRefusesALayerThatCameBackWrong(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096)
	m, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), request(volumeID))
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}
	body, err := store.Get(t.Context(), m.Layer.ObjectKey)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	corrupt := bytes.Clone(body)
	corrupt[len(corrupt)/2] ^= 0x40
	if _, err := store.Put(t.Context(), m.Layer.ObjectKey, corrupt, objectstore.PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	var out bytes.Buffer
	err = commit.Fetch(t.Context(), store, d, m, &out)
	if !errors.Is(err, commit.ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("%d bytes of a layer that failed its digest reached the caller", out.Len())
	}
	if !strings.Contains(err.Error(), m.Layer.SHA256) {
		t.Errorf("the error does not say what was expected: %v", err)
	}
}

// TestFetchRefusesAnotherVolumesLayer: the object is intact and its digest is right, and
// it still must not open. Only the DEK and the AAD stand between a bucket-level mix-up
// and one tenant's disk being served to another.
func TestFetchRefusesAnotherVolumesLayer(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096)
	m, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), request(volumeID))
	if err != nil {
		t.Fatalf("publishing: %v", err)
	}
	m.VolumeID = newID()
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, d, m, &out); err == nil {
		t.Fatal("a layer opened under another volume's identity")
	}
}

// failAfter is a store that stops accepting writes after n of them. It is how the
// ordering below is observed at all: within one process that does not crash, publishing
// in the wrong order reaches the same end state as publishing in the right one, so a
// test that only ran Publish to completion could not tell them apart — and planting the
// reversal proved exactly that, by turning nothing red.
type failAfter struct {
	objectstore.Store
	left int
}

var errCrashed = errors.New("the host stopped here")

func (f *failAfter) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if f.left <= 0 {
		return objectstore.PutResult{}, errCrashed
	}
	f.left--
	return f.Store.Put(ctx, key, data, opts)
}

// The layer is the one write that takes the streaming path, so a fake that counts only
// Put stops counting the first of the three writes this test is about — and every kill
// point moves one step later, silently.
func (f *failAfter) PutStream(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if f.left <= 0 {
		return objectstore.PutResult{}, errCrashed
	}
	f.left--
	return f.Store.PutStream(ctx, key, body, size, opts)
}

// TestAPublishedCommitIsAlwaysReconstructible is the commit contract, checked at every
// point a commit can be cut in half: crash after every write and assert the invariant
// rather than the steps — whatever HEAD names must be fully there, and so must every
// ancestor. Any order but `PUT layer → PUT manifest → CAS HEAD` can leave HEAD naming a
// manifest that is not there, which is a commit that was acknowledged and cannot be
// rebuilt.
func TestAPublishedCommitIsAlwaysReconstructible(t *testing.T) {
	t.Parallel()
	for writes := 0; writes < 6; writes++ {
		t.Run(fmt.Sprintf("the host stops after %d writes", writes), func(t *testing.T) {
			t.Parallel()
			volumeID := newID()
			base, d := sim.NewObjectStore(), dek(t, volumeID)
			plain := layerBytes(t, 4096*2)

			// A first commit that completes, so the crash cases below are cutting into
			// a volume with a history rather than into an empty bucket.
			if _, err := commit.Publish(t.Context(), base, d, bytes.NewReader(plain), request(volumeID)); err != nil {
				t.Fatalf("the first commit: %v", err)
			}
			store := &failAfter{Store: base, left: writes}
			second := layerBytes(t, 4096*3)
			_, _ = commit.Publish(t.Context(), store, d, bytes.NewReader(second), request(volumeID))

			// Whatever survived, HEAD must name a commit that can be rebuilt.
			head, _, err := commit.ReadHead(t.Context(), base, volumeID)
			if err != nil {
				t.Fatalf("reading HEAD after the crash: %v", err)
			}
			m, err := commit.ReadManifest(t.Context(), base, volumeID, head.CommitID)
			if err != nil {
				t.Fatalf("HEAD names %s and its manifest is not readable: %v", head.CommitID, err)
			}
			var out bytes.Buffer
			if err := commit.Fetch(t.Context(), base, d, m, &out); err != nil {
				t.Fatalf("HEAD names a commit whose layer cannot be fetched: %v", err)
			}
			// And every ancestor, because a chain is only as reconstructible as its
			// weakest link and a recovery walks all of it (v6 §14).
			for id := m.ParentCommitID; id != ""; {
				ancestor, err := commit.ReadManifest(t.Context(), base, volumeID, id)
				if err != nil {
					t.Fatalf("the chain from HEAD reaches %s, which is not readable: %v", id, err)
				}
				var discard bytes.Buffer
				if err := commit.Fetch(t.Context(), base, d, ancestor, &discard); err != nil {
					t.Fatalf("ancestor %s cannot be fetched: %v", id, err)
				}
				id = ancestor.ParentCommitID
			}
		})
	}
}

// TestPublishRefusesAKeyForAnotherVolume: sealing with another volume's key produces a
// perfectly valid object discovered to be unopenable by a recovery — see boundLayerID.
func TestPublishRefusesAKeyForAnotherVolume(t *testing.T) {
	t.Parallel()
	mine, theirs := newID(), newID()
	store, wrongKey := sim.NewObjectStore(), dek(t, theirs)
	plain := layerBytes(t, 100)

	_, err := commit.Publish(t.Context(), store, wrongKey, bytes.NewReader(plain), request(mine))
	if err == nil {
		t.Fatal("a commit was published under a key belonging to another volume")
	}
	if !strings.Contains(err.Error(), theirs) {
		t.Errorf("the error does not say whose key it is: %v", err)
	}
	// Fetch is the same check on the way back, and it has to be: a manifest can be read
	// by a host holding some other volume's key material.
	m := commit.Manifest{VolumeID: mine, Layer: commit.Layer{LayerID: newID()}}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, wrongKey, m, &out); err == nil {
		t.Fatal("a layer was fetched under a key belonging to another volume")
	}
	if err := commit.Fetch(t.Context(), store, nil, m, &out); err == nil {
		t.Fatal("a layer was fetched with no key at all")
	}
}
