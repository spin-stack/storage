package commit_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// killedReader is the host dying with the sealed layer half-read: n bytes come out and
// then the file stops answering. It is how a kill *inside* the seal-and-hash step is
// reached at all — that step touches no object store, so no store fault can aim at it.
//
// It rewinds, because the publish protocol rewinds: the file answers the same n bytes and
// then stops on every pass, which is what a file that has gone away actually does.
type killedReader struct {
	body []byte
	left int
	off  int
}

var errKilled = errors.New("the host stopped mid-read")

func (r *killedReader) Read(p []byte) (int, error) {
	if r.off >= r.left || r.off >= len(r.body) {
		return 0, errKilled
	}
	n := min(len(p), r.left-r.off, len(r.body)-r.off)
	n = copy(p[:n], r.body[r.off:])
	r.off += n
	return n, nil
}

func (r *killedReader) Seek(int64, int) (int64, error) {
	r.off = 0
	return 0, nil
}

// killWorld is a volume that already holds one commit, so every kill below cuts into a
// history rather than into an empty bucket — the only setting in which "one history, not
// two" can be observed at all.
type killWorld struct {
	store  *sim.ObjectStore
	key    *crypto.Encryption
	vol    string
	first  commit.Manifest
	plain  []byte
	req    commit.Request
	layerK string
}

func newKillWorld(t *testing.T) killWorld {
	t.Helper()
	vol := newID()
	store, d := sim.NewObjectStore(), dek(t, vol)
	first, err := commit.Publish(t.Context(), store, d, bytes.NewReader(layerBytes(t, 4096)), request(vol))
	if err != nil {
		t.Fatalf("the commit this volume already had: %v", err)
	}
	plain := layerBytes(t, 4096*3)
	req := request(vol)

	// Where the second commit's layer will land. Sealing is deterministic in the volume,
	// the layer id and the frame size, so a rehearsal on a throwaway store names the key
	// a fault has to be aimed at, without reaching into the code under test.
	rehearsal, err := commit.Publish(t.Context(), sim.NewObjectStore(), d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("working out the layer's key: %v", err)
	}
	return killWorld{store: store, key: d, vol: vol, first: first, plain: plain, req: req,
		layerK: rehearsal.Layer.ObjectKey}
}

// count is the bucket's own answer for how many objects sit under a prefix, not a number
// this code kept.
func (w killWorld) count(t *testing.T, prefix string) int {
	t.Helper()
	objs, err := w.store.List(t.Context(), prefix)
	if err != nil {
		t.Fatalf("listing %s: %v", prefix, err)
	}
	return len(objs)
}

// headIs asserts what the volume currently *is* to anyone who opens the bucket.
func (w killWorld) headIs(t *testing.T, want string) {
	t.Helper()
	head, _, err := commit.ReadHead(t.Context(), w.store, w.vol)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	if head.CommitID != want {
		t.Fatalf("HEAD names %s, want %s", head.CommitID, want)
	}
}

// noManifestYet asserts the interrupted commit left nothing a reader could follow.
func (w killWorld) noManifestYet(t *testing.T) {
	t.Helper()
	if _, err := commit.ReadManifest(t.Context(), w.store, w.vol, w.req.CommitID); err == nil {
		t.Fatalf("commit %s has a manifest although it never got past its layer", w.req.CommitID)
	}
}

// oneHistory walks back from HEAD: every link must be fetchable, the walk must end at the
// commit the volume started with, and the bucket must hold no manifest the walk did not
// reach. A retry that published a *second* commit, or left a manifest nothing points at,
// fails here — which is the question every kill in this file asks.
func (w killWorld) oneHistory(t *testing.T, tip string) {
	t.Helper()
	walked, oldest := 0, ""
	for id := tip; id != ""; walked++ {
		m, err := commit.ReadManifest(t.Context(), w.store, w.vol, id)
		if err != nil {
			t.Fatalf("the chain reaches %s, which is not readable: %v", id, err)
		}
		var out bytes.Buffer
		if err := commit.Fetch(t.Context(), w.store, w.key, m, &out); err != nil {
			t.Fatalf("commit %s cannot be rebuilt from the bucket: %v", id, err)
		}
		oldest, id = id, m.ParentCommitID
	}
	if oldest != w.first.CommitID {
		t.Errorf("the chain from HEAD bottoms out at %s, want the volume's first commit %s", oldest, w.first.CommitID)
	}
	if walked != 2 {
		t.Errorf("the chain from HEAD is %d commits long, want 2", walked)
	}
	if got := w.count(t, "volumes/"+w.vol+"/commits/"); got != walked {
		t.Errorf("the bucket holds %d manifests and the chain from HEAD reaches %d: this volume has two histories", got, walked)
	}
}

// republish is the recovery §29 allows and the only one: run the same commit again,
// under the same commit id, with the same bytes.
func (w killWorld) republish(t *testing.T) commit.Manifest {
	t.Helper()
	m, err := commit.Publish(t.Context(), w.store, w.key, bytes.NewReader(w.plain), w.req)
	if err != nil {
		t.Fatalf("republishing commit %s after the kill: %v", w.req.CommitID, err)
	}
	return m
}

// TestAdversaryKilledWhileHashingTheSealedLayer — v6 §29, kill-point "during the SHA-256
// of a sealed layer". The host reads the sealed file, seals and hashes it, and dies with
// the digest half-computed; nothing has been sent anywhere yet.
//
//   - Which commit is visible: the one before. HEAD still names it and a reader walking
//     the bucket sees no trace of the attempt.
//   - What local state is left: the sealed layer on disk, untouched — the file is only
//     read here. Nothing in this package writes locally.
//   - What remote objects are left: none. The kill is before the first PUT, so not even
//     the orphan §26 permits is created.
//   - Does it recover by itself: yes, by republishing under the same commit id.
//   - Could a confirmed commit be lost: no. Nothing was confirmed — Commit() never
//     returned SUCCESS — and the commit that *was* confirmed is still HEAD, before and
//     after.
func TestAdversaryKilledWhileHashingTheSealedLayer(t *testing.T) {
	t.Parallel()
	w := newKillWorld(t)
	layersBefore := w.count(t, "layers/sha256/")

	// The kill: the sealed layer stops being readable one frame in.
	_, err := commit.Publish(t.Context(), w.store, w.key,
		&killedReader{body: w.plain, left: 4096 + 17}, w.req)
	if !errors.Is(err, errKilled) {
		t.Fatalf("want the read to be reported, got %v", err)
	}

	w.headIs(t, w.first.CommitID)
	w.noManifestYet(t)
	if got := w.count(t, "layers/sha256/"); got != layersBefore {
		t.Errorf("%d layer objects after a kill before the first PUT, want %d: half a layer reached the bucket",
			got, layersBefore)
	}

	m := w.republish(t)
	w.headIs(t, m.CommitID)
	if m.CommitID != w.req.CommitID {
		t.Errorf("the retry published %s, want the same commit id %s", m.CommitID, w.req.CommitID)
	}
	if got := w.count(t, "layers/sha256/"); got != layersBefore+1 {
		t.Errorf("%d layer objects after the retry, want %d", got, layersBefore+1)
	}
	w.oneHistory(t, m.CommitID)
}

// TestAdversaryKilledWhileTheLayerUploads — v6 §29, kill-point "during the layer upload".
// The PUT lands in the bucket and its answer never comes back, which is the shape that
// matters: the host cannot tell it from an upload that did not happen, and must be right
// either way.
//
//   - Which commit is visible: the one before. A layer with no manifest is not a commit.
//   - What local state is left: the sealed layer on disk, still needed for the retry.
//   - What remote objects are left: the layer object, orphaned. §26 allows it — GC sweeps
//     what no manifest names — and content addressing is why the retry adopts it instead
//     of writing a second copy.
//   - Does it recover by itself: yes. Republishing the same commit finds the key taken,
//     reads it back, hashes it, recognises its own bytes and carries on.
//   - Could a confirmed commit be lost: no. HEAD is untouched here, and after the retry
//     the volume is one chain with the old commit still on it.
func TestAdversaryKilledWhileTheLayerUploads(t *testing.T) {
	t.Parallel()
	w := newKillWorld(t)
	layersBefore := w.count(t, "layers/sha256/")

	// The upload persists; the acknowledgement is lost.
	w.store.InjectLostResponse(w.layerK)
	if _, err := commit.Publish(t.Context(), w.store, w.key, bytes.NewReader(w.plain), w.req); err == nil {
		t.Fatal("a commit reported success although the layer upload never came back")
	}

	w.headIs(t, w.first.CommitID)
	w.noManifestYet(t)
	if got := w.count(t, "layers/sha256/"); got != layersBefore+1 {
		t.Fatalf("%d layer objects after the lost response, want %d: the fault did not land on the layer PUT",
			got, layersBefore+1)
	}

	m := w.republish(t)
	if m.Layer.ObjectKey != w.layerK {
		t.Errorf("the retry uploaded to %s, want the orphan's own key %s", m.Layer.ObjectKey, w.layerK)
	}
	if got := w.count(t, "layers/sha256/"); got != layersBefore+1 {
		t.Errorf("%d layer objects after the retry, want %d: the same layer was stored twice", got, layersBefore+1)
	}
	w.headIs(t, w.req.CommitID)
	w.oneHistory(t, w.req.CommitID)
}

// TestAdversaryKilledAfterTheLayerAndBeforeTheManifest — v6 §29, kill-point "after the
// layer is uploaded and before the manifest is written". This is the window the ordering
// `PUT layer → PUT manifest → CAS HEAD` exists to make harmless, and the one where a
// retry is most tempted to mint a fresh commit id and publish the same bytes twice.
//
//   - Which commit is visible: the one before. Nothing names the uploaded layer, so
//     nothing about the volume has changed for a reader.
//   - What local state is left: the sealed layer on disk.
//   - What remote objects are left: the layer object alone — the legal orphan of §26.
//   - Does it recover by itself: yes, by republishing under the same commit id; the
//     manifest is then written for the first time and HEAD moves once.
//   - Could a confirmed commit be lost: no. HEAD never moved, and the retry's CAS is
//     against the ETag of the commit that is still there, so the predecessor stays on
//     the chain.
func TestAdversaryKilledAfterTheLayerAndBeforeTheManifest(t *testing.T) {
	t.Parallel()
	w := newKillWorld(t)
	layersBefore := w.count(t, "layers/sha256/")

	// The layer goes up; the manifest write is the operation that never happens.
	w.store.InjectThrottleKey(commit.ManifestKey(w.vol, w.req.CommitID), 1)
	if _, err := commit.Publish(t.Context(), w.store, w.key, bytes.NewReader(w.plain), w.req); err == nil {
		t.Fatal("a commit reported success although its manifest was never written")
	}

	w.headIs(t, w.first.CommitID)
	w.noManifestYet(t)
	if got := w.count(t, "layers/sha256/"); got != layersBefore+1 {
		t.Fatalf("%d layer objects, want %d: the kill did not land between the layer and the manifest",
			got, layersBefore+1)
	}

	m := w.republish(t)
	if m.ParentCommitID != w.first.CommitID {
		t.Errorf("the retry chained onto %q, want the commit that was there all along %s",
			m.ParentCommitID, w.first.CommitID)
	}
	if got := w.count(t, "layers/sha256/"); got != layersBefore+1 {
		t.Errorf("%d layer objects after the retry, want %d", got, layersBefore+1)
	}
	w.headIs(t, w.req.CommitID)
	w.oneHistory(t, w.req.CommitID)
}
