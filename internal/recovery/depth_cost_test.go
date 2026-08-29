package recovery_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// countingStore records the reads a rebuild makes, in order. Reads only: a restore writes
// nothing to the bucket, and a Put appearing here would be a bigger finding than a count.
type countingStore struct {
	objectstore.Store
	mu    sync.Mutex
	reads []string
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.record("GET " + key)
	return c.Store.Get(ctx, key)
}

func (c *countingStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	c.record("HEAD " + key)
	return c.Store.Head(ctx, key)
}

func (c *countingStore) record(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, s)
}

// taken returns what has been read since the last call and starts a fresh count.
func (c *countingStore) taken() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := slices.Clone(c.reads)
	c.reads = nil
	return out
}

// lineage is a bucket holding whole volumes and a host that can be thrown away. A
// re-placement onto a host that holds nothing is the case chain depth costs anything in:
// a host that already has its parent's layers copies them off its own disk.
type lineage struct {
	t     *testing.T
	store *countingStore
	kms   *crypto.DevKMS
	keys  *fakeKeys
	dek   crypto.DEK
	files *fakeFiles
	rec   *recovery.Recoverer
	// layerOf and plainOf are what each commit published, by commit id: the layer's id,
	// which names the file a restore must write, and the plaintext that must be in it.
	// Kept because the only assertion that cannot be satisfied by a plausible-looking
	// chain is the one on the bytes.
	layerOf map[string]string
	plainOf map[string][]byte
}

func newLineage(t *testing.T) *lineage {
	t.Helper()
	var kek [crypto.DEKSize]byte
	if _, err := rand.Read(kek[:]); err != nil {
		t.Fatalf("drawing a KEK: %v", err)
	}
	kms := crypto.NewDevKMS(kek, crypto.KEKID(kek))
	dek, err := crypto.GenerateDEK(rand.Reader, 3)
	if err != nil {
		t.Fatalf("generating a DEK: %v", err)
	}
	l := &lineage{
		t: t, store: &countingStore{Store: sim.NewObjectStore()}, kms: kms, dek: dek,
		keys:    &fakeKeys{keys: map[string]agent.VolumeKeys{}},
		layerOf: map[string]string{}, plainOf: map[string][]byte{},
	}
	l.newHost()
	return l
}

// newHost throws the disk away: the next restore holds nothing and must go to the bucket
// for every layer it needs.
func (l *lineage) newHost() {
	l.files = newFiles()
	l.rec = recovery.New(root, qemuImg, l.store, l.kms, l.keys, l.files, newRunner(l.files))
	l.store.taken()
}

// volume mints a root volume: an id, the lineage DEK wrapped under it — §10's shared key,
// which is why every volume here can open every other one's layers — and the descriptor
// controlplane.Provisioner.Provision writes.
func (l *lineage) volume() string { return l.cloneOf("") }

// cloneOf mints a clone of parentVolumeID, with the descriptor controlplane.Clone writes:
// the parent link is in the bucket, which is what makes "the ancestry is unreachable" a
// statement about this walk rather than about the objects.
func (l *lineage) cloneOf(parentVolumeID string) string {
	l.t.Helper()
	id := ids.New().String()
	wrapped, err := l.kms.WrapDEK(rand.Reader, l.dek, uuid.MustParse(id))
	if err != nil {
		l.t.Fatalf("wrapping the DEK for %s: %v", id, err)
	}
	l.keys.keys[id] = agent.VolumeKeys{
		VolumeID: id, DEKWrapped: wrapped, KEKID: l.kms.KEKID(), DEKKeyID: l.dek.KeyID,
	}
	if err := descriptor.Write(l.t.Context(), l.store, descriptor.Descriptor{
		VolumeID: id, SizeBytes: virtualSize, BlockSize: 4096, CurrentEpoch: 1,
		KEKID: l.kms.KEKID(), DEKWrapped: wrapped, DEKKeyID: l.dek.KeyID,
		ParentVolumeID: parentVolumeID,
	}); err != nil {
		l.t.Fatalf("writing the descriptor of %s: %v", id, err)
	}
	return id
}

// publish appends n commits to volumeID the way its own Agent would have: sealed under its
// own id, chained on its own HEAD. A clone's first commit therefore names no parent commit
// — its HEAD does not exist yet — which is the whole of what a clone's published history
// says about where the rest of its bytes are.
func (l *lineage) publish(volumeID string, n int) (commits, layerKeys []string) {
	l.t.Helper()
	enc, err := crypto.NewEncryption(l.dek, uuid.MustParse(volumeID))
	if err != nil {
		l.t.Fatalf("binding the key to %s: %v", volumeID, err)
	}
	for i := range n {
		plain := make([]byte, 4096*(i+1))
		if _, err := rand.Read(plain); err != nil {
			l.t.Fatalf("drawing a layer: %v", err)
		}
		commitID, layerID := ids.New().String(), ids.New().String()
		m, err := commit.Publish(l.t.Context(), l.store, enc, bytes.NewReader(plain), commit.Request{
			VolumeID: volumeID, CommitID: commitID, LayerID: layerID,
			Epoch: 7, VirtualSize: virtualSize,
		})
		if err != nil {
			l.t.Fatalf("publishing to %s: %v", volumeID, err)
		}
		commits = append(commits, commitID)
		layerKeys = append(layerKeys, m.Layer.ObjectKey)
		l.layerOf[commitID], l.plainOf[commitID] = layerID, plain
	}
	l.store.taken()
	return commits, layerKeys
}

// TestWhatOneChainLinkCostsAtAttach is the measurement controlplane.MaxChainDepth's
// justification rests on, pinned here so the number in that comment cannot drift away from
// the code that produces it.
//
// It asserts the exact requests, not a total: a count alone is satisfied by a rebuild that
// read the wrong volume's prefix the right number of times. Two shapes of one two-commit
// history — the volume itself, and a clone of it that has published nothing of its own.
func TestWhatOneChainLinkCostsAtAttach(t *testing.T) {
	t.Parallel()
	l := newLineage(t)
	parent := l.volume()
	commits, layers := l.publish(parent, 2)

	l.newHost()
	if _, err := l.rec.Restore(t.Context(), parent, virtualSize); err != nil {
		t.Fatalf("restoring the root volume: %v", err)
	}
	wantRoot := []string{
		// HEAD is a Head for the ETag and a Get for the body: one object, two requests.
		"HEAD " + commit.HeadKey(parent),
		"GET " + commit.HeadKey(parent),
		// The walk, newest first, one manifest per commit.
		"GET " + commit.ManifestKey(parent, commits[1]),
		"GET " + commit.ManifestKey(parent, commits[0]),
		// Then the layers, oldest first, because each is repointed at the one below it.
		"GET " + layers[0],
		"GET " + layers[1],
	}
	if got := l.store.taken(); !slices.Equal(got, wantRoot) {
		t.Errorf("a two-commit root volume cost these reads:\n%s\nwant:\n%s",
			strings.Join(got, "\n"), strings.Join(wantRoot, "\n"))
	}

	child := l.cloneOf(parent)
	l.newHost()
	if _, err := l.rec.RestoreFrom(t.Context(), qcow.Lineage{
		VolumeID: child, Ancestry: []qcow.Ancestor{{VolumeID: parent, CommitID: commits[1]}},
	}, virtualSize); err != nil {
		t.Fatalf("restoring the clone: %v", err)
	}
	wantClone := []string{
		// The ancestor's leg: two requests per commit in its history and not one more.
		// Its HEAD is never read: a clone is its parent as it was at the *named* commit
		// and not whatever it has published since, so a link carries no fixed cost.
		"GET " + commit.ManifestKey(parent, commits[1]),
		"GET " + commit.ManifestKey(parent, commits[0]),
		"GET " + layers[0],
		"GET " + layers[1],
		// The clone's own HEAD, which is not there: one Head, and no Get behind it.
		"HEAD " + commit.HeadKey(child),
	}
	if got := l.store.taken(); !slices.Equal(got, wantClone) {
		t.Errorf("a clone of a two-commit volume cost these reads:\n%s\nwant:\n%s",
			strings.Join(got, "\n"), strings.Join(wantClone, "\n"))
	}
}

// TestARestoreRebuildsEveryGeneration is the other half of the measurement: what a link
// buys, once the walk follows the whole ancestry the Control Plane sends.
//
// A depth-two clone, re-placed on a host that holds nothing. Its own manifests say nothing
// about where the root volume's bytes are — a clone's first commit is an overlay over a
// base it was restored onto — so the only thing that puts them on this disk is the walk
// being driven once per generation.
//
// It asserts the bytes, not the shape: every layer of every generation is compared against
// the plaintext that generation published. A chain of the right depth built out of the
// wrong volume's layers passes any assertion on the count, and each generation's layers
// are sealed under a *different* volume id (crypto.layerNonce binds it), so a restore that
// carried one binding through the walk would produce exactly that.
func TestARestoreRebuildsEveryGeneration(t *testing.T) {
	t.Parallel()
	l := newLineage(t)
	oldest := l.volume()
	oldestCommits, oldestLayers := l.publish(oldest, 2)

	// The middle volume: a clone of `oldest` that has since published a commit of its own,
	// which is all its history in the bucket amounts to.
	middle := l.cloneOf(oldest)
	middleCommits, _ := l.publish(middle, 1)

	grandchild := l.cloneOf(middle)
	l.newHost()
	got, err := l.rec.RestoreFrom(t.Context(), qcow.Lineage{
		VolumeID: grandchild,
		Ancestry: []qcow.Ancestor{
			{VolumeID: oldest, CommitID: oldestCommits[1]},
			{VolumeID: middle, CommitID: middleCommits[0]},
		},
	}, virtualSize)
	if err != nil {
		t.Fatalf("restoring a depth-two clone: %v", err)
	}

	// Oldest first: the order the layers must sit in for a guest to read the newest write
	// at any offset. The tip the caller is told to build over is the newest of them.
	whole := append(append([]string{}, oldestCommits...), middleCommits...)
	if want := qcow.LayerImage(root, grandchild, l.layerOf[whole[len(whole)-1]]); got.Base != want {
		t.Errorf("the clone would be built over %q, want the newest generation's layer %q", got.Base, want)
	}
	if got.HeadCommitID != middleCommits[0] {
		t.Errorf("the restore reports HEAD %s, want the commit its parent's snapshot named, %s",
			got.HeadCommitID, middleCommits[0])
	}
	for _, id := range whole {
		at := qcow.LayerImage(root, grandchild, l.layerOf[id])
		if !bytes.Equal(l.files.content(at), l.plainOf[id]) {
			t.Errorf("commit %s's layer is not on this disk as the bytes it published: %s", id, at)
		}
	}
	// And it paid for them: the grandparent's layer objects were fetched, which is what a
	// walk that stopped at the nearest ancestor never did.
	reads := l.store.taken()
	for _, key := range oldestLayers {
		if !slices.Contains(reads, "GET "+key) {
			t.Errorf("the grandparent's layer %s was never fetched", key)
		}
	}
}

// TestEveryAncestorOnThisDiskIsCopiedRatherThanDownloaded is §19's local reuse over more
// than one generation (§32's eleventh criterion), which is the half a deeper walk is most
// likely to lose: the reuse is decided from the ancestor's own state.json, and there is
// now one of those per generation.
//
// Every layer object is REMOVED from the bucket before the grandchild is restored, so a
// restore that downloads anything cannot pass and one that copies does not notice. The
// manifests and HEADs stay: a clone must still be told what each generation's history is.
func TestEveryAncestorOnThisDiskIsCopiedRatherThanDownloaded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	l := newLineage(t)
	oldest := l.volume()
	oldestCommits, _ := l.publish(oldest, 2)
	middle := l.cloneOf(oldest)
	middleCommits, _ := l.publish(middle, 1)

	// Both ancestors are served by this host first, which is what "same host" means: the
	// middle volume's own restore put the root's layers under it, and its commit on top.
	if _, err := l.rec.Restore(ctx, oldest, virtualSize); err != nil {
		t.Fatalf("restoring the root volume: %v", err)
	}
	if _, err := l.rec.RestoreFrom(ctx, qcow.Lineage{
		VolumeID: middle,
		Ancestry: []qcow.Ancestor{{VolumeID: oldest, CommitID: oldestCommits[1]}},
	}, virtualSize); err != nil {
		t.Fatalf("restoring the middle volume: %v", err)
	}

	objs, err := l.store.List(ctx, "layers/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		t.Fatal("the fixture published no layer objects")
	}
	for _, o := range objs {
		if err := l.store.Delete(ctx, o.Key); err != nil {
			t.Fatal(err)
		}
	}

	grandchild := l.cloneOf(middle)
	if _, err := l.rec.RestoreFrom(ctx, qcow.Lineage{
		VolumeID: grandchild,
		Ancestry: []qcow.Ancestor{
			{VolumeID: oldest, CommitID: oldestCommits[1]},
			{VolumeID: middle, CommitID: middleCommits[0]},
		},
	}, virtualSize); err != nil {
		t.Fatalf("a clone went to the object store for layers this host already holds: %v", err)
	}
	for _, id := range append(append([]string{}, oldestCommits...), middleCommits...) {
		at := qcow.LayerImage(root, grandchild, l.layerOf[id])
		if !bytes.Equal(l.files.content(at), l.plainOf[id]) {
			t.Errorf("commit %s's layer did not reach %s as the bytes it published", id, at)
		}
	}
}

// TestOneLineageSpendsOneRestoreBudget is what controlplane.MaxChainDepth is derived from.
//
// recovery.maxRestoreDepth is a bound on how many layers one image opens, and a rebuilt
// clone is one image whose backing chain is every generation's layers end to end — so the
// budget is the lineage's, not a generation's. Counted per generation instead, a depth-D
// clone builds D times the chain the measurement says a process can open, and nothing
// refuses it until QEMU runs out of file descriptors on a host with a guest waiting.
//
// The numbers straddle the bound from both sides with the same shape, so a build that
// dropped the check entirely fails the second case and one that counts wrong fails the
// first.
func TestOneLineageSpendsOneRestoreBudget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		older, younger int
		refused        bool
	}{
		{name: "two generations inside one budget", older: 200, younger: 50},
		{name: "two generations that only fit one each", older: 200, younger: 100, refused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := newLineage(t)
			parent := l.volume()
			parentCommits, _ := l.publish(parent, tt.older)
			child := l.cloneOf(parent)
			childCommits, _ := l.publish(child, tt.younger)

			grandchild := l.cloneOf(child)
			l.newHost()
			_, err := l.rec.RestoreFrom(t.Context(), qcow.Lineage{
				VolumeID: grandchild,
				Ancestry: []qcow.Ancestor{
					{VolumeID: parent, CommitID: parentCommits[len(parentCommits)-1]},
					{VolumeID: child, CommitID: childCommits[len(childCommits)-1]},
				},
			}, virtualSize)
			switch {
			case tt.refused && !errors.Is(err, recovery.ErrIncomplete):
				t.Fatalf("a lineage of %d layers was rebuilt anyway: %v", tt.older+tt.younger, err)
			case tt.refused && !strings.Contains(err.Error(), "layers to restore"):
				t.Errorf("the refusal does not say what the bound is: %v", err)
			case !tt.refused && err != nil:
				t.Fatalf("a lineage of %d layers was refused: %v", tt.older+tt.younger, err)
			}
		})
	}
}
