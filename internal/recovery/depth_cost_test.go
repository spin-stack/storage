package recovery_test

import (
	"bytes"
	"context"
	"crypto/rand"
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
		keys: &fakeKeys{keys: map[string]agent.VolumeKeys{}},
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
			Epoch: 7, VirtualSize: virtualSize, PlainBytes: int64(len(plain)),
		})
		if err != nil {
			l.t.Fatalf("publishing to %s: %v", volumeID, err)
		}
		commits = append(commits, commitID)
		layerKeys = append(layerKeys, m.Layer.ObjectKey)
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
		VolumeID: child, ParentVolumeID: parent, ParentCommitID: commits[1],
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

// TestARestoreReadsOneAncestorAndStops is the other half of the measurement, and the
// reason controlplane.MaxChainDepth's ceiling is wrong rather than merely stale.
//
// qcow.Lineage carries one parent and RestoreFrom does not recurse: it rebuilds the named
// parent's own published history and stops. A grandparent's layers are under nobody's
// published chain — a clone's first commit is an overlay over a base it was restored onto,
// and its manifest says nothing about where that base is — so at depth two the bytes the
// root volume wrote are never requested and never land on this disk.
//
// It asserts on the bucket and on the disk rather than on a refusal because there is no
// refusal; the rebuild reports success. A change that makes the walk follow the whole
// ancestry turns this red, which is the point: the ceiling and the walk move together.
func TestARestoreReadsOneAncestorAndStops(t *testing.T) {
	t.Parallel()
	l := newLineage(t)
	oldest := l.volume()
	_, oldestLayers := l.publish(oldest, 2)

	// The middle volume: a clone of `oldest` that has since published a commit of its own,
	// which is all its history in the bucket amounts to.
	middle := l.cloneOf(oldest)
	middleCommits, _ := l.publish(middle, 1)

	grandchild := l.cloneOf(middle)
	l.newHost()
	got, err := l.rec.RestoreFrom(t.Context(), qcow.Lineage{
		VolumeID: grandchild, ParentVolumeID: middle, ParentCommitID: middleCommits[0],
	}, virtualSize)
	if err != nil {
		t.Fatalf("restoring a depth-two clone: %v", err)
	}
	if got.Base == "" {
		t.Fatal("the restore named no base")
	}

	reads := l.store.taken()
	if len(reads) == 0 {
		t.Fatal("the restore read nothing at all, so this proves nothing")
	}
	for _, r := range reads {
		if strings.Contains(r, oldest) {
			t.Errorf("the grandparent's prefix was read after all: %s", r)
		}
	}
	for _, key := range oldestLayers {
		if slices.Contains(reads, "GET "+key) {
			t.Errorf("the grandparent's layer %s was fetched after all", key)
		}
	}
	// Which is what a guest would notice: the chain this restore handed back is served
	// without a single byte of what the root volume wrote.
	if n := len(l.files.names()); n != len(middleCommits)+1 {
		t.Errorf("the disk holds %v, want the middle volume's one layer and its state file",
			l.files.names())
	}
}
