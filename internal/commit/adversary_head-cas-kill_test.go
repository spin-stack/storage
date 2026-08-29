package commit_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// advCASKill is the object store as it looks to a host that is killed with the
// compare-and-set on HEAD in flight. The write to HEAD answers with a transport error
// exactly once; `applied` decides which of the two indistinguishable things happened
// underneath — the store never got it, or the store took it and the answer was lost.
//
// Indistinguishable *to the caller* is the whole difficulty of this boundary, so the two
// arms are the same fault with one bit changed rather than two different fakes.
type advCASKill struct {
	objectstore.Store
	headKey string
	applied bool // true: the CAS takes effect and the answer is lost
	armed   bool
	puts    int // writes to HEAD the store actually performed
}

func (s *advCASKill) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if key != s.headKey {
		return s.Store.Put(ctx, key, data, opts)
	}
	if !s.armed {
		res, err := s.Store.Put(ctx, key, data, opts)
		if err == nil {
			s.puts++
		}
		return res, err
	}
	s.armed = false
	if s.applied {
		if _, err := s.Store.Put(ctx, key, data, opts); err != nil {
			return objectstore.PutResult{}, err
		}
		s.puts++
	}
	return objectstore.PutResult{}, errors.New("connection reset by peer")
}

// advChain walks back from HEAD and returns the commit ids from HEAD to the root. It
// fails the test rather than returning an error, because every object it touches was
// written by the code under test and any of them being unreadable is the finding.
//
// It also refuses to walk for ever: a manifest whose parent is itself, or a cycle of any
// length, is what an interrupted commit that re-reads HEAD too late produces.
func advChain(t *testing.T, store objectstore.Store, volumeID string) []string {
	t.Helper()
	head, _, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	seen := map[string]bool{}
	var chain []string
	for id := head.CommitID; id != ""; {
		if seen[id] {
			t.Fatalf("the published history loops at commit %s: %v", id, chain)
		}
		seen[id] = true
		chain = append(chain, id)
		m, err := commit.ReadManifest(t.Context(), store, volumeID, id)
		if err != nil {
			t.Fatalf("HEAD's chain reaches commit %s, which is not readable: %v", id, err)
		}
		id = m.ParentCommitID
	}
	return chain
}

// advCountManifests is how many commits the bucket claims this volume has.
func advCountManifests(t *testing.T, store objectstore.Store, volumeID string) int {
	t.Helper()
	objs, err := store.List(t.Context(), "volumes/"+volumeID+"/commits/")
	if err != nil {
		t.Fatalf("listing commits: %v", err)
	}
	return len(objs)
}

// TestAdversaryTheCASOnHeadIsInterruptedAndNeverLands is the kill-point in the middle of
// the compare-and-set, on the arm where the store never took the write (v6 §29).
//
//   - Which commit is visible: the parent. A commit exists when HEAD names it, and HEAD
//     was not moved, so the interrupted commit is simply not there.
//   - What local state is left: none here — the caller holds the same Request, which is
//     what makes the retry a retry and not a second commit.
//   - What remote objects are left: the sealed layer and its manifest, both complete and
//     both unreferenced. Garbage a sweep collects, never a dangling HEAD.
//   - Does it recover by itself: yes. Publishing the same Request again re-uses the
//     content-addressed layer, accepts the byte-identical manifest, and moves HEAD once.
//     The volume ends with one manifest per commit, not two.
//   - Could a confirmed commit be lost: no. The caller was told the commit failed, and it
//     is the one that did not happen; the commit before it is untouched and still fetches.
func TestAdversaryTheCASOnHeadIsInterruptedAndNeverLands(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	enc := dek(t, volumeID)
	store := &advCASKill{Store: sim.NewObjectStore(), headKey: commit.HeadKey(volumeID)}

	base := layerBytes(t, 4096*2)
	parent, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(base), request(volumeID, len(base)))
	if err != nil {
		t.Fatalf("the commit this one is layered on: %v", err)
	}

	plain := layerBytes(t, 4096*2)
	req := request(volumeID, len(plain))
	store.armed, store.applied = true, false
	if m, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req); err == nil {
		t.Fatalf("the interrupted commit reported success: %+v", m)
	}

	// What the bucket says, which is all the next host has: HEAD still names the parent,
	// and the parent still reconstructs.
	if chain := advChain(t, store, volumeID); len(chain) != 1 || chain[0] != parent.CommitID {
		t.Fatalf("HEAD's chain is %v, want only the parent %s", chain, parent.CommitID)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, enc, parent, &out); err != nil {
		t.Fatalf("the commit before the interruption no longer fetches: %v", err)
	}
	if !bytes.Equal(out.Bytes(), base) {
		t.Fatal("the commit before the interruption came back as different bytes")
	}

	// It recovers by doing the whole commit again, with the same commit id.
	again, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("the retry of an interrupted commit was refused: %v", err)
	}
	if again.ParentCommitID != parent.CommitID {
		t.Errorf("the retry is layered on %q, want the parent %s", again.ParentCommitID, parent.CommitID)
	}
	chain := advChain(t, store, volumeID)
	if len(chain) != 2 || chain[0] != req.CommitID || chain[1] != parent.CommitID {
		t.Fatalf("HEAD's chain is %v, want %s on %s", chain, req.CommitID, parent.CommitID)
	}
	if n := advCountManifests(t, store, volumeID); n != 2 {
		t.Errorf("the bucket holds %d manifests for two commits, one of which was published twice", n)
	}
	out.Reset()
	if err := commit.Fetch(t.Context(), store, enc, again, &out); err != nil {
		t.Fatalf("the recovered commit cannot be fetched: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the recovered commit came back as different bytes")
	}
}

// TestAdversaryTheCASOnHeadLandsAndTheAnswerIsLost is v6 §26's row: the compare-and-set
// took effect and the host never learned it. The retry must read the HEAD that already
// names this commit as the success it was — not as somebody else's conflict, and above
// all not as a parent to build on, which would make the commit its own parent.
//
//   - Which commit is visible: the new one. HEAD moved; only the answer was lost. That is
//     safe because the caller was *not* told SUCCESS, and a commit appearing when the
//     caller was told nothing breaks no promise.
//   - What local state is left: the Request, retried unchanged.
//   - What remote objects are left: exactly one layer, one manifest and a HEAD naming it.
//     The retry uploads nothing new — the layer's key is its digest and the manifest is
//     byte-identical.
//   - Does it recover by itself: yes, and it must recover as SUCCESS. The manifest handed
//     back is the one in the bucket.
//   - Could a confirmed commit be lost: no. Nothing on this path overwrites HEAD, so the
//     commit that landed stays landed and the one before it stays reachable.
func TestAdversaryTheCASOnHeadLandsAndTheAnswerIsLost(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	enc := dek(t, volumeID)
	store := &advCASKill{Store: sim.NewObjectStore(), headKey: commit.HeadKey(volumeID)}

	base := layerBytes(t, 4096*2)
	parent, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(base), request(volumeID, len(base)))
	if err != nil {
		t.Fatalf("the commit this one is layered on: %v", err)
	}

	plain := layerBytes(t, 4096*2)
	req := request(volumeID, len(plain))
	store.armed, store.applied = true, true
	if m, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req); err == nil {
		t.Fatalf("the commit whose answer was lost reported success: %+v", m)
	}
	// The write did land: the bucket, not the caller, is the truth about it.
	if head, _, err := commit.ReadHead(t.Context(), store, volumeID); err != nil || head.CommitID != req.CommitID {
		t.Fatalf("HEAD is %v (%v); this test only means anything if the lost CAS took effect", head, err)
	}

	writesBefore := store.puts
	again, err := commit.Publish(t.Context(), store, enc, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("the retry of a commit whose answer was lost was reported as a failure: %v", err)
	}
	if again.CommitID != req.CommitID || again.ParentCommitID != parent.CommitID {
		t.Fatalf("the retry describes commit %s on parent %q, want %s on %s",
			again.CommitID, again.ParentCommitID, req.CommitID, parent.CommitID)
	}
	if store.puts != writesBefore {
		t.Errorf("the retry moved HEAD %d more times; a commit that is already published must not be republished",
			store.puts-writesBefore)
	}

	// And what the bucket holds: one chain, no cycle, both commits reconstructible, and
	// the manifest the caller was handed is the one that is stored.
	chain := advChain(t, store, volumeID)
	if len(chain) != 2 || chain[0] != req.CommitID || chain[1] != parent.CommitID {
		t.Fatalf("HEAD's chain is %v, want %s on %s", chain, req.CommitID, parent.CommitID)
	}
	if n := advCountManifests(t, store, volumeID); n != 2 {
		t.Errorf("the bucket holds %d manifests for two commits", n)
	}
	stored, err := commit.ReadManifest(t.Context(), store, volumeID, req.CommitID)
	if err != nil {
		t.Fatalf("reading the published manifest: %v", err)
	}
	if stored != again {
		t.Errorf("the retry returned %+v and the bucket holds %+v", again, stored)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), store, enc, stored, &out); err != nil {
		t.Fatalf("the commit whose answer was lost cannot be fetched: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the commit whose answer was lost came back as different bytes")
	}
}
