package commit_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func newID() string { return ids.New().String() }

func manifest(volumeID, commitID, parent string) commit.Manifest {
	digest := strings.Repeat("ab", 32)
	return commit.Manifest{
		VolumeID: volumeID, CommitID: commitID, ParentCommitID: parent,
		Epoch: 7, VirtualSize: 1 << 30,
		Layer: commit.Layer{
			ObjectKey: commit.LayerKey(digest), SizeBytes: 4096,
			SHA256: digest, FrameBytes: 65536, LayerID: newID(),
		},
	}
}

func TestKeys(t *testing.T) {
	t.Parallel()
	// Literal, because these are an interface to a bucket that outlives every process
	// here: a key scheme changed by accident is a volume whose history nobody can find.
	digest := strings.Repeat("ab", 32)
	tests := []struct{ name, got, want string }{
		{"a layer", commit.LayerKey(digest), "layers/sha256/ab/ab/" + digest},
		{"a manifest", commit.ManifestKey("v1", "c1"), "volumes/v1/commits/c1.json"},
		{"HEAD", commit.HeadKey("v1"), "volumes/v1/HEAD"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

// TestWriteManifestIsIdempotent is what makes step 12 of v6 §9 safe to repeat. An
// interrupted commit is recovered by doing the whole commit again, so a manifest that
// refused its own retry would turn every interruption into an operator's problem.
func TestWriteManifestIsIdempotent(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	m := manifest(newID(), newID(), "")

	if err := commit.WriteManifest(t.Context(), store, m); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := commit.WriteManifest(t.Context(), store, m); err != nil {
		t.Fatalf("the same manifest again was refused: %v", err)
	}
}

// TestWriteManifestRefusesToReplaceADifferentCommit: create-only is not a formality. A
// manifest is immutable because readers follow it; one replaced under a live commit id
// is a chain that means something different to whoever read it first.
func TestWriteManifestRefusesToReplaceADifferentCommit(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	volumeID, commitID := newID(), newID()
	first := manifest(volumeID, commitID, "")
	if err := commit.WriteManifest(t.Context(), store, first); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	second := first
	second.Epoch = 9
	err := commit.WriteManifest(t.Context(), store, second)
	if !errors.Is(err, commit.ErrManifestConflict) {
		t.Fatalf("want ErrManifestConflict, got %v", err)
	}
	// And what is in the bucket is still the first one.
	got, err := commit.ReadManifest(t.Context(), store, volumeID, commitID)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if got.Epoch != first.Epoch {
		t.Errorf("the manifest was replaced: epoch %d", got.Epoch)
	}
}

// TestReadManifestRefusesOneFromSomewhereElse: the digest proves the bytes are the bytes
// that were written and says nothing about where. A manifest copied or restored under
// another volume's prefix passes it intact, and every layer it names is then attributed
// to the wrong volume — another tenant's disk served to this guest.
func TestReadManifestRefusesOneFromSomewhereElse(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	mine, theirs := newID(), newID()
	commitID := newID()
	if err := commit.WriteManifest(t.Context(), store, manifest(theirs, commitID, "")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	body, err := store.Get(t.Context(), commit.ManifestKey(theirs, commitID))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := store.Put(t.Context(), commit.ManifestKey(mine, commitID), body, objectstore.PutOptions{}); err != nil {
		t.Fatalf("copying it under another volume: %v", err)
	}
	if got, err := commit.ReadManifest(t.Context(), store, mine, commitID); err == nil {
		t.Fatalf("another volume's manifest was accepted as ours: %+v", got)
	}
}

// TestHeadIsCreatedOnceAndThenCompareAndSet walks the ordinary life of the one mutable
// object: it does not exist, it is created, and every move after that is against the
// ETag the mover read.
func TestHeadIsCreatedOnceAndThenCompareAndSet(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	volumeID, first, second := newID(), newID(), newID()

	if _, _, err := commit.ReadHead(t.Context(), store, volumeID); !errors.Is(err, commit.ErrNoHead) {
		t.Fatalf("want ErrNoHead for a volume that never published, got %v", err)
	}
	if err := commit.CASHead(t.Context(), store, volumeID, first, ""); err != nil {
		t.Fatalf("creating HEAD: %v", err)
	}
	h, etag, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil || h.CommitID != first {
		t.Fatalf("ReadHead = %+v, %v", h, err)
	}
	if etag == "" {
		t.Fatal("HEAD came back with no ETag, so nothing could ever compare-and-set it")
	}
	if err := commit.CASHead(t.Context(), store, volumeID, second, etag); err != nil {
		t.Fatalf("moving HEAD: %v", err)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != second {
		t.Errorf("HEAD is %q, want %q", h.CommitID, second)
	}
}

// TestCASHeadRefusesToCreateOverAHeadThatAppeared is the two-hosts case at its sharpest:
// both read "no HEAD", both build a first commit, and the second must not win by
// arriving later. It is create-only that stops it, and it is the only thing that does.
func TestCASHeadRefusesToCreateOverAHeadThatAppeared(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	volumeID, theirs, mine := newID(), newID(), newID()

	if err := commit.CASHead(t.Context(), store, volumeID, theirs, ""); err != nil {
		t.Fatalf("their publish: %v", err)
	}
	err := commit.CASHead(t.Context(), store, volumeID, mine, "")
	if !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("want ErrHeadMoved, got %v", err)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != theirs {
		t.Errorf("HEAD is %q; the loser overwrote the winner", h.CommitID)
	}
}

// TestCASHeadNeverOverwritesAHeadThatMoved. v6 §15: a CAS that fails means another actor
// moved HEAD, and with a correct single writer that is a fencing failure, a concurrent
// recovery or a bug. Every one of those is made worse by taking the object.
func TestCASHeadNeverOverwritesAHeadThatMoved(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	volumeID, first, theirs, mine := newID(), newID(), newID(), newID()

	if err := commit.CASHead(t.Context(), store, volumeID, first, ""); err != nil {
		t.Fatalf("creating HEAD: %v", err)
	}
	_, stale, err := commit.ReadHead(t.Context(), store, volumeID)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	// Somebody else publishes while this commit is being assembled.
	if err := commit.CASHead(t.Context(), store, volumeID, theirs, stale); err != nil {
		t.Fatalf("their publish: %v", err)
	}

	err = commit.CASHead(t.Context(), store, volumeID, mine, stale)
	if !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("want ErrHeadMoved, got %v", err)
	}
	if !strings.Contains(err.Error(), theirs) {
		t.Errorf("the error does not say what HEAD holds now: %v", err)
	}
	if h, _, _ := commit.ReadHead(t.Context(), store, volumeID); h.CommitID != theirs {
		t.Errorf("HEAD is %q, want the other writer's %q", h.CommitID, theirs)
	}
}

// TestCASHeadReadsALostAnswerAsTheSuccessItWas is v6 §15's row "CAS exitoso, respuesta
// perdida": the write landed and the reply did not come back. A retry then sees its own
// success as somebody else's conflict, and a volume that published perfectly well is
// reported as a fencing incident.
func TestCASHeadReadsALostAnswerAsTheSuccessItWas(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	volumeID, first, mine := newID(), newID(), newID()

	if err := commit.CASHead(t.Context(), store, volumeID, first, ""); err != nil {
		t.Fatalf("creating HEAD: %v", err)
	}
	_, stale, _ := commit.ReadHead(t.Context(), store, volumeID)
	// The write that "did not answer" — the same commit this caller is about to retry.
	if err := commit.CASHead(t.Context(), store, volumeID, mine, stale); err != nil {
		t.Fatalf("the first attempt: %v", err)
	}
	if err := commit.CASHead(t.Context(), store, volumeID, mine, stale); err != nil {
		t.Fatalf("the retry was reported as a conflict: %v", err)
	}
}

// TestReadHeadRefusesOneNamingAnotherVolume: same reasoning as the manifest, and worse
// here. HEAD is the whole volume; one restored under the wrong key points a guest at
// another tenant's history with nothing anywhere reporting an error.
func TestReadHeadRefusesOneNamingAnotherVolume(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	mine, theirs := newID(), newID()
	if err := commit.CASHead(t.Context(), store, theirs, newID(), ""); err != nil {
		t.Fatalf("their HEAD: %v", err)
	}
	body, err := store.Get(t.Context(), commit.HeadKey(theirs))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := store.Put(t.Context(), commit.HeadKey(mine), body, objectstore.PutOptions{}); err != nil {
		t.Fatalf("copying: %v", err)
	}
	if h, _, err := commit.ReadHead(t.Context(), store, mine); err == nil {
		t.Fatalf("another volume's HEAD was accepted as ours: %+v", h)
	}
}

// TestAnObjectFromAnotherFormatIsRefused is INV-19's detection half, and the only test
// here that writes an object this binary would never write.
//
// It is needed because every other test in this file stamps the version on the way in,
// so no stored object ever carries a foreign one — planting the removal of the version
// check turned nothing red until this existed. What it rules out is the silent case:
// json.Unmarshal discards fields it does not know, so a manifest from a newer format
// decodes without complaint into whatever subset this build understands, and the commit
// is then followed with its unknown half missing.
func TestAnObjectFromAnotherFormatIsRefused(t *testing.T) {
	t.Parallel()
	volumeID, commitID := newID(), newID()

	tests := []struct {
		name string
		body string
		read func(objectstore.Store) error
		key  string
	}{
		{
			name: "a manifest from a newer format",
			body: `{"format_version":2,"volume_id":"` + volumeID + `","commit_id":"` + commitID + `"}`,
			key:  commit.ManifestKey(volumeID, commitID),
			read: func(s objectstore.Store) error {
				_, err := commit.ReadManifest(t.Context(), s, volumeID, commitID)
				return err
			},
		},
		{
			name: "a HEAD from a newer format",
			body: `{"format_version":2,"volume_id":"` + volumeID + `","commit_id":"` + commitID + `"}`,
			key:  commit.HeadKey(volumeID),
			read: func(s objectstore.Store) error {
				_, _, err := commit.ReadHead(t.Context(), s, volumeID)
				return err
			},
		},
		{
			name: "a manifest with no version at all, which is what everything written before the field looks like",
			body: `{"volume_id":"` + volumeID + `","commit_id":"` + commitID + `"}`,
			key:  commit.ManifestKey(volumeID, commitID),
			read: func(s objectstore.Store) error {
				_, err := commit.ReadManifest(t.Context(), s, volumeID, commitID)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := sim.NewObjectStore()
			if _, err := store.Put(t.Context(), tt.key, framed.Frame([]byte(tt.body)), objectstore.PutOptions{}); err != nil {
				t.Fatalf("put: %v", err)
			}
			err := tt.read(store)
			if err == nil {
				t.Fatal("it was read")
			}
			// The digest is intact — it was framed properly — so this must be the
			// version refusing it and not corruption catching it by accident.
			if errors.Is(err, commit.ErrCorrupt) {
				t.Fatalf("refused as corrupt rather than as the wrong format: %v", err)
			}
		})
	}
}
