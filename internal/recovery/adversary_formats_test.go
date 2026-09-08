package recovery_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/qcow"
)

// putFramed writes a hand-built structural object the way a writer of this format would:
// JSON, framed by internal/framed. The digest is over the bytes as stored, so every
// object below is intact by every check the readers make — which is the point: these are
// the objects that get *past* the integrity layer.
func putFramed(t *testing.T, store objectstore.Store, key string, v any) {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encoding %s: %v", key, err)
	}
	if _, err := store.Put(t.Context(), key, framed.Frame(body), objectstore.PutOptions{}); err != nil {
		t.Fatalf("writing %s: %v", key, err)
	}
}

// TestAdversaryHeadNamingNoCommit is a HEAD whose `commit_id` is the empty string.
//
// Every check on the way in passes: the digest matches, the format version is this
// binary's, and the object names this volume. commit.ReadHead validates the volume and
// says nothing about the commit, so Restore walks a history of length zero and then
// indexes the last element of it.
func TestAdversaryHeadNamingNoCommit(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 0)
	putFramed(t, w.store, commit.HeadKey(w.vol), commit.Head{
		FormatVersion: framed.FormatVersion, VolumeID: w.vol, CommitID: "",
	})

	_, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if !errors.Is(err, recovery.ErrIncomplete) {
		t.Fatalf("restoring against a HEAD that names no commit: %v, want ErrIncomplete", err)
	}
}

// TestAdversaryManifestLayerIDIsAPath is a manifest whose `layer_id` is not an id at all
// but a relative path out of this volume's layer directory.
//
// commit.ReadManifest checks the volume and the commit the object claims, and nothing
// checks the layer id before qcow.LayerImage joins it onto a directory and recovery
// creates a file there. The uuid.Parse that would refuse it happens inside commit.Fetch,
// which runs after the file has been created.
func TestAdversaryManifestLayerIDIsAPath(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 0)
	victim := "/data/volumes/another-volume/layers/theirs"
	escape := "../../another-volume/layers/theirs"
	commitID := "01930000-0000-7000-8000-00000000abcd"
	putFramed(t, w.store, commit.ManifestKey(w.vol, commitID), commit.Manifest{
		FormatVersion: framed.FormatVersion, VolumeID: w.vol, CommitID: commitID,
		Epoch: 7, VirtualSize: virtualSize,
		Layer: commit.Layer{
			ObjectKey: commit.LayerKey(strings.Repeat("a", 64)), SizeBytes: 16,
			SHA256: strings.Repeat("a", 64), FrameBytes: 65536, LayerID: escape,
		},
	})
	putFramed(t, w.store, commit.HeadKey(w.vol), commit.Head{
		FormatVersion: framed.FormatVersion, VolumeID: w.vol, CommitID: commitID,
	})
	// The other volume's layer directory exists on this host, as it would if the two
	// volumes were placed together.
	if err := w.files.MkdirAll("/data/volumes/another-volume/layers"); err != nil {
		t.Fatalf("making the neighbour's directory: %v", err)
	}
	if err := w.files.WriteAtomic(victim+".qcow2", []byte("the neighbour's layer")); err != nil {
		t.Fatalf("writing the neighbour's layer: %v", err)
	}

	before := map[string]bool{}
	for _, name := range w.files.names() {
		before[name] = true
	}

	if _, err := w.rec.Restore(t.Context(), w.vol, virtualSize); err == nil {
		t.Fatal("a manifest whose layer id is a path restored without complaint")
	}
	mine := qcow.VolumeDir(root, w.vol)
	for _, name := range w.files.names() {
		if !before[name] && !strings.HasPrefix(name, mine) {
			t.Errorf("the restore of volume %s created %s", w.vol, name)
		}
	}
	for _, name := range w.files.removed {
		if !strings.HasPrefix(name, mine) {
			t.Errorf("the restore of volume %s wrote and then deleted %s", w.vol, name)
		}
	}
	if got := string(w.files.content(victim + ".qcow2")); got != "the neighbour's layer" {
		t.Errorf("the neighbour's layer now holds %q", got)
	}
}
