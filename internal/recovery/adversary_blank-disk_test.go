package recovery_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	realio "github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/qcow"
)

// TestAdversaryAHeadThatNamesNoCommitCrashesTheRestore.
//
// HEAD is the one mutable object in the bucket and the one this design says a turned bit
// in is the whole volume. ReadHead checks the frame's digest, the format version and the
// volume id — and not the one field the walk is driven by. A HEAD whose commit_id is
// empty walks zero commits, and Restore then indexes manifests[len-1] on an empty slice.
//
// The refusal this package exists to produce is replaced by a panic, and the panic is in
// the Agent's reconciliation goroutine: every volume on the host loses its Agent because
// one volume's HEAD says nothing.
func TestAdversaryAHeadThatNamesNoCommitCrashesTheRestore(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 0)
	// Planted directly rather than through CASHead, which refuses it now: an object with
	// an intact frame and a correct version, of the kind another writer could leave in a
	// bucket. What is asserted is that the *reader* refuses it.
	plantHead(w, "")

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("a HEAD naming no commit panicked instead of being refused: %v", r)
		}
	}()
	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err == nil {
		t.Fatalf("a HEAD naming no commit was rebuilt into %+v", got)
	}
	if !errors.Is(err, recovery.ErrIncomplete) {
		t.Errorf("the caller cannot tell this from a volume that is new: %v", err)
	}
}

// TestAdversaryALayerIdFromTheBucketWritesOutsideTheVolumeDirectory.
//
// Every local path a restore writes is LayerImage(root, m.Layer.LayerID), and
// layer_id is a string out of a manifest. ReadManifest checks that the object describes
// the volume and commit it was asked for; nothing checks that the layer id is an id.
//
// The file is created before commit.Fetch is ever called — Fetch is what would reject a
// layer id that does not parse as a UUID — so the escape happens on the way to the
// refusal, and discard() then unlinks whatever was at that path.
func TestAdversaryALayerIdFromTheBucketWritesOutsideTheVolumeDirectory(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 1)
	// A manifest that could only have been written by something that is not this code:
	// a corrupted bucket, a partial restore, a second writer. The frame is intact and
	// the volume and commit ids are the ones being asked for, so every check there is
	// passes.
	m := manifest(w, w.commits[0])
	m.Layer.LayerID = "../../../victim"
	republish(w, m)

	// A real filesystem, because the escape is the path resolution: an in-memory map
	// keyed by the uncleaned string cannot show it.
	tmp := t.TempDir()
	files := &osFiles{}
	rec := recovery.New(tmp, qemuImg, w.store, w.kms, w.keys, files, &silentRunner{})
	escaped := filepath.Join(tmp, "victim.qcow2"+".part")
	if err := realio.NewPaths().WriteAtomic(escaped, []byte("a file that has nothing to do with this volume")); err != nil {
		t.Fatalf("planting the bystander: %v", err)
	}

	if _, err := rec.Restore(t.Context(), w.vol, virtualSize); err == nil {
		t.Fatal("a manifest whose layer id is not an id was rebuilt")
	}
	layers := qcow.LayersDir(tmp)
	for _, path := range files.created() {
		if !strings.HasPrefix(filepath.Clean(path), layers+string(filepath.Separator)) {
			t.Errorf("a manifest in the bucket made this host write %s, outside %s", filepath.Clean(path), layers)
		}
	}
	if _, err := os.Stat(escaped); errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s was deleted by a restore of volume %s", escaped, w.vol)
	}
}

// osFiles is real.Paths — the production filesystem — plus the three verbs recovery.Files
// adds to qcow.Paths. The real one and not a stand-in, because a path escape is a property
// of how the production implementation resolves names.
type osFiles struct {
	realio.Paths
	mu    sync.Mutex
	paths []string
}

// Create records what was created, so the test can say which paths a restore touched.
func (f *osFiles) Create(path string) (io.WriteCloser, error) {
	f.mu.Lock()
	f.paths = append(f.paths, path)
	f.mu.Unlock()
	return f.Paths.Create(path)
}

func (f *osFiles) created() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

// silentRunner is a qemu-img that answers nothing: this test never reaches one.
type silentRunner struct{}

func (*silentRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	return nil, fmt.Errorf("%s %s: this test should never have reached qemu-img", name, strings.Join(args, " "))
}
