package recovery_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/recovery"
)

// rottenRunner is a qemu-img that opens the file before it answers about it.
//
// The fake runner the rest of this package uses is content-blind: a path present in the
// filesystem is an image, whatever its bytes are. That is enough for every question about
// argv and about which layer backs which, and it cannot express the one thing this test
// is about — a file that is *there*, is the right length, and is not a qcow2 any more.
//
// The rule it applies is the one the real binary applies: every verb opens the image and
// reads its header, so a file whose bytes have been scribbled over fails `rebase`, fails
// `info`, and fails the chain walk. `rebase -u` does not open the backing file, which is
// why only the image argument is checked here.
type rottenRunner struct {
	mu    sync.Mutex
	files *fakeFiles
	// sound is the set of contents this qemu-img can open, by content and not by path:
	// a layer is a sound image because of the bytes in it, which is the whole subject.
	sound   [][]byte
	backing map[string]string
	runs    []string
}

func newRottenRunner(w *world) *rottenRunner {
	return &rottenRunner{files: w.files, sound: w.plain, backing: map[string]string{}}
}

func (r *rottenRunner) open(path string) error {
	body := r.files.content(path)
	if body == nil {
		return fmt.Errorf("Could not open '%s': No such file or directory", path)
	}
	for _, ok := range r.sound {
		if bytes.Equal(body, ok) {
			return nil
		}
	}
	return fmt.Errorf("Could not open '%s': Image is not in qcow2 format", path)
}

func (r *rottenRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, strings.Join(append([]string{name}, args...), " "))
	image := args[len(args)-1]
	switch args[0] {
	case "rebase":
		if err := r.open(image); err != nil {
			return nil, err
		}
		for i, a := range args {
			if a == "-b" && i+1 < len(args) {
				r.backing[image] = args[i+1]
			}
		}
		return nil, nil
	case "info":
		if err := r.open(image); err != nil {
			return nil, err
		}
		one := func(path string) map[string]any {
			return map[string]any{
				"format": "qcow2", "virtual-size": virtualSize, "filename": path,
				"full-backing-filename": r.backing[path],
			}
		}
		if !slices.Contains(args, "--backing-chain") {
			return json.Marshal(one(image))
		}
		var chain []map[string]any
		for path := image; path != ""; path = r.backing[path] {
			// The walk opens every layer, which is what makes it a different question
			// from plain info.
			if err := r.open(path); err != nil {
				return nil, fmt.Errorf("Could not open backing file: %w", err)
			}
			chain = append(chain, one(path))
		}
		return json.Marshal(chain)
	}
	return nil, fmt.Errorf("rotten qemu-img: unknown verb %q", strings.Join(args, " "))
}

// rot scribbles over a file that is already on disk, leaving its length alone.
func rot(files *fakeFiles, path string) error {
	body := files.content(path)
	if body == nil {
		return fmt.Errorf("nothing at %s to damage", path)
	}
	broken := bytes.Clone(body)
	for i := range broken {
		broken[i] ^= 0x5A
	}
	f, err := files.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(broken); err != nil {
		return err
	}
	return f.Close()
}

// TestAdversaryALayerThisHostHoldsRotsOnDiskAndIsRefusedBeforeAnythingBootsOnIt is §29's
// third kill-point on the one path where nothing hashes the bytes.
//
// A layer that is downloaded is checked: commit.Fetch verifies the digest over the object
// as stored and GCM-authenticates every frame. A layer this host *already holds* is
// checked by nobody — the state file vouches for it, and it cannot be re-hashed, because
// `qemu-img rebase -u` rewrites the header of every layer it lands so a local file no
// longer hashes to the object it came from. So between the download that verified it and
// the guest that reads it there is a window with no digest in it: a partial write, a bad
// sector, a truncation by whatever else runs on the host. What stands in that window is
// qemu-img refusing to open the file, and this asserts that the refusal is reached and
// that nothing bootable is handed back when it is.
//
// The five questions:
//
//   - Which commit is visible? All three, unchanged: HEAD still names the newest, and
//     every manifest and layer object is still in the bucket. The damage is to one file
//     on one host.
//   - What local state is left? The rotten file, exactly as it was found — not repaired
//     and not silently replaced — and state.json still naming the commits this host held.
//     No `.part`: nothing was half-downloaded.
//   - What remote objects are left? Every one that was there, byte for byte. A refused
//     restore is a read path and writes nothing.
//   - Does it recover by itself? Yes, once the file stops being there: the record alone
//     does not satisfy materialize, so removing the damaged layer sends the next restore
//     back to the bucket, which still holds the sound copy. That second half is asserted
//     below, because a refusal with no way out is an operator's volume forever.
//   - Could a confirmed commit be lost? No. Nothing here deletes or overwrites a
//     published object, and the layer that rotted is still in the bucket intact — which
//     is what the second half proves by rebuilding from it.
func TestAdversaryALayerThisHostHoldsRotsOnDiskAndIsRefusedBeforeAnythingBootsOnIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)
	run := newRottenRunner(w)
	w.rec = recovery.New(root, qemuImg, w.store, w.kms, w.keys, w.files, run)
	plant(w, 0)
	plant(w, 1)
	if err := rot(w.files, w.image(1)); err != nil {
		t.Fatalf("damaging the layer on disk: %v", err)
	}
	rotten := bytes.Clone(w.files.content(w.image(1)))

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err == nil {
		t.Fatalf("a chain with a layer that is not a qcow2 any more came back as %+v", got)
	}
	if !errors.Is(err, recovery.ErrIncomplete) {
		t.Errorf("the caller cannot tell this from a volume that is new: %v", err)
	}
	if !strings.Contains(err.Error(), w.image(1)) {
		t.Errorf("the refusal does not name the layer that could not be opened: %v", err)
	}
	// Nothing to boot on: no base, and no tip could be built over one.
	if got != (qcow.Restored{}) {
		t.Errorf("a refusal handed back %+v", got)
	}
	// The damaged file is left alone rather than repaired behind the operator's back,
	// and nothing half-downloaded is lying beside it.
	if !bytes.Equal(w.files.content(w.image(1)), rotten) {
		t.Error("the refused restore rewrote the damaged layer")
	}
	for _, name := range w.files.names() {
		if strings.HasSuffix(name, ".part") {
			t.Errorf("a refused restore left %s behind", name)
		}
	}
	// And the bucket is untouched: the published history is exactly where it was.
	head, err := commit.ReadHeadCommit(t.Context(), w.store, w.vol)
	if err != nil {
		t.Fatalf("reading HEAD after the refusal: %v", err)
	}
	if want := w.commits[len(w.commits)-1]; head != want {
		t.Errorf("HEAD names %s after a refused restore, want %s", head, want)
	}

	// The way out. The record is not enough on its own, so a layer that is gone is
	// fetched again — from the object the digest still vouches for.
	if err := w.files.Remove(w.image(1)); err != nil {
		t.Fatalf("removing the damaged layer: %v", err)
	}
	restored, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err != nil {
		t.Fatalf("restoring after the damaged layer was removed: %v", err)
	}
	if !bytes.Equal(w.files.content(w.image(1)), w.plain[1]) {
		t.Error("the layer was not fetched back from the bucket")
	}
	if restored.Base != w.image(2) {
		t.Errorf("the rebuilt chain is based on %q, want the newest layer %q", restored.Base, w.image(2))
	}
	if restored.HeadCommitID != w.commits[2] {
		t.Errorf("the rebuilt chain is at commit %s, want %s", restored.HeadCommitID, w.commits[2])
	}
}
