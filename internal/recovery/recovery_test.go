package recovery_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const (
	root        = "/data"
	qemuImg     = "/usr/bin/qemu-img"
	virtualSize = int64(1) << 30
)

// fakeFiles is a filesystem of absolute paths holding bytes. It is deliberately strict
// about two things the real one is: a Create under a directory nobody made fails, and a
// file only appears under its final name through Rename.
type fakeFiles struct {
	mu      sync.Mutex
	dirs    map[string]bool
	files   map[string][]byte
	removed []string
	// opened records local reads, which is how a same-host clone is told apart from a
	// download: the chain looks identical either way.
	opened []string
	// createErr, when set, is what every Create fails with.
	createErr error
}

func newFiles() *fakeFiles {
	return &fakeFiles{dirs: map[string]bool{}, files: map[string][]byte{}}
}

func (f *fakeFiles) MkdirAll(dir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for d := dir; d != "/" && d != "."; d = filepath.Dir(d) {
		f.dirs[d] = true
	}
	return nil
}

func (f *fakeFiles) Exists(path string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.files[path]
	return ok, nil
}

// Size, ReadFile and WriteAtomic are here because qcow.ReadState and qcow.WriteState
// take a qcow.Paths: recovery and qcow.Manager write one state.json, through one
// implementation of one format.
func (f *fakeFiles) Size(path string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.files[path]
	if !ok {
		return 0, fmt.Errorf("no such file: %s", path)
	}
	return int64(len(body)), nil
}

func (f *fakeFiles) ReadFile(path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.files[path]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", path)
	}
	return slices.Clone(body), nil
}

func (f *fakeFiles) WriteAtomic(path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirs[filepath.Dir(path)] {
		return fmt.Errorf("no such directory: %s", filepath.Dir(path))
	}
	f.files[path] = slices.Clone(data)
	return nil
}

// Open serves a file this fake already holds. It counts the reads, because the whole
// point of a same-host clone is that the object store is not touched — and a test that
// only checked the resulting chain could not tell a copy from a download.
func (f *fakeFiles) Open(path string) (io.ReadSeekCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.files[path]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", path)
	}
	f.opened = append(f.opened, path)
	return nopSeekCloser{bytes.NewReader(b)}, nil
}

// nopSeekCloser is a file this fake holds: readable, rewindable, and closing it costs
// nothing.
type nopSeekCloser struct{ *bytes.Reader }

func (nopSeekCloser) Close() error { return nil }

func (f *fakeFiles) Create(path string) (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if !f.dirs[filepath.Dir(path)] {
		return nil, fmt.Errorf("no such directory: %s", filepath.Dir(path))
	}
	f.files[path] = nil
	return &fakeFile{files: f, path: path}, nil
}

func (f *fakeFiles) Rename(oldPath, newPath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.files[oldPath]
	if !ok {
		return fmt.Errorf("no such file: %s", oldPath)
	}
	delete(f.files, oldPath)
	f.files[newPath] = body
	return nil
}

// List names the files this fake holds directly under dir. qcow.Paths carries it for the
// sweep; nothing in this package lists anything.
func (f *fakeFiles) List(dir string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for path := range f.files {
		if filepath.Dir(path) == dir {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (f *fakeFiles) Remove(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, path)
	delete(f.files, path)
	return nil
}

func (f *fakeFiles) content(path string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.files[path]
}

// names is every path that holds a file, sorted.
func (f *fakeFiles) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.files))
	for name := range f.files {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

type fakeFile struct {
	files *fakeFiles
	path  string
	buf   bytes.Buffer
}

func (w *fakeFile) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *fakeFile) Close() error {
	w.files.mu.Lock()
	defer w.files.mu.Unlock()
	if _, ok := w.files.files[w.path]; !ok {
		return nil // removed under us; the caller is already failing
	}
	w.files.files[w.path] = slices.Clone(w.buf.Bytes())
	return nil
}

// fakeRunner stands in for qemu-img, and it is not a recorder: it models the two verbs
// this package uses closely enough that a wrong argv changes the answer.
//
//   - `rebase` refuses without -F, the way the pinned binary does ("backing file format
//     must be specified"), and otherwise records the new backing;
//   - `info --backing-chain` walks the recorded backings and fails on a link whose file
//     is not there, which plain `info` does not.
//
// Without that, an assertion on the argv alone would pass for a rebase that pointed every
// layer at the wrong parent.
type fakeRunner struct {
	mu      sync.Mutex
	files   *fakeFiles
	runs    [][]string
	backing map[string]string
	// corrupt marks images whose qcow2 header carries the corrupt bit. It is per image
	// and not a global flag because what matters is that a *layer under the tip* can be
	// the corrupt one: a guest reads through all of them.
	corrupt map[string]bool
	// chainErr, when set, fails the whole-chain walk only.
	chainErr error
	// rebaseTo, when set, is what every rebase points the image at whatever -b said.
	// It models the measured hazard this package's check exists for: qemu-img
	// repointing a layer at a wrong-but-existing parent in complete silence.
	rebaseTo string
	// failOn, when set, fails every run whose argv contains it — the way a qemu-img that
	// is present and cannot do one particular thing behaves.
	failOn string
	// onRun, when set, runs before every invocation. A restore's time is spent in these
	// processes and in the downloads between them, and the simulated clock moves only
	// when something moves it.
	onRun func()
}

func newRunner(files *fakeFiles) *fakeRunner {
	return &fakeRunner{files: files, backing: map[string]string{}, corrupt: map[string]bool{}}
}

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	if r.onRun != nil {
		r.onRun()
	}
	if r.failOn != "" && strings.Contains(strings.Join(args, " "), r.failOn) {
		return nil, fmt.Errorf("qemu-img: %s: cannot do that here", r.failOn)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs = append(r.runs, append([]string{name}, args...))
	switch {
	case len(args) > 0 && args[0] == "rebase":
		return nil, r.rebase(args)
	case len(args) > 0 && args[0] == "info":
		return r.info(args)
	}
	return nil, fmt.Errorf("fake qemu-img: unknown verb %q", strings.Join(args, " "))
}

func (r *fakeRunner) rebase(args []string) error {
	image := args[len(args)-1]
	if !slices.Contains(args, "-u") {
		return errors.New("fake qemu-img: a rebase that reads clusters is not what this does")
	}
	if !slices.Contains(args, "-F") {
		return errors.New("backing file format must be specified")
	}
	var backing string
	for i, a := range args {
		if a == "-b" && i+1 < len(args) {
			backing = args[i+1]
		}
	}
	if _, ok := r.files.files[image]; !ok {
		return fmt.Errorf("Could not open '%s'", image)
	}
	if r.rebaseTo != "" {
		backing = r.rebaseTo
	}
	r.backing[image] = backing
	return nil
}

func (r *fakeRunner) info(args []string) ([]byte, error) {
	image := args[len(args)-1]
	if _, ok := r.files.files[image]; !ok {
		return nil, fmt.Errorf("Could not open '%s': No such file or directory", image)
	}
	one := func(path string) map[string]any {
		m := map[string]any{
			"format": "qcow2", "virtual-size": virtualSize, "filename": path,
			"full-backing-filename": r.backing[path],
		}
		if r.corrupt[path] {
			// The shape QEMU really answers with: the flag lives under
			// format-specific.data, not at the top level.
			m["format-specific"] = map[string]any{"data": map[string]any{"corrupt": true}}
		}
		return m
	}
	if !slices.Contains(args, "--backing-chain") {
		return json.Marshal(one(image))
	}
	if r.chainErr != nil {
		return nil, r.chainErr
	}
	var chain []map[string]any
	for path := image; path != ""; path = r.backing[path] {
		if _, ok := r.files.files[path]; !ok {
			// What the real one does and plain `info` does not: it opens every
			// backing file, so a dangling link is exit 1.
			return nil, fmt.Errorf("Could not open backing file: Could not open '%s'", path)
		}
		chain = append(chain, one(path))
	}
	return json.Marshal(chain)
}

func (r *fakeRunner) commands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.runs))
	for _, run := range r.runs {
		out = append(out, strings.Join(run, " "))
	}
	return out
}

type fakeKeys struct {
	keys map[string]agent.VolumeKeys
	err  error
}

func (k *fakeKeys) VolumeKeys(_ context.Context, volumeID string) (agent.VolumeKeys, error) {
	if k.err != nil {
		return agent.VolumeKeys{}, k.err
	}
	got, ok := k.keys[volumeID]
	if !ok {
		return agent.VolumeKeys{}, errors.New("no such volume")
	}
	return got, nil
}

// deadStore is a bucket that cannot be reached at all — the case that must never be read
// as "this volume is new".
type deadStore struct {
	objectstore.Store
	err error
}

func (d *deadStore) Head(_ context.Context, _ string) (objectstore.ObjectInfo, error) {
	return objectstore.ObjectInfo{}, d.err
}

// world is one volume, its key, a bucket, and a host that holds nothing.
type world struct {
	t     *testing.T
	rec   *recovery.Recoverer
	store *sim.ObjectStore
	files *fakeFiles
	run   *fakeRunner
	keys  *fakeKeys
	kms   crypto.KMS
	vol   string
	enc   *crypto.Encryption
	// commits and layers are the published chain, oldest first, with plain[i] the bytes
	// commit i's layer holds.
	commits []string
	layers  []string
	plain   [][]byte
}

func newWorld(t *testing.T, depth int) *world {
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
	volumeID := ids.New().String()
	// Wrapped under the volume that carries it: a DEK is now custody-bound, so a wrap
	// minted for one volume does not unwrap for another.
	wrapped, err := kms.WrapDEK(rand.Reader, dek, uuid.MustParse(volumeID))
	if err != nil {
		t.Fatalf("wrapping it: %v", err)
	}
	enc, err := crypto.NewEncryption(dek, uuid.MustParse(volumeID))
	if err != nil {
		t.Fatalf("binding the key: %v", err)
	}
	files := newFiles()
	w := &world{
		t: t, store: sim.NewObjectStore(), files: files, run: newRunner(files),
		kms: kms, vol: volumeID, enc: enc,
		keys: &fakeKeys{keys: map[string]agent.VolumeKeys{volumeID: {
			VolumeID: volumeID, DEKWrapped: wrapped, KEKID: kms.KEKID(), DEKKeyID: dek.KeyID,
		}}},
	}
	w.rec = recovery.New(root, qemuImg, w.store, kms, w.keys, files, w.run)
	for range depth {
		w.publish()
	}
	return w
}

// publish adds one commit to the bucket the way the Agent that owned this volume would
// have: real layers, real manifests, a real CAS on HEAD.
func (w *world) publish() {
	w.t.Helper()
	plain := make([]byte, 4096*(len(w.plain)+1))
	if _, err := rand.Read(plain); err != nil {
		w.t.Fatalf("drawing a layer: %v", err)
	}
	commitID, layerID := ids.New().String(), ids.New().String()
	if _, err := commit.Publish(w.t.Context(), w.store, w.enc, bytes.NewReader(plain), commit.Request{
		VolumeID: w.vol, CommitID: commitID, LayerID: layerID,
		Epoch: 7, VirtualSize: virtualSize,
	}); err != nil {
		w.t.Fatalf("publishing: %v", err)
	}
	w.commits = append(w.commits, commitID)
	w.layers = append(w.layers, layerID)
	w.plain = append(w.plain, plain)
}

// image is where the i-th layer of the chain must land.
func (w *world) image(i int) string { return qcow.LayerImage(root, w.vol, w.layers[i]) }

// TestRestoreRebuildsAOneCommitChain is the thinnest real answer: a host that holds
// nothing is handed a volume with one published commit and ends up with the layer's
// plaintext on disk, backed by nothing, named as the base for the new tip.
func TestRestoreRebuildsAOneCommitChain(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 1)

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err != nil {
		t.Fatalf("restoring: %v", err)
	}
	switch {
	case got.Base != w.image(0):
		t.Errorf("the new tip would be built over %q, want %q", got.Base, w.image(0))
	case got.VirtualSize != virtualSize:
		t.Errorf("the chain reconstructs %d bytes, want %d", got.VirtualSize, virtualSize)
	case got.HeadCommitID != w.commits[0]:
		t.Errorf("restored to commit %q, want HEAD's %q", got.HeadCommitID, w.commits[0])
	}
	if !bytes.Equal(w.files.content(w.image(0)), w.plain[0]) {
		t.Error("the layer on disk is not the bytes that were published")
	}
	// A chain of one is never rebased: it is the root, and a root that claims a backing
	// file is a clone lineage the commit protocol does not express.
	want := []string{
		qemuImg + " info --output=json " + w.image(0),
		qemuImg + " info --output=json --backing-chain " + w.image(0),
	}
	if got := w.run.commands(); !slices.Equal(got, want) {
		t.Errorf("qemu-img was run as\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestRestoreRebuildsAThreeCommitChain is the argv contract. Each layer is repointed at
// where its parent actually landed, oldest first, with the backing format spelled out —
// and the whole thing is walked once at the end.
func TestRestoreRebuildsAThreeCommitChain(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err != nil {
		t.Fatalf("restoring: %v", err)
	}
	if got.Base != w.image(2) {
		t.Errorf("the new tip would be built over %q, want the newest layer %q", got.Base, w.image(2))
	}
	for i := range 3 {
		if !bytes.Equal(w.files.content(w.image(i)), w.plain[i]) {
			t.Errorf("layer %d on disk is not the bytes that were published", i)
		}
	}
	want := []string{
		qemuImg + " info --output=json " + w.image(0),
		qemuImg + " rebase -u -f qcow2 -b " + w.image(0) + " -F qcow2 " + w.image(1),
		qemuImg + " info --output=json " + w.image(1),
		qemuImg + " rebase -u -f qcow2 -b " + w.image(1) + " -F qcow2 " + w.image(2),
		qemuImg + " info --output=json " + w.image(2),
		qemuImg + " info --output=json --backing-chain " + w.image(2),
	}
	if got := w.run.commands(); !slices.Equal(got, want) {
		t.Errorf("qemu-img was run as\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	// Oldest first, and every commit recorded in the file qcow.Manager reads — that
	// record is the only thing that can vouch for these files afterwards, because a
	// rebased layer no longer hashes to the object it came from.
	if got := recorded(w); !slices.Equal(got, w.commits) {
		t.Errorf("this host remembers holding %v, want %v", got, w.commits)
	}
}

// TestRestoreRefusesAChainItCannotAssemble is the whole point of the package: every way
// the bucket can fail to answer is a refusal, and none of them is "the volume is new".
func TestRestoreRefusesAChainItCannotAssemble(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// breaks makes the world unrestorable.
		breaks func(*world)
		// size is what the catalog claims, zero meaning it agrees with the bucket.
		size int64
		want string
		is   error
	}{
		{
			name: "a manifest is missing",
			breaks: func(w *world) {
				drop(w, commit.ManifestKey(w.vol, w.commits[1]))
			},
			want: "commits/",
		},
		{
			name: "a layer is missing",
			breaks: func(w *world) {
				m := manifest(w, w.commits[1])
				drop(w, m.Layer.ObjectKey)
			},
			want: "downloading",
		},
		{
			name: "a layer came back corrupt",
			breaks: func(w *world) {
				m := manifest(w, w.commits[1])
				body, err := w.store.Get(w.t.Context(), m.Layer.ObjectKey)
				if err != nil {
					w.t.Fatalf("reading the layer: %v", err)
				}
				body = slices.Clone(body)
				body[len(body)/2] ^= 0x40
				if _, err := w.store.Put(w.t.Context(), m.Layer.ObjectKey, body, objectstore.PutOptions{}); err != nil {
					w.t.Fatalf("rewriting the layer: %v", err)
				}
			},
			want: "hashes to",
			is:   commit.ErrCorrupt,
		},
		{
			name: "the bucket cannot be reached",
			breaks: func(w *world) {
				w.rec = recovery.New(root, qemuImg, &deadStore{Store: w.store, err: objectstore.ErrBucketNotFound},
					w.kms, w.keys, w.files, w.run)
			},
			want: "reading the HEAD",
			is:   objectstore.ErrBucketNotFound,
		},
		{
			name:   "the catalog and the bucket disagree about the size",
			breaks: func(*world) {},
			size:   virtualSize * 2,
			want:   "bytes at commit",
		},
		{
			name: "two commits of one volume reconstruct different sizes",
			breaks: func(w *world) {
				m := manifest(w, w.commits[1])
				m.VirtualSize = virtualSize * 2
				republish(w, m)
			},
			want: "reconstructs",
		},
		{
			name: "the history is a cycle",
			breaks: func(w *world) {
				m := manifest(w, w.commits[0])
				m.ParentCommitID = w.commits[2]
				republish(w, m)
			},
			want: "twice",
		},
		{
			name: "two commits name one layer",
			breaks: func(w *world) {
				m := manifest(w, w.commits[1])
				m.Layer = manifest(w, w.commits[0]).Layer
				republish(w, m)
			},
			want: "naming layer",
		},
		{
			name: "the volume's key cannot be had",
			breaks: func(w *world) {
				w.keys.err = errors.New("permission_denied: not this volume's host")
			},
			want: "not this volume's host",
		},
		{
			name: "the rebuilt chain does not resolve",
			breaks: func(w *world) {
				w.run.chainErr = errors.New("Could not open backing file")
			},
			want: "walking the chain",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t, 3)
			tt.breaks(w)
			size := tt.size
			if size == 0 {
				size = virtualSize
			}

			got, err := w.rec.Restore(t.Context(), w.vol, size)
			if err == nil {
				t.Fatal("it rebuilt a chain it could not assemble")
			}
			if !errors.Is(err, recovery.ErrIncomplete) {
				t.Errorf("the caller cannot tell this from a volume that is new: %v", err)
			}
			if errors.Is(err, commit.ErrNoHead) {
				t.Error("a broken chain was reported as a volume that has never published")
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Errorf("the error loses %v: %v", tt.is, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the error does not say what went wrong: %v", err)
			}
			if got != (qcow.Restored{}) {
				t.Errorf("a refusal handed back %+v", got)
			}
			// Nothing half-downloaded is left where a layer belongs.
			for _, name := range w.files.names() {
				if strings.HasSuffix(name, ".part") {
					t.Errorf("a refused restore left %s behind", name)
				}
			}
		})
	}
}

// TestRestoreDoesNotTrustAFileNothingVouchesFor. A layer file can be on disk without the
// record naming it: an interrupted restore, a sweep that never ran, a copy somebody made.
// The path is derived from the layer id, so it looks exactly like the real thing, and the
// only reason to believe it is a record that says this commit's layer is this layer.
func TestRestoreDoesNotTrustAFileNothingVouchesFor(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 2)
	if err := w.files.MkdirAll(qcow.LayersDir(root, w.vol)); err != nil {
		t.Fatalf("making the layer directory: %v", err)
	}
	f, err := w.files.Create(w.image(0))
	if err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	if _, err := f.Write([]byte("not this volume's first layer")); err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	// A record for this commit that names some *other* layer — the shape a stale state
	// file has after a chain was rebuilt differently somewhere else.
	if err := qcow.WriteState(w.files, root, w.vol, qcow.State{
		Commits: []qcow.CommitLayer{{CommitID: w.commits[0], LayerID: ids.New().String()}},
	}); err != nil {
		t.Fatalf("writing the state: %v", err)
	}

	if _, err := w.rec.Restore(t.Context(), w.vol, virtualSize); err != nil {
		t.Fatalf("restoring: %v", err)
	}
	if !bytes.Equal(w.files.content(w.image(0)), w.plain[0]) {
		t.Error("a layer file no record vouches for was left in the chain")
	}
}

// TestRestoreIsIdempotent. A volume can be placed here, released, and placed here again;
// the second restore must find everything local and must not grow the record by the whole
// chain each time. The layers are dropped from the bucket after the first pass so that a
// download would fail outright.
func TestRestoreIsIdempotent(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)
	if _, err := w.rec.Restore(t.Context(), w.vol, virtualSize); err != nil {
		t.Fatalf("restoring: %v", err)
	}
	for i := range 3 {
		drop(w, manifest(w, w.commits[i]).Layer.ObjectKey)
	}

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err != nil {
		t.Fatalf("restoring a second time: %v", err)
	}
	if got.Base != w.image(2) {
		t.Errorf("the second restore would build over %q, want %q", got.Base, w.image(2))
	}
	if got := recorded(w); !slices.Equal(got, w.commits) {
		t.Errorf("two restores left this host remembering %v, want %v", got, w.commits)
	}
}

// TestRestoreRefusesALayerWhoseHeaderDoesNotNameItsParent is the case nothing else in
// this package catches. The chain still walks — the wrong parent is a real qcow2 that is
// really there, and it is the right length — so the whole-chain check at the end passes
// and a guest would boot a disk assembled out of another chain's layer.
func TestRestoreRefusesALayerWhoseHeaderDoesNotNameItsParent(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 2)
	decoy := qcow.LayerImage(root, w.vol, ids.New().String())
	if err := w.files.MkdirAll(qcow.LayersDir(root, w.vol)); err != nil {
		t.Fatalf("making the layer directory: %v", err)
	}
	f, err := w.files.Create(decoy)
	if err != nil {
		t.Fatalf("planting the decoy: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("planting the decoy: %v", err)
	}
	w.run.rebaseTo = decoy

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err == nil {
		t.Fatal("a chain assembled out of the wrong parent was handed over")
	}
	if !errors.Is(err, recovery.ErrIncomplete) {
		t.Errorf("the caller cannot tell this from a volume that is new: %v", err)
	}
	if !strings.Contains(err.Error(), "is backed by") {
		t.Errorf("the error does not say what went wrong: %v", err)
	}
	if got != (qcow.Restored{}) {
		t.Errorf("a refusal handed back %+v", got)
	}
}

// TestRestoreSaysTheVolumeIsNewWhenItHasNoHead. This is the one case that is not a
// refusal, and it must be distinguishable by errors.Is alone: the caller creates an empty
// layer here and must never do so anywhere else.
func TestRestoreSaysTheVolumeIsNewWhenItHasNoHead(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 0)

	got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if !errors.Is(err, commit.ErrNoHead) {
		t.Fatalf("a volume with no HEAD answered %v", err)
	}
	if errors.Is(err, recovery.ErrIncomplete) {
		t.Error("a new volume is reported as a chain that could not be rebuilt")
	}
	if got != (qcow.Restored{}) {
		t.Errorf("a volume with nothing published handed back %+v", got)
	}
	switch {
	case len(w.run.commands()) != 0:
		t.Errorf("a new volume ran qemu-img: %v", w.run.commands())
	case len(w.files.names()) != 0:
		t.Errorf("a new volume touched the disk: %v", w.files.names())
	}
}

// TestRestoreDoesNotRefetchWhatThisHostAlreadyHolds. The durable record plus the file is
// what makes a re-placement onto a host that has most of the chain cheap; both layers are
// removed from the bucket so that a download would fail outright.
//
// The held layers are also repointed, and that is the half worth having a test for: a
// layer this host published sits under whatever parent it had then, and nothing in the
// argv or in the download says whether its header now names the parent this restore put
// on this disk. Only the check after the rebase does.
func TestRestoreDoesNotRefetchWhatThisHostAlreadyHolds(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)
	for i := range 2 {
		plant(w, i)
		drop(w, manifest(w, w.commits[i]).Layer.ObjectKey)
	}

	if _, err := w.rec.Restore(t.Context(), w.vol, virtualSize); err != nil {
		t.Fatalf("restoring: %v", err)
	}
	for i := range 2 {
		if !bytes.Equal(w.files.content(w.image(i)), w.plain[i]) {
			t.Errorf("layer %d, which this host already held, was replaced", i)
		}
	}
	for i := 1; i < 3; i++ {
		rebase := qemuImg + " rebase -u -f qcow2 -b " + w.image(i-1) + " -F qcow2 " + w.image(i)
		if !slices.Contains(w.run.commands(), rebase) {
			t.Errorf("layer %d was not repointed at its parent: %v", i, w.run.commands())
		}
	}
}

// plant puts the i-th layer's plaintext on disk without a restore having fetched it, and
// records it the way a publish by this host would have.
func plant(w *world, i int) {
	w.t.Helper()
	if err := w.files.MkdirAll(qcow.LayersDir(root, w.vol)); err != nil {
		w.t.Fatalf("making the layer directory: %v", err)
	}
	f, err := w.files.Create(w.image(i))
	if err != nil {
		w.t.Fatalf("planting layer %d: %v", i, err)
	}
	if _, err := f.Write(w.plain[i]); err != nil {
		w.t.Fatalf("planting layer %d: %v", i, err)
	}
	if err := f.Close(); err != nil {
		w.t.Fatalf("planting layer %d: %v", i, err)
	}
	st, err := qcow.ReadState(w.files, root, w.vol)
	if err != nil {
		w.t.Fatalf("reading the state of volume %s: %v", w.vol, err)
	}
	st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: w.commits[i], LayerID: w.layers[i]})
	if err := qcow.WriteState(w.files, root, w.vol, st); err != nil {
		w.t.Fatalf("recording layer %d: %v", i, err)
	}
}

// recorded is the commits this host's state.json says it holds, oldest first.
func recorded(w *world) []string {
	w.t.Helper()
	st, err := qcow.ReadState(w.files, root, w.vol)
	if err != nil {
		w.t.Fatalf("reading the state of volume %s: %v", w.vol, err)
	}
	out := make([]string, 0, len(st.Commits))
	for _, c := range st.Commits {
		out = append(out, c.CommitID)
	}
	return out
}

// TestRestoreRefusesALayerRecordedButNotOnDisk. The record alone is not enough: a state
// file that outlived the files it vouches for must send the restore back to the bucket.
func TestRestoreRefusesALayerRecordedButNotOnDisk(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 2)
	plant(w, 0)
	if err := w.files.Remove(w.image(0)); err != nil {
		t.Fatalf("removing the layer under the record: %v", err)
	}

	if _, err := w.rec.Restore(t.Context(), w.vol, virtualSize); err != nil {
		t.Fatalf("restoring: %v", err)
	}
	if !bytes.Equal(w.files.content(w.image(0)), w.plain[0]) {
		t.Error("a layer the record vouched for was not downloaded when it was not there")
	}
}

// drop removes an object from the bucket.
func drop(w *world, key string) {
	w.t.Helper()
	if err := w.store.Delete(w.t.Context(), key); err != nil {
		w.t.Fatalf("dropping %s: %v", key, err)
	}
}

// manifest reads one published commit back.
func manifest(w *world, commitID string) commit.Manifest {
	w.t.Helper()
	m, err := commit.ReadManifest(w.t.Context(), w.store, w.vol, commitID)
	if err != nil {
		w.t.Fatalf("reading commit %s: %v", commitID, err)
	}
	return m
}

// republish replaces a manifest in the bucket with a doctored one, framing and PUTting the
// object itself rather than going through commit.WriteManifest — which validates every
// identifier now, so a doctored manifest cannot be planted through the writer at all.
// What is asserted is what a restore does when the bucket holds something no version of
// this code produced.
func republish(w *world, m commit.Manifest) {
	w.t.Helper()
	m.FormatVersion = framed.FormatVersion
	body, err := json.Marshal(m)
	if err != nil {
		w.t.Fatalf("marshalling commit %s: %v", m.CommitID, err)
	}
	drop(w, commit.ManifestKey(w.vol, m.CommitID))
	if _, err := w.store.Put(w.t.Context(), commit.ManifestKey(w.vol, m.CommitID),
		framed.Frame(body), objectstore.PutOptions{}); err != nil {
		w.t.Fatalf("rewriting commit %s: %v", m.CommitID, err)
	}
}

// plantHead writes a HEAD the compare-and-set would refuse, the same way and for the same
// reason as republish.
func plantHead(w *world, commitID string) {
	w.t.Helper()
	body, err := json.Marshal(commit.Head{
		FormatVersion: framed.FormatVersion, VolumeID: w.vol, CommitID: commitID,
	})
	if err != nil {
		w.t.Fatalf("marshalling HEAD: %v", err)
	}
	// Not dropped first: a volume with no history has no HEAD to drop, and this helper is
	// used by both cases.
	if _, err := w.store.Put(w.t.Context(), commit.HeadKey(w.vol), framed.Frame(body),
		objectstore.PutOptions{}); err != nil {
		w.t.Fatalf("planting HEAD: %v", err)
	}
}

// TestCurrentIsAHeadReadAndNothingElse. qcow.Open asks this on every open of a chain that
// is already on disk, to find out whether the published history has moved past it, so it
// has to be cheap: one small object, no layers, no qemu-img.
func TestCurrentIsAHeadReadAndNothingElse(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 2)

	got, err := w.rec.Current(t.Context(), w.vol)
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if want := w.commits[len(w.commits)-1]; got != want {
		t.Errorf("Current = %q, want the newest commit %q", got, want)
	}
	if cmds := w.run.commands(); len(cmds) != 0 {
		t.Errorf("asking what the published history is ran %v", cmds)
	}
}

// TestCurrentSaysWhenAVolumeHasNeverPublished, which is not an error to its caller: it is
// the ordinary state of a volume created a second ago, and the answer that lets one be
// born empty.
func TestCurrentSaysWhenAVolumeHasNeverPublished(t *testing.T) {
	t.Parallel()
	// depth 0: a volume with no commits, so there is no HEAD to drop.
	w := newWorld(t, 0)

	if _, err := w.rec.Current(t.Context(), w.vol); !errors.Is(err, commit.ErrNoHead) {
		t.Fatalf("want ErrNoHead, got %v", err)
	}
}

// TestAbsentAnswersThatNothingIsPublished is the deployment with no object store: nothing
// was ever published there, so every volume is new. The case it does NOT cover — a host
// that published and is now started against no store — is caught by qcow.Open reading
// this host's own state.json.
func TestAbsentAnswersThatNothingIsPublished(t *testing.T) {
	t.Parallel()
	var a recovery.Absent

	if _, err := a.Current(t.Context(), "any"); !errors.Is(err, commit.ErrNoHead) {
		t.Errorf("Current = %v, want a wrapped ErrNoHead", err)
	}
	if _, err := a.RestoreFrom(t.Context(), qcow.Lineage{VolumeID: "any"}, 1<<30); !errors.Is(err, commit.ErrNoHead) {
		t.Errorf("RestoreFrom = %v, want a wrapped ErrNoHead", err)
	}
	// A clone is the one thing it must not answer that way: "born empty" for a volume
	// advertised as a copy is DEV-0007, served blank with no error anywhere.
	cloned := qcow.Lineage{VolumeID: "any", Ancestry: []qcow.Ancestor{{VolumeID: "parent", CommitID: "commit"}}}
	if _, err := a.RestoreFrom(t.Context(), cloned, 1<<30); err == nil || errors.Is(err, commit.ErrNoHead) {
		t.Errorf("RestoreFrom for a clone = %v, want a refusal and not ErrNoHead", err)
	}
}

// TestRestoreRefusesRatherThanLeavingHalfAChain walks the failures a rebuild meets when
// what it is reading is a bucket — which is a place other things write to — and asserts
// the same thing about every one of them: no chain, not half a chain.
//
// Half a volume is worse than no volume, because a guest will boot it. A missing layer in
// the middle is a hole the guest reads as zeros; a layer that will not open is a chain
// whose depth is right and whose bytes are somebody else's.
func TestRestoreRefusesRatherThanLeavingHalfAChain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		break_ func(w *world)
		wants  string
	}{
		{
			name:   "a layer object that is not in the bucket",
			break_: func(w *world) { drop(w, manifest(w, w.commits[0]).Layer.ObjectKey) },
			wants:  "downloading",
		},
		{
			name: "a layer whose bytes came back changed",
			break_: func(w *world) {
				m := manifest(w, w.commits[0])
				body, err := w.store.Get(w.t.Context(), m.Layer.ObjectKey)
				if err != nil {
					w.t.Fatal(err)
				}
				corrupt := bytes.Clone(body)
				corrupt[len(corrupt)/2] ^= 0x20
				drop(w, m.Layer.ObjectKey)
				if _, err := w.store.Put(w.t.Context(), m.Layer.ObjectKey, corrupt, objectstore.PutOptions{}); err != nil {
					w.t.Fatal(err)
				}
			},
			wants: "hashes to",
		},
		{
			name:   "the key material cannot be had",
			break_: func(w *world) { w.keys.err = errors.New("permission_denied: not this volume's host") },
			wants:  "not this volume's host",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t, 2)
			tt.break_(w)

			got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
			if err == nil {
				t.Fatalf("a chain that cannot be rebuilt came back as %+v", got)
			}
			if !strings.Contains(err.Error(), tt.wants) {
				t.Errorf("the refusal does not say what went wrong: %v", err)
			}
			// And nothing was left behind for a later boot to find and believe.
			if got.Base != "" {
				t.Errorf("a failed restore still named a base: %q", got.Base)
			}
		})
	}
}

// TestRestoreRefusesWhenTheDiskOrQemuImgWillNotCooperate covers the other half of "no
// chain, not half a chain": the failures that happen on *this* host rather than in the
// bucket.
//
// The rebase is the one worth naming. A downloaded layer's header still points at the
// absolute path of the host that created it, so every layer is repointed at where its
// parent actually landed; a rebase that failed and was ignored would leave a chain that
// opens on the machine that wrote it and nowhere else — which is discovered by the guest
// that needed it, on the day the original host is gone.
func TestRestoreRefusesWhenTheDiskOrQemuImgWillNotCooperate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		break_ func(w *world)
		wants  string
	}{
		{
			name:   "the layer cannot be written to disk",
			break_: func(w *world) { w.files.createErr = errors.New("no space left on device") },
			wants:  "creating",
		},
		{
			name:   "qemu-img will not repoint a layer at its parent",
			break_: func(w *world) { w.run.failOn = "rebase" },
			wants:  "repointing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t, 2)
			tt.break_(w)

			got, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
			if err == nil {
				t.Fatalf("a chain that could not be built came back as %+v", got)
			}
			if !errors.Is(err, recovery.ErrIncomplete) {
				t.Errorf("want ErrIncomplete, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.wants) {
				t.Errorf("the refusal does not say what went wrong: %v", err)
			}
			if got.Base != "" {
				t.Errorf("a failed restore still named a base: %q", got.Base)
			}
		})
	}
}

// TestASameHostCloneReadsTheParentsLayersOffTheLocalDisk is v6 §19's "reutilizar
// files/cache locales" and §32's eleventh success criterion.
//
// The parent's layers are already on this disk. Fetching them again costs one download per
// layer of the whole chain for bytes that are feet away — and nothing about the resulting
// chain would look different, which is why this asserts on the object store rather than on
// the chain: every layer object is REMOVED from the bucket before the clone is restored.
// A clone that downloads cannot pass; one that copies from its parent does not notice.
//
// Copied rather than linked or shared: `qemu-img rebase -u` rewrites a layer's header to
// point at its local parent, so a hard link would rewrite the parent volume's own file,
// which is the one thing §19 forbids.
func TestASameHostCloneReadsTheParentsLayersOffTheLocalDisk(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)
	ctx := t.Context()

	// The parent is restored here first, which is what "same host" means.
	if _, err := w.rec.Restore(ctx, w.vol, virtualSize); err != nil {
		t.Fatalf("restoring the parent: %v", err)
	}

	// Every layer object goes. The manifests and HEAD stay: a clone must still be told
	// what its parent's history is, and that is a read this test does not begrudge.
	objs, err := w.store.List(ctx, "layers/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		t.Fatal("the fixture published no layer objects")
	}
	for _, o := range objs {
		if err := w.store.Delete(ctx, o.Key); err != nil {
			t.Fatal(err)
		}
	}

	clone := ids.New().String()
	w.keys.keys[clone] = cloneKeys(t, w, clone)

	got, err := w.rec.RestoreFrom(ctx, qcow.Lineage{
		VolumeID: clone, Ancestry: []qcow.Ancestor{{VolumeID: w.vol, CommitID: w.commits[len(w.commits)-1]}},
	}, virtualSize)
	if err != nil {
		t.Fatalf("the clone went to the object store for layers this host already holds: %v", err)
	}
	if got.Base == "" {
		t.Fatal("the clone was restored to no base at all")
	}
	for _, layerID := range w.layers {
		there, ferr := w.files.Exists(qcow.LayerImage(root, clone, layerID))
		if ferr != nil || !there {
			t.Fatalf("layer %s is not under the clone: %v", layerID, ferr)
		}
	}
}

// cloneKeys is what controlplane.Clone hands a clone: the parent's DEK rewrapped under the
// child's id. The same key bytes, bound to a different volume — which is why a clone can
// open its parent's layers at all (§10).
func cloneKeys(t *testing.T, w *world, clone string) agent.VolumeKeys {
	t.Helper()
	parent := w.keys.keys[w.vol]
	dek, err := w.kms.UnwrapDEK(parent.DEKWrapped, parent.DEKKeyID, uuid.MustParse(w.vol))
	if err != nil {
		t.Fatalf("unwrapping the parent's DEK: %v", err)
	}
	wrapped, err := w.kms.WrapDEK(rand.Reader, dek, uuid.MustParse(clone))
	if err != nil {
		t.Fatalf("rewrapping it for the clone: %v", err)
	}
	return agent.VolumeKeys{VolumeID: clone, DEKWrapped: wrapped, KEKID: w.kms.KEKID(), DEKKeyID: dek.KeyID}
}

// TestARebuiltChainWithTheCorruptFlagIsRefusedBeforeAGuestSeesIt.
//
// The qcow2 corrupt bit rides through the object store intact: QEMU sets it inside the
// image, so the sealed layer's SHA-256 matches its manifest and every integrity check on
// the way back passes. The bytes are the bytes that were published; what they say is that
// QEMU already found an inconsistency it could not resolve.
//
// The five questions §29 asks: the visible commit is unchanged, the local state is a
// half-built chain the next attempt replaces, no remote object is touched, the volume is
// refused rather than served, and no confirmed commit is lost — this is a read path.
//
// It was found by auditing the boundary rather than by a failure: the same corrupt layer
// is refused on the *second* open, because qcow.Open's adopt-a-local-chain branch walks
// the chain and checks the flag, and was served on the first, because nothing on the
// rebuild path looked. Detection after the guest is the ordering §29 exists to forbid.
func TestARebuiltChainWithTheCorruptFlagIsRefusedBeforeAGuestSeesIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 3)

	// Not the tip: a guest reads through every layer, so the one under it is the case
	// that a tip-only check would miss.
	w.run.corrupt[w.image(0)] = true

	_, err := w.rec.Restore(t.Context(), w.vol, virtualSize)
	if err == nil {
		t.Fatal("a chain carrying the qcow2 corrupt flag was handed back ready to boot")
	}
	if !errors.Is(err, recovery.ErrIncomplete) {
		t.Fatalf("refused with %v, want a wrapped ErrIncomplete", err)
	}
	if !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}
