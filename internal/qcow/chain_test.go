package qcow_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/qcow"
)

// fakeRunner stands in for `qemu-img`. It records every argv it was handed, which is
// what the assertions below are on: the contract with qemu-img is the command line, and
// a test that only checked the returned Chain would pass for a manager that created the
// image with the wrong size, the wrong format, or in the wrong place.
type fakeRunner struct {
	mu   sync.Mutex
	runs [][]string
	// info is what `qemu-img info --output=json` answers.
	info string
	// chain is what `qemu-img info --output=json --backing-chain` answers: a JSON
	// array, one element per layer, top first. Unset means "a chain of exactly this
	// one image", which is what every test that is not about the walk wants.
	chain string
	// version is what `qemu-img --version` answers. The Manager runs it once at
	// start-up; nothing else in the package does.
	version string
	// err, when set, is what every run fails with.
	err error
	// chainErr, when set, is what only `info --backing-chain` fails with. It models the
	// one behaviour the walk exists for: a tip whose backing file is gone passes plain
	// `info` with exit 0 and fails the walk with exit 1 (both measured against the
	// pinned 11.1.1), so a fake that failed both would keep a plain-info build green.
	chainErr error
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = append(f.runs, append([]string{name}, args...))
	if f.err != nil {
		return nil, f.err
	}
	switch {
	case len(args) > 0 && args[0] == "info":
		for _, a := range args {
			if a != "--backing-chain" {
				continue
			}
			if f.chainErr != nil {
				return nil, f.chainErr
			}
			if f.chain != "" {
				return []byte(f.chain), nil
			}
			return []byte("[" + f.info + "]"), nil
		}
		return []byte(f.info), nil
	case len(args) > 0 && args[0] == "--version":
		return []byte(f.version), nil
	}
	return nil, nil
}

// reset forgets what has been run so far.
func (f *fakeRunner) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = nil
}

func (f *fakeRunner) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.runs))
	for _, r := range f.runs {
		out = append(out, strings.Join(r, " "))
	}
	return out
}

// fakePaths is a filesystem of names and of the one file this package's contents matter
// for, `active/current`. Images are names only — what is inside them is qemu-img's
// business, and the fake runner answers for that.
type fakePaths struct {
	mu       sync.Mutex
	made     []string
	present  map[string]bool
	files    map[string]string
	sizes    map[string]int64
	statFail error
	// removed is what a test has unlinked; see Size.
	removed map[string]bool
	// log records writes in order, so a test can assert that the pointer moved before
	// QEMU was told to switch rather than only that both happened.
	log *[]string
}

func newPaths(present ...string) *fakePaths {
	p := &fakePaths{present: map[string]bool{}, files: map[string]string{}, sizes: map[string]int64{}, removed: map[string]bool{}}
	for _, name := range present {
		p.present[name] = true
	}
	return p
}

// withPointer is a volume whose `active/current` already names a layer.
func newPathsAt(image string) *fakePaths {
	p := newPaths(image)
	p.present[qcow.ActivePointer(root, vol)] = true
	p.files[qcow.ActivePointer(root, vol)] = image
	return p
}

func (p *fakePaths) MkdirAll(dir string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.made = append(p.made, dir)
	return nil
}

func (p *fakePaths) Exists(path string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statFail != nil {
		return false, p.statFail
	}
	return p.present[path], nil
}

// Size fails for a path a test has removed. It used to answer (0, nil) for everything,
// which made "this layer is gone" and "this layer is empty" the same answer here and
// different answers in production — and a test whose subject is a vanished file cannot be
// written against a fake with no way to say a file vanished.
//
// Removal is declared rather than derived from `present`, and that is the honest shape:
// this fake never learns about the layers `qemu-img create` makes, because the runner is
// a fake too, so absence from `present` means "nobody mentioned it" and not "it is not
// there". Only `remove` means the second.
func (p *fakePaths) Size(path string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statFail != nil {
		return 0, p.statFail
	}
	if p.removed[path] {
		return 0, fmt.Errorf("stat %s: %w", path, fs.ErrNotExist)
	}
	return p.sizes[path], nil
}

// remove is a file being unlinked out from under this Agent. It is the same unlink the
// Agent itself performs, which is why it goes through Remove.
func (p *fakePaths) remove(path string) { _ = p.Remove(path) }

// List names the files this fake holds directly under dir.
func (p *fakePaths) List(dir string) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statFail != nil {
		return nil, p.statFail
	}
	var names []string
	for path := range p.present {
		if filepath.Dir(path) == dir {
			names = append(names, filepath.Base(path))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (p *fakePaths) Remove(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removed[path] = true
	delete(p.present, path)
	delete(p.files, path)
	return nil
}

func (p *fakePaths) ReadFile(path string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	body, ok := p.files[path]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", path)
	}
	return []byte(body), nil
}

func (p *fakePaths) WriteAtomic(path string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.present[path], p.files[path] = true, string(data)
	if p.log != nil {
		*p.log = append(*p.log, "wrote "+path+" = "+string(data))
	}
	return nil
}

func (p *fakePaths) pointer() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.files[qcow.ActivePointer(root, vol)]
}

// infoJSON is a trimmed `qemu-img info --output=json` answer.
func infoJSON(format string, size int64, corrupt bool) string {
	return fmt.Sprintf(`{"virtual-size": %d, "filename": "x", "format": %q,
	  "format-specific": {"type": "qcow2", "data": {"corrupt": %t}}}`, size, format, corrupt)
}

// overlayJSON is what qemu-img says about a layer that has a backing file.
func overlayJSON(size int64, backing string) string {
	return fmt.Sprintf(`{"virtual-size": %d, "filename": "x", "format": "qcow2",
	  "full-backing-filename": %q,
	  "format-specific": {"type": "qcow2", "data": {"corrupt": false}}}`, size, backing)
}

const (
	root = "/var/lib/volume-agent"
	vol  = "0198c0de-0000-7000-8000-00000000cafe"
	size = int64(268435456)
	// A real v7 uuid, not a readable word: layer ids name files the sweep has to be able
	// to place in time, so a fake one that is not a uuid tests a layout production cannot
	// produce.
	layerID = "0198c0de-0000-7000-8000-0000000f1057"
	nextID  = "0198c0de-0000-7000-8000-000000005ec0"
	baseID  = "0198c0de-0000-7000-8000-000000000ba5"
	// headCommit is what HEAD named when the chain was rebuilt.
	headCommit = "0198c0de-0000-7000-8000-00000000c0m1"
)

// fakeRecovery is the object store's answer about a volume's published history — the
// one question that separates "this volume is new" from "this volume's data is on
// somebody else's disk". It records what it was asked, because a guard that is not
// consulted is not a guard and the call is the only observable proof it was.
type fakeRecovery struct {
	res   qcow.Restored
	err   error
	calls []string
	// head is what the object store says the volume's newest commit is. Empty means the
	// volume has never published, which is what every test that is not about staleness
	// wants and is the reason it is the zero value.
	head    string
	headErr error
	// lineage is what the last RestoreFrom was asked for. A clone is served by passing a
	// parent down; a test that only checked the returned chain could not tell a clone
	// from a volume born empty.
	lineage qcow.Lineage
}

func (f *fakeRecovery) RestoreFrom(_ context.Context, l qcow.Lineage, sizeBytes int64) (qcow.Restored, error) {
	f.lineage = l
	f.calls = append(f.calls, fmt.Sprintf("%s/%d", l.VolumeID, sizeBytes))
	return f.res, f.err
}

func (f *fakeRecovery) Current(_ context.Context, volumeID string) (string, error) {
	f.calls = append(f.calls, "current/"+volumeID)
	switch {
	case f.headErr != nil:
		return "", f.headErr
	case f.head == "":
		return "", fmt.Errorf("recovery: volume %s: %w", volumeID, commit.ErrNoHead)
	}
	return f.head, nil
}

// bornEmpty is the ordinary answer for a volume created a second ago: no HEAD, so an
// empty chain is correct.
func bornEmpty() *fakeRecovery {
	return &fakeRecovery{err: fmt.Errorf("recovery: volume %s: %w", vol, commit.ErrNoHead)}
}

// recovered is a volume whose published chain has been rebuilt on this host.
func recovered(base string) *fakeRecovery {
	return &fakeRecovery{res: qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}}
}

// req is the ordinary Open for this volume, with one field varied per test.
func req(mut func(*qcow.OpenRequest)) qcow.OpenRequest {
	r := qcow.OpenRequest{Root: root, Lineage: qcow.Lineage{VolumeID: vol}, SizeBytes: size, NewLayerID: layerID, Recovery: bornEmpty()}
	if mut != nil {
		mut(&r)
	}
	return r
}

func TestPathsAreTheContractWithWhoeverLaunchesQEMU(t *testing.T) {
	t.Parallel()
	// Asserted literally, because these strings are an interface to another repository:
	// spin's runner reads the pointer and builds QEMU's command line from it. Deriving
	// the expectation the same way the code does would assert nothing.
	tests := []struct{ name, got, want string }{
		{"the pointer", qcow.ActivePointer(root, vol), root + "/volumes/" + vol + "/active/current"},
		{"a layer", qcow.LayerImage(root, vol, layerID), root + "/volumes/" + vol + "/layers/" + layerID + ".qcow2"},
		{"the layer directory", qcow.LayersDir(root, vol), root + "/volumes/" + vol + "/layers"},
		{"the socket", qcow.QMPSocket(root, vol), root + "/volumes/" + vol + "/qmp.sock"},
		{"the volume", qcow.VolumeDir(root, vol), root + "/volumes/" + vol},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
	// And back, because a layer met by anything other than the code that created it —
	// a publish after a restart, a sweep, a recovery — has only its filename to learn
	// its identity from, and that identity is in the nonce of every frame it is sealed
	// with.
	if got := qcow.LayerIDOfImage(qcow.LayerImage(root, vol, layerID)); got != layerID {
		t.Errorf("LayerIDOfImage round trip = %q, want %q", got, layerID)
	}
}

func TestOpenCreatesTheFirstLayerAndPointsAtIt(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	p := newPaths()

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(nil))
	if err != nil {
		t.Fatalf("opening a fresh volume: %v", err)
	}
	image := qcow.LayerImage(root, vol, layerID)
	if chain.Active != image {
		t.Errorf("the tip is %q, want %q", chain.Active, image)
	}
	want := fmt.Sprintf("/qemu-img create -f qcow2 %s %d", image, size)
	if got := r.commands(); len(got) != 1 || got[0] != want {
		t.Fatalf("qemu-img was run as %v, want exactly [%q]", got, want)
	}
	// The pointer is the whole contract with the launcher: an image nothing names is an
	// image no VM will ever be started against.
	if got := p.pointer(); got != image {
		t.Errorf("active/current names %q, want %q", got, image)
	}
}

func TestOpenChecksAnImageThatIsAlreadyThere(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)

	tests := []struct {
		name string
		info string
		want string // a substring of the refusal, or "" for success
	}{
		{name: "a good image", info: infoJSON("qcow2", size, false)},
		{name: "the wrong format", info: infoJSON("raw", size, false), want: "not qcow2"},
		{name: "the wrong size", info: infoJSON("qcow2", size/2, false), want: "the catalog says"},
		{name: "the corrupt flag", info: infoJSON("qcow2", size, true), want: "corrupt flag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{info: tt.info}
			chain, err := qcow.Open(t.Context(), r, newPathsAt(image), "/qemu-img", req(nil))

			if tt.want == "" {
				if err != nil {
					t.Fatalf("a valid image was refused: %v", err)
				}
				if chain.SizeBytes != size {
					t.Errorf("size read back as %d, want %d", chain.SizeBytes, size)
				}
			} else {
				if !errors.Is(err, qcow.ErrChainMismatch) {
					t.Fatalf("want ErrChainMismatch, got %v", err)
				}
				if !strings.Contains(err.Error(), tt.want) {
					t.Errorf("the refusal does not say why: %v", err)
				}
			}
			// Whatever the verdict, the image must have been inspected and never
			// re-created: `create` over an image that is already there is a guest's
			// disk replaced with an empty one.
			cmds := r.commands()
			if len(cmds) != 1 || !strings.HasPrefix(cmds[0], "/qemu-img info --output=json ") {
				t.Fatalf("qemu-img was run as %v, want one `info`", cmds)
			}
		})
	}
}

// TestOpenNeverTouchesAnImageQEMUHasOpen is the rule v6 §5 states and the reason
// LiveImage exists at all. It is asserted as "the runner was not called", which is the
// only observable that distinguishes obeying the rule from getting away with it:
// `qemu-img info` on an image QEMU holds fails on a write lock, so a build that ran it
// anyway would refuse a perfectly good volume every time an Agent restarted under a
// running guest.
func TestOpenNeverTouchesAnImageQEMUHasOpen(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	r := &fakeRunner{err: errors.New(`Failed to get shared "write" lock`)}
	p := newPathsAt(image)

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(func(o *qcow.OpenRequest) { o.LiveImage = image }))
	if err != nil {
		t.Fatalf("opening a live chain: %v", err)
	}
	if chain.Active != image {
		t.Errorf("the tip is %q", chain.Active)
	}
	if cmds := r.commands(); len(cmds) != 0 {
		t.Fatalf("an image QEMU has open was handed to qemu-img: %v", cmds)
	}
}

// TestOpenRepairsThePointerFromWhatQEMUHasOpen is the crash window Rotate leaves behind:
// the pointer is moved before QEMU is told to switch, so a crash in between leaves it
// one layer ahead of the guest. Believing it there hands the next boot a layer with
// none of the guest's writes in it — and the guest is running, so nothing would look
// wrong until it stopped.
func TestOpenRepairsThePointerFromWhatQEMUHasOpen(t *testing.T) {
	t.Parallel()
	live := qcow.LayerImage(root, vol, layerID)
	ahead := qcow.LayerImage(root, vol, nextID)
	p := newPathsAt(ahead)

	chain, err := qcow.Open(t.Context(), &fakeRunner{}, p, "/qemu-img",
		req(func(o *qcow.OpenRequest) { o.LiveImage = live }))
	if err != nil {
		t.Fatalf("opening a live chain: %v", err)
	}
	if chain.Active != live {
		t.Errorf("the tip is %q, want what QEMU has open, %q", chain.Active, live)
	}
	if got := p.pointer(); got != live {
		t.Errorf("active/current still names %q, want it repaired to %q", got, live)
	}
}

func TestOpenRefusesAPointerItCannotBelieve(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, body string }{
		{"truncated to nothing by a crash", ""},
		{"a relative path", "layers/x.qcow2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newPaths()
			pointer := qcow.ActivePointer(root, vol)
			p.present[pointer], p.files[pointer] = true, tt.body
			r := &fakeRunner{}

			_, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(nil))
			if !errors.Is(err, qcow.ErrChainMismatch) {
				t.Fatalf("want ErrChainMismatch, got %v", err)
			}
			// And above all it did not decide the volume was new. Creating a fresh
			// layer here is a guest handed a blank disk, with its data still on the
			// filesystem and nothing naming it.
			if cmds := r.commands(); len(cmds) != 0 {
				t.Errorf("an unreadable pointer led to %v", cmds)
			}
		})
	}
}

func TestOpenRefusesAVolumeWithNoSize(t *testing.T) {
	t.Parallel()
	_, err := qcow.Open(t.Context(), &fakeRunner{}, newPaths(), "/qemu-img",
		req(func(o *qcow.OpenRequest) { o.SizeBytes = 0 }))
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("want ErrChainMismatch for a zero-sized volume, got %v", err)
	}
}

func TestOpenReportsTheFailuresOfTheThingsItDrives(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	tests := []struct {
		name  string
		runs  *fakeRunner
		paths *fakePaths
		want  string
	}{
		{
			name: "qemu-img create failed",
			runs: &fakeRunner{err: errors.New("Formatting failed: No space left on device")},
			// The volume has no pointer, so `create` is what runs and what fails.
			paths: newPaths(),
			want:  "creating",
		},
		{
			name:  "the pointer cannot be examined",
			runs:  &fakeRunner{},
			paths: &fakePaths{present: map[string]bool{}, files: map[string]string{}, statFail: errors.New("permission denied")},
			want:  "looking for",
		},
		{
			name:  "qemu-img answered with something that is not JSON",
			runs:  &fakeRunner{info: "qemu-img: unrecognized option"},
			paths: newPathsAt(image),
			want:  "decoding",
		},
		{
			name:  "qemu-img could not open it",
			runs:  &fakeRunner{err: errors.New(`Failed to get shared "write" lock`)},
			paths: newPathsAt(image),
			// The wording moved with the behaviour: a write lock is now ErrImageBusy,
			// which is retried next cycle rather than read as a broken image.
			want: "a VM has this image open",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := qcow.Open(t.Context(), tt.runs, tt.paths, "/qemu-img", req(nil))
			if err == nil {
				t.Fatal("the failure was swallowed")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the error does not say what was being done: %v", err)
			}
		})
	}
}

func TestOpenReportsADirectoryItCannotMake(t *testing.T) {
	t.Parallel()
	p := &failingMkdir{fakePaths: *newPaths()}
	if _, err := qcow.Open(t.Context(), &fakeRunner{}, p, "/qemu-img", req(nil)); err == nil {
		t.Fatal("a directory that could not be made was ignored")
	}
}

type failingMkdir struct{ fakePaths }

func (*failingMkdir) MkdirAll(string) error { return errors.New("read-only file system") }

// TestRotateMovesThePointerBeforeQEMUSwitches is the ordering the whole rotation is
// built around, and the assertion is on the *sequence* rather than on both having
// happened. The other order is not a style preference: it leaves a window where
// `active/current` names the layer that is already the backing of the live tip, and a VM
// relaunched in that window writes into a file QEMU is reading through.
func TestRotateMovesThePointerBeforeQEMUSwitches(t *testing.T) {
	t.Parallel()
	tip := qcow.LayerImage(root, vol, layerID)
	next := qcow.LayerImage(root, vol, nextID)
	var events []string
	p := newPathsAt(tip)
	p.log = &events
	r := &fakeRunner{info: overlayJSON(size, tip)}
	chain := &qcow.Chain{Active: tip, SizeBytes: size}

	sealed, err := chain.Rotate(t.Context(), r, p, "/qemu-img", root, vol, nextID, func(newTip string) error {
		events = append(events, "switched QEMU to "+newTip)
		return nil
	})
	if err != nil {
		t.Fatalf("rotating: %v", err)
	}
	want := []string{"wrote " + qcow.ActivePointer(root, vol) + " = " + next, "switched QEMU to " + next}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Errorf("the order was %v, want %v", events, want)
	}
	if sealed != tip {
		t.Errorf("sealed %q, want the previous tip %q", sealed, tip)
	}
	if chain.Active != next {
		t.Errorf("the tip is %q, want %q", chain.Active, next)
	}
	// -u, and the backing named explicitly: qemu-img may not open the backing file,
	// because QEMU holds its write lock.
	create := fmt.Sprintf("/qemu-img create -f qcow2 -b %s -F qcow2 -u %s %d", tip, next, size)
	if got := r.commands(); len(got) != 2 || got[0] != create {
		t.Errorf("qemu-img was run as %v, want %q first", got, create)
	}
}

// TestRotateRefusesAnOverlayThatIsNotOverTheTip is the check that has exactly one moment
// in which it can be made. QEMU does not verify that an overlay's recorded backing is
// the node it attaches, so a layer created over the wrong file runs perfectly until the
// VM stops, and comes back on the next boot as somebody else's disk.
func TestRotateRefusesAnOverlayThatIsNotOverTheTip(t *testing.T) {
	t.Parallel()
	tip := qcow.LayerImage(root, vol, layerID)
	tests := []struct {
		name, info, want string
	}{
		{"backed by another layer", overlayJSON(size, "/var/lib/volume-agent/volumes/other/layers/x.qcow2"), "not by the tip"},
		{"backed by nothing at all", infoJSON("qcow2", size, false), "not by the tip"},
		{"the wrong virtual size", overlayJSON(size/2, tip), "and the chain is"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newPathsAt(tip)
			chain := &qcow.Chain{Active: tip, SizeBytes: size}
			switched := false

			_, err := chain.Rotate(t.Context(), &fakeRunner{info: tt.info}, p, "/qemu-img", root, vol, nextID,
				func(string) error { switched = true; return nil })
			if !errors.Is(err, qcow.ErrChainMismatch) {
				t.Fatalf("want ErrChainMismatch, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the refusal does not say why: %v", err)
			}
			if switched {
				t.Error("QEMU was told to write into a layer that had not been checked")
			}
			if got := p.pointer(); got != tip {
				t.Errorf("active/current was moved to %q despite the refusal", got)
			}
		})
	}
}

func TestRotateKeepsTheTipWhenQEMUWillNotSwitch(t *testing.T) {
	t.Parallel()
	tip := qcow.LayerImage(root, vol, layerID)
	p := newPathsAt(tip)
	chain := &qcow.Chain{Active: tip, SizeBytes: size}

	_, err := chain.Rotate(t.Context(), &fakeRunner{info: overlayJSON(size, tip)}, p, "/qemu-img", root, vol, nextID,
		func(string) error { return errors.New("Device 'virtio0' not found") })
	if err == nil {
		t.Fatal("a snapshot QEMU refused was reported as a rotation")
	}
	// The guest is still writing to the old tip, and this process still says so. The
	// pointer is one layer ahead, which is the window Open converges out of by asking
	// QEMU — asserted here so that the state after a failed rotation is a decision and
	// not an accident.
	if chain.Active != tip {
		t.Errorf("the tip moved to %q after a snapshot that did not happen", chain.Active)
	}
	if got := p.pointer(); got != qcow.LayerImage(root, vol, nextID) {
		t.Errorf("active/current names %q; the window this leaves is the one Open repairs", got)
	}
}

func TestRotateRefusesToReuseALayerId(t *testing.T) {
	t.Parallel()
	tip := qcow.LayerImage(root, vol, layerID)
	next := qcow.LayerImage(root, vol, nextID)
	p := newPathsAt(tip)
	p.present[next] = true
	chain := &qcow.Chain{Active: tip, SizeBytes: size}
	r := &fakeRunner{info: overlayJSON(size, tip)}

	_, err := chain.Rotate(t.Context(), r, p, "/qemu-img", root, vol, nextID, func(string) error { return nil })
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("want ErrChainMismatch, got %v", err)
	}
	if cmds := r.commands(); len(cmds) != 0 {
		t.Errorf("a layer that already existed was overwritten by %v", cmds)
	}
}

// TestOpenRefusesToDecideAnythingWithoutARecovery is why Recovery is a required
// interface and not a bool the caller works out. A bool is a thing a caller forgets to
// set, and the caller forgetting is the whole defect: this package used to run
// `qemu-img create` for any volume whose pointer was absent, so a volume with published
// commits placed on a host that has no local copy was handed a guest as a blank disk.
func TestOpenRefusesToDecideAnythingWithoutARecovery(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	p := newPaths()

	_, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(func(o *qcow.OpenRequest) { o.Recovery = nil }))
	if !errors.Is(err, qcow.ErrChainMissing) {
		t.Fatalf("want ErrChainMissing, got %v", err)
	}
	if cmds := r.commands(); len(cmds) != 0 {
		t.Errorf("a volume nobody could ask about led to %v", cmds)
	}
	if p.pointer() != "" {
		t.Errorf("active/current was written for a volume nobody could ask about: %q", p.pointer())
	}
}

// TestOpenAsksAboutTheHistoryBeforeCreatingAVolumeEmpty: the ordinary case, and the one
// that must stay cheap. A volume created a second ago has no HEAD, so exactly one
// question is asked and the answer is the same `create` as before.
func TestOpenAsksAboutTheHistoryBeforeCreatingAVolumeEmpty(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	p := newPaths()
	rec := bornEmpty()

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(func(o *qcow.OpenRequest) { o.Recovery = rec }))
	if err != nil {
		t.Fatalf("opening a volume that has never published: %v", err)
	}
	image := qcow.LayerImage(root, vol, layerID)
	if chain.Active != image {
		t.Errorf("the tip is %q, want %q", chain.Active, image)
	}
	want := fmt.Sprintf("/qemu-img create -f qcow2 %s %d", image, size)
	if got := r.commands(); len(got) != 1 || got[0] != want {
		t.Fatalf("qemu-img was run as %v, want exactly [%q]", got, want)
	}
	if got := fmt.Sprint(rec.calls); got != fmt.Sprintf("[%s/%d]", vol, size) {
		t.Errorf("the object store was asked %v, want one question about this volume at its catalog size", rec.calls)
	}
	if got := p.pointer(); got != image {
		t.Errorf("active/current names %q, want %q", got, image)
	}
}

// TestOpenRefusesAVolumeWhoseHistoryCouldNotBeRebuilt is the defect this stage exists to
// close, and the assertion that matters is the absence of a file: an unreachable bucket,
// a missing layer or a digest that does not match all mean "this volume's data is
// somewhere and it is not here", and creating an empty image for any of them hands a
// guest a blank disk with nothing anywhere saying so.
func TestOpenRefusesAVolumeWhoseHistoryCouldNotBeRebuilt(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, why string }{
		{"the bucket cannot be reached", "dial tcp: connection refused"},
		{"a layer named by a commit is gone", "recovery: layer 0198c0de is not in the object store"},
		{"a layer came back corrupt", "recovery: the digest of layer 0198c0de does not match"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &fakeRunner{}
			p := newPaths()
			rec := &fakeRecovery{err: errors.New(tt.why)}

			_, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(func(o *qcow.OpenRequest) { o.Recovery = rec }))
			if !errors.Is(err, qcow.ErrChainMissing) {
				t.Fatalf("want ErrChainMissing, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.why) {
				t.Errorf("the refusal does not carry what went wrong: %v", err)
			}
			if cmds := r.commands(); len(cmds) != 0 {
				t.Fatalf("a volume whose chain is elsewhere was given an image by %v", cmds)
			}
			if p.pointer() != "" {
				t.Fatalf("active/current names %q, so a VM would be launched against it", p.pointer())
			}
		})
	}
}

// TestOpenBuildsANewTipOverARecoveredChain: the rebuild put the published layers on this
// disk, and this package's job is the one decision it owns — which file the VM writes to.
// An overlay and never a flatten: an overlay is O(1) and a flatten is a second pass over
// every byte the recovery just wrote (§19 owns flattening, as compaction).
func TestOpenBuildsANewTipOverARecoveredChain(t *testing.T) {
	t.Parallel()
	base := qcow.LayerImage(root, vol, baseID)
	tip := qcow.LayerImage(root, vol, layerID)
	r := &fakeRunner{info: overlayJSON(size, base)}
	p := newPaths(base)

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(func(o *qcow.OpenRequest) { o.Recovery = recovered(base) }))
	if err != nil {
		t.Fatalf("opening a recovered volume: %v", err)
	}
	if chain.Active != tip {
		t.Errorf("the tip is %q, want a new layer %q over the recovered chain", chain.Active, tip)
	}
	if chain.SizeBytes != size {
		t.Errorf("the chain is %d bytes, want the head commit's %d", chain.SizeBytes, size)
	}
	create := fmt.Sprintf("/qemu-img create -f qcow2 -b %s -F qcow2 -u %s %d", base, tip, size)
	got := r.commands()
	if len(got) != 2 || got[0] != create {
		t.Fatalf("qemu-img was run as %v, want %q first", got, create)
	}
	if !strings.HasPrefix(got[1], "/qemu-img info --output=json "+tip) {
		t.Errorf("the new tip was not read back before it was published: %v", got)
	}
	if p.pointer() != tip {
		t.Errorf("active/current names %q, want the new tip %q", p.pointer(), tip)
	}
}

// TestOpenRefusesATipThatIsNotOverTheRecoveredChain is the same check Rotate makes and
// for the same reason: qemu-img accepts a wrong-but-existing backing in silence, and
// nothing later would catch it — the guest runs perfectly on somebody else's history
// until it stops.
func TestOpenRefusesATipThatIsNotOverTheRecoveredChain(t *testing.T) {
	t.Parallel()
	base := qcow.LayerImage(root, vol, baseID)
	tests := []struct{ name, info, want string }{
		{"backed by another layer", overlayJSON(size, qcow.LayerImage(root, "other", nextID)), "not by the recovered chain"},
		{"backed by nothing at all", infoJSON("qcow2", size, false), "not by the recovered chain"},
		{"the wrong virtual size", overlayJSON(size/2, base), "the head commit says"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := newPaths(base)
			_, err := qcow.Open(t.Context(), &fakeRunner{info: tt.info}, p, "/qemu-img",
				req(func(o *qcow.OpenRequest) { o.Recovery = recovered(base) }))
			if !errors.Is(err, qcow.ErrChainMismatch) {
				t.Fatalf("want ErrChainMismatch, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the refusal does not say why: %v", err)
			}
			if p.pointer() != "" {
				t.Errorf("active/current names %q despite the refusal", p.pointer())
			}
		})
	}
}

// TestAVolumeThatAlreadyHasAChainIsStillCheckedAgainstTheBucket.
//
// This asserted the opposite until an adversary showed what the opposite costs. A local
// chain is not the same thing as the current one: this host keeps its layers when a
// volume leaves its desired state, and in between another host may have served it and
// published commits. Opening the local chain without asking hands the guest an older
// disk, complete and sound and missing everything the other host wrote.
//
// The check is one HEAD read, on a path that was about to run qemu-img anyway.
func TestAVolumeThatAlreadyHasAChainIsStillCheckedAgainstTheBucket(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	rec := bornEmpty()
	r := &fakeRunner{info: infoJSON("qcow2", size, false)}

	if _, err := qcow.Open(t.Context(), r, newPathsAt(image), "/qemu-img",
		req(func(o *qcow.OpenRequest) { o.Recovery = rec })); err != nil {
		t.Fatalf("opening a volume whose chain is current: %v", err)
	}
	if len(rec.calls) != 1 || rec.calls[0] != "current/"+vol {
		t.Errorf("the object store was asked %v, want exactly one HEAD read", rec.calls)
	}
	// And it is a HEAD read and not a rebuild: a chain that is already here must not be
	// downloaded again.
	for _, c := range rec.calls {
		if !strings.HasPrefix(c, "current/") {
			t.Errorf("opening a chain that is already here did %q", c)
		}
	}
}

// TestOpenWalksTheWholeChainOfAnImageItAdopts. `qemu-img info` exits 0 on a tip whose
// backing file is gone — format, virtual size and the corrupt flag all pass — so the
// Agent would log "volume ready" and the failure would land on whoever launches QEMU
// (`Could not open backing file`). `--backing-chain` opens every layer and exits 1.
// It is safe here and only here: this branch is an image no VM has open.
func TestOpenWalksTheWholeChainOfAnImageItAdopts(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	base := qcow.LayerImage(root, vol, baseID)

	t.Run("the walk is what is run", func(t *testing.T) {
		t.Parallel()
		r := &fakeRunner{info: infoJSON("qcow2", size, false)}
		if _, err := qcow.Open(t.Context(), r, newPathsAt(image), "/qemu-img", req(nil)); err != nil {
			t.Fatalf("opening: %v", err)
		}
		want := "/qemu-img info --output=json --backing-chain " + image
		if got := r.commands(); len(got) != 1 || got[0] != want {
			t.Fatalf("qemu-img was run as %v, want exactly [%q]", got, want)
		}
	})

	t.Run("a chain that does not resolve", func(t *testing.T) {
		t.Parallel()
		r := &fakeRunner{
			info:     infoJSON("qcow2", size, false),
			chainErr: errors.New("Could not open backing file: No such file or directory"),
		}
		_, err := qcow.Open(t.Context(), r, newPathsAt(image), "/qemu-img", req(nil))
		if err == nil {
			t.Fatal("a tip whose backing file is gone was reported as a ready volume")
		}
	})

	t.Run("a lower layer carries the corrupt flag", func(t *testing.T) {
		t.Parallel()
		r := &fakeRunner{chain: "[" + overlayJSON(size, base) + "," + infoJSON("qcow2", size, true) + "]"}
		_, err := qcow.Open(t.Context(), r, newPathsAt(image), "/qemu-img", req(nil))
		if !errors.Is(err, qcow.ErrChainMismatch) {
			t.Fatalf("want ErrChainMismatch, got %v", err)
		}
		if !strings.Contains(err.Error(), "corrupt flag") {
			t.Errorf("the refusal does not say why: %v", err)
		}
	})
}

// TestTargetForNamesWhateverTheLauncherLeftUsToNameItWith.
//
// A snapshot has to name the node it acts on, and there is no single way to do that: this
// Agent does not launch the VM (ADR-0021). A `-drive file=...,if=virtio` disk has a
// generated drive id and an anonymous node QMP refuses as input; a `-blockdev
// node-name=vol` disk has a real node and no drive id at all. This took the drive id and
// refused everything else — which is a VM shaped the way any libvirt-derived runner
// shapes one, so rotation broke on exactly the launcher we expect to meet.
func TestTargetForNamesWhateverTheLauncherLeftUsToNameItWith(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	open := func(device, node string) []string {
		return []string{
			`{"QMP": {"version": {}, "capabilities": []}}`,
			`{"return": {}}`,
			`{"return": [{"device": "` + device + `", "inserted": {"file": "` + image +
				`", "drv": "qcow2", "node-name": "` + node + `"}}]}`,
			`{"return": [{"node-name": "spin1"}]}`,
		}
	}

	tests := []struct {
		name, want string
		script     []string
	}{
		{
			name:   "launched with -drive: the generated id, and no overlay name needed",
			script: open("virtio0", "#block126"),
			want:   "drive virtio0",
		},
		{
			name:   "launched with -blockdev: the node, because there is no drive id",
			script: open("", "vol"),
			want:   "node vol",
		},
		{
			name:   "neither, which nothing can name",
			script: open("", "#block126"),
			want:   "neither a drive id nor a node name",
		},
		{
			name:   "a VM that has moved on to another image",
			script: attachedTo("/somewhere/else.qcow2"),
			want:   "no longer has",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.dialer.scripts[qcow.QMPSocket(root, vol)] = tt.script
			got, err := h.m.DeviceForTest(t.Context(), vol, image)
			if err != nil {
				if !strings.Contains(err.Error(), tt.want) {
					t.Errorf("the error does not say why: %v", err)
				}
				return
			}
			if got != tt.want {
				t.Errorf("the target is %q, want %q", got, tt.want)
			}
		})
	}
}

// TestWritePointerReportsADirectoryItCannotMake: the pointer is how a VM finds its disk,
// and a write that failed silently would leave whoever launches it pointed at nothing.
func TestWritePointerReportsADirectoryItCannotMake(t *testing.T) {
	t.Parallel()
	p := &failingMkdir{fakePaths: *newPaths()}
	rec := bornEmpty()
	_, err := qcow.Open(t.Context(), &fakeRunner{}, p, "/qemu-img",
		req(func(o *qcow.OpenRequest) { o.Recovery = rec }))
	if err == nil {
		t.Fatal("a directory that could not be made was ignored")
	}
}

// TestInspectSaysWhichKindOfFailureItMet. `qemu-img` meeting QEMU's write lock is not a
// broken image: it is a VM having the file open, reached from the one angle that cannot
// lie about it. The two are told apart because the operator's next move differs — one is
// "check the socket path the VM was launched with", the other is "look at the image" —
// and because a lock failure is retried next cycle while a corrupt image is not.
func TestInspectSaysWhichKindOfFailureItMet(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)

	tests := []struct {
		name  string
		run   error
		busy  bool
		wants string
	}{
		{
			name:  "a VM has it open",
			run:   errors.New(`qemu-img: Failed to get shared "write" lock`),
			busy:  true,
			wants: "a VM has this image open",
		},
		{
			name:  "something else entirely",
			run:   errors.New("qemu-img: No space left on device"),
			wants: "No space left",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := qcow.Open(t.Context(), &fakeRunner{err: tt.run}, newPathsAt(image), "/qemu-img",
				req(func(o *qcow.OpenRequest) { o.Recovery = bornEmpty() }))
			if err == nil {
				t.Fatal("the failure was swallowed")
			}
			if got := errors.Is(err, qcow.ErrImageBusy); got != tt.busy {
				t.Errorf("ErrImageBusy = %v, want %v (%v)", got, tt.busy, err)
			}
			if !strings.Contains(err.Error(), tt.wants) {
				t.Errorf("the error does not say what happened: %v", err)
			}
		})
	}
}

// TestCheckNotStaleReportsAStoreThatWillNotAnswer: a local chain and an object store that
// cannot say whether it is current is a refusal, not a shrug. Serving it would be a guess,
// and the guess that is wrong hands a guest a fork of its own history.
func TestCheckNotStaleReportsAStoreThatWillNotAnswer(t *testing.T) {
	t.Parallel()
	image := qcow.LayerImage(root, vol, layerID)
	rec := bornEmpty()
	rec.headErr = errors.New("dial tcp: connection refused")

	_, err := qcow.Open(t.Context(), &fakeRunner{info: infoJSON("qcow2", size, false)},
		newPathsAt(image), "/qemu-img", req(func(o *qcow.OpenRequest) { o.Recovery = rec }))
	if !errors.Is(err, qcow.ErrChainMissing) {
		t.Fatalf("want ErrChainMissing, got %v", err)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("the refusal does not carry what the store said: %v", err)
	}
}
