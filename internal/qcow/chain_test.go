package qcow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

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
	// version is what `qemu-img --version` answers. The Manager runs it once at
	// start-up; nothing else in the package does.
	version string
	// err, when set, is what every run fails with.
	err error
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
	// log records writes in order, so a test can assert that the pointer moved before
	// QEMU was told to switch rather than only that both happened.
	log *[]string
}

func newPaths(present ...string) *fakePaths {
	p := &fakePaths{present: map[string]bool{}, files: map[string]string{}, sizes: map[string]int64{}}
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

func (p *fakePaths) Size(path string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.statFail != nil {
		return 0, p.statFail
	}
	return p.sizes[path], nil
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
	root    = "/var/lib/volume-agent"
	vol     = "0198c0de-0000-7000-8000-00000000cafe"
	size    = int64(268435456)
	layerID = "0198c0de-0000-7000-8000-0000000f1r57"
	nextID  = "0198c0de-0000-7000-8000-000000005ec0"
)

// req is the ordinary Open for this volume, with one field varied per test.
func req(mut func(*qcow.OpenRequest)) qcow.OpenRequest {
	r := qcow.OpenRequest{Root: root, VolumeID: vol, SizeBytes: size, NewLayerID: layerID}
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
			want:  "a lock failure here means a VM has it open",
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
