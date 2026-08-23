package qcow_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

// fakePaths is a filesystem of names. Nothing is written; what matters to the chain is
// which directories were made and whether the image was already there.
type fakePaths struct {
	mu       sync.Mutex
	made     []string
	present  map[string]bool
	statFail error
}

func newPaths(present ...string) *fakePaths {
	p := &fakePaths{present: map[string]bool{}}
	for _, name := range present {
		p.present[name] = true
	}
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

// infoJSON is a trimmed `qemu-img info --output=json` answer.
func infoJSON(format string, size int64, corrupt bool) string {
	return fmt.Sprintf(`{"virtual-size": %d, "filename": "x", "format": %q,
	  "format-specific": {"type": "qcow2", "data": {"corrupt": %t}}}`, size, format, corrupt)
}

const (
	root = "/var/lib/volume-agent"
	vol  = "0198c0de-0000-7000-8000-00000000cafe"
	size = int64(268435456)
)

func TestPathsAreTheContractWithWhoeverLaunchesQEMU(t *testing.T) {
	t.Parallel()
	// Asserted literally, because these two strings are an interface to another
	// repository: spin's runner computes them to build QEMU's command line. Deriving
	// the expectation the same way the code does would assert nothing.
	if got, want := qcow.ActiveImage(root, vol), root+"/volumes/"+vol+"/active/current.qcow2"; got != want {
		t.Errorf("ActiveImage = %q, want %q", got, want)
	}
	if got, want := qcow.QMPSocket(root, vol), root+"/volumes/"+vol+"/qmp.sock"; got != want {
		t.Errorf("QMPSocket = %q, want %q", got, want)
	}
	if got, want := qcow.VolumeDir(root, vol), root+"/volumes/"+vol; got != want {
		t.Errorf("VolumeDir = %q, want %q", got, want)
	}
}

func TestOpenCreatesTheImageOnceAndWithTheCatalogsSize(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{}
	p := newPaths()

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", root, vol, size, false)
	if err != nil {
		t.Fatalf("opening a fresh volume: %v", err)
	}
	image := qcow.ActiveImage(root, vol)
	if chain.Active != image {
		t.Errorf("the tip is %q, want %q", chain.Active, image)
	}
	want := fmt.Sprintf("/qemu-img create -f qcow2 %s %d", image, size)
	if got := r.commands(); len(got) != 1 || got[0] != want {
		t.Fatalf("qemu-img was run as %v, want exactly [%q]", got, want)
	}
	if len(p.made) != 1 || p.made[0] != filepath.Dir(image) {
		t.Errorf("made %v, want just %q", p.made, filepath.Dir(image))
	}
}

func TestOpenChecksAnImageThatIsAlreadyThere(t *testing.T) {
	t.Parallel()
	image := qcow.ActiveImage(root, vol)

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
			chain, err := qcow.Open(t.Context(), r, newPaths(image), "/qemu-img", root, vol, size, false)

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

// TestOpenNeverTouchesAnImageQEMUHasOpen is the rule v6 §5 states and the reason the
// `live` argument exists at all. It is asserted as "the runner was not called", which
// is the only observable that distinguishes obeying the rule from getting away with it:
// `qemu-img info` on an image QEMU holds fails on a write lock, so a build that ran it
// anyway would refuse a perfectly good volume every time an Agent restarted under a
// running guest.
func TestOpenNeverTouchesAnImageQEMUHasOpen(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{err: errors.New(`Failed to get shared "write" lock`)}
	p := newPaths(qcow.ActiveImage(root, vol))

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", root, vol, size, true)
	if err != nil {
		t.Fatalf("opening a live chain: %v", err)
	}
	if chain.Active != qcow.ActiveImage(root, vol) {
		t.Errorf("the tip is %q", chain.Active)
	}
	if cmds := r.commands(); len(cmds) != 0 {
		t.Fatalf("an image QEMU has open was handed to qemu-img: %v", cmds)
	}
	if len(p.made) != 0 {
		t.Errorf("a live chain made directories: %v", p.made)
	}
}

func TestOpenRefusesAVolumeWithNoSize(t *testing.T) {
	t.Parallel()
	_, err := qcow.Open(t.Context(), &fakeRunner{}, newPaths(), "/qemu-img", root, vol, 0, false)
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("want ErrChainMismatch for a zero-sized volume, got %v", err)
	}
}

func TestOpenReportsTheFailuresOfTheThingsItDrives(t *testing.T) {
	t.Parallel()
	image := qcow.ActiveImage(root, vol)
	tests := []struct {
		name  string
		runs  *fakeRunner
		paths *fakePaths
		want  string
	}{
		{
			name: "qemu-img create failed",
			runs: &fakeRunner{err: errors.New("Formatting failed: No space left on device")},
			// The image is absent, so `create` is what runs and what fails.
			paths: newPaths(),
			want:  "creating",
		},
		{
			name:  "the image cannot be examined",
			runs:  &fakeRunner{},
			paths: &fakePaths{present: map[string]bool{}, statFail: errors.New("permission denied")},
			want:  "looking for",
		},
		{
			name:  "qemu-img answered with something that is not JSON",
			runs:  &fakeRunner{info: "qemu-img: unrecognized option"},
			paths: newPaths(image),
			want:  "decoding",
		},
		{
			name:  "qemu-img could not open it",
			runs:  &fakeRunner{err: errors.New(`Failed to get shared "write" lock`)},
			paths: newPaths(image),
			want:  "a lock failure here means a VM has it open",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := qcow.Open(t.Context(), tt.runs, tt.paths, "/qemu-img", root, vol, size, false)
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
	if _, err := qcow.Open(t.Context(), &fakeRunner{}, p, "/qemu-img", root, vol, size, false); err == nil {
		t.Fatal("a directory that could not be made was ignored")
	}
}

type failingMkdir struct{ fakePaths }

func (*failingMkdir) MkdirAll(string) error { return errors.New("read-only file system") }
