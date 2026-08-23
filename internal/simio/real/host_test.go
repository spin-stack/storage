package real_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/simio/real"
)

// These three are the primitives that reach another process: a program that is run, a
// directory that is made, a socket that is dialled. They are driven against the real
// world here — a real `/bin/sh`, a real directory, a real listener — because that is
// the whole of what they are, and a test with a fake underneath would be asserting on
// its own fake.

func TestRunnerReturnsStandardOutputOnly(t *testing.T) {
	t.Parallel()
	out, err := real.NewRunner().Run(t.Context(), "/bin/sh", "-c", "printf answer; printf noise >&2")
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	// `qemu-img info --output=json` must come back parseable, so a byte of diagnostics
	// in the output is a decoding failure with a misleading message.
	if string(out) != "answer" {
		t.Errorf("stdout = %q, want %q — standard error leaked into it", out, "answer")
	}
}

func TestRunnerPutsTheProgramsDiagnosticsInTheError(t *testing.T) {
	t.Parallel()
	_, err := real.NewRunner().Run(t.Context(),
		"/bin/sh", "-c", "echo 'Formatting failed: No space left on device' >&2; exit 1")
	if err == nil {
		t.Fatal("a program that exited non-zero was reported as success")
	}
	// "exit status 1" alone is a failure an operator cannot act on: qemu-img's own
	// sentence is the entire explanation of what is wrong with an image.
	if !strings.Contains(err.Error(), "No space left on device") {
		t.Errorf("the error does not carry the program's own words: %v", err)
	}
}

func TestRunnerReportsAProgramThatIsNotThere(t *testing.T) {
	t.Parallel()
	if _, err := real.NewRunner().Run(t.Context(), "/nonexistent/qemu-img", "info"); err == nil {
		t.Fatal("a missing binary was reported as success")
	}
}

func TestRunnerIsBoundByItsContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := real.NewRunner().Run(ctx, "/bin/sh", "-c", "printf hi"); err == nil {
		t.Fatal("a cancelled context still ran the program")
	}
}

func TestPaths(t *testing.T) {
	t.Parallel()
	p := real.NewPaths()
	dir := filepath.Join(t.TempDir(), "volumes", "v1", "active")

	exists, err := p.Exists(dir)
	if err != nil || exists {
		t.Fatalf("Exists on a missing directory = %v, %v; want false, nil", exists, err)
	}
	if err := p.MkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	exists, err = p.Exists(dir)
	if err != nil || !exists {
		t.Fatalf("Exists after MkdirAll = %v, %v; want true, nil", exists, err)
	}
	// Idempotent: the manager calls it on every open of a chain.
	if err := p.MkdirAll(dir); err != nil {
		t.Fatalf("MkdirAll again: %v", err)
	}
}

func TestPathsReportsAPathItCannotExamine(t *testing.T) {
	t.Parallel()
	// A component of the path is a file, so stat fails with ENOTDIR rather than
	// ENOENT. "I could not look" and "it is not there" lead to opposite decisions
	// about an image file, so this must not come back as a plain false.
	dir := t.TempDir()
	file := filepath.Join(dir, "notadir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	exists, err := real.NewPaths().Exists(filepath.Join(file, "current.qcow2"))
	if err == nil {
		t.Fatalf("a path that cannot be examined came back as exists=%v with no error", exists)
	}
}

// listen starts a Unix listener that answers every connection with reply.
func listen(t *testing.T, reply string) string {
	t.Helper()
	// Short, because sun_path is 108 bytes: t.TempDir() under a long test name has
	// overflowed it before.
	path := filepath.Join(t.TempDir(), "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_, _ = io.WriteString(conn, reply)
			_ = conn.Close()
		}
	}()
	return path
}

func TestUnixDialer(t *testing.T) {
	t.Parallel()
	conn, err := real.NewUnixDialer().Dial(t.Context(), listen(t, "hello\n"))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer func() { _ = conn.Close() }()

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	if string(got) != "hello\n" {
		t.Errorf("read %q, want %q", got, "hello\n")
	}
}

func TestUnixDialerReportsAnAbsentSocket(t *testing.T) {
	t.Parallel()
	_, err := real.NewUnixDialer().Dial(t.Context(), filepath.Join(t.TempDir(), "nothing.sock"))
	if err == nil {
		t.Fatal("dialling a socket that does not exist succeeded")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("want a not-exist error, got %v", err)
	}
}

// TestACancelledContextEndsAReadOnTheStream is what gives a QMP exchange a deadline
// without production code touching a clock: net.Conn.SetDeadline takes a wall-clock
// time.Time, which INV-01 forbids producing, so the context closes the connection
// instead. Without it a QEMU that accepts and then says nothing parks the Agent's
// reconciliation cycle for ever, and a cycle that does not finish is a lease that lapses.
func TestACancelledContextEndsAReadOnTheStream(t *testing.T) {
	t.Parallel()
	// A listener that accepts and never answers.
	path := filepath.Join(t.TempDir(), "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer func() { _ = l.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := l.Accept()
		if aerr == nil {
			accepted <- c
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	conn, err := real.NewUnixDialer().Dial(ctx, path)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer func() { _ = conn.Close() }()
	server := <-accepted
	defer func() { _ = server.Close() }()

	read := make(chan error, 1)
	go func() {
		_, rerr := conn.Read(make([]byte, 1))
		read <- rerr
	}()
	cancel()
	if err := <-read; err == nil {
		t.Fatal("a read on a cancelled stream returned without an error; it would have blocked for ever")
	}
}

// TestPathsWriteAtomicReplacesInOneStep pins the property the pointer file needs and
// that a plain create-truncate-write would not have: the reader is another process
// picking its own moment, and what it must never see is a file that is half a path.
//
// Atomicity itself cannot be observed from one goroutine, so what is asserted is the
// implementation's two consequences — a shorter payload replaces a longer one exactly,
// with no tail of the old contents, and the temp file the rename came from is gone. A
// truncate-and-write would fail the first; a rename that forgot to clean up would fail
// the second, and leave a directory that grows a file per rotation.
func TestPathsWriteAtomicReplacesInOneStep(t *testing.T) {
	t.Parallel()
	p := real.NewPaths()
	dir := t.TempDir()
	pointer := filepath.Join(dir, "current")

	long := "/var/lib/volume-agent/volumes/v1/layers/0198c0de-0000-7000-8000-00000000cafe.qcow2"
	short := "/tmp/x.qcow2"
	for _, want := range []string{long, short} {
		if err := p.WriteAtomic(pointer, []byte(want)); err != nil {
			t.Fatalf("WriteAtomic(%q): %v", want, err)
		}
		got, err := p.ReadFile(pointer)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(got) != want {
			t.Errorf("read back %q, want %q", got, want)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "current" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("the directory holds %v, want just the pointer", names)
	}
}

func TestPathsSizeAndReadFileReportWhatIsThere(t *testing.T) {
	t.Parallel()
	p := real.NewPaths()
	file := filepath.Join(t.TempDir(), "layer.qcow2")

	if _, err := p.Size(file); err == nil {
		t.Error("Size of a file that is not there succeeded")
	}
	if _, err := p.ReadFile(file); err == nil {
		t.Error("ReadFile of a file that is not there succeeded")
	}
	if err := os.WriteFile(file, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("writing: %v", err)
	}
	got, err := p.Size(file)
	if err != nil || got != 4096 {
		t.Errorf("Size = %d, %v; want 4096, nil", got, err)
	}
}

// TestPathsWriteAtomicReportsADirectoryItCannotWriteIn: the pointer is how a VM finds
// its disk, and a write that failed silently would leave the launcher pointed at a layer
// that is no longer the tip.
func TestPathsWriteAtomicReportsADirectoryItCannotWriteIn(t *testing.T) {
	t.Parallel()
	p := real.NewPaths()
	if err := p.WriteAtomic(filepath.Join(t.TempDir(), "no", "such", "dir", "current"), []byte("x")); err == nil {
		t.Fatal("writing into a directory that does not exist succeeded")
	}
}
