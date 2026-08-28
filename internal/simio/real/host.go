package real

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// The three real primitives a qcow2 chain needs — run a program, make a directory, open
// a stream — and not disk.Disk, which is a rooted namespace of append-only files that
// deliberately hides absolute paths and any way to hand a file to somebody else. Both are
// what qcow2 needs: QEMU and `qemu-img` are other processes taking a path on the command
// line. The interfaces these satisfy are declared where they are consumed (internal/qcow,
// internal/qmp), so a test fakes them without importing this package.

// Runner runs external programs to completion. It is what `qemu-img` is reached
// through: v6 §7 forbids a qcow2 parser of our own, so creating and inspecting an
// image is a process this one starts and waits for.
type Runner struct{}

// NewRunner returns the production Runner.
func NewRunner() *Runner { return &Runner{} }

// Run executes name with args and returns its standard output.
//
// Standard error is captured separately and folded into the returned error: `qemu-img
// info --output=json` must hand back parseable JSON and nothing else, and "exit status 1"
// is a failure an operator cannot act on. The context is the only deadline — a caller
// that wants one builds it from its injected clock, so the wait stays simulable.
func (*Runner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return stdout.Bytes(), fmt.Errorf("%s: %w: %s", name, err, detail)
		}
		return stdout.Bytes(), fmt.Errorf("%s: %w", name, err)
	}
	return stdout.Bytes(), nil
}

// Paths is the part of a real filesystem addressed by absolute path.
type Paths struct{}

// NewPaths returns the production Paths.
func NewPaths() *Paths { return &Paths{} }

// MkdirAll creates dir and its parents.
func (*Paths) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

// Exists reports whether anything is at path. A path that cannot be examined is an
// error, never a false: "I could not look" and "it is not there" lead to opposite
// decisions about an image file, and collapsing them would create one.
func (*Paths) Exists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Size is the space the file occupies. It is the apparent size and not the allocated
// one: a qcow2 is written sequentially as clusters are needed, so the two agree closely
// enough for a threshold, and the apparent size is the number a `ls -l` in an incident
// will show.
func (*Paths) Size(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// ReadFile returns the file's contents.
func (*Paths) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

// Open returns the file as a stream. It is what a sealed layer is read through: layers
// are the one thing here measured in tens of megabytes, and reading one into memory to
// hand it to a sealer that is going to stream it anyway would double the only allocation
// in the publish path that is worth counting.
func (*Paths) Open(path string) (io.ReadCloser, error) { return os.Open(path) }

// WriteAtomic replaces path's contents with data in one step: temp file, fsync, rename,
// fsync the directory. The reader is another process choosing its own moment and the
// thing written is which qcow2 a VM is launched against — a pointer that reached the
// directory entry but not the disk starts a VM against the wrong layer, silently.
func (*Paths) WriteAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return fsyncDirAt(dir)
}

// Create truncates or creates the file; its Close makes the file and its directory entry
// both durable. A downloaded layer whose contents reached the platter but whose name did
// not is a chain missing a link after a power cut, with nothing reporting it.
func (*Paths) Create(path string) (io.WriteCloser, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &syncOnClose{File: f, dir: filepath.Dir(path)}, nil
}

// Rename moves a file within the filesystem. It is atomic, which is why a download lands
// under a temporary name and arrives under its own.
func (*Paths) Rename(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }

// Remove deletes a file. A path that is already gone is not an error: the only caller is
// a cleanup after a failed rebuild, and it must be safe to run twice.
func (*Paths) Remove(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// syncOnClose is a file whose Close is the durability boundary.
type syncOnClose struct {
	*os.File
	dir  string
	once sync.Once
}

func (f *syncOnClose) Close() error {
	var err error
	f.once.Do(func() {
		// f.File.Close, not f.Close: this *is* f.Close, and calling it re-enters the
		// once that is already held. staticcheck asks for the embedded selector to go
		// (QF1008) and it is right about Sync, which the wrapper does not define, and
		// wrong about Close, which it does. Applying it to both deadlocked every caller
		// that finished writing a downloaded layer.
		err = errors.Join(f.Sync(), f.File.Close(), fsyncDirAt(f.dir))
	})
	return err
}

// fsyncDirAt makes a directory entry durable.
func fsyncDirAt(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// UnixDialer opens a byte stream to a Unix domain socket. QMP is a line-oriented JSON
// protocol over one, which simio/network's length-delimited message interface cannot
// express — so the stream is what is injected, and the framing lives in internal/qmp.
type UnixDialer struct{}

// NewUnixDialer returns the production dialer.
func NewUnixDialer() *UnixDialer { return &UnixDialer{} }

// Dial connects to the socket at path.
//
// The returned stream is closed when ctx is done, and that is what gives a read a
// deadline without a clock. `net.Conn.SetDeadline` takes a wall-clock time.Time, which
// production code here may not produce (INV-01); a caller that wants to bound a QMP
// exchange builds a context from its injected clock and this turns that into a closed
// connection. A QEMU that stops answering therefore ends the read rather than parking
// the Agent's reconciliation cycle for ever.
func (*UnixDialer) Dial(ctx context.Context, path string) (io.ReadWriteCloser, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	c := &ctxConn{Conn: conn, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-c.done:
		}
	}()
	return c, nil
}

// ctxConn is a connection whose watchdog goroutine ends when it is closed. Without the
// done channel the goroutine would outlive every short-lived probe and leak one per
// cycle, for the life of the process.
type ctxConn struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

func (c *ctxConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.Conn.Close()
}
