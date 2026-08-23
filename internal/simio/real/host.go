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

// The three real primitives a qcow2 chain needs, and why none of them is disk.Disk.
//
// disk.Disk is a rooted namespace of append-only files with a crash model. It was built
// for a format this process writes itself, byte by byte, and it deliberately hides two
// things: the absolute path of a file, and any way to hand that file to somebody else.
// Both are exactly what qcow2 needs. QEMU writes the image, `qemu-img` creates and
// inspects it, and each of them is a *different process* that takes a path on the
// command line. There is nothing here for an append-only file interface to model, and
// wrapping one around a subprocess would only move the real primitive one call deeper.
//
// So the primitives are named for what they are — run a program, make a directory,
// open a stream — and they live here, where INV-01 puts every real implementation. The
// interfaces they satisfy are declared where they are consumed (internal/qcow,
// internal/qmp), so a test injects a fake without either package importing this one.

// Runner runs external programs to completion. It is what `qemu-img` is reached
// through: v6 §7 forbids a qcow2 parser of our own, so creating and inspecting an
// image is a process this one starts and waits for.
type Runner struct{}

// NewRunner returns the production Runner.
func NewRunner() *Runner { return &Runner{} }

// Run executes name with args and returns its standard output.
//
// Standard error is captured separately and folded into the returned error rather than
// into the output, for two reasons that pull the same way: `qemu-img info
// --output=json` must hand back parseable JSON and nothing else, and a failure whose
// message is "exit status 1" is a failure an operator cannot act on. `qemu-img`'s
// diagnostics are the whole explanation of what went wrong with an image, so they go
// where an error is read.
//
// The context is the only deadline. There is no timeout of its own here — a caller that
// wants one builds it from its injected clock, so the wait is simulable like every
// other wait in this tree.
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

// Paths is the part of a real filesystem that is addressed by absolute path. Every verb
// here is one a chain's owner performs on paths it then hands to another process, or on
// the little pointer file that tells that process which path to take.
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

// WriteAtomic replaces path's contents with data in one step.
//
// Temp file, fsync, rename, fsync the directory — all four, because the reader is
// another process choosing its own moment and the thing being written is which qcow2 a
// VM is about to be launched against. A half-written pointer is a VM that does not
// start; a pointer that reached the directory entry but not the disk is a VM that starts
// against the wrong layer after a power cut, which is worse and silent.
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
