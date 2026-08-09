package vhost

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// RawDevice is the simplest thing that satisfies Backend: a fixed-capacity raw
// block device held in host memory, where FLUSH is a no-op because there is no
// stable media under it.
//
// It exists so the unit tests can prove the *transport* — handshake, ring
// walking, request completion — against something with no behaviour of its own,
// and so they can do it without touching a filesystem. Nothing outside this
// package's tests has ever constructed one, which is why it lives in a _test.go
// file: a production file that ships a fake is a production file a binary can
// link the fake in from by accident, and the compiler is the only thing that
// reliably prevents that. It was in backend.go until 2026-08-03.
//
// The device a real guest is served from is blockdev.Device over a wal.Log.
// hostio.RawFile is the file-backed variant the QEMU lane serves so that a real
// kernel can be driven against a Backend with no WAL underneath it — it stayed
// in a production file because integration/vhost is a different package and
// cannot import this one's tests.
//
// Neither this nor hostio.RawFile is backed by simio/disk: disk.File is
// append-only (Append, ReadAt, Truncate, Sync) because that is what a WAL needs,
// and a block device needs random writes. The missing primitive is
// disk.File.WriteAt(p []byte, off int64) (int, error); ADR-0020 records why it
// is not being added for scaffolding.
type RawDevice struct {
	mu       sync.Mutex
	data     []byte
	syncs    int
	writes   int
	discards int
}

// NewRawDevice returns an all-zero device of size bytes.
func NewRawDevice(size int64) *RawDevice {
	return &RawDevice{data: make([]byte, size)}
}

// NewRawDeviceFrom returns a device over data, which it does not copy: the
// caller can seed a boot sector or inspect the result afterwards.
func NewRawDeviceFrom(data []byte) *RawDevice { return &RawDevice{data: data} }

// Size implements Backend.
func (d *RawDevice) Size() int64 { return int64(len(d.data)) }

// ReadAt implements Backend.
func (d *RawDevice) ReadAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(len(p), off); err != nil {
		return 0, err
	}
	return copy(p, d.data[off:]), nil
}

// WriteAt implements Backend.
func (d *RawDevice) WriteAt(p []byte, off int64) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(len(p), off); err != nil {
		return 0, err
	}
	d.writes++
	return copy(d.data[off:], p), nil
}

// Flush implements Backend. There is nothing to make durable.
// Discard zeroes the range, which is what every Backend in this tree makes a
// discarded range read back as. The counter is separate from Writes() so a test can
// tell "the guest trimmed" from "the guest wrote zeros" — on this wire they are two
// request types and only one of them is allowed to free anything.
func (d *RawDevice) Discard(off, length int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.check(int(length), off); err != nil {
		return err
	}
	clear(d.data[off : off+length])
	d.discards++
	return nil
}

// WriteZeroes has the same observable as Discard here, and ignores unmap for the
// reason vhost.Backend states: the flag permits releasing the space, it does not
// require it.
func (d *RawDevice) WriteZeroes(off, length int64, _ bool) error { return d.Discard(off, length) }

// Discards reports how many discard ranges were applied.
func (d *RawDevice) Discards() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.discards
}

func (d *RawDevice) Flush(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.syncs++
	return nil
}

// Flushes reports how many FLUSHes the device has served, so a test can assert
// the guest's FLUSH reached the backend rather than being absorbed en route.
func (d *RawDevice) Flushes() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.syncs
}

// Writes reports how many WRITEs the device has served.
func (d *RawDevice) Writes() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writes
}

// Snapshot copies out length bytes at off, for assertions.
func (d *RawDevice) Snapshot(off int64, length int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]byte, length)
	copy(out, d.data[off:])
	return out
}

func (d *RawDevice) check(n int, off int64) error {
	if off < 0 || n < 0 || off > int64(len(d.data)) || int64(n) > int64(len(d.data))-off {
		return fmt.Errorf("%w: %d bytes at %d, capacity %d", ErrOutOfRange, n, off, len(d.data))
	}
	return nil
}

// interface assertions: RawDevice is the reference Backend, and the io
// interfaces are what a file-backed device satisfies for free.
var (
	_ Backend     = (*RawDevice)(nil)
	_ io.ReaderAt = (*RawDevice)(nil)
	_ io.WriterAt = (*RawDevice)(nil)
)
