package vhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Backend is the block device behind the virtqueue: what a READ returns, what a
// WRITE stores, and what a FLUSH makes durable.
//
// It is small on purpose, and it is defined here because here is where it is
// consumed. virtio-blk needs exactly these four things, so this is the whole
// seam between the transport and the storage engine: Increment 3.1 satisfies it
// with a raw device, and Phase 04's wal.Log satisfies it later without the
// protocol code learning anything about WALs, epochs or object stores.
//
// The contract is deliberately stricter than io.ReaderAt/io.WriterAt: a short
// read or short write is an error, never a partial success. A guest that is told
// "I wrote 3 of your 8 sectors" has no way to act on that, and virtio-blk has no
// way to say it — a request completes with OK or with IOERR.
type Backend interface {
	// ReadAt fills p entirely from off, or returns an error.
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt stores all of p at off, or returns an error. It does not imply
	// durability; that is Flush.
	WriteAt(p []byte, off int64) (int, error)
	// Flush makes every previously completed WriteAt durable. This is the
	// guest's VIRTIO_BLK_T_FLUSH, and it is the request the §14.4 ACK rules
	// hang off once the WAL is behind this interface.
	Flush(ctx context.Context) error
	// Size is the capacity in bytes. The guest is told Size/512 sectors, so a
	// capacity that is not a whole number of sectors is truncated, never
	// rounded up into space the device does not have.
	Size() int64
}

// ErrOutOfRange is returned by a Backend when a request falls outside the
// device. The virtqueue path turns it into VIRTIO_BLK_S_IOERR, the same as any
// other failure, but it is a distinct sentinel because "the guest asked for a
// sector that does not exist" is a guest bug and "the disk failed" is not.
var ErrOutOfRange = errors.New("vhost: request is outside the device")

// RawDevice is the simplest thing that satisfies Backend: a fixed-capacity raw
// block device held in host memory, where FLUSH is a no-op because there is no
// stable media under it.
//
// It exists so Increment 3.1 can prove the *transport* — handshake, ring
// walking, request completion — against something with no behaviour of its own.
// The WAL is the next increment's job, and wiring it here would mean debugging
// two new things at once.
//
// It is not backed by simio/disk on purpose: disk.File is append-only (Append,
// ReadAt, Truncate, Sync) because that is what a WAL needs, and a raw block
// device needs random writes. See the note on WriteAt in the increment report —
// the missing primitive is disk.File.WriteAt(p []byte, off int64) (int, error).
type RawDevice struct {
	mu     sync.Mutex
	data   []byte
	syncs  int
	writes int
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
// interfaces are what a future file-backed device will satisfy for free.
var (
	_ Backend     = (*RawDevice)(nil)
	_ io.ReaderAt = (*RawDevice)(nil)
	_ io.WriterAt = (*RawDevice)(nil)
)
