package hostio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/spin-stack/storage/internal/vhost"
)

// RawFile is a raw block device backed by an ordinary file: the Backend
// Increment 3.1 serves to QEMU.
//
// It is scaffolding, and saying so is the point. Its job is to make the
// *transport* provable against a real front-end — handshake, ring walking,
// request completion, a FLUSH that actually reaches the disk — against storage
// with no behaviour of its own. Phase 04 puts wal.Log behind vhost.Backend and
// this becomes a test fixture.
//
// # Why not simio/disk (ADR-0020)
//
// disk.File is append-only — Append, ReadAt, Truncate, Sync — because that is
// what a WAL needs. A block device needs random writes at arbitrary offsets, and
// the missing primitive is WriteAt(p []byte, off int64) (int, error). Adding a
// verb to a durability-critical interface, to both of its implementations and to
// its contract tests, for a backend that Phase 04 deletes is the wrong trade; so
// this lives inside the already-exempt hostio package and uses the file directly.
// If a later phase needs random-write durable files on the data path — not as
// scaffolding — that is when disk.File grows WriteAt, with its own crash model.
//
// Reads and writes go through pread/pwrite, which are safe to issue
// concurrently, so the only lock is the one that keeps Close from racing them.
type RawFile struct {
	size int64

	mu     sync.RWMutex
	f      *os.File
	closed bool
}

// errClosed is what every entry point returns after Close. Operating on a
// descriptor the runtime may have handed to someone else is worse than failing.
var errClosed = errors.New("hostio: the raw device is closed")

// CreateRawFile creates (or truncates) path to exactly size bytes and serves it.
func CreateRawFile(path string, size int64) (*RawFile, error) {
	if err := checkCapacity(size); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("hostio: creating raw device %s: %w", path, err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("hostio: sizing raw device %s to %d bytes: %w", path, size, err)
	}
	return &RawFile{f: f, size: size}, nil
}

// OpenRawFile serves an existing file at the capacity it already has.
func OpenRawFile(path string) (*RawFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("hostio: opening raw device %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("hostio: sizing raw device %s: %w", path, err)
	}
	if err := checkCapacity(fi.Size()); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("hostio: raw device %s: %w", path, err)
	}
	return &RawFile{f: f, size: fi.Size()}, nil
}

// checkCapacity refuses a size the guest could not be told about honestly. The
// capacity in the virtio-blk config is in 512-byte sectors, so a partial
// trailing sector is either capacity the guest would address and the device
// would refuse, or capacity silently thrown away — and which of the two it is
// depends on a rounding decision nobody should have to guess at.
func checkCapacity(size int64) error {
	if size <= 0 {
		return fmt.Errorf("hostio: a raw device of %d bytes has no capacity", size)
	}
	if size%vhost.SectorSize != 0 {
		return fmt.Errorf("hostio: a raw device of %d bytes is not a whole number of %d-byte sectors", size, vhost.SectorSize)
	}
	return nil
}

// Size implements vhost.Backend.
func (d *RawFile) Size() int64 { return d.size }

// ReadAt implements vhost.Backend: it fills p entirely or returns an error.
func (d *RawFile) ReadAt(p []byte, off int64) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return 0, errClosed
	}
	if err := d.inRange(len(p), off); err != nil {
		return 0, err
	}
	n, err := d.f.ReadAt(p, off)
	if err != nil {
		// The range check above already excluded reading past the end, so an EOF
		// here means the file shrank under us — a truncated device file, not a
		// guest asking for a sector that does not exist.
		if errors.Is(err, io.EOF) {
			return n, fmt.Errorf("hostio: raw device is shorter than its %d-byte capacity: %w", d.size, err)
		}
		return n, fmt.Errorf("hostio: reading %d bytes at %d: %w", len(p), off, err)
	}
	return n, nil
}

// WriteAt implements vhost.Backend: it stores all of p or returns an error, and
// it never extends the device.
func (d *RawFile) WriteAt(p []byte, off int64) (int, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return 0, errClosed
	}
	if err := d.inRange(len(p), off); err != nil {
		return 0, err
	}
	n, err := d.f.WriteAt(p, off)
	if err != nil {
		return n, fmt.Errorf("hostio: writing %d bytes at %d: %w", len(p), off, err)
	}
	return n, nil
}

// Flush implements vhost.Backend. This is the guest's VIRTIO_BLK_T_FLUSH, and
// it is the only thing on this interface that makes a durability claim: it must
// not return until the writes it covers are on stable media.
func (d *RawFile) Flush(context.Context) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return errClosed
	}
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("hostio: flushing the raw device: %w", err)
	}
	return nil
}

// Close releases the file. It is idempotent: the session's teardown and an
// operator's shutdown can both reach it.
func (d *RawFile) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if err := d.f.Close(); err != nil {
		return fmt.Errorf("hostio: closing the raw device: %w", err)
	}
	return nil
}

// inRange refuses anything that does not fall wholly inside the device. The
// arithmetic is written to avoid the overflow it is checking for: off+n could
// wrap, and a wrapped offset is not a large request, it is a request for
// somebody else's data.
func (d *RawFile) inRange(n int, off int64) error {
	if off < 0 || n < 0 || off > d.size || int64(n) > d.size-off {
		return fmt.Errorf("%w: %d bytes at %d, capacity %d", vhost.ErrOutOfRange, n, off, d.size)
	}
	return nil
}

var _ vhost.Backend = (*RawFile)(nil)
