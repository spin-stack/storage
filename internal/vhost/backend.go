package vhost

import (
	"context"
	"errors"
)

// Backend is the block device behind the virtqueue: what a READ returns, what a
// WRITE stores, and what a FLUSH makes durable.
//
// It is small on purpose, and it is defined here because here is where it is
// consumed. virtio-blk needs exactly these four things, so this is the whole
// seam between the transport and the storage engine: Increment 3.1 satisfied it
// with a raw device, and internal/blockdev now satisfies it over wal.Log
// without the protocol code learning anything about WALs, epochs or object
// stores — and without wal learning anything about descriptor chains.
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
	// hang off now that the WAL is behind this interface: an implementation
	// that cannot establish durability must return an error, because the only
	// alternative is telling the guest its data is safe when it is not.
	Flush(ctx context.Context) error
	// Discard releases [off, off+length). The guest is telling the device it no
	// longer needs the contents, so the space may be reclaimed; virtio leaves
	// what a later read returns unspecified, and this interface narrows that to
	// zeros, because every implementation behind it serves the absence of an
	// extent as zeros and a caller that could not rely on it would have to
	// write the zeros itself.
	Discard(off, length int64) error
	// WriteZeroes makes [off, off+length) read back as zeros. unmap carries the
	// guest's may_unmap flag: it *permits* releasing the space rather than
	// requiring it, so an implementation that always unmaps is conforming and
	// one that never does is too. The flag is passed through rather than
	// swallowed because it is the guest's only way to say "I care about the
	// allocation", and an implementation that grows a reason to honour it
	// should not have to change this interface to hear it.
	WriteZeroes(off, length int64, unmap bool) error
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
