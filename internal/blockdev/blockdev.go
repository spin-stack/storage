package blockdev

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// ErrDeviceFull says the local WAL device has no room left: wal.Degraded() reports
// OUT_OF_SPACE after the append the caller is being told about.
//
// It is a classification laid over the device's own error, never a replacement for it
// — the underlying disk error is still in the chain — because "the device is full" is
// the one I/O failure whose remedy (truncate after a checkpoint, grow the device,
// restore the object store so the remote gap can close) is different from every
// other's, and an operator reading a log line needs to be told which one they have.
//
// It is deliberately not a fencing condition. See the package doc.
var ErrDeviceFull = errors.New("the local WAL device is out of space")

// Device serves one volume's guest-visible block device out of a wal.Log. It
// satisfies vhost.Backend; see the package doc for what each method promises.
//
// The zero value is not usable: a device with no log and no capacity has nothing to
// tell a guest. Use New.
//
// It holds no lock of its own. That is a statement about where the invariants live,
// not an omission: wal.Log owns them and is safe for concurrent use, with two mutexes
// and a documented rule that neither is held across an object-store PUT. A mutex here
// could only re-serialize what the Log already serializes correctly — and, held across
// Flush, it would put S3 latency back into the guest's read path through the door the
// Log's design exists to close.
//
// The hazard a lock here would have been for is real and is handled in wal: a WRITE
// arriving in the middle of a FLUSH must not be ACKed by that FLUSH. Log.Flush captures
// its target sequence under the same mutex Log.Write appends under, so a WRITE that
// returns afterwards has a strictly higher sequence and advanceDurable(target) cannot
// reach it (`TestAGuestWriteCompletesWhileAFlushIsUploading`).
type Device struct {
	log *wal.Log
	cap int64
}

// New returns a Device of capacity bytes over l.
//
// The capacity must be a whole number of 512-byte sectors: that is the only unit a
// virtio-blk configuration space can express, so a partial trailing sector is either
// capacity the guest addresses and the device refuses, or capacity silently discarded
// — and which of the two it is should not depend on a rounding decision made here.
//
// The Device does not take ownership of l: the caller keeps it for checkpointing,
// truncation and reporting, and closes it.
func New(l *wal.Log, capacity int64) (*Device, error) {
	if l == nil {
		return nil, errors.New("blockdev: a device needs a WAL to serve")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("blockdev: a device of %d bytes has no capacity", capacity)
	}
	if capacity%vhost.SectorSize != 0 {
		return nil, fmt.Errorf("blockdev: a device of %d bytes is not a whole number of %d-byte sectors",
			capacity, vhost.SectorSize)
	}
	return &Device{log: l, cap: capacity}, nil
}

// Size implements vhost.Backend: the capacity the guest is told about.
func (d *Device) Size() int64 { return d.cap }

// ReadAt implements vhost.Backend. It answers out of the WAL's read view, which holds
// every write this log has taken whether or not it has been flushed — so a guest that
// writes a block and reads it back without a FLUSH sees its own bytes. Ranges nothing
// has written read as zero, which is what an empty volume is.
func (d *Device) ReadAt(p []byte, off int64) (int, error) {
	if err := d.inRange(len(p), off, "READ"); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := d.log.Read(uint64(off), p); err != nil {
		// A read that cannot be answered must fail, never return the zeros it happens
		// to hold: the guest cannot tell those from a range it never wrote.
		return 0, d.refuse("READ", len(p), off, err)
	}
	return len(p), nil
}

// WriteAt implements vhost.Backend: it appends a WAL record and returns. It makes no
// durability claim and issues no object-store PUT (§5.3, INV-18); that is Flush.
//
// The flags are 0 and never format.FlagFUA. wal.Log.Write refuses FUA on purpose
// because it implements none of the FUA ACK contract, and a virtio-blk request has no
// way to ask for it in any case — see the package doc.
func (d *Device) WriteAt(p []byte, off int64) (int, error) {
	if err := d.inRange(len(p), off, "WRITE"); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		// A zero-length WRITE is not a record. Appending one would consume a sequence
		// and a header to describe nothing, and replay would have to carry it forever.
		return 0, nil
	}
	if _, err := d.log.Write(uint64(off), p, 0); err != nil {
		return 0, d.refuse("WRITE", len(p), off, err)
	}
	return len(p), nil
}

// Flush implements vhost.Backend, and it is the guest's VIRTIO_BLK_T_FLUSH. It is the
// §14.4 ACK path and not an fsync: in `remote` mode it returns only after every
// covering object is verified in the object store and the lease is confirmed valid on
// the monotonic clock (INV-06, INV-07).
//
// A failure is returned, never swallowed. The guest completes the request with IOERR
// and knows its writes are not safe; a Flush that reported success on a lapsed lease
// or an unreachable store would be the one lie this whole design exists to prevent.
func (d *Device) Flush(ctx context.Context) error {
	if err := d.log.Flush(ctx); err != nil {
		return d.refuse("FLUSH", 0, 0, err)
	}
	return nil
}

// inRange refuses anything that does not fall wholly inside the device. The
// arithmetic avoids the overflow it is checking for: off+n can wrap, and a wrapped
// offset is not a large request, it is a request for somebody else's data.
func (d *Device) inRange(n int, off int64, op string) error {
	if off < 0 || n < 0 || off > d.cap || int64(n) > d.cap-off {
		return fmt.Errorf("blockdev: %s of %d bytes at %d is outside the %d-byte device: %w",
			op, n, off, d.cap, vhost.ErrOutOfRange)
	}
	return nil
}

// refuse turns a wal error into the one the guest's request fails with. All of them
// complete as VIRTIO_BLK_S_IOERR — the wire has nothing finer — so what this adds is
// for the host side: a sentinel the Agent can branch on and a sentence an operator can
// act on. See the package doc for why the three cases are not the same failure.
func (d *Device) refuse(op string, n int, off int64, err error) error {
	where := fmt.Sprintf("%s of %d bytes at %d", op, n, off)
	if op == "FLUSH" {
		where = "FLUSH"
	}
	switch {
	case errors.Is(err, wal.ErrLogBroken):
		// Not a loss of authority — nothing took the volume away. This log cannot say
		// what its tail holds after a failed rollback, so it will not confirm anything
		// against it. The lease-fencing case that used to be here went with the
		// lease-gated ACK (ADR-0026).
		return fmt.Errorf("blockdev: %s refused: this volume's log cannot describe its own tail: %w", where, err)
	case errors.Is(err, wal.ErrBackpressure):
		return fmt.Errorf("blockdev: %s refused: the unflushed backlog is at its bound (§5.7); a successful FLUSH clears it: %w", where, err)
	}
	// The device state is read *after* the failed append, never before it: the
	// out-of-space latch is cleared only by an append the device took (see
	// wal.Degraded), so a caller that pre-checked it would refuse every write from the
	// first ENOSPC onwards and the volume would never come back.
	if d.log.Degraded() == wal.DegradedOutOfSpace {
		return fmt.Errorf("blockdev: %s refused: %w — truncate after a checkpoint, grow the device, or restore the object store: %w",
			where, ErrDeviceFull, err)
	}
	return fmt.Errorf("blockdev: %s failed: %w", where, err)
}

// blockdev.Device is what a guest is served from; hostio.RawFile is the scaffolding it
// replaces.
var _ vhost.Backend = (*Device)(nil)
