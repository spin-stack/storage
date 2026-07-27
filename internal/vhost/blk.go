package vhost

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// SectorSize is the unit virtio-blk counts in: the `sector` field of every
// request header is in 512-byte units regardless of the block size the device
// advertises.
const SectorSize = 512

// virtio-blk request types (virtio 1.2 §5.2.6).
const (
	blkTypeIn          uint32 = 0
	blkTypeOut         uint32 = 1
	blkTypeFlush       uint32 = 4
	blkTypeGetID       uint32 = 8
	blkTypeDiscard     uint32 = 11
	blkTypeWriteZeroes uint32 = 13
)

// virtio-blk completion status.
const (
	blkStatusOK     byte = 0
	blkStatusIOErr  byte = 1
	blkStatusUnsupp byte = 2
)

// blkHeaderSize is `struct virtio_blk_outhdr`: type, ioprio, sector.
const blkHeaderSize = 16

// blkIDLength is the fixed length of a VIRTIO_BLK_T_GET_ID answer.
const blkIDLength = 20

// blkRequest is one decoded virtio-blk request: its header, the data the guest
// supplied, the buffer the device must fill, and the single byte it reports
// status in.
type blkRequest struct {
	typ    uint32
	sector uint64
	in     [][]byte // driver-supplied data (WRITE payload)
	out    [][]byte // device-filled data (READ destination)
	status []byte   // exactly one byte
}

// parseRequest splits a descriptor chain into the virtio-blk shape.
//
// virtio does not promise the header is its own descriptor, nor that the status
// byte is: a driver may lay the whole request out in one buffer if
// VIRTIO_F_ANY_LAYOUT is negotiated, and VIRTIO_F_VERSION_1 implies it. So the
// header is taken from the front of the readable bytes and the status from the
// back of the writable bytes, across segment boundaries — not from
// readable[0] and writable[len-1], which is the assumption that makes a backend
// work against Linux and fail against a firmware.
func parseRequest(c chain) (blkRequest, error) {
	head, in, err := takeFront(c.readable, blkHeaderSize)
	if err != nil {
		return blkRequest{}, fmt.Errorf("%w: request header: %w", ErrRing, err)
	}
	status, out, err := takeBack(c.writable, 1)
	if err != nil {
		return blkRequest{}, fmt.Errorf("%w: status byte: %w", ErrRing, err)
	}
	hdr := flatten(head, blkHeaderSize)
	return blkRequest{
		typ:    binary.LittleEndian.Uint32(hdr[0:4]),
		sector: binary.LittleEndian.Uint64(hdr[8:16]),
		in:     in,
		out:    out,
		status: status[0][:1],
	}, nil
}

// serve executes one request against the backend and returns the number of
// bytes written into the device-writable area, which is what the used ring
// reports back to the guest.
//
// Every failure completes the request — with IOERR or UNSUPP — rather than
// propagating. A backend that drops a request because it did not like it leaves
// the guest waiting on a completion that will never come, which looks like a
// hung disk and is far harder to diagnose than an I/O error. Only a malformed
// *ring* is fatal, and that is caught before we get here.
//
// Every Backend error becomes IOERR, and that is not laziness about the ones
// that differ. The status byte has three values (virtio 1.2 §5.2.6): OK, IOERR
// and UNSUPP, of which Linux maps UNSUPP to ENOTSUPP and everything else to
// EIO. So a backend that is fenced, one whose backlog is at its bound, and one
// whose device is full are indistinguishable *on this wire* — there is no
// ENOSPC to send, and inventing a status the front-end does not know would be
// worse than the loss of detail. The distinction is carried where it can be
// acted on: in the error, which OnError reports to the host side, and which
// internal/blockdev classifies for exactly this reason.
func (d *Device) serve(ctx context.Context, r blkRequest) uint32 {
	switch r.typ {
	case blkTypeIn:
		n, err := d.read(r)
		if err != nil {
			r.status[0] = blkStatusIOErr
			d.trace(blkTypeIn, err)
			return 1
		}
		r.status[0] = blkStatusOK
		return n + 1

	case blkTypeOut:
		if err := d.write(r); err != nil {
			r.status[0] = blkStatusIOErr
			d.trace(blkTypeOut, err)
			return 1
		}
		r.status[0] = blkStatusOK
		return 1

	case blkTypeFlush:
		if err := d.backend.Flush(ctx); err != nil {
			r.status[0] = blkStatusIOErr
			d.trace(blkTypeFlush, err)
			return 1
		}
		r.status[0] = blkStatusOK
		return 1

	case blkTypeGetID:
		n := d.getID(r)
		r.status[0] = blkStatusOK
		return n + 1

	case blkTypeDiscard, blkTypeWriteZeroes:
		// Neither feature bit is offered (see DeviceFeatures), so a conforming
		// driver never sends these. Answering UNSUPP is what the spec says to
		// do with the ones that do anyway, and it is strictly better than
		// silently writing zeros over a range the guest expected to be
		// reclaimed. UNSUPP is also the one place the wire is more specific
		// than IOERR: the guest's block layer reads it as ENOTSUPP and stops
		// asking, which is exactly right for a feature we never advertised.
		r.status[0] = blkStatusUnsupp
		return 1

	default:
		r.status[0] = blkStatusUnsupp
		d.trace(r.typ, fmt.Errorf("unsupported virtio-blk request type %d", r.typ))
		return 1
	}
}

func (d *Device) read(r blkRequest) (uint32, error) {
	off, err := byteOffset(r.sector)
	if err != nil {
		return 0, err
	}
	var total uint32
	for _, seg := range r.out {
		n, err := d.backend.ReadAt(seg, off)
		if err != nil {
			return 0, err
		}
		if n != len(seg) {
			return 0, fmt.Errorf("%w: backend read %d of %d bytes at %d", ErrOutOfRange, n, len(seg), off)
		}
		off += int64(n)
		total += uint32(n)
	}
	return total, nil
}

func (d *Device) write(r blkRequest) error {
	off, err := byteOffset(r.sector)
	if err != nil {
		return err
	}
	for _, seg := range r.in {
		n, err := d.backend.WriteAt(seg, off)
		if err != nil {
			return err
		}
		if n != len(seg) {
			return fmt.Errorf("%w: backend wrote %d of %d bytes at %d", ErrOutOfRange, n, len(seg), off)
		}
		off += int64(n)
	}
	return nil
}

// getID answers VIRTIO_BLK_T_GET_ID with the device's serial, padded with NULs
// to the 20 bytes the spec fixes. Linux reads it for /sys/block/*/serial and
// SeaBIOS ignores it; refusing it is legal but produces a scary kernel log line
// for no reason.
func (d *Device) getID(r blkRequest) uint32 {
	id := make([]byte, blkIDLength)
	copy(id, d.serial)
	var total uint32
	for _, seg := range r.out {
		if len(id) == 0 {
			break
		}
		n := copy(seg, id)
		id = id[n:]
		total += uint32(n)
	}
	return total
}

// byteOffset converts a virtio-blk sector number to a byte offset, refusing the
// multiplication that would wrap. A wrapped offset is not a large read, it is a
// read of somebody else's data.
func byteOffset(sector uint64) (int64, error) {
	const maxSector = uint64(1)<<63/SectorSize - 1
	if sector > maxSector {
		return 0, fmt.Errorf("%w: sector %d overflows a byte offset", ErrOutOfRange, sector)
	}
	return int64(sector * SectorSize), nil
}

// errShortChain says the chain did not carry the bytes the shape requires.
var errShortChain = errors.New("chain is shorter than the fixed part it must carry")

// takeFront splits n bytes off the front of segs, returning the pieces that
// make up those n bytes and the remainder.
func takeFront(segs [][]byte, n int) (front, rest [][]byte, err error) {
	for i, s := range segs {
		if n == 0 {
			return front, segs[i:], nil
		}
		if len(s) <= n {
			front = append(front, s)
			n -= len(s)
			continue
		}
		front = append(front, s[:n])
		rest = append(rest, s[n:])
		rest = append(rest, segs[i+1:]...)
		return front, rest, nil
	}
	if n != 0 {
		return nil, nil, fmt.Errorf("%w: %d bytes short", errShortChain, n)
	}
	return front, nil, nil
}

// takeBack splits n bytes off the back of segs.
func takeBack(segs [][]byte, n int) (back, rest [][]byte, err error) {
	for i := len(segs) - 1; i >= 0; i-- {
		s := segs[i]
		if len(s) <= n {
			back = append([][]byte{s}, back...)
			n -= len(s)
			if n == 0 {
				return back, segs[:i], nil
			}
			continue
		}
		cut := len(s) - n
		back = append([][]byte{s[cut:]}, back...)
		rest = append(rest, segs[:i]...)
		rest = append(rest, s[:cut])
		return back, rest, nil
	}
	if n != 0 {
		return nil, nil, fmt.Errorf("%w: %d bytes short", errShortChain, n)
	}
	return back, nil, nil
}

// flatten concatenates segs into exactly n bytes. It copies only when the
// header straddles a descriptor boundary, which drivers in practice never do
// but the spec permits.
func flatten(segs [][]byte, n int) []byte {
	if len(segs) == 1 && len(segs[0]) == n {
		return segs[0]
	}
	out := make([]byte, 0, n)
	for _, s := range segs {
		out = append(out, s...)
	}
	return out[:n]
}
