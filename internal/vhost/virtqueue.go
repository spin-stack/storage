package vhost

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrRing is returned when the ring itself is malformed — a descriptor index
// past the table, a chain that loops, a buffer outside shared memory. Like
// ErrProtocol it condemns the connection, not the request: a device that keeps
// serving out of a ring it cannot parse is a device writing guest memory at
// addresses it invented.
var ErrRing = errors.New("vhost: malformed virtqueue")

// MaxQueueSize is the queue depth §4's decision table fixes: one queue, 128
// entries. It also bounds the descriptor chain walk, so a ring whose `next`
// pointers form a cycle terminates with ErrRing instead of spinning.
const MaxQueueSize = 128

// descSize is one split-ring descriptor: addr, len, flags, next.
const descSize = 16

// Descriptor flags (virtio 1.2 §2.7.5).
const (
	descNext     uint16 = 1
	descWrite    uint16 = 2
	descIndirect uint16 = 4
)

// usedElemSize is one used-ring element: the head descriptor id and the number
// of bytes the device wrote into the chain.
const usedElemSize = 8

// ringHeaderSize is the flags + idx pair at the front of both the available and
// the used ring.
const ringHeaderSize = 4

// descriptor is one split-ring descriptor, decoded.
type descriptor struct {
	addr  uint64
	len   uint32
	flags uint16
	next  uint16
}

// vring is one configured split virtqueue: the three areas the front-end
// mapped for us, plus where this backend has consumed up to.
//
// desc, avail and used are slices of the *shared* mapping, so every read here
// races with the guest by construction. That is the normal state of a virtqueue
// and the ring's rules handle it: the guest publishes a buffer by bumping
// avail.idx after the descriptor is fully written, and the device publishes a
// completion by bumping used.idx after the used element is fully written. Both
// of those hand-offs need the stores before them to be visible first, which is
// what barrier() is for.
type vring struct {
	num   uint16
	desc  []byte
	avail []byte
	used  []byte

	lastAvail uint16 // the next avail-ring slot this backend will consume
	usedIdx   uint16 // the next used-ring slot this backend will publish

	// fence is never read for its value. Add(0) compiles to a locked
	// read-modify-write, which is a full memory barrier on every architecture
	// Go supports — the portable way to order the plain stores into the shared
	// mapping without reaching for unsafe to get a properly typed atomic view
	// of a 16-bit field inside a byte slice.
	fence atomic.Uint32
}

func (v *vring) barrier() { v.fence.Add(0) }

// configured reports whether SET_VRING_NUM and SET_VRING_ADDR have both landed.
func (v *vring) configured() bool {
	return v.num > 0 && v.desc != nil && v.avail != nil && v.used != nil
}

// availIdx is the guest's publish counter: everything below it is available.
func (v *vring) availIdx() uint16 {
	idx := binary.LittleEndian.Uint16(v.avail[2:4])
	// Order the descriptor reads that follow *after* this load: the guest wrote
	// the descriptor before bumping idx, and we must not read it before.
	v.barrier()
	return idx
}

// availRingEntry is the descriptor head published in slot i.
func (v *vring) availRingEntry(i uint16) uint16 {
	off := ringHeaderSize + int(i%v.num)*2
	return binary.LittleEndian.Uint16(v.avail[off : off+2])
}

// readDesc decodes descriptor i from a descriptor table.
func readDesc(table []byte, i uint16) (descriptor, error) {
	off := int(i) * descSize
	if off+descSize > len(table) {
		return descriptor{}, fmt.Errorf("%w: descriptor %d is past a %d-byte table", ErrRing, i, len(table))
	}
	p := table[off:]
	return descriptor{
		addr:  binary.LittleEndian.Uint64(p[0:8]),
		len:   binary.LittleEndian.Uint32(p[8:12]),
		flags: binary.LittleEndian.Uint16(p[12:14]),
		next:  binary.LittleEndian.Uint16(p[14:16]),
	}, nil
}

func writeDesc(table []byte, i uint16, d descriptor) {
	p := table[int(i)*descSize:]
	binary.LittleEndian.PutUint64(p[0:8], d.addr)
	binary.LittleEndian.PutUint32(p[8:12], d.len)
	binary.LittleEndian.PutUint16(p[12:14], d.flags)
	binary.LittleEndian.PutUint16(p[14:16], d.next)
}

// chain is one descriptor chain, already translated into host memory: what the
// device may read (driver-to-device) and what it must fill
// (device-to-driver), in ring order.
type chain struct {
	head     uint16
	readable [][]byte
	writable [][]byte
}

// next walks the chain starting at head, resolving every descriptor into a
// slice of shared memory.
//
// The walk is bounded twice over: by the descriptor table's own size and by
// MaxQueueSize per table. A ring whose `next` chain cycles is a guest that hangs
// the backend forever if the walk is unbounded, and hanging the backend hangs
// every other volume on the host.
func (v *vring) collect(space *AddressSpace, head uint16) (chain, error) {
	c := chain{head: head}
	table := v.desc
	limit := int(v.num)
	i := head
	for steps := 0; ; steps++ {
		if steps >= limit {
			return chain{}, fmt.Errorf("%w: descriptor chain from %d exceeds %d links", ErrRing, head, limit)
		}
		d, err := readDesc(table, i)
		if err != nil {
			return chain{}, err
		}
		if d.flags&descIndirect != 0 {
			// An indirect descriptor replaces the rest of the chain with a
			// table of its own. virtio forbids nesting, so the inner walk uses
			// a fresh budget and never recurses again.
			if err := v.collectIndirect(space, &c, d); err != nil {
				return chain{}, err
			}
			return c, nil
		}
		if err := appendSegment(space, &c, d); err != nil {
			return chain{}, err
		}
		if d.flags&descNext == 0 {
			return c, nil
		}
		i = d.next
	}
}

func (v *vring) collectIndirect(space *AddressSpace, c *chain, d descriptor) error {
	if d.len%descSize != 0 || d.len == 0 {
		return fmt.Errorf("%w: indirect table of %d bytes is not a whole number of descriptors", ErrRing, d.len)
	}
	table, err := space.Guest(d.addr, uint64(d.len))
	if err != nil {
		return fmt.Errorf("%w: indirect table: %w", ErrRing, err)
	}
	limit := int(d.len / descSize)
	if limit > MaxQueueSize {
		limit = MaxQueueSize
	}
	i := uint16(0)
	for steps := 0; ; steps++ {
		if steps >= limit {
			return fmt.Errorf("%w: indirect chain exceeds %d links", ErrRing, limit)
		}
		id, err := readDesc(table, i)
		if err != nil {
			return err
		}
		if id.flags&descIndirect != 0 {
			return fmt.Errorf("%w: nested indirect descriptor", ErrRing)
		}
		if err := appendSegment(space, c, id); err != nil {
			return err
		}
		if id.flags&descNext == 0 {
			return nil
		}
		i = id.next
	}
}

func appendSegment(space *AddressSpace, c *chain, d descriptor) error {
	if d.len == 0 {
		return nil
	}
	buf, err := space.Guest(d.addr, uint64(d.len))
	if err != nil {
		return fmt.Errorf("%w: descriptor buffer: %w", ErrRing, err)
	}
	if d.flags&descWrite != 0 {
		c.writable = append(c.writable, buf)
	} else {
		c.readable = append(c.readable, buf)
	}
	return nil
}

// complete publishes one finished chain: the used element first, then the
// index the guest polls, with a barrier between them.
func (v *vring) complete(head uint16, written uint32) {
	slot := int(v.usedIdx%v.num)*usedElemSize + ringHeaderSize
	binary.LittleEndian.PutUint32(v.used[slot:slot+4], uint32(head))
	binary.LittleEndian.PutUint32(v.used[slot+4:slot+8], written)
	// The used element must be visible before the index that advertises it;
	// otherwise the guest reads a slot that still holds the previous
	// completion and hands the wrong buffer back to its caller.
	v.barrier()
	v.usedIdx++
	binary.LittleEndian.PutUint16(v.used[2:4], v.usedIdx)
	v.barrier()
}

// availFlags is the guest's notification suppression bit
// (VRING_AVAIL_F_NO_INTERRUPT).
const availNoInterrupt uint16 = 1

// shouldNotify reports whether the guest wants an interrupt for the completions
// just published. Without VIRTIO_RING_F_EVENT_IDX this is the single flag bit,
// which is the whole reason that feature is not negotiated (see DeviceFeatures).
func (v *vring) shouldNotify() bool {
	return binary.LittleEndian.Uint16(v.avail[0:2])&availNoInterrupt == 0
}
