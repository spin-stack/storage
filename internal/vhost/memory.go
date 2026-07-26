package vhost

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// ErrUnmapped is returned when an address the front-end handed us does not fall
// inside any shared memory region. It is a hard failure on purpose: a virtqueue
// descriptor pointing outside the memory table is either a bug in the front-end
// or a hostile guest, and the one thing a block backend must never do with such
// an address is guess.
var ErrUnmapped = errors.New("vhost: address is outside the shared memory table")

// Mapper turns a memory-region file descriptor from SET_MEM_TABLE into host
// memory this process can address. It is defined here, where the device
// consumes it: the production implementation mmaps (internal/vhost/hostio), and
// tests hand back an ordinary byte slice — which is what lets the address
// translation and the whole virtqueue walk be tested without a VM.
type Mapper interface {
	// Map returns size bytes of the file starting at offset, shared with the
	// front-end (writes through the returned slice must be visible to it).
	Map(f *os.File, offset, size uint64) ([]byte, error)
	// Unmap releases a slice returned by Map.
	Unmap(b []byte) error
}

// regionSize is one `struct vhost_user_memory_region` on the wire.
const regionSize = 32

// memTableHeaderSize is the nregions + padding pair that precedes the regions.
const memTableHeaderSize = 8

// maxRegions is VHOST_MEMORY_BASELINE_NREGIONS. Without
// VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS — which this backend does not
// advertise — the front-end may not send more.
const maxRegions = 8

// Region is one entry of the front-end's memory table: a slice of guest RAM,
// described in three address spaces at once.
type Region struct {
	// GuestPhys is where the guest sees these bytes. Virtqueue descriptors
	// address buffers this way.
	GuestPhys uint64
	// Size is the length of the region in all three address spaces.
	Size uint64
	// UserAddr is where the *front-end process* sees these bytes.
	// SET_VRING_ADDR addresses the ring this way.
	UserAddr uint64
	// MmapOffset is where the region starts inside the file descriptor that
	// arrived with it.
	MmapOffset uint64

	mem []byte
}

// AddressSpace is the guest memory the front-end shared with this backend, and
// the two translations into it.
//
// The two translations are the subtlety of the whole protocol. SET_VRING_ADDR
// carries addresses in the *front-end's* virtual address space, while the
// descriptors inside that ring carry *guest-physical* addresses. Both are valid
// addresses for overlapping bytes, they differ by a per-region constant, and
// using one where the other belongs yields a readable slice of the wrong memory
// rather than an error.
type AddressSpace struct {
	mapper  Mapper
	regions []Region
}

// NewAddressSpace maps every region of a SET_MEM_TABLE. On any failure it
// unmaps whatever it already mapped: a half-installed memory table would let
// later translations succeed for some addresses and silently fail for others.
func NewAddressSpace(mapper Mapper, regions []Region, files []*os.File) (*AddressSpace, error) {
	if len(files) != len(regions) {
		return nil, fmt.Errorf("%w: memory table has %d regions but %d file descriptors", ErrProtocol, len(regions), len(files))
	}
	a := &AddressSpace{mapper: mapper}
	for i, r := range regions {
		mem, err := mapper.Map(files[i], r.MmapOffset, r.Size)
		if err != nil {
			a.Close()
			return nil, fmt.Errorf("vhost: mapping region %d (gpa %#x, %d bytes): %w", i, r.GuestPhys, r.Size, err)
		}
		r.mem = mem
		a.regions = append(a.regions, r)
	}
	return a, nil
}

// Close unmaps every region. It is safe on a nil or partially built space.
func (a *AddressSpace) Close() {
	if a == nil {
		return
	}
	for _, r := range a.regions {
		if r.mem != nil {
			_ = a.mapper.Unmap(r.mem)
		}
	}
	a.regions = nil
}

// Guest translates a guest-physical address: what virtqueue descriptors carry.
func (a *AddressSpace) Guest(addr, n uint64) ([]byte, error) {
	return a.translate(addr, n, func(r Region) uint64 { return r.GuestPhys }, "guest-physical")
}

// User translates a front-end userspace address: what SET_VRING_ADDR carries.
func (a *AddressSpace) User(addr, n uint64) ([]byte, error) {
	return a.translate(addr, n, func(r Region) uint64 { return r.UserAddr }, "front-end userspace")
}

func (a *AddressSpace) translate(addr, n uint64, base func(Region) uint64, space string) ([]byte, error) {
	if a == nil {
		return nil, fmt.Errorf("%w: no memory table installed", ErrUnmapped)
	}
	for _, r := range a.regions {
		b := base(r)
		if addr < b || addr-b >= r.Size {
			continue
		}
		off := addr - b
		// A buffer that starts inside a region but runs past its end is not
		// servable: the bytes past the boundary belong to a different mapping
		// (or to nothing), and stitching regions together would hide a
		// front-end bug behind a correct-looking read.
		if n > r.Size-off {
			return nil, fmt.Errorf("%w: %s %#x+%d runs past the end of its region (%#x, %d bytes)", ErrUnmapped, space, addr, n, b, r.Size)
		}
		return r.mem[off : off+n : off+n], nil
	}
	return nil, fmt.Errorf("%w: %s %#x+%d", ErrUnmapped, space, addr, n)
}

// decodeMemTable parses a SET_MEM_TABLE payload.
func decodeMemTable(m Message) ([]Region, error) {
	if len(m.Payload) < memTableHeaderSize {
		return nil, fmt.Errorf("%w: SET_MEM_TABLE wants at least %d bytes, got %d", ErrProtocol, memTableHeaderSize, len(m.Payload))
	}
	n := int(binary.LittleEndian.Uint32(m.Payload[0:4]))
	if n == 0 || n > maxRegions {
		return nil, fmt.Errorf("%w: SET_MEM_TABLE declares %d regions (limit %d)", ErrProtocol, n, maxRegions)
	}
	if len(m.Payload) < memTableHeaderSize+n*regionSize {
		return nil, fmt.Errorf("%w: SET_MEM_TABLE declares %d regions but carries %d bytes", ErrProtocol, n, len(m.Payload))
	}
	regions := make([]Region, 0, n)
	for i := range n {
		p := m.Payload[memTableHeaderSize+i*regionSize:]
		r := Region{
			GuestPhys:  binary.LittleEndian.Uint64(p[0:8]),
			Size:       binary.LittleEndian.Uint64(p[8:16]),
			UserAddr:   binary.LittleEndian.Uint64(p[16:24]),
			MmapOffset: binary.LittleEndian.Uint64(p[24:32]),
		}
		if r.Size == 0 {
			return nil, fmt.Errorf("%w: SET_MEM_TABLE region %d is empty", ErrProtocol, i)
		}
		regions = append(regions, r)
	}
	return regions, nil
}

// encodeMemTable builds a SET_MEM_TABLE payload. It exists so the simulated
// front-end in the tests speaks exactly the bytes the real one does — a decoder
// tested only against its own encoder proves nothing, so the QEMU integration
// lane is what anchors this shape to reality.
func encodeMemTable(regions []Region) []byte {
	p := make([]byte, memTableHeaderSize+len(regions)*regionSize)
	binary.LittleEndian.PutUint32(p[0:4], uint32(len(regions)))
	for i, r := range regions {
		q := p[memTableHeaderSize+i*regionSize:]
		binary.LittleEndian.PutUint64(q[0:8], r.GuestPhys)
		binary.LittleEndian.PutUint64(q[8:16], r.Size)
		binary.LittleEndian.PutUint64(q[16:24], r.UserAddr)
		binary.LittleEndian.PutUint64(q[24:32], r.MmapOffset)
	}
	return p
}
