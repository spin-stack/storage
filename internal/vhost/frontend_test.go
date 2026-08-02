package vhost

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
)

// This file is the simulated vhost-user front-end: everything QEMU does to this
// backend, expressed in ordinary Go over an ordinary byte slice.
//
// It is what makes the protocol testable at all. The real front-end needs a
// VM, a kernel, shared memory and a boot; this one is a []byte and a struct,
// and it exercises the same Handle/ProcessQueue code paths byte for byte. The
// QEMU integration lane (integration/vhost) is what anchors the *shape* of
// those bytes to reality — a decoder tested only against its own encoder proves
// only that it is self-consistent.

// guestRAMSize is the simulated guest's memory. Large enough for a ring plus
// buffers, small enough to allocate per test.
const guestRAMSize = 1 << 20

// The two bases are deliberately far apart and deliberately different. The
// protocol carries ring addresses in the front-end's virtual address space and
// buffer addresses in the guest's physical one; if the backend ever confuses
// them, a translation must fail rather than land on plausible bytes.
const (
	testGuestPhysBase uint64 = 0x4000_0000
	testUserAddrBase  uint64 = 0x7f00_0000_0000
)

// Ring layout inside the simulated RAM.
const (
	descOffset  uint64 = 0x0000
	availOffset uint64 = 0x2000
	usedOffset  uint64 = 0x4000
	dataOffset  uint64 = 0x8000
)

// fakeGuest is a simulated front-end plus the guest memory behind it.
type fakeGuest struct {
	ram      []byte
	num      uint16
	nextData uint64
	avail    uint16 // the driver's published index
}

func newFakeGuest(num uint16) *fakeGuest {
	return &fakeGuest{ram: make([]byte, guestRAMSize), num: num, nextData: dataOffset}
}

// mapper hands the backend the simulated RAM instead of mmapping anything.
type fakeMapper struct {
	g        *fakeGuest
	unmapped int
	failAt   int // 1-based: fail the Nth Map call, 0 = never
	calls    int
}

func (m *fakeMapper) Map(_ *os.File, offset, size uint64) ([]byte, error) {
	m.calls++
	if m.failAt != 0 && m.calls == m.failAt {
		return nil, fmt.Errorf("simulated mmap failure on call %d", m.calls)
	}
	if offset+size > uint64(len(m.g.ram)) {
		return nil, fmt.Errorf("region %d+%d is past the %d-byte simulated RAM", offset, size, len(m.g.ram))
	}
	return m.g.ram[offset : offset+size], nil
}

func (m *fakeMapper) Unmap([]byte) error { m.unmapped++; return nil }

func (g *fakeGuest) region() Region {
	return Region{
		GuestPhys:  testGuestPhysBase,
		Size:       uint64(len(g.ram)),
		UserAddr:   testUserAddrBase,
		MmapOffset: 0,
	}
}

// msg builds a front-end request.
func msg(r Request, payload []byte, files ...*os.File) Message {
	return Message{Request: r, Flags: flagVersion1, Payload: payload, Files: files}
}

func u64Payload(v uint64) []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, v)
	return p
}

// handshakeMessages is the sequence QEMU 11.0.2 performs, in order: everything
// up to "the queue is live". Increment 3.1's whole claim is that a backend
// answering these serves a real guest, so the sequence is written once and both
// the unit tests and the assertions in integration/vhost refer to it.
func (g *fakeGuest) handshakeMessages() []Message {
	return []Message{
		msg(ReqSetOwner, nil),
		msg(ReqGetFeatures, nil),
		msg(ReqGetProtocolFeatures, nil),
		msg(ReqSetProtocolFeatures, u64Payload(ProtocolFeatures)),
		msg(ReqGetQueueNum, nil),
		msg(ReqGetConfig, encodeConfig(configRequest{Size: blkConfigSize, Region: make([]byte, blkConfigSize)})),
		msg(ReqSetFeatures, u64Payload(DeviceFeatures)),
		msg(ReqSetMemTable, encodeMemTable([]Region{g.region()}), nil),
		msg(ReqSetVringNum, encodeVringState(vringState{Num: uint32(g.num)})),
		msg(ReqSetVringBase, encodeVringState(vringState{Num: 0})),
		msg(ReqSetVringAddr, encodeVringAddr(vringAddr{
			DescUserAddr:  testUserAddrBase + descOffset,
			AvailUserAddr: testUserAddrBase + availOffset,
			UsedUserAddr:  testUserAddrBase + usedOffset,
		})),
		msg(ReqSetVringKick, u64Payload(0), mustEventFile()),
		msg(ReqSetVringCall, u64Payload(0), mustEventFile()),
		msg(ReqSetVringEnable, encodeVringState(vringState{Num: 1})),
	}
}

// reinitMessages is what QEMU sends when the *guest* re-initialises the device: the
// firmware brings it up, boots an OS, and the OS's driver brings it up again with its
// own rings. Captured from a real QEMU 11.0.2 handing a Linux 7.1 guest over from
// SeaBIOS — GET_VRING_BASE stops the queue, and then the whole configuration is
// replayed with **new** kick and call descriptors.
//
// It is not reconnection (3.2): the connection never drops. It is one session in which
// the device is set up twice, which is what every real boot does and what no test did
// until DEV-0018.
func (g *fakeGuest) reinitMessages() []Message {
	return []Message{
		msg(ReqGetVringBase, encodeVringState(vringState{Num: 0})),
		msg(ReqSetFeatures, u64Payload(DeviceFeatures)),
		msg(ReqSetVringCall, u64Payload(0), mustEventFile()),
		msg(ReqSetMemTable, encodeMemTable([]Region{g.region()}), nil),
		msg(ReqSetVringNum, encodeVringState(vringState{Num: uint32(g.num)})),
		msg(ReqSetVringBase, encodeVringState(vringState{Num: 0})),
		msg(ReqSetVringAddr, encodeVringAddr(vringAddr{
			DescUserAddr:  testUserAddrBase + descOffset,
			AvailUserAddr: testUserAddrBase + availOffset,
			UsedUserAddr:  testUserAddrBase + usedOffset,
		})),
		msg(ReqSetVringKick, u64Payload(0), mustEventFile()),
		msg(ReqSetVringEnable, encodeVringState(vringState{Num: 1})),
	}
}

// mustEventFile stands in for the eventfd QEMU passes over SCM_RIGHTS. The
// device only stores it and hands it to the EventFDFunc, so any open
// descriptor is a faithful stand-in for the protocol's purposes; os.Pipe gives
// one without a syscall the simulable analyzer objects to.
func mustEventFile() *os.File {
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}
	_ = w.Close()
	return r
}

// handshake drives dev through the full sequence and fails the test on any
// error.
func (g *fakeGuest) handshake(t *testing.T, dev *Device) {
	t.Helper()
	for _, m := range g.handshakeMessages() {
		if _, err := dev.Handle(t.Context(), m); err != nil {
			t.Fatalf("%s: %v", m.Request, err)
		}
	}
	if !dev.Ready() {
		t.Fatal("device is not ready after the full handshake")
	}
}

// newTestDevice builds a device over a raw block device of the given size.
func newTestDevice(t *testing.T, g *fakeGuest, size int64) (*Device, *RawDevice) {
	t.Helper()
	raw := NewRawDevice(size)
	dev, err := NewDevice(Config{Backend: raw, Mapper: &fakeMapper{g: g}, QueueSize: g.num, Serial: "spin-test"})
	if err != nil {
		t.Fatalf("NewDevice: %v", err)
	}
	t.Cleanup(dev.Close)
	return dev, raw
}

// --- ring construction ------------------------------------------------------

// seg is one descriptor the simulated driver publishes. A segment either
// carries bytes to the device (data) or reserves room for the device to fill
// (size + write).
type seg struct {
	data  []byte
	size  int
	write bool
}

func readable(b []byte) seg { return seg{data: b} }
func writable(n int) seg    { return seg{size: n, write: true} }

// alloc reserves n bytes of guest memory and returns its guest-physical
// address and the backing slice.
func (g *fakeGuest) alloc(n int) (uint64, []byte) {
	off := g.nextData
	// 16-byte aligned, so an indirect table is properly aligned too.
	g.nextData = (g.nextData + uint64(n) + 15) &^ 15
	return testGuestPhysBase + off, g.ram[off : off+uint64(n)]
}

func (g *fakeGuest) descTable() []byte {
	return g.ram[descOffset : descOffset+uint64(g.num)*descSize]
}

// publish lays out a descriptor chain, links it, and makes it available.
// It returns the head index and the backing slice of every segment, so a test
// can read back what the device wrote.
func (g *fakeGuest) publish(head uint16, segs ...seg) [][]byte {
	return g.publishChain(head, false, segs...)
}

// publishIndirect does the same, but through a single VRING_DESC_F_INDIRECT
// descriptor — the layout a Linux guest actually uses once
// VIRTIO_RING_F_INDIRECT_DESC is negotiated.
func (g *fakeGuest) publishIndirect(head uint16, segs ...seg) [][]byte {
	return g.publishChain(head, true, segs...)
}

func (g *fakeGuest) publishChain(head uint16, indirect bool, segs ...seg) [][]byte {
	bufs := make([][]byte, len(segs))
	descs := make([]descriptor, len(segs))
	for i, s := range segs {
		n := s.size
		if !s.write {
			n = len(s.data)
		}
		addr, buf := g.alloc(n)
		copy(buf, s.data)
		bufs[i] = buf
		d := descriptor{addr: addr, len: uint32(n)}
		if s.write {
			d.flags |= descWrite
		}
		if i < len(segs)-1 {
			d.flags |= descNext
		}
		descs[i] = d
	}

	if indirect {
		addr, table := g.alloc(len(descs) * descSize)
		for i, d := range descs {
			d.next = uint16(i + 1)
			writeDesc(table, uint16(i), d)
		}
		writeDesc(g.descTable(), head, descriptor{addr: addr, len: uint32(len(descs) * descSize), flags: descIndirect})
	} else {
		for i, d := range descs {
			d.next = head + uint16(i) + 1
			writeDesc(g.descTable(), head+uint16(i), d)
		}
	}

	g.makeAvailable(head)
	return bufs
}

// makeAvailable pushes head into the available ring and bumps the index — the
// hand-off the whole protocol turns on.
func (g *fakeGuest) makeAvailable(head uint16) {
	slot := availOffset + uint64(ringHeaderSize) + uint64(g.avail%g.num)*2
	binary.LittleEndian.PutUint16(g.ram[slot:slot+2], head)
	g.avail++
	binary.LittleEndian.PutUint16(g.ram[availOffset+2:availOffset+4], g.avail)
}

// suppressInterrupts sets VRING_AVAIL_F_NO_INTERRUPT.
func (g *fakeGuest) suppressInterrupts() {
	binary.LittleEndian.PutUint16(g.ram[availOffset:availOffset+2], availNoInterrupt)
}

func (g *fakeGuest) usedIdx() uint16 {
	return binary.LittleEndian.Uint16(g.ram[usedOffset+2 : usedOffset+4])
}

// usedElem returns the head descriptor id and the byte count the device
// reported for the i-th completion.
func (g *fakeGuest) usedElem(i uint16) (uint32, uint32) {
	off := usedOffset + uint64(ringHeaderSize) + uint64(i%g.num)*usedElemSize
	return binary.LittleEndian.Uint32(g.ram[off : off+4]), binary.LittleEndian.Uint32(g.ram[off+4 : off+8])
}

// blkHeader builds a `struct virtio_blk_outhdr`.
func blkHeader(typ uint32, sector uint64) []byte {
	h := make([]byte, blkHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], typ)
	binary.LittleEndian.PutUint64(h[8:16], sector)
	return h
}

// --- simulated transport ----------------------------------------------------

// fakeConn is an in-memory vhost.Conn: the front-end pushes requests in and
// reads replies out.
type fakeConn struct {
	in     chan Message
	out    chan Message
	closed chan struct{}
	once   sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{in: make(chan Message, 64), out: make(chan Message, 64), closed: make(chan struct{})}
}

func (c *fakeConn) Recv() (Message, error) {
	select {
	case m := <-c.in:
		return m, nil
	case <-c.closed:
		// A real front-end disconnecting gives the reader io.EOF.
		return Message{}, io.EOF
	}
}

func (c *fakeConn) Send(m Message) error {
	select {
	case c.out <- m:
		return nil
	case <-c.closed:
		return io.ErrClosedPipe
	}
}

func (c *fakeConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// fakeListener hands out a prepared connection once, then blocks until closed.
type fakeListener struct {
	conns  chan Conn
	closed chan struct{}
	once   sync.Once
}

func newFakeListener(conns ...Conn) *fakeListener {
	l := &fakeListener{conns: make(chan Conn, len(conns)), closed: make(chan struct{})}
	for _, c := range conns {
		l.conns <- c
	}
	return l
}

func (l *fakeListener) Accept() (Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *fakeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// fakeEvent is a channel standing in for an eventfd.
//
// signal is fixed at construction. It used to be assigned after the queue loop
// had already wired itself up, which is a data race between the test goroutine
// and the loop's — and, worse, a lost notification: the loop can drain and
// signal before the assignment lands, and the test then waits forever for a
// completion that was already delivered to a nil hook.
type fakeEvent struct {
	ch     chan struct{}
	closed chan struct{}
	once   sync.Once
	signal func()

	// waiting announces that the drain preceding a Wait has finished. A real
	// front-end and a real backend share the ring across a process boundary, so
	// their accesses race by construction and the ring's own rules order them.
	// Here both sides are goroutines in one process, and the race detector is
	// right to object: a test that writes the ring while the queue loop is
	// reading it has no happens-before edge at all. Receiving from this channel
	// gives the test one, and it is the loop's own progress that provides it
	// rather than a sleep.
	waiting chan struct{}
}

func newFakeEvent() *fakeEvent {
	return &fakeEvent{
		ch:      make(chan struct{}, 64),
		closed:  make(chan struct{}),
		waiting: make(chan struct{}, 64),
	}
}

func (e *fakeEvent) Wait(ctx context.Context) error {
	select {
	case e.waiting <- struct{}{}:
	default:
	}
	// Prefer a pending signal over a close: the two are often both ready when a
	// session is shutting down, and a random choice there is a flaky test.
	select {
	case <-e.ch:
		return nil
	default:
	}
	select {
	case <-e.ch:
		return nil
	case <-e.closed:
		return io.EOF
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *fakeEvent) Signal() error {
	if e.signal != nil {
		e.signal()
	}
	select {
	case e.ch <- struct{}{}:
	default:
	}
	return nil
}

func (e *fakeEvent) Close() error {
	e.once.Do(func() { close(e.closed) })
	return nil
}
