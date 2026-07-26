package vhost

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

const testDeviceSize = 1 << 20

// pattern returns n recognisable bytes: a wrong offset produces a different
// pattern rather than more zeros.
func pattern(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed ^ byte(i*7+3)
	}
	return b
}

// TestServeReadWriteFlush is the increment's objective in one table: the three
// request types §30.3 names, driven through a real descriptor chain by the
// simulated front-end and completed against the backend.
func TestServeReadWriteFlush(t *testing.T) {
	tests := []struct {
		name string
		// drive publishes one request and returns the segments it should
		// inspect, plus the expectation.
		drive func(t *testing.T, g *fakeGuest, raw *RawDevice) (bufs [][]byte, wantStatus byte, wantWritten uint32)
	}{
		{
			name: "WRITE stores the guest's bytes at the sector it named",
			drive: func(t *testing.T, g *fakeGuest, raw *RawDevice) ([][]byte, byte, uint32) {
				data := pattern(0x11, 4*SectorSize)
				bufs := g.publish(0, readable(blkHeader(blkTypeOut, 9)), readable(data), writable(1))
				t.Cleanup(func() {
					if got := raw.Snapshot(9*SectorSize, len(data)); !bytes.Equal(got, data) {
						t.Fatalf("the device does not hold what the guest wrote at sector 9")
					}
					if raw.Writes() != 1 {
						t.Fatalf("backend saw %d writes, want 1", raw.Writes())
					}
				})
				return bufs, blkStatusOK, 1
			},
		},
		{
			name: "READ returns what the device holds",
			drive: func(t *testing.T, g *fakeGuest, raw *RawDevice) ([][]byte, byte, uint32) {
				data := pattern(0x22, 2*SectorSize)
				if _, err := raw.WriteAt(data, 3*SectorSize); err != nil {
					t.Fatal(err)
				}
				bufs := g.publish(0, readable(blkHeader(blkTypeIn, 3)), writable(len(data)), writable(1))
				t.Cleanup(func() {
					if !bytes.Equal(bufs[1], data) {
						t.Fatal("the guest's buffer does not hold what the device had at sector 3")
					}
				})
				return bufs, blkStatusOK, uint32(len(data)) + 1
			},
		},
		{
			name: "READ of never-written sectors reads as zero",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeIn, 100)), writable(SectorSize), writable(1))
				// Poison the destination so "unchanged" cannot pass for "zeroed".
				copy(bufs[1], pattern(0xff, SectorSize))
				t.Cleanup(func() {
					if !bytes.Equal(bufs[1], make([]byte, SectorSize)) {
						t.Fatal("an unwritten sector did not read back as zero")
					}
				})
				return bufs, blkStatusOK, SectorSize + 1
			},
		},
		{
			name: "FLUSH reaches the backend",
			drive: func(t *testing.T, g *fakeGuest, raw *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
				t.Cleanup(func() {
					if raw.Flushes() != 1 {
						t.Fatalf("backend saw %d flushes, want 1", raw.Flushes())
					}
				})
				return bufs, blkStatusOK, 1
			},
		},
		{
			name: "GET_ID answers the device serial",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeGetID, 0)), writable(blkIDLength), writable(1))
				t.Cleanup(func() {
					want := make([]byte, blkIDLength)
					copy(want, "spin-test")
					if !bytes.Equal(bufs[1], want) {
						t.Fatalf("serial %q, want %q", bufs[1], want)
					}
				})
				return bufs, blkStatusOK, blkIDLength + 1
			},
		},
		{
			name: "DISCARD is refused rather than silently emulated",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeDiscard, 0)), readable(make([]byte, 16)), writable(1))
				return bufs, blkStatusUnsupp, 1
			},
		},
		{
			name: "WRITE_ZEROES is refused rather than silently emulated",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeWriteZeroes, 0)), readable(make([]byte, 16)), writable(1))
				return bufs, blkStatusUnsupp, 1
			},
		},
		{
			name: "an unknown request type is refused, not dropped",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(0x7f, 0)), writable(1))
				return bufs, blkStatusUnsupp, 1
			},
		},
		{
			name: "a READ past the end of the device fails the request, not the connection",
			drive: func(t *testing.T, g *fakeGuest, raw *RawDevice) ([][]byte, byte, uint32) {
				last := uint64(raw.Size()/SectorSize) - 1
				bufs := g.publish(0, readable(blkHeader(blkTypeIn, last)), writable(4*SectorSize), writable(1))
				return bufs, blkStatusIOErr, 1
			},
		},
		{
			name: "a WRITE past the end of the device fails the request",
			drive: func(t *testing.T, g *fakeGuest, raw *RawDevice) ([][]byte, byte, uint32) {
				last := uint64(raw.Size()/SectorSize) - 1
				bufs := g.publish(0, readable(blkHeader(blkTypeOut, last)), readable(pattern(3, 4*SectorSize)), writable(1))
				return bufs, blkStatusIOErr, 1
			},
		},
		{
			name: "a sector number that would overflow a byte offset fails the request",
			drive: func(t *testing.T, g *fakeGuest, _ *RawDevice) ([][]byte, byte, uint32) {
				bufs := g.publish(0, readable(blkHeader(blkTypeIn, 1<<62)), writable(SectorSize), writable(1))
				return bufs, blkStatusIOErr, 1
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, raw := newTestDevice(t, g, testDeviceSize)
			g.handshake(t, dev)

			bufs, wantStatus, wantWritten := tc.drive(t, g, raw)

			served, notify, err := dev.ProcessQueue(t.Context())
			if err != nil {
				t.Fatalf("ProcessQueue: %v", err)
			}
			if served != 1 {
				t.Fatalf("served %d requests, want 1", served)
			}
			if !notify {
				t.Fatal("the guest did not suppress interrupts, so it must be notified")
			}
			status := bufs[len(bufs)-1][0]
			if status != wantStatus {
				t.Fatalf("status %d, want %d", status, wantStatus)
			}
			if g.usedIdx() != 1 {
				t.Fatalf("used index %d, want 1", g.usedIdx())
			}
			id, written := g.usedElem(0)
			if id != 0 {
				t.Fatalf("used element reports descriptor %d, want 0", id)
			}
			if written != wantWritten {
				t.Fatalf("used element reports %d bytes written, want %d", written, wantWritten)
			}
		})
	}
}

// TestServeAcceptsEveryLegalChainLayout. virtio does not promise the header is
// its own descriptor or that the status byte is; VIRTIO_F_VERSION_1 implies
// ANY_LAYOUT. A backend that indexes readable[0] and writable[last] works
// against Linux and fails against a firmware.
func TestServeAcceptsEveryLegalChainLayout(t *testing.T) {
	data := pattern(0x5a, SectorSize)
	tests := []struct {
		name    string
		publish func(g *fakeGuest) [][]byte
		// status is the index of the segment holding the status byte and the
		// offset of that byte within it.
		statusSeg int
		statusOff int
	}{
		{
			name: "the conventional three descriptors",
			publish: func(g *fakeGuest) [][]byte {
				return g.publish(0, readable(blkHeader(blkTypeOut, 1)), readable(data), writable(1))
			},
			statusSeg: 2,
		},
		{
			name: "header and payload in one descriptor",
			publish: func(g *fakeGuest) [][]byte {
				return g.publish(0, readable(append(blkHeader(blkTypeOut, 1), data...)), writable(1))
			},
			statusSeg: 1,
		},
		{
			name: "the header split across two descriptors",
			publish: func(g *fakeGuest) [][]byte {
				h := blkHeader(blkTypeOut, 1)
				return g.publish(0, readable(h[:8]), readable(h[8:]), readable(data), writable(1))
			},
			statusSeg: 3,
		},
		{
			name: "the payload split across several descriptors",
			publish: func(g *fakeGuest) [][]byte {
				return g.publish(0,
					readable(blkHeader(blkTypeOut, 1)),
					readable(data[:100]), readable(data[100:300]), readable(data[300:]),
					writable(1))
			},
			statusSeg: 4,
		},
		{
			name: "an indirect descriptor table",
			publish: func(g *fakeGuest) [][]byte {
				return g.publishIndirect(0, readable(blkHeader(blkTypeOut, 1)), readable(data), writable(1))
			},
			statusSeg: 2,
		},
		{
			name: "a zero-length descriptor in the middle of the chain",
			publish: func(g *fakeGuest) [][]byte {
				return g.publish(0,
					readable(blkHeader(blkTypeOut, 1)), readable(nil), readable(data), writable(1))
			},
			statusSeg: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, raw := newTestDevice(t, g, testDeviceSize)
			g.handshake(t, dev)

			bufs := tc.publish(g)
			served, _, err := dev.ProcessQueue(t.Context())
			if err != nil {
				t.Fatalf("ProcessQueue: %v", err)
			}
			if served != 1 {
				t.Fatalf("served %d requests, want 1", served)
			}
			if got := bufs[tc.statusSeg][tc.statusOff]; got != blkStatusOK {
				t.Fatalf("status %d, want OK", got)
			}
			if got := raw.Snapshot(SectorSize, len(data)); !bytes.Equal(got, data) {
				t.Fatal("the device does not hold what the guest wrote")
			}
		})
	}
}

// TestServeSplitsAReadAcrossWritableSegments: the guest may scatter a READ into
// several buffers, and every byte must land at the right offset.
func TestServeSplitsAReadAcrossWritableSegments(t *testing.T) {
	g := newFakeGuest(128)
	dev, raw := newTestDevice(t, g, testDeviceSize)
	g.handshake(t, dev)

	data := pattern(0x77, 3*SectorSize)
	if _, err := raw.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	bufs := g.publish(0, readable(blkHeader(blkTypeIn, 0)),
		writable(SectorSize), writable(SectorSize), writable(SectorSize), writable(1))
	if _, _, err := dev.ProcessQueue(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := append(append(append([]byte{}, bufs[1]...), bufs[2]...), bufs[3]...)
	if !bytes.Equal(got, data) {
		t.Fatal("a scattered READ did not reassemble into the device's contents")
	}
	if _, written := g.usedElem(0); written != uint32(len(data))+1 {
		t.Fatalf("used element reports %d bytes, want %d", written, len(data)+1)
	}
}

func TestServeDrainsEveryAvailableRequestInOneCall(t *testing.T) {
	g := newFakeGuest(128)
	dev, raw := newTestDevice(t, g, testDeviceSize)
	g.handshake(t, dev)

	const n = 16
	for i := range uint16(n) {
		// Four descriptors apart so the chains do not overlap in the table.
		g.publish(i*4, readable(blkHeader(blkTypeOut, uint64(i))), readable(pattern(byte(i), SectorSize)), writable(1))
	}
	served, notify, err := dev.ProcessQueue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if served != n {
		t.Fatalf("served %d of %d available requests", served, n)
	}
	if !notify {
		t.Fatal("want a notification for a batch the guest did not suppress")
	}
	if g.usedIdx() != n {
		t.Fatalf("used index %d, want %d", g.usedIdx(), n)
	}
	for i := range uint16(n) {
		id, _ := g.usedElem(i)
		if id != uint32(i*4) {
			t.Fatalf("completion %d names descriptor %d, want %d", i, id, i*4)
		}
		if got := raw.Snapshot(int64(i)*SectorSize, SectorSize); !bytes.Equal(got, pattern(byte(i), SectorSize)) {
			t.Fatalf("sector %d holds the wrong request's data", i)
		}
	}
}

func TestServeHonoursTheGuestsInterruptSuppression(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, testDeviceSize)
	g.handshake(t, dev)
	g.suppressInterrupts()

	g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
	served, notify, err := dev.ProcessQueue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if served != 1 {
		t.Fatalf("served %d, want 1", served)
	}
	if notify {
		t.Fatal("the guest set VRING_AVAIL_F_NO_INTERRUPT and was interrupted anyway")
	}
}

// TestMalformedRingsAreFatal. A ring the backend cannot parse is not a failed
// request: continuing would mean writing guest memory at addresses invented
// from a corrupt descriptor.
func TestMalformedRingsAreFatal(t *testing.T) {
	tests := []struct {
		name string
		want error
		// corrupt runs after a well-formed request has been published.
		corrupt func(g *fakeGuest)
	}{
		{
			name: "a descriptor pointing outside shared memory",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.addr = 0xdead_0000_0000
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "a descriptor running past the end of its region",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.len = uint32(guestRAMSize)
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "a chain that loops back on itself",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.flags |= descNext
				d.next = 0
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "a next pointer past the descriptor table",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.flags |= descNext
				d.next = 1000
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "an indirect table that is not a whole number of descriptors",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.flags = descIndirect
				d.len = 17
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "a chain with no status byte",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				// Drop the writable descriptor: the header alone is left.
				d, _ := readDesc(g.descTable(), 0)
				d.flags &^= descNext
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "a chain too short to hold a request header",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				d, _ := readDesc(g.descTable(), 0)
				d.len = 4
				writeDesc(g.descTable(), 0, d)
			},
		},
		{
			name: "an available index that jumped past a whole ring",
			want: ErrRing,
			corrupt: func(g *fakeGuest) {
				binary.LittleEndian.PutUint16(g.ram[availOffset+2:availOffset+4], g.num+2)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, testDeviceSize)
			g.handshake(t, dev)
			g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
			tc.corrupt(g)
			if _, _, err := dev.ProcessQueue(t.Context()); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestNestedIndirectDescriptorsAreRefused(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, testDeviceSize)
	g.handshake(t, dev)
	g.publishIndirect(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))

	// Reach into the indirect table and mark its first entry indirect too.
	outer, _ := readDesc(g.descTable(), 0)
	table, err := dev.space.Guest(outer.addr, uint64(outer.len))
	if err != nil {
		t.Fatal(err)
	}
	inner, _ := readDesc(table, 0)
	inner.flags |= descIndirect
	writeDesc(table, 0, inner)

	if _, _, err := dev.ProcessQueue(t.Context()); !errors.Is(err, ErrRing) {
		t.Fatalf("want ErrRing for a nested indirect descriptor, got %v", err)
	}
}

// TestBackendFailuresBecomeIOErrorsNotHangs: a backend error must complete the
// request. Dropping it leaves the guest waiting on a completion that never
// comes, which looks like a hung disk.
func TestBackendFailuresBecomeIOErrorsNotHangs(t *testing.T) {
	tests := []struct {
		name string
		typ  uint32
		segs func(g *fakeGuest) [][]byte
	}{
		{"READ", blkTypeIn, func(g *fakeGuest) [][]byte {
			return g.publish(0, readable(blkHeader(blkTypeIn, 0)), writable(SectorSize), writable(1))
		}},
		{"WRITE", blkTypeOut, func(g *fakeGuest) [][]byte {
			return g.publish(0, readable(blkHeader(blkTypeOut, 0)), readable(pattern(1, SectorSize)), writable(1))
		}},
		{"FLUSH", blkTypeFlush, func(g *fakeGuest) [][]byte {
			return g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			var failures []uint32
			dev, err := NewDevice(Config{
				Backend: failingBackend{size: testDeviceSize},
				Mapper:  &fakeMapper{g: g},
				OnError: func(typ uint32, _ error) { failures = append(failures, typ) },
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(dev.Close)
			g.handshake(t, dev)

			bufs := tc.segs(g)
			served, _, err := dev.ProcessQueue(t.Context())
			if err != nil {
				t.Fatalf("a backend failure must not kill the connection: %v", err)
			}
			if served != 1 {
				t.Fatalf("served %d, want 1", served)
			}
			if got := bufs[len(bufs)-1][0]; got != blkStatusIOErr {
				t.Fatalf("status %d, want IOERR", got)
			}
			if len(failures) != 1 || failures[0] != tc.typ {
				t.Fatalf("OnError saw %v, want one %d", failures, tc.typ)
			}
		})
	}
}

// failingBackend fails everything.
type failingBackend struct{ size int64 }

var errBackendDown = errors.New("simulated backend failure")

func (b failingBackend) ReadAt([]byte, int64) (int, error)  { return 0, errBackendDown }
func (b failingBackend) WriteAt([]byte, int64) (int, error) { return 0, errBackendDown }
func (b failingBackend) Flush(context.Context) error        { return errBackendDown }
func (b failingBackend) Size() int64                        { return b.size }

// shortBackend reports success but moves fewer bytes than asked. virtio-blk has
// no way to express a partial completion, so the request must fail rather than
// be reported OK with a half-filled buffer.
type shortBackend struct{ size int64 }

func (b shortBackend) ReadAt(p []byte, _ int64) (int, error)  { return len(p) / 2, nil }
func (b shortBackend) WriteAt(p []byte, _ int64) (int, error) { return len(p) / 2, nil }
func (b shortBackend) Flush(context.Context) error            { return nil }
func (b shortBackend) Size() int64                            { return b.size }

func TestAShortBackendTransferIsAnIOError(t *testing.T) {
	for _, tc := range []struct {
		name string
		segs func(g *fakeGuest) [][]byte
	}{
		{"READ", func(g *fakeGuest) [][]byte {
			return g.publish(0, readable(blkHeader(blkTypeIn, 0)), writable(SectorSize), writable(1))
		}},
		{"WRITE", func(g *fakeGuest) [][]byte {
			return g.publish(0, readable(blkHeader(blkTypeOut, 0)), readable(pattern(1, SectorSize)), writable(1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, err := NewDevice(Config{Backend: shortBackend{size: testDeviceSize}, Mapper: &fakeMapper{g: g}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(dev.Close)
			g.handshake(t, dev)
			bufs := tc.segs(g)
			if _, _, err := dev.ProcessQueue(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := bufs[len(bufs)-1][0]; got != blkStatusIOErr {
				t.Fatalf("status %d, want IOERR for a short transfer", got)
			}
		})
	}
}

func TestTakeFrontAndTakeBack(t *testing.T) {
	segs := func() [][]byte { return [][]byte{{1, 2, 3}, {4, 5}, {6}} }
	tests := []struct {
		name      string
		n         int
		takeFront bool
		wantTaken []byte
		wantRest  []byte
		wantErr   bool
	}{
		{name: "front, less than the first segment", n: 2, takeFront: true, wantTaken: []byte{1, 2}, wantRest: []byte{3, 4, 5, 6}},
		{name: "front, exactly the first segment", n: 3, takeFront: true, wantTaken: []byte{1, 2, 3}, wantRest: []byte{4, 5, 6}},
		{name: "front, across segments", n: 4, takeFront: true, wantTaken: []byte{1, 2, 3, 4}, wantRest: []byte{5, 6}},
		{name: "front, everything", n: 6, takeFront: true, wantTaken: []byte{1, 2, 3, 4, 5, 6}},
		{name: "front, more than there is", n: 7, takeFront: true, wantErr: true},
		{name: "front, nothing", n: 0, takeFront: true, wantRest: []byte{1, 2, 3, 4, 5, 6}},
		{name: "back, the last segment", n: 1, wantTaken: []byte{6}, wantRest: []byte{1, 2, 3, 4, 5}},
		{name: "back, across segments", n: 3, wantTaken: []byte{4, 5, 6}, wantRest: []byte{1, 2, 3}},
		{name: "back, part of a segment", n: 4, wantTaken: []byte{3, 4, 5, 6}, wantRest: []byte{1, 2}},
		{name: "back, more than there is", n: 7, wantErr: true},
	}
	flat := func(s [][]byte) []byte {
		var out []byte
		for _, b := range s {
			out = append(out, b...)
		}
		return out
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var taken, rest [][]byte
			var err error
			if tc.takeFront {
				taken, rest, err = takeFront(segs(), tc.n)
			} else {
				taken, rest, err = takeBack(segs(), tc.n)
			}
			if tc.wantErr {
				if !errors.Is(err, errShortChain) {
					t.Fatalf("want errShortChain, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(flat(taken), tc.wantTaken) {
				t.Fatalf("taken %v, want %v", flat(taken), tc.wantTaken)
			}
			if !bytes.Equal(flat(rest), tc.wantRest) {
				t.Fatalf("rest %v, want %v", flat(rest), tc.wantRest)
			}
		})
	}
}

func TestByteOffsetRefusesToWrap(t *testing.T) {
	if _, err := byteOffset(1 << 62); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("want ErrOutOfRange, got %v", err)
	}
	got, err := byteOffset(2)
	if err != nil || got != 2*SectorSize {
		t.Fatalf("byteOffset(2) = %d, %v", got, err)
	}
}

func TestRawDeviceRefusesRequestsOutsideItself(t *testing.T) {
	d := NewRawDevice(4 * SectorSize)
	tests := []struct {
		name string
		call func() (int, error)
	}{
		{"read past the end", func() (int, error) { return d.ReadAt(make([]byte, SectorSize), 4*SectorSize) }},
		{"read straddling the end", func() (int, error) { return d.ReadAt(make([]byte, 2*SectorSize), 3*SectorSize) }},
		{"write past the end", func() (int, error) { return d.WriteAt(make([]byte, SectorSize), 4*SectorSize) }},
		{"negative offset", func() (int, error) { return d.ReadAt(make([]byte, 1), -1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.call(); !errors.Is(err, ErrOutOfRange) {
				t.Fatalf("want ErrOutOfRange, got %v", err)
			}
		})
	}
}

func TestRawDeviceRoundTrip(t *testing.T) {
	d := NewRawDeviceFrom(make([]byte, 8*SectorSize))
	data := pattern(0x3c, 2*SectorSize)
	if n, err := d.WriteAt(data, SectorSize); err != nil || n != len(data) {
		t.Fatalf("WriteAt: %d, %v", n, err)
	}
	got := make([]byte, len(data))
	if n, err := d.ReadAt(got, SectorSize); err != nil || n != len(got) {
		t.Fatalf("ReadAt: %d, %v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the raw device did not return what was written")
	}
	if err := d.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if d.Flushes() != 1 || d.Writes() != 1 {
		t.Fatalf("counters: %d flushes, %d writes", d.Flushes(), d.Writes())
	}
	if d.Size() != 8*SectorSize {
		t.Fatalf("Size %d", d.Size())
	}
}
