package vhost

import (
	"encoding/binary"
	"errors"
	"os"
	"testing"
)

func TestNewDeviceRejectsAnUnservableConfig(t *testing.T) {
	g := newFakeGuest(64)
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no backend", Config{Mapper: &fakeMapper{g: g}}},
		{"no mapper", Config{Backend: NewRawDevice(4096)}},
		{"queue deeper than the ring supports", Config{Backend: NewRawDevice(4096), Mapper: &fakeMapper{g: g}, QueueSize: MaxQueueSize * 2}},
		{"queue size that is not a power of two", Config{Backend: NewRawDevice(4096), Mapper: &fakeMapper{g: g}, QueueSize: 100}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewDevice(tc.cfg); err == nil {
				t.Fatal("want an error, got a device")
			}
		})
	}
}

// TestHandshakeReachesReady is the shape of the whole increment: the sequence a
// QEMU 11.0.2 front-end performs, answered message by message, ending with a
// queue the guest can use.
func TestHandshakeReachesReady(t *testing.T) {
	g := newFakeGuest(128)
	dev, raw := newTestDevice(t, g, 1<<20)

	var seen []Request
	dev.onRequest = func(r Request) { seen = append(seen, r) }

	replies := map[Request]*Message{}
	for _, m := range g.handshakeMessages() {
		r, err := dev.Handle(t.Context(), m)
		if err != nil {
			t.Fatalf("%s: %v", m.Request, err)
		}
		if r != nil {
			replies[m.Request] = r
		}
		if dev.Ready() && m.Request != ReqSetVringEnable {
			t.Fatalf("device reported ready at %s, before the queue was enabled", m.Request)
		}
	}
	if !dev.Ready() {
		t.Fatal("device is not ready after the full handshake")
	}
	if len(seen) != len(g.handshakeMessages()) {
		t.Fatalf("OnRequest saw %d of %d messages", len(seen), len(g.handshakeMessages()))
	}

	t.Run("GET_FEATURES answers the offered set", func(t *testing.T) {
		got := binary.LittleEndian.Uint64(replies[ReqGetFeatures].Payload)
		if got != DeviceFeatures {
			t.Fatalf("features %#x, want %#x", got, DeviceFeatures)
		}
	})
	t.Run("GET_PROTOCOL_FEATURES answers the offered set", func(t *testing.T) {
		got := binary.LittleEndian.Uint64(replies[ReqGetProtocolFeatures].Payload)
		if got != ProtocolFeatures {
			t.Fatalf("protocol features %#x, want %#x", got, ProtocolFeatures)
		}
	})
	t.Run("GET_QUEUE_NUM answers one queue", func(t *testing.T) {
		if got := binary.LittleEndian.Uint64(replies[ReqGetQueueNum].Payload); got != 1 {
			t.Fatalf("queue count %d, want 1 (§30.3)", got)
		}
	})
	t.Run("GET_CONFIG reports the backend's capacity in sectors", func(t *testing.T) {
		cfg, err := decodeConfig(*replies[ReqGetConfig])
		if err != nil {
			t.Fatalf("decode config reply: %v", err)
		}
		if int(cfg.Size) != len(cfg.Region) {
			t.Fatalf("config reply says %d bytes but carries %d", cfg.Size, len(cfg.Region))
		}
		if got := binary.LittleEndian.Uint64(cfg.Region[0:8]); got != uint64(raw.Size()/SectorSize) {
			t.Fatalf("capacity %d sectors, want %d", got, raw.Size()/SectorSize)
		}
		if got := binary.LittleEndian.Uint32(cfg.Region[20:24]); got != SectorSize {
			t.Fatalf("blk_size %d, want %d", got, SectorSize)
		}
		if got := binary.LittleEndian.Uint32(cfg.Region[12:16]); got != uint32(g.num)-2 {
			t.Fatalf("seg_max %d, want %d (queue depth less header and status)", got, g.num-2)
		}
		if got := binary.LittleEndian.Uint16(cfg.Region[34:36]); got != 1 {
			t.Fatalf("num_queues %d, want 1", got)
		}
	})
	t.Run("the negotiated features are what the front-end set", func(t *testing.T) {
		if got := dev.Features(); got != DeviceFeatures {
			t.Fatalf("negotiated %#x, want %#x", got, DeviceFeatures)
		}
	})
}

// TestGetConfigTruncatesAPartialTrailingSector proves the capacity the guest is
// told is capacity it can actually address. Rounding up would hand the guest a
// sector the backend refuses.
func TestGetConfigTruncatesAPartialTrailingSector(t *testing.T) {
	g := newFakeGuest(64)
	dev, _ := newTestDevice(t, g, 10*SectorSize+17)
	reply, err := dev.Handle(t.Context(), msg(ReqGetConfig, encodeConfig(configRequest{Size: blkConfigSize})))
	if err != nil {
		t.Fatalf("GET_CONFIG: %v", err)
	}
	cfg, err := decodeConfig(*reply)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint64(cfg.Region[0:8]); got != 10 {
		t.Fatalf("capacity %d sectors, want 10 (the partial 11th is not addressable)", got)
	}
}

// TestGetConfigHonoursTheRequestedSize covers the version skew this backend
// lives with: QEMU asks for however many bytes its own struct virtio_blk_config
// has, which grows between releases.
func TestGetConfigHonoursTheRequestedSize(t *testing.T) {
	g := newFakeGuest(64)
	dev, _ := newTestDevice(t, g, 1<<20)
	for _, size := range []uint32{8, blkConfigSize, 96, 200} {
		reply, err := dev.Handle(t.Context(), msg(ReqGetConfig, encodeConfig(configRequest{Size: size})))
		if err != nil {
			t.Fatalf("GET_CONFIG(%d): %v", size, err)
		}
		cfg, err := decodeConfig(*reply)
		if err != nil {
			t.Fatal(err)
		}
		if uint32(len(cfg.Region)) != size {
			t.Fatalf("GET_CONFIG(%d) answered %d bytes", size, len(cfg.Region))
		}
		if got := binary.LittleEndian.Uint64(cfg.Region[0:8]); got != 1<<20/SectorSize {
			t.Fatalf("GET_CONFIG(%d): capacity %d", size, got)
		}
	}
}

func TestHandleRefusesWhatItDoesNotImplement(t *testing.T) {
	// Each of these is a request whose *silent* acceptance would produce a
	// device that looks configured and is not. The refusal is the feature.
	tests := []struct {
		name string
		msg  Message
	}{
		{"log base, without LOG_SHMFD", msg(ReqSetLogBase, u64Payload(0))},
		{"log fd, without LOG_SHMFD", msg(ReqSetLogFd, u64Payload(0), mustEventFile())},
		{"get inflight fd, before increment 3.3", msg(ReqGetInflightFd, make([]byte, 16))},
		{"set inflight fd, before increment 3.3", msg(ReqSetInflightFd, make([]byte, 16), mustEventFile())},
		{"a request number that does not exist", msg(Request(9999), nil)},
		{"a second queue", msg(ReqSetVringNum, encodeVringState(vringState{Index: 1, Num: 128}))},
		{"a protocol version this backend does not speak", Message{Request: ReqGetFeatures, Flags: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			if _, err := dev.Handle(t.Context(), tc.msg); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}

func TestSetFeaturesRefusesWhatWasNotOffered(t *testing.T) {
	tests := []struct {
		name     string
		features uint64
	}{
		{"a bit the backend never offered", DeviceFeatures | 1<<featureRingEventIdx},
		{"packed rings", DeviceFeatures | 1<<featureRingPacked},
		{"multiqueue", DeviceFeatures | 1<<featureBlkMQ},
		{"a legacy guest without VERSION_1", DeviceFeatures &^ (1 << featureVersion1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			if _, err := dev.Handle(t.Context(), msg(ReqSetFeatures, u64Payload(tc.features))); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}

func TestVringConfigurationOrderIsEnforced(t *testing.T) {
	addr := encodeVringAddr(vringAddr{
		DescUserAddr:  testUserAddrBase + descOffset,
		AvailUserAddr: testUserAddrBase + availOffset,
		UsedUserAddr:  testUserAddrBase + usedOffset,
	})
	tests := []struct {
		name   string
		before []Message
	}{
		{"SET_VRING_ADDR before SET_VRING_NUM", []Message{
			msg(ReqSetMemTable, encodeMemTable([]Region{newFakeGuest(128).region()}), nil),
		}},
		{"SET_VRING_ADDR before SET_MEM_TABLE", []Message{
			msg(ReqSetVringNum, encodeVringState(vringState{Num: 128})),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			for _, m := range tc.before {
				if _, err := dev.Handle(t.Context(), m); err != nil {
					t.Fatalf("setup %s: %v", m.Request, err)
				}
			}
			if _, err := dev.Handle(t.Context(), msg(ReqSetVringAddr, addr)); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}

func TestSetVringNumRejectsAnImpossibleDepth(t *testing.T) {
	tests := []struct {
		name string
		num  uint32
	}{
		{"zero", 0},
		{"deeper than the advertised queue", MaxQueueSize * 2},
		{"not a power of two", 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			if _, err := dev.Handle(t.Context(), msg(ReqSetVringNum, encodeVringState(vringState{Num: tc.num}))); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}

// TestSetVringAddrRejectsAddressesOutsideSharedMemory is the check that keeps a
// front-end bug from becoming a write to whatever this process happens to have
// mapped.
func TestSetVringAddrRejectsAddressesOutsideSharedMemory(t *testing.T) {
	good := vringAddr{
		DescUserAddr:  testUserAddrBase + descOffset,
		AvailUserAddr: testUserAddrBase + availOffset,
		UsedUserAddr:  testUserAddrBase + usedOffset,
	}
	tests := []struct {
		name string
		mut  func(*vringAddr)
	}{
		{"descriptor table outside the table", func(a *vringAddr) { a.DescUserAddr = 0xdead0000 }},
		{"available ring outside the table", func(a *vringAddr) { a.AvailUserAddr = 0xdead0000 }},
		{"used ring outside the table", func(a *vringAddr) { a.UsedUserAddr = 0xdead0000 }},
		{"used ring running past the end of its region", func(a *vringAddr) {
			a.UsedUserAddr = testUserAddrBase + guestRAMSize - 8
		}},
		{"guest-physical addresses where userspace ones belong", func(a *vringAddr) {
			a.DescUserAddr = testGuestPhysBase + descOffset
			a.AvailUserAddr = testGuestPhysBase + availOffset
			a.UsedUserAddr = testGuestPhysBase + usedOffset
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			for _, m := range []Message{
				msg(ReqSetMemTable, encodeMemTable([]Region{g.region()}), nil),
				msg(ReqSetVringNum, encodeVringState(vringState{Num: uint32(g.num)})),
			} {
				if _, err := dev.Handle(t.Context(), m); err != nil {
					t.Fatalf("setup %s: %v", m.Request, err)
				}
			}
			a := good
			tc.mut(&a)
			if _, err := dev.Handle(t.Context(), msg(ReqSetVringAddr, encodeVringAddr(a))); !errors.Is(err, ErrUnmapped) {
				t.Fatalf("want ErrUnmapped, got %v", err)
			}
		})
	}
}

func TestSetMemTableRejectsAMismatchedDescriptorCount(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	// Two regions declared, one descriptor sent.
	table := encodeMemTable([]Region{g.region(), g.region()})
	if _, err := dev.Handle(t.Context(), msg(ReqSetMemTable, table, nil)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}

// TestSetMemTableUnmapsEverythingOnAPartialFailure: a half-installed memory
// table would translate some addresses and silently fail others.
func TestSetMemTableUnmapsEverythingOnAPartialFailure(t *testing.T) {
	g := newFakeGuest(128)
	m := &fakeMapper{g: g, failAt: 2}
	dev, err := NewDevice(Config{Backend: NewRawDevice(1 << 20), Mapper: m})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dev.Close)
	table := encodeMemTable([]Region{g.region(), g.region()})
	if _, err := dev.Handle(t.Context(), msg(ReqSetMemTable, table, nil, nil)); err == nil {
		t.Fatal("want the mmap failure to propagate")
	}
	if m.unmapped != 1 {
		t.Fatalf("unmapped %d regions after a partial failure, want 1", m.unmapped)
	}
	if dev.Ready() {
		t.Fatal("device claims to be ready with no memory table")
	}
}

// TestSetMemTableReplacesAndUnmapsThePreviousTable: leaking the guest's whole
// RAM once per reconnection exhausts the host.
func TestSetMemTableReplacesAndUnmapsThePreviousTable(t *testing.T) {
	g := newFakeGuest(128)
	m := &fakeMapper{g: g}
	dev, err := NewDevice(Config{Backend: NewRawDevice(1 << 20), Mapper: m})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dev.Close)
	table := encodeMemTable([]Region{g.region()})
	for range 3 {
		if _, err := dev.Handle(t.Context(), msg(ReqSetMemTable, table, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if m.unmapped != 2 {
		t.Fatalf("unmapped %d of the 2 superseded tables", m.unmapped)
	}
	dev.Close()
	if m.unmapped != 3 {
		t.Fatalf("Close left %d of 3 mappings behind", 3-m.unmapped)
	}
}

func TestReplyAckAcknowledgesRequestsWithNoNaturalAnswer(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	if _, err := dev.Handle(t.Context(), msg(ReqSetProtocolFeatures, u64Payload(ProtocolFeatures))); err != nil {
		t.Fatal(err)
	}
	m := msg(ReqSetMemTable, encodeMemTable([]Region{g.region()}), nil)
	m.Flags |= flagNeedReply
	reply, err := dev.Handle(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	if reply == nil {
		t.Fatal("NEED_REPLY went unanswered; the front-end cannot tell success from failure")
	}
	if got := binary.LittleEndian.Uint64(reply.Payload); got != 0 {
		t.Fatalf("REPLY_ACK payload %d, want 0 (success)", got)
	}
	if !reply.IsReply() {
		t.Fatal("the acknowledgement is not flagged as a reply")
	}
}

func TestReplyAckIsSilentWhenNotNegotiated(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	m := msg(ReqSetMemTable, encodeMemTable([]Region{g.region()}), nil)
	m.Flags |= flagNeedReply
	reply, err := dev.Handle(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	if reply != nil {
		t.Fatal("answered a NEED_REPLY without having negotiated REPLY_ACK")
	}
}

// TestSetConfigIsRefusedOnlyWhenTheFrontEndAsked. The guest may not reconfigure
// its own device, so SET_CONFIG is refused — but a refusal is only allowed to
// take the shape of a reply when the front-end set NEED_REPLY. vhost-user has no
// message length on the wire beyond the header, so a reply nobody is reading
// becomes the *next* message's header: the socket desynchronizes permanently,
// and the symptom appears several requests later as a nonsense request number.
func TestSetConfigIsRefusedOnlyWhenTheFrontEndAsked(t *testing.T) {
	body := encodeConfig(configRequest{Size: 8, Region: make([]byte, 8)})
	tests := []struct {
		name      string
		negotiate bool
		needReply bool
		wantReply bool
	}{
		{"asked, and REPLY_ACK negotiated", true, true, true},
		{"not asked", true, false, false},
		{"asked, but REPLY_ACK never negotiated", false, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			if tc.negotiate {
				if _, err := dev.Handle(t.Context(), msg(ReqSetProtocolFeatures, u64Payload(ProtocolFeatures))); err != nil {
					t.Fatal(err)
				}
			}
			m := msg(ReqSetConfig, body)
			if tc.needReply {
				m.Flags |= flagNeedReply
			}
			reply, err := dev.Handle(t.Context(), m)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantReply {
				if reply != nil {
					t.Fatal("sent an unsolicited reply; the next message the front-end writes will be read as this reply's tail")
				}
				return
			}
			if reply == nil || binary.LittleEndian.Uint64(reply.Payload) == 0 {
				t.Fatal("SET_CONFIG must be refused with a non-zero status; the guest may not resize its own device")
			}
		})
	}
}

// TestGetConfigRejectsAnOffsetOutsideTheConfigSpace. The offset is an attacker-
// or bug-controlled uint32 in a 12-byte message, and the reply buffer used to be
// sized offset+size: a front-end asking for 4 bytes at 0xffff_f000 made this
// backend allocate four gigabytes.
//
// The bound is the protocol's VHOST_USER_MAX_CONFIG_SIZE, not the 60 bytes this
// backend fills. Requests past 60 must still be *answered*, with zeros: QEMU
// asks for the size of its own struct virtio_blk_config, and that struct grows
// between QEMU versions. QEMU 11.0.2 asks for 60 (asserted in the integration
// lane), so the cases past it are the compatibility guarantee, not decoration.
func TestGetConfigRejectsAnOffsetOutsideTheConfigSpace(t *testing.T) {
	tests := []struct {
		name           string
		offset, size   uint32
		wantErr        bool
		wantRegionSize int
	}{
		{name: "the whole config space", size: blkConfigSize, wantRegionSize: blkConfigSize},
		{name: "a field in the middle", offset: 20, size: 4, wantRegionSize: 4},
		{name: "the last byte this backend fills", offset: blkConfigSize - 1, size: 1, wantRegionSize: 1},
		{name: "a newer front-end's larger struct", size: blkConfigSize + 16, wantRegionSize: blkConfigSize + 16},
		{name: "a window straddling what we fill", offset: blkConfigSize - 2, size: 4, wantRegionSize: 4},
		{name: "the whole protocol maximum", size: MaxConfigSize, wantRegionSize: MaxConfigSize},
		{name: "one byte past the protocol maximum", offset: MaxConfigSize, size: 1, wantErr: true},
		{name: "a window straddling the protocol maximum", offset: MaxConfigSize - 2, size: 4, wantErr: true},
		{name: "an offset that would allocate the host", offset: 0xffff_f000, size: 4, wantErr: true},
		{name: "a size that would allocate the host", size: MaxPayload, wantErr: true},
		{name: "an empty window", size: 0, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			m := msg(ReqGetConfig, encodeConfig(configRequest{Offset: tc.offset, Size: tc.size, Region: make([]byte, tc.size%1024)}))
			reply, err := dev.Handle(t.Context(), m)
			if tc.wantErr {
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("GET_CONFIG offset=%d size=%d: want ErrProtocol, got reply=%v err=%v", tc.offset, tc.size, reply, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeConfig(*reply)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Region) != tc.wantRegionSize {
				t.Fatalf("reply carries %d config bytes, want %d", len(got.Region), tc.wantRegionSize)
			}
		})
	}
}

func TestGetVringBaseStopsTheQueueAndReportsWhereItGotTo(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	g.handshake(t, dev)

	g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
	if _, _, err := dev.ProcessQueue(t.Context()); err != nil {
		t.Fatal(err)
	}

	reply, err := dev.Handle(t.Context(), msg(ReqGetVringBase, encodeVringState(vringState{})))
	if err != nil {
		t.Fatal(err)
	}
	s, err := decodeVringState(*reply)
	if err != nil {
		t.Fatal(err)
	}
	if s.Num != 1 {
		t.Fatalf("GET_VRING_BASE reported %d, want 1 (one request consumed)", s.Num)
	}
	if dev.Ready() {
		t.Fatal("the queue is still live after GET_VRING_BASE took the ring back")
	}
}

// TestSetVringBaseResumesFromTheFrontEndsIndex: a reconnecting front-end hands
// back a non-zero index, and starting from zero would re-serve every request
// the ring still holds.
func TestSetVringBaseResumesFromTheFrontEndsIndex(t *testing.T) {
	g := newFakeGuest(128)
	dev, raw := newTestDevice(t, g, 1<<20)
	for _, m := range g.handshakeMessages() {
		if m.Request == ReqSetVringBase {
			m = msg(ReqSetVringBase, encodeVringState(vringState{Num: 5}))
		}
		if _, err := dev.Handle(t.Context(), m); err != nil {
			t.Fatalf("%s: %v", m.Request, err)
		}
	}
	// The guest's avail index is already at 5; nothing new is available.
	g.avail = 5
	binary.LittleEndian.PutUint16(g.ram[availOffset+2:availOffset+4], 5)
	served, _, err := dev.ProcessQueue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if served != 0 {
		t.Fatalf("served %d requests from a ring the front-end said was drained", served)
	}
	if raw.Writes() != 0 {
		t.Fatal("a resumed device re-served requests the previous one had already completed")
	}
}

func TestResetReturnsTheDeviceToItsInitialState(t *testing.T) {
	for _, req := range []Request{ReqResetOwner, ReqResetDevice} {
		t.Run(req.String(), func(t *testing.T) {
			g := newFakeGuest(128)
			dev, _ := newTestDevice(t, g, 1<<20)
			g.handshake(t, dev)
			if _, err := dev.Handle(t.Context(), msg(req, nil)); err != nil {
				t.Fatal(err)
			}
			if dev.Ready() {
				t.Fatalf("device is still ready after %s", req)
			}
			if dev.Features() != 0 || dev.Kick() != nil || dev.Call() != nil {
				t.Fatalf("%s left negotiated state behind", req)
			}
		})
	}
}

func TestSetVringKickWithTheNoFDBitTakesNoDescriptor(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	if _, err := dev.Handle(t.Context(), msg(ReqSetVringKick, u64Payload(vringFDMask))); err != nil {
		t.Fatal(err)
	}
	if dev.Kick() != nil {
		t.Fatal("the NOFD bit means polling mode; the device kept a descriptor anyway")
	}
}

func TestSetVringKickWithoutADescriptorIsAProtocolError(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	if _, err := dev.Handle(t.Context(), msg(ReqSetVringKick, u64Payload(0))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}

func TestSetVringKickReplacesAndClosesThePreviousDescriptor(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	first := mustEventFile()
	if _, err := dev.Handle(t.Context(), msg(ReqSetVringKick, u64Payload(0), first)); err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Handle(t.Context(), msg(ReqSetVringKick, u64Payload(0), mustEventFile())); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("the superseded kick descriptor was leaked (Stat: %v)", err)
	}
}

func TestProcessQueueIsANoOpBeforeTheQueueIsLive(t *testing.T) {
	g := newFakeGuest(128)
	dev, _ := newTestDevice(t, g, 1<<20)
	served, notify, err := dev.ProcessQueue(t.Context())
	if err != nil || served != 0 || notify {
		t.Fatalf("ProcessQueue on an unconfigured device: %d, %v, %v", served, notify, err)
	}
}
