package vhost

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		msg     Message
		wantErr bool
	}{
		{name: "no payload", msg: Message{Request: ReqGetFeatures, Flags: flagVersion1}},
		{name: "reply with payload", msg: Message{Request: ReqGetConfig, Flags: flagVersion1 | flagReply, Payload: []byte{1, 2, 3}}},
		{name: "need reply", msg: Message{Request: ReqSetMemTable, Flags: flagVersion1 | flagNeedReply, Payload: make([]byte, 40)}},
		{name: "max payload", msg: Message{Request: ReqSetConfig, Flags: flagVersion1, Payload: make([]byte, MaxPayload)}},
		{name: "over max payload", msg: Message{Request: ReqSetConfig, Flags: flagVersion1, Payload: make([]byte, MaxPayload+1)}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, HeaderSize)
			EncodeHeader(buf, tc.msg)
			got, n, err := DecodeHeader(buf)
			if tc.wantErr {
				if !errors.Is(err, ErrProtocol) {
					t.Fatalf("want ErrProtocol for an oversized payload, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeHeader: %v", err)
			}
			if got.Request != tc.msg.Request || got.Flags != tc.msg.Flags {
				t.Fatalf("got %v/%#x, want %v/%#x", got.Request, got.Flags, tc.msg.Request, tc.msg.Flags)
			}
			if n != len(tc.msg.Payload) {
				t.Fatalf("payload length %d, want %d", n, len(tc.msg.Payload))
			}
		})
	}
}

func TestDecodeHeaderRejectsAShortBuffer(t *testing.T) {
	if _, _, err := DecodeHeader(make([]byte, HeaderSize-1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}

func TestRequestNamesAreStable(t *testing.T) {
	// The wire numbering is shared with every other vhost-user implementation,
	// so a renumbering here is a silent incompatibility with QEMU. Pin the ones
	// this backend depends on against the values in QEMU's
	// docs/interop/vhost-user.rst.
	tests := []struct {
		req  Request
		code uint32
		name string
	}{
		{ReqGetFeatures, 1, "GET_FEATURES"},
		{ReqSetFeatures, 2, "SET_FEATURES"},
		{ReqSetOwner, 3, "SET_OWNER"},
		{ReqSetMemTable, 5, "SET_MEM_TABLE"},
		{ReqSetVringNum, 8, "SET_VRING_NUM"},
		{ReqSetVringAddr, 9, "SET_VRING_ADDR"},
		{ReqSetVringBase, 10, "SET_VRING_BASE"},
		{ReqGetVringBase, 11, "GET_VRING_BASE"},
		{ReqSetVringKick, 12, "SET_VRING_KICK"},
		{ReqSetVringCall, 13, "SET_VRING_CALL"},
		{ReqGetProtocolFeatures, 15, "GET_PROTOCOL_FEATURES"},
		{ReqSetProtocolFeatures, 16, "SET_PROTOCOL_FEATURES"},
		{ReqGetQueueNum, 17, "GET_QUEUE_NUM"},
		{ReqSetVringEnable, 18, "SET_VRING_ENABLE"},
		{ReqGetConfig, 24, "GET_CONFIG"},
		{ReqGetInflightFd, 31, "GET_INFLIGHT_FD"},
		{ReqSetInflightFd, 32, "SET_INFLIGHT_FD"},
		{ReqSetStatus, 39, "SET_STATUS"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if uint32(tc.req) != tc.code {
				t.Fatalf("%s is %d, want %d", tc.name, tc.req, tc.code)
			}
			if got := tc.req.String(); got != tc.name {
				t.Fatalf("String() = %q, want %q", got, tc.name)
			}
		})
	}
	if got := Request(9999).String(); !strings.Contains(got, "9999") {
		t.Fatalf("an unknown request must name its number, got %q", got)
	}
}

func TestFeatureSetsMatchWhatTheBackendImplements(t *testing.T) {
	// These two constants are what the guest builds its driver around. Each
	// assertion below is a behaviour this increment either implements or
	// deliberately refuses; a bit added without the behaviour is a lie the
	// guest cannot detect.
	tests := []struct {
		name string
		mask uint64
		bit  uint
		want bool
	}{
		{"VERSION_1 is offered", DeviceFeatures, featureVersion1, true},
		{"FLUSH is offered", DeviceFeatures, featureBlkFlush, true},
		{"SEG_MAX is offered", DeviceFeatures, featureBlkSegMax, true},
		{"INDIRECT_DESC is offered", DeviceFeatures, featureRingIndirectDesc, true},
		{"PROTOCOL_FEATURES is offered", DeviceFeatures, featureProtocol, true},
		{"EVENT_IDX is not offered", DeviceFeatures, featureRingEventIdx, false},
		{"RING_PACKED is not offered", DeviceFeatures, featureRingPacked, false},
		{"MQ is not offered", DeviceFeatures, featureBlkMQ, false},
		{"DISCARD is not offered", DeviceFeatures, featureBlkDiscard, false},
		{"WRITE_ZEROES is not offered", DeviceFeatures, featureBlkWriteZeroes, false},
		{"RO is not offered", DeviceFeatures, featureBlkRO, false},
		{"CONFIG is offered", ProtocolFeatures, protocolConfig, true},
		{"REPLY_ACK is offered", ProtocolFeatures, protocolReplyAck, true},
		{"INFLIGHT_SHMFD is not offered (increment 3.3)", ProtocolFeatures, protocolInflightShmfd, false},
		{"LOG_SHMFD is not offered", ProtocolFeatures, protocolLogShmfd, false},
		{"BACKEND_REQ is not offered", ProtocolFeatures, protocolBackendReq, false},
		{"STATUS is not offered", ProtocolFeatures, protocolStatus, false},
		{"protocol MQ is not offered", ProtocolFeatures, protocolMQ, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := has(tc.mask, tc.bit); got != tc.want {
				t.Fatalf("bit %d present = %v, want %v", tc.bit, got, tc.want)
			}
		})
	}
}

func TestPayloadDecodersRejectShortBodies(t *testing.T) {
	tests := []struct {
		name string
		call func(Message) error
	}{
		{"u64", func(m Message) error { _, err := payloadU64(m); return err }},
		{"vring state", func(m Message) error { _, err := decodeVringState(m); return err }},
		{"vring addr", func(m Message) error { _, err := decodeVringAddr(m); return err }},
		{"config", func(m Message) error { _, err := decodeConfig(m); return err }},
		{"mem table", func(m Message) error { _, err := decodeMemTable(m); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(Message{Request: ReqSetFeatures, Payload: []byte{1}}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}

func TestVringStateAndAddrRoundTrip(t *testing.T) {
	s := vringState{Index: 0, Num: 128}
	got, err := decodeVringState(Message{Payload: encodeVringState(s)})
	if err != nil || got != s {
		t.Fatalf("vring state round-trip: %+v, %v", got, err)
	}
	a := vringAddr{Index: 0, Flags: 1, DescUserAddr: 0x1000, UsedUserAddr: 0x2000, AvailUserAddr: 0x3000, LogGuestAddr: 0x4000}
	gotA, err := decodeVringAddr(Message{Payload: encodeVringAddr(a)})
	if err != nil || gotA != a {
		t.Fatalf("vring addr round-trip: %+v, %v", gotA, err)
	}
}

func TestDecodeMemTableRejectsImpossibleTables(t *testing.T) {
	body := func(n uint32, regions int) []byte {
		p := make([]byte, memTableHeaderSize+regions*regionSize)
		binary.LittleEndian.PutUint32(p[0:4], n)
		for i := range regions {
			binary.LittleEndian.PutUint64(p[memTableHeaderSize+i*regionSize+8:], 4096)
		}
		return p
	}
	tests := []struct {
		name    string
		payload []byte
	}{
		{"zero regions", body(0, 0)},
		{"more regions than the baseline allows", body(maxRegions+1, maxRegions+1)},
		{"declared regions the payload does not carry", body(3, 1)},
		{"an empty region", func() []byte {
			p := body(1, 1)
			binary.LittleEndian.PutUint64(p[memTableHeaderSize+8:], 0)
			return p
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeMemTable(Message{Request: ReqSetMemTable, Payload: tc.payload}); !errors.Is(err, ErrProtocol) {
				t.Fatalf("want ErrProtocol, got %v", err)
			}
		})
	}
}
