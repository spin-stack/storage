package vhost

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
)

// ErrProtocol is the class of every "the front-end sent something this backend
// cannot honour" failure: a short payload, an unknown request, a vring index
// that does not exist. Callers branch on it to decide that the *connection* is
// unusable, as opposed to a request that merely failed.
var ErrProtocol = errors.New("vhost: protocol violation")

// Request is a vhost-user message type. The numbering is the wire protocol's
// (QEMU docs/interop/vhost-user.rst); it is fixed and shared with every other
// implementation, so the constants are written out rather than derived.
type Request uint32

// The front-end-to-back-end requests. The set is complete up to the ones QEMU
// 11.0.2 can send us; requests we never advertise support for still need names,
// because refusing an unknown request intelligibly is part of the protocol.
const (
	ReqGetFeatures         Request = 1
	ReqSetFeatures         Request = 2
	ReqSetOwner            Request = 3
	ReqResetOwner          Request = 4
	ReqSetMemTable         Request = 5
	ReqSetLogBase          Request = 6
	ReqSetLogFd            Request = 7
	ReqSetVringNum         Request = 8
	ReqSetVringAddr        Request = 9
	ReqSetVringBase        Request = 10
	ReqGetVringBase        Request = 11
	ReqSetVringKick        Request = 12
	ReqSetVringCall        Request = 13
	ReqSetVringErr         Request = 14
	ReqGetProtocolFeatures Request = 15
	ReqSetProtocolFeatures Request = 16
	ReqGetQueueNum         Request = 17
	ReqSetVringEnable      Request = 18
	ReqSendRARP            Request = 19
	ReqNetSetMTU           Request = 20
	ReqSetBackendReqFd     Request = 21
	ReqIOTLBMsg            Request = 22
	ReqSetVringEndian      Request = 23
	ReqGetConfig           Request = 24
	ReqSetConfig           Request = 25
	ReqCreateCryptoSession Request = 26
	ReqCloseCryptoSession  Request = 27
	ReqPostcopyAdvise      Request = 28
	ReqPostcopyListen      Request = 29
	ReqPostcopyEnd         Request = 30
	ReqGetInflightFd       Request = 31
	ReqSetInflightFd       Request = 32
	ReqGPUSetSocket        Request = 33
	ReqResetDevice         Request = 34
	ReqVringKick           Request = 35
	ReqGetMaxMemSlots      Request = 36
	ReqAddMemReg           Request = 37
	ReqRemMemReg           Request = 38
	ReqSetStatus           Request = 39
	ReqGetStatus           Request = 40
	ReqGetSharedObject     Request = 41
	ReqSetDeviceStateFd    Request = 42
	ReqCheckDeviceState    Request = 43
)

var requestNames = map[Request]string{
	ReqGetFeatures: "GET_FEATURES", ReqSetFeatures: "SET_FEATURES",
	ReqSetOwner: "SET_OWNER", ReqResetOwner: "RESET_OWNER",
	ReqSetMemTable: "SET_MEM_TABLE", ReqSetLogBase: "SET_LOG_BASE",
	ReqSetLogFd: "SET_LOG_FD", ReqSetVringNum: "SET_VRING_NUM",
	ReqSetVringAddr: "SET_VRING_ADDR", ReqSetVringBase: "SET_VRING_BASE",
	ReqGetVringBase: "GET_VRING_BASE", ReqSetVringKick: "SET_VRING_KICK",
	ReqSetVringCall: "SET_VRING_CALL", ReqSetVringErr: "SET_VRING_ERR",
	ReqGetProtocolFeatures: "GET_PROTOCOL_FEATURES",
	ReqSetProtocolFeatures: "SET_PROTOCOL_FEATURES",
	ReqGetQueueNum:         "GET_QUEUE_NUM", ReqSetVringEnable: "SET_VRING_ENABLE",
	ReqSendRARP: "SEND_RARP", ReqNetSetMTU: "NET_SET_MTU",
	ReqSetBackendReqFd: "SET_BACKEND_REQ_FD", ReqIOTLBMsg: "IOTLB_MSG",
	ReqSetVringEndian: "SET_VRING_ENDIAN", ReqGetConfig: "GET_CONFIG",
	ReqSetConfig: "SET_CONFIG", ReqCreateCryptoSession: "CREATE_CRYPTO_SESSION",
	ReqCloseCryptoSession: "CLOSE_CRYPTO_SESSION", ReqPostcopyAdvise: "POSTCOPY_ADVISE",
	ReqPostcopyListen: "POSTCOPY_LISTEN", ReqPostcopyEnd: "POSTCOPY_END",
	ReqGetInflightFd: "GET_INFLIGHT_FD", ReqSetInflightFd: "SET_INFLIGHT_FD",
	ReqGPUSetSocket: "GPU_SET_SOCKET", ReqResetDevice: "RESET_DEVICE",
	ReqVringKick: "VRING_KICK", ReqGetMaxMemSlots: "GET_MAX_MEM_SLOTS",
	ReqAddMemReg: "ADD_MEM_REG", ReqRemMemReg: "REM_MEM_REG",
	ReqSetStatus: "SET_STATUS", ReqGetStatus: "GET_STATUS",
	ReqGetSharedObject: "GET_SHARED_OBJECT", ReqSetDeviceStateFd: "SET_DEVICE_STATE_FD",
	ReqCheckDeviceState: "CHECK_DEVICE_STATE",
}

func (r Request) String() string {
	if s, ok := requestNames[r]; ok {
		return s
	}
	return "VHOST_USER_UNKNOWN(" + strconv.FormatUint(uint64(r), 10) + ")"
}

// HeaderSize is the fixed vhost-user message header: request, flags and payload
// length, three little-endian uint32s.
const HeaderSize = 12

// MaxPayload bounds a single message body. The largest thing QEMU sends is a
// SET_MEM_TABLE with a handful of 32-byte regions or a GET_CONFIG of at most
// 256 bytes, so this is three orders of magnitude of headroom — it exists so a
// corrupt length cannot make the backend allocate the host's memory.
const MaxPayload = 1 << 16

// Header flag bits. The low two bits carry the protocol version (always 1);
// Reply marks a back-end-to-front-end answer, and NeedReply asks for an
// acknowledgement on a request that has no natural one (REPLY_ACK).
const (
	flagVersionMask uint32 = 0x3
	flagVersion1    uint32 = 0x1
	flagReply       uint32 = 0x4
	flagNeedReply   uint32 = 0x8
)

// Message is one vhost-user message together with the file descriptors that
// rode with it over SCM_RIGHTS. Files is what makes this protocol impossible to
// express over simio/network: the memory table, the kick and the call are all
// file descriptors, not bytes.
type Message struct {
	Request Request
	Flags   uint32
	Payload []byte
	Files   []*os.File
}

// NeedsReply reports whether the front-end asked for a REPLY_ACK.
func (m Message) NeedsReply() bool { return m.Flags&flagNeedReply != 0 }

// IsReply reports whether this message is a back-end answer.
func (m Message) IsReply() bool { return m.Flags&flagReply != 0 }

// Version reports the protocol version in the header's low bits.
func (m Message) Version() uint32 { return m.Flags & flagVersionMask }

// CloseFiles releases every descriptor that arrived with the message. A
// vhost-user back-end that forgets this leaks one descriptor per memory region
// per reconnection, which is a slow, silent death rather than a crash.
func (m Message) CloseFiles() {
	for _, f := range m.Files {
		if f != nil {
			_ = f.Close()
		}
	}
}

// reply builds the answer to m carrying payload.
func (m Message) reply(payload []byte) *Message {
	return &Message{Request: m.Request, Flags: flagVersion1 | flagReply, Payload: payload}
}

// replyU64 is the shape of most answers: GET_FEATURES, GET_QUEUE_NUM and every
// REPLY_ACK are a single little-endian uint64.
func (m Message) replyU64(v uint64) *Message {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, v)
	return m.reply(p)
}

// EncodeHeader writes m's header into dst, which must be HeaderSize bytes.
func EncodeHeader(dst []byte, m Message) {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(m.Request))
	binary.LittleEndian.PutUint32(dst[4:8], m.Flags)
	binary.LittleEndian.PutUint32(dst[8:12], uint32(len(m.Payload)))
}

// DecodeHeader reads a message header from src, returning the message (without
// its payload) and the payload length still to be read. It rejects a length
// beyond MaxPayload rather than trusting the peer with an allocation.
func DecodeHeader(src []byte) (Message, int, error) {
	if len(src) < HeaderSize {
		return Message{}, 0, fmt.Errorf("%w: header is %d bytes, want %d", ErrProtocol, len(src), HeaderSize)
	}
	m := Message{
		Request: Request(binary.LittleEndian.Uint32(src[0:4])),
		Flags:   binary.LittleEndian.Uint32(src[4:8]),
	}
	n := int(binary.LittleEndian.Uint32(src[8:12]))
	if n > MaxPayload {
		return Message{}, 0, fmt.Errorf("%w: %s payload of %d bytes exceeds %d", ErrProtocol, m.Request, n, MaxPayload)
	}
	return m, n, nil
}

// payloadU64 reads the single uint64 that carries most request bodies.
func payloadU64(m Message) (uint64, error) {
	if len(m.Payload) < 8 {
		return 0, fmt.Errorf("%w: %s wants an 8-byte payload, got %d", ErrProtocol, m.Request, len(m.Payload))
	}
	return binary.LittleEndian.Uint64(m.Payload[:8]), nil
}

// vringState is `struct vhost_vring_state`: an index and a number. It carries
// SET_VRING_NUM, SET_VRING_BASE, GET_VRING_BASE and SET_VRING_ENABLE.
type vringState struct {
	Index uint32
	Num   uint32
}

const vringStateSize = 8

func decodeVringState(m Message) (vringState, error) {
	if len(m.Payload) < vringStateSize {
		return vringState{}, fmt.Errorf("%w: %s wants %d bytes, got %d", ErrProtocol, m.Request, vringStateSize, len(m.Payload))
	}
	return vringState{
		Index: binary.LittleEndian.Uint32(m.Payload[0:4]),
		Num:   binary.LittleEndian.Uint32(m.Payload[4:8]),
	}, nil
}

func encodeVringState(s vringState) []byte {
	p := make([]byte, vringStateSize)
	binary.LittleEndian.PutUint32(p[0:4], s.Index)
	binary.LittleEndian.PutUint32(p[4:8], s.Num)
	return p
}

// vringAddr is `struct vhost_vring_addr`. Every address in it is a *front-end
// userspace* address, translated through the memory table's userspace_addr —
// unlike the addresses inside descriptors, which are guest-physical. Mixing the
// two reads plausible memory at the wrong offset, so they have separate
// translations (see AddressSpace).
type vringAddr struct {
	Index         uint32
	Flags         uint32
	DescUserAddr  uint64
	UsedUserAddr  uint64
	AvailUserAddr uint64
	LogGuestAddr  uint64
}

const vringAddrSize = 40

func decodeVringAddr(m Message) (vringAddr, error) {
	if len(m.Payload) < vringAddrSize {
		return vringAddr{}, fmt.Errorf("%w: SET_VRING_ADDR wants %d bytes, got %d", ErrProtocol, vringAddrSize, len(m.Payload))
	}
	p := m.Payload
	return vringAddr{
		Index:         binary.LittleEndian.Uint32(p[0:4]),
		Flags:         binary.LittleEndian.Uint32(p[4:8]),
		DescUserAddr:  binary.LittleEndian.Uint64(p[8:16]),
		UsedUserAddr:  binary.LittleEndian.Uint64(p[16:24]),
		AvailUserAddr: binary.LittleEndian.Uint64(p[24:32]),
		LogGuestAddr:  binary.LittleEndian.Uint64(p[32:40]),
	}, nil
}

func encodeVringAddr(a vringAddr) []byte {
	p := make([]byte, vringAddrSize)
	binary.LittleEndian.PutUint32(p[0:4], a.Index)
	binary.LittleEndian.PutUint32(p[4:8], a.Flags)
	binary.LittleEndian.PutUint64(p[8:16], a.DescUserAddr)
	binary.LittleEndian.PutUint64(p[16:24], a.UsedUserAddr)
	binary.LittleEndian.PutUint64(p[24:32], a.AvailUserAddr)
	binary.LittleEndian.PutUint64(p[32:40], a.LogGuestAddr)
	return p
}

// vringFDMask marks a SET_VRING_KICK/CALL/ERR payload as carrying no descriptor:
// the front-end is saying "polling mode / no notification", not "here is fd 0".
const vringFDMask uint64 = 0x100

// vringIndexMask is the queue index in a SET_VRING_KICK/CALL/ERR payload.
const vringIndexMask uint64 = 0xff

// configHeaderSize is the fixed part of `struct vhost_user_config`: offset,
// size and flags, before the configuration bytes themselves.
const configHeaderSize = 12

// configRequest is a GET_CONFIG/SET_CONFIG body.
type configRequest struct {
	Offset uint32
	Size   uint32
	Flags  uint32
	Region []byte
}

func decodeConfig(m Message) (configRequest, error) {
	if len(m.Payload) < configHeaderSize {
		return configRequest{}, fmt.Errorf("%w: %s wants at least %d bytes, got %d", ErrProtocol, m.Request, configHeaderSize, len(m.Payload))
	}
	return configRequest{
		Offset: binary.LittleEndian.Uint32(m.Payload[0:4]),
		Size:   binary.LittleEndian.Uint32(m.Payload[4:8]),
		Flags:  binary.LittleEndian.Uint32(m.Payload[8:12]),
		Region: m.Payload[configHeaderSize:],
	}, nil
}

func encodeConfig(c configRequest) []byte {
	p := make([]byte, configHeaderSize+len(c.Region))
	binary.LittleEndian.PutUint32(p[0:4], c.Offset)
	binary.LittleEndian.PutUint32(p[4:8], c.Size)
	binary.LittleEndian.PutUint32(p[8:12], c.Flags)
	copy(p[configHeaderSize:], c.Region)
	return p
}
