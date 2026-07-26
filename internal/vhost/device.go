package vhost

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// Config builds a Device. Backend and Mapper have no useful zero value; the
// rest do.
type Config struct {
	// Backend is the block device served to the guest.
	Backend Backend
	// Mapper turns the front-end's memory-region descriptors into host memory.
	Mapper Mapper
	// QueueSize is the queue depth advertised to the front-end. Zero means
	// MaxQueueSize (128, §30.3).
	QueueSize uint16
	// Serial answers VIRTIO_BLK_T_GET_ID. Truncated to 20 bytes.
	Serial string
	// BlockSize is the logical block size advertised in the virtio-blk config.
	// Zero means SectorSize.
	BlockSize uint32
	// OnRequest, if set, is called with every vhost-user message as it is
	// handled. It exists so a test — including the QEMU integration lane, which
	// cannot observe the socket any other way — can assert *which* handshake the
	// front-end actually performed and *what* it said, rather than assert that it
	// did not crash.
	//
	// The message is the one the device is about to dispatch: the hook may read
	// it, but the descriptors in Files belong to the device and must not be
	// retained or closed.
	OnRequest func(Message)
	// OnError, if set, is called with every request this backend completed as
	// an I/O error. A device that fails every READ and a device that serves
	// them are indistinguishable from the outside until the guest gives up.
	OnError func(reqType uint32, err error)
}

// Device is one vhost-user connection's worth of state: the negotiated
// features, the mapped guest memory, and the single virtqueue §30.3 fixes.
//
// Every method is safe to call from the message loop and the queue loop at the
// same time; there is exactly one of each.
type Device struct {
	backend   Backend
	mapper    Mapper
	queueSize uint16
	serial    string
	blockSize uint32
	onRequest func(Message)
	onError   func(uint32, error)

	mu       sync.Mutex
	owned    bool
	features uint64 // what the front-end negotiated with SET_FEATURES
	protocol uint64 // what SET_PROTOCOL_FEATURES negotiated
	space    *AddressSpace
	vq       vring
	enabled  bool
	kick     *os.File
	call     *os.File
}

// NewDevice builds a Device from cfg.
func NewDevice(cfg Config) (*Device, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("vhost: Config.Backend is required")
	}
	if cfg.Mapper == nil {
		return nil, fmt.Errorf("vhost: Config.Mapper is required")
	}
	qs := cfg.QueueSize
	if qs == 0 {
		qs = MaxQueueSize
	}
	if qs > MaxQueueSize || qs&(qs-1) != 0 {
		return nil, fmt.Errorf("vhost: queue size %d must be a power of two no greater than %d", qs, MaxQueueSize)
	}
	bs := cfg.BlockSize
	if bs == 0 {
		bs = SectorSize
	}
	return &Device{
		backend:   cfg.Backend,
		mapper:    cfg.Mapper,
		queueSize: qs,
		serial:    cfg.Serial,
		blockSize: bs,
		onRequest: cfg.OnRequest,
		onError:   cfg.OnError,
	}, nil
}

func (d *Device) trace(reqType uint32, err error) {
	if d.onError != nil {
		d.onError(reqType, err)
	}
}

// Ready reports whether the virtqueue is configured, kicked and enabled — the
// point at which the guest can be served. It is the single question the
// integration lane asks of a real QEMU handshake.
func (d *Device) Ready() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ready()
}

func (d *Device) ready() bool {
	return d.enabled && d.space != nil && d.vq.configured()
}

// Features reports the feature set the front-end negotiated.
func (d *Device) Features() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.features
}

// Kick is the front-end's doorbell descriptor, or nil if none was set.
func (d *Device) Kick() *os.File {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.kick
}

// Call is the descriptor to signal completions on, or nil.
func (d *Device) Call() *os.File {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.call
}

// Close releases the mapped memory and the notification descriptors.
func (d *Device) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reset()
}

// reset returns the device to the state a fresh connection starts in. Called by
// RESET_OWNER / RESET_DEVICE and by Close.
func (d *Device) reset() {
	d.space.Close()
	d.space = nil
	d.vq = vring{}
	d.enabled = false
	if d.kick != nil {
		_ = d.kick.Close()
		d.kick = nil
	}
	if d.call != nil {
		_ = d.call.Close()
		d.call = nil
	}
	d.owned = false
	d.features = 0
	d.protocol = 0
}

// Handle processes one front-end message and returns the reply to send, or nil
// when the request needs none. Every descriptor that arrived with the message
// and was not adopted by the device is closed before returning.
//
// A non-nil error is fatal to the connection: the front-end and the backend no
// longer agree about the device's state, and continuing would mean serving I/O
// out of a ring or a memory map we cannot describe.
func (d *Device) Handle(ctx context.Context, m Message) (*Message, error) {
	if d.onRequest != nil {
		d.onRequest(m)
	}
	if v := m.Version(); v != flagVersion1 {
		m.CloseFiles()
		return nil, fmt.Errorf("%w: %s carries protocol version %d, want 1", ErrProtocol, m.Request, v)
	}
	reply, err := d.dispatch(ctx, m)
	if err != nil {
		m.CloseFiles()
		return nil, err
	}
	// REPLY_ACK: the front-end may ask for an acknowledgement on a request that
	// has no answer of its own. Zero means success. Without this, a
	// SET_MEM_TABLE this backend rejected looks to QEMU exactly like one it
	// accepted, and the failure surfaces later as a device that never responds.
	if reply == nil && m.NeedsReply() && has(d.protocolMask(), protocolReplyAck) {
		reply = m.replyU64(0)
	}
	return reply, nil
}

func (d *Device) protocolMask() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.protocol
}

// dispatch answers one front-end request. It is a flat switch over the wire's
// request numbers: splitting it into helpers by category would hide the one
// property that matters here — that every request QEMU can send has a case —
// behind a call graph.
//
//nolint:gocyclo // the flat switch is the point; see above.
func (d *Device) dispatch(ctx context.Context, m Message) (*Message, error) {
	switch m.Request {
	case ReqGetFeatures:
		return m.replyU64(DeviceFeatures), nil

	case ReqSetFeatures:
		v, err := payloadU64(m)
		if err != nil {
			return nil, err
		}
		return nil, d.setFeatures(v)

	case ReqGetProtocolFeatures:
		return m.replyU64(ProtocolFeatures), nil

	case ReqSetProtocolFeatures:
		v, err := payloadU64(m)
		if err != nil {
			return nil, err
		}
		d.mu.Lock()
		d.protocol = v & ProtocolFeatures
		d.mu.Unlock()
		return nil, nil

	case ReqSetOwner:
		d.mu.Lock()
		d.owned = true
		d.mu.Unlock()
		return nil, nil

	case ReqResetOwner, ReqResetDevice:
		d.mu.Lock()
		d.reset()
		d.mu.Unlock()
		return nil, nil

	case ReqGetQueueNum:
		// One queue, as §30.3 fixes. VIRTIO_BLK_F_MQ is not offered, so a
		// conforming front-end will not ask; answering honestly costs nothing.
		return m.replyU64(1), nil

	case ReqGetConfig:
		return d.getConfig(m)

	case ReqSetConfig:
		// The guest may not resize or reconfigure the device from inside.
		// REPLY_ACK turns this into a visible refusal — but only when the
		// front-end asked for one. vhost-user frames by header alone, so a reply
		// nobody is reading is read as the front of the next request, and the
		// socket never recovers.
		if m.NeedsReply() && has(d.protocolMask(), protocolReplyAck) {
			return m.replyU64(1), nil
		}
		return nil, nil

	case ReqSetMemTable:
		return nil, d.setMemTable(m)

	case ReqSetVringNum:
		return nil, d.setVringNum(m)

	case ReqSetVringAddr:
		return nil, d.setVringAddr(m)

	case ReqSetVringBase:
		return nil, d.setVringBase(m)

	case ReqGetVringBase:
		return d.getVringBase(m)

	case ReqSetVringKick:
		return nil, d.setVringFD(m, &d.kick)

	case ReqSetVringCall:
		return nil, d.setVringFD(m, &d.call)

	case ReqSetVringErr:
		// The error descriptor is accepted and dropped: this backend reports
		// per-request failures in the status byte, which is where a guest can
		// actually see them, and has nothing to say on a channel the guest
		// never reads.
		m.CloseFiles()
		return nil, nil

	case ReqSetVringEnable:
		return nil, d.setVringEnable(m)

	case ReqSetLogBase, ReqSetLogFd:
		// Dirty-page logging is a live-migration facility. This backend does
		// not advertise LOG_SHMFD, so accepting the descriptor and ignoring it
		// would let a migration silently lose writes. Refuse it.
		m.CloseFiles()
		return nil, fmt.Errorf("%w: %s, but this backend does not advertise VHOST_USER_PROTOCOL_F_LOG_SHMFD", ErrProtocol, m.Request)

	case ReqGetInflightFd, ReqSetInflightFd:
		// Increment 3.3. Not advertising INFLIGHT_SHMFD means a conforming
		// front-end never sends these; refusing loudly is what keeps a future
		// "why did the guest lose a request" from starting here.
		m.CloseFiles()
		return nil, fmt.Errorf("%w: %s, but this backend does not advertise VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD", ErrProtocol, m.Request)

	default:
		m.CloseFiles()
		return nil, fmt.Errorf("%w: unhandled request %s", ErrProtocol, m.Request)
	}
}

func (d *Device) setFeatures(v uint64) error {
	if unknown := v &^ DeviceFeatures; unknown != 0 {
		return fmt.Errorf("%w: SET_FEATURES asks for bits %#x this backend did not offer", ErrProtocol, unknown)
	}
	if !has(v, featureVersion1) {
		// A legacy (virtio 0.9) guest lays its rings out differently and uses
		// guest-endian fields. Serving it out of a modern parser produces
		// plausible garbage; refusing produces a device the guest cannot use,
		// which is the honest outcome.
		return fmt.Errorf("%w: SET_FEATURES without VIRTIO_F_VERSION_1; this backend serves modern virtio only", ErrProtocol)
	}
	d.mu.Lock()
	d.features = v
	d.mu.Unlock()
	return nil
}

// blkConfigSize is the size of `struct virtio_blk_config` this backend knows how
// to fill. QEMU asks for whatever its own struct is, which grows between
// versions; anything past what we fill is left zero, which is what an unset
// feature bit means for every field in it.
const blkConfigSize = 60

// MaxConfigSize is VHOST_USER_MAX_CONFIG_SIZE: the largest configuration space
// the protocol allows a GET_CONFIG/SET_CONFIG to describe. It is the bound on
// the peer-controlled offset and size, and it is deliberately the protocol's
// number rather than this device's 60 bytes — see getConfig.
const MaxConfigSize = 256

func (d *Device) getConfig(m Message) (*Message, error) {
	req, err := decodeConfig(m)
	if err != nil {
		return nil, err
	}
	// Both fields are peer-controlled uint32s inside a 12-byte message, so the
	// window they describe is bounded by the protocol's own maximum before it is
	// allowed to size an allocation. Sizing the buffer from offset+size instead —
	// as this did — turns a GET_CONFIG of four bytes at 0xffff_f000 into a
	// four-gigabyte allocation.
	//
	// The bound is MaxConfigSize, not the 60 bytes this backend fills: QEMU asks
	// for whatever its own struct virtio_blk_config is, and that struct grows
	// between QEMU versions. Refusing the excess would break against a front-end
	// newer than the one we tested; answering it with zeros is what an unset
	// feature bit means for every field past our 60.
	if req.Size == 0 || req.Offset >= MaxConfigSize || req.Size > MaxConfigSize-req.Offset {
		return nil, fmt.Errorf("%w: GET_CONFIG asks for %d bytes at offset %d; the configuration space is at most %d bytes",
			ErrProtocol, req.Size, req.Offset, MaxConfigSize)
	}
	full := make([]byte, MaxConfigSize)
	d.fillConfig(full)
	region := full[req.Offset : req.Offset+req.Size]
	return m.reply(encodeConfig(configRequest{Offset: req.Offset, Size: req.Size, Region: region})), nil
}

// fillConfig writes the virtio-blk configuration space. Only the fields whose
// feature bits are offered are set; the rest stay zero.
func (d *Device) fillConfig(c []byte) {
	// capacity, in 512-byte sectors, truncated: a trailing partial sector is
	// capacity the guest would address and the backend would refuse.
	binary.LittleEndian.PutUint64(c[0:8], uint64(d.backend.Size()/SectorSize))
	// seg_max: the longest descriptor chain a request may use. The chain also
	// carries the header and the status byte, so the data segments are two
	// fewer than the queue depth — promising the depth itself would have the
	// guest build a request that cannot fit in the ring.
	binary.LittleEndian.PutUint32(c[12:16], uint32(d.queueSize)-2)
	// blk_size.
	binary.LittleEndian.PutUint32(c[20:24], d.blockSize)
	// num_queues. VIRTIO_BLK_F_MQ is not offered, so the field is informational;
	// it is filled because a zero here reads as "no queues" in a guest that
	// looks at it anyway.
	binary.LittleEndian.PutUint16(c[34:36], 1)
}

func (d *Device) setMemTable(m Message) error {
	regions, err := decodeMemTable(m)
	if err != nil {
		m.CloseFiles()
		return err
	}
	space, err := NewAddressSpace(d.mapper, regions, m.Files)
	// The descriptors are ours the moment they arrive, and the mapping keeps
	// the memory alive independently of them, so they are closed either way.
	m.CloseFiles()
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// A second SET_MEM_TABLE replaces the first. Dropping the old mapping is
	// not optional: it is the whole guest's RAM, and leaking it once per
	// reconnection exhausts the host.
	d.space.Close()
	d.space = space
	return nil
}

func (d *Device) setVringNum(m Message) error {
	s, err := decodeVringState(m)
	if err != nil {
		return err
	}
	if err := checkIndex(s.Index); err != nil {
		return err
	}
	if s.Num == 0 || s.Num > uint32(d.queueSize) || s.Num&(s.Num-1) != 0 {
		return fmt.Errorf("%w: SET_VRING_NUM %d, want a power of two in (0, %d]", ErrProtocol, s.Num, d.queueSize)
	}
	d.mu.Lock()
	d.vq.num = uint16(s.Num)
	d.mu.Unlock()
	return nil
}

func (d *Device) setVringAddr(m Message) error {
	a, err := decodeVringAddr(m)
	if err != nil {
		return err
	}
	if err := checkIndex(a.Index); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.vq.num == 0 {
		return fmt.Errorf("%w: SET_VRING_ADDR before SET_VRING_NUM", ErrProtocol)
	}
	if d.space == nil {
		return fmt.Errorf("%w: SET_VRING_ADDR before SET_MEM_TABLE", ErrProtocol)
	}
	n := uint64(d.vq.num)
	// The three ring areas are addressed in the *front-end's* address space,
	// and their sizes follow from the queue length. Translating them here — and
	// failing now — is what turns "QEMU sent us an address in the wrong space"
	// into a startup error instead of a wrong read three layers down.
	desc, err := d.space.User(a.DescUserAddr, n*descSize)
	if err != nil {
		return fmt.Errorf("descriptor table: %w", err)
	}
	avail, err := d.space.User(a.AvailUserAddr, ringHeaderSize+n*2+2)
	if err != nil {
		return fmt.Errorf("available ring: %w", err)
	}
	used, err := d.space.User(a.UsedUserAddr, ringHeaderSize+n*usedElemSize+2)
	if err != nil {
		return fmt.Errorf("used ring: %w", err)
	}
	d.vq.desc, d.vq.avail, d.vq.used = desc, avail, used
	return nil
}

func (d *Device) setVringBase(m Message) error {
	s, err := decodeVringState(m)
	if err != nil {
		return err
	}
	if err := checkIndex(s.Index); err != nil {
		return err
	}
	d.mu.Lock()
	// The front-end is telling us where the guest's ring counters stand — after
	// a reconnection this is not zero, and starting from zero would re-serve
	// every request the ring still holds.
	d.vq.lastAvail = uint16(s.Num)
	d.vq.usedIdx = uint16(s.Num)
	d.mu.Unlock()
	return nil
}

func (d *Device) getVringBase(m Message) (*Message, error) {
	s, err := decodeVringState(m)
	if err != nil {
		return nil, err
	}
	if err := checkIndex(s.Index); err != nil {
		return nil, err
	}
	d.mu.Lock()
	last := uint32(d.vq.lastAvail)
	// GET_VRING_BASE stops the queue: the front-end is taking the ring back.
	d.enabled = false
	d.mu.Unlock()
	return m.reply(encodeVringState(vringState{Index: s.Index, Num: last})), nil
}

func (d *Device) setVringFD(m Message, dst **os.File) error {
	v, err := payloadU64(m)
	if err != nil {
		m.CloseFiles()
		return err
	}
	if err := checkIndex(uint32(v & vringIndexMask)); err != nil {
		m.CloseFiles()
		return err
	}
	var f *os.File
	if v&vringFDMask == 0 {
		if len(m.Files) != 1 {
			m.CloseFiles()
			return fmt.Errorf("%w: %s without the VRING_NOFD bit carries %d descriptors, want 1", ErrProtocol, m.Request, len(m.Files))
		}
		f = m.Files[0]
	} else {
		// The NOFD bit means polling mode. Any descriptor sent with it is not
		// ours to keep.
		m.CloseFiles()
	}
	d.mu.Lock()
	old := *dst
	*dst = f
	d.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func (d *Device) setVringEnable(m Message) error {
	s, err := decodeVringState(m)
	if err != nil {
		return err
	}
	if err := checkIndex(s.Index); err != nil {
		return err
	}
	d.mu.Lock()
	d.enabled = s.Num != 0
	d.mu.Unlock()
	return nil
}

// checkIndex rejects any queue but the single one this backend serves.
func checkIndex(i uint32) error {
	if i != 0 {
		return fmt.Errorf("%w: queue index %d, but this backend serves one queue (§30.3)", ErrProtocol, i)
	}
	return nil
}

// ProcessQueue serves every request the guest has made available and reports
// how many it completed and whether the guest wants an interrupt.
//
// It is the whole data path: read the avail ring, walk each descriptor chain,
// run the request against the Backend, publish the used element. It takes no
// descriptors and does no I/O of its own, so a simulated front-end that has
// laid a ring out in an ordinary byte slice exercises exactly the code a real
// QEMU does.
func (d *Device) ProcessQueue(ctx context.Context) (served int, notify bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready() {
		return 0, false, nil
	}
	for {
		avail := d.vq.availIdx()
		if d.vq.lastAvail == avail {
			break
		}
		// The guest may publish at most `num` buffers before we consume one, so
		// a gap larger than the ring means the ring is lying about its state.
		if avail-d.vq.lastAvail > d.vq.num {
			return served, false, fmt.Errorf("%w: available index jumped from %d to %d on a %d-entry ring", ErrRing, d.vq.lastAvail, avail, d.vq.num)
		}
		for d.vq.lastAvail != avail {
			head := d.vq.availRingEntry(d.vq.lastAvail)
			c, cerr := d.vq.collect(d.space, head)
			if cerr != nil {
				return served, false, cerr
			}
			req, perr := parseRequest(c)
			if perr != nil {
				return served, false, perr
			}
			written := d.serve(ctx, req)
			d.vq.complete(head, written)
			d.vq.lastAvail++
			served++
		}
	}
	if served == 0 {
		return 0, false, nil
	}
	return served, d.vq.shouldNotify(), nil
}
