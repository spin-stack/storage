package vhost

// virtio and vhost-user feature bits. Only the ones this backend has an opinion
// about are named: a bit we neither offer nor need is not a constant, it is
// noise.
const (
	// virtio-blk device features (virtio 1.2 §5.2.3).
	featureBlkSizeMax     = 1  // VIRTIO_BLK_F_SIZE_MAX
	featureBlkSegMax      = 2  // VIRTIO_BLK_F_SEG_MAX
	featureBlkGeometry    = 4  // VIRTIO_BLK_F_GEOMETRY
	featureBlkRO          = 5  // VIRTIO_BLK_F_RO
	featureBlkBlkSize     = 6  // VIRTIO_BLK_F_BLK_SIZE
	featureBlkFlush       = 9  // VIRTIO_BLK_F_FLUSH
	featureBlkTopology    = 10 // VIRTIO_BLK_F_TOPOLOGY
	featureBlkMQ          = 12 // VIRTIO_BLK_F_MQ
	featureBlkDiscard     = 13 // VIRTIO_BLK_F_DISCARD
	featureBlkWriteZeroes = 14 // VIRTIO_BLK_F_WRITE_ZEROES

	// Transport / ring features.
	featureRingIndirectDesc = 28 // VIRTIO_RING_F_INDIRECT_DESC
	featureRingEventIdx     = 29 // VIRTIO_RING_F_EVENT_IDX
	featureProtocol         = 30 // VHOST_USER_F_PROTOCOL_FEATURES
	featureVersion1         = 32 // VIRTIO_F_VERSION_1
	featureRingPacked       = 34 // VIRTIO_F_RING_PACKED
)

// vhost-user protocol feature bits.
const (
	protocolMQ            = 0  // VHOST_USER_PROTOCOL_F_MQ
	protocolLogShmfd      = 1  // VHOST_USER_PROTOCOL_F_LOG_SHMFD
	protocolReplyAck      = 3  // VHOST_USER_PROTOCOL_F_REPLY_ACK
	protocolBackendReq    = 5  // VHOST_USER_PROTOCOL_F_BACKEND_REQ
	protocolConfig        = 9  // VHOST_USER_PROTOCOL_F_CONFIG
	protocolInflightShmfd = 12 // VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD
	protocolStatus        = 16 // VHOST_USER_PROTOCOL_F_STATUS
)

func bit(n uint) uint64 { return 1 << n }

func has(mask uint64, n uint) bool { return mask&bit(n) != 0 }

// DeviceFeatures is the virtio feature set this backend offers.
//
// What is deliberately absent is as load-bearing as what is present:
//
//   - VIRTIO_RING_F_EVENT_IDX is not offered. With it, suppressing an interrupt
//     becomes a race between the driver's published event index and the
//     backend's completion, and getting it wrong loses a completion — the guest
//     hangs. Flow control is not what Increment 3.1 is proving.
//   - VIRTIO_F_RING_PACKED is not offered: this backend walks split rings only.
//   - VIRTIO_BLK_F_MQ is not offered. §30.3 is explicit about a single queue at
//     depth 128; a queue count the backend does not serve is a lie the guest
//     acts on.
//   - VIRTIO_BLK_F_DISCARD / WRITE_ZEROES are not offered *yet*: wal.Log has
//     Discard and WriteZeroes, so they arrive with the WAL wiring, and offering
//     them over a raw device that would emulate them with a write of zeros
//     would make the guest pay for a trim that reclaims nothing.
//
// VIRTIO_BLK_F_FLUSH is offered, and it is the important one: without it the
// guest has no way to ask for durability, and every FLUSH-based ACK rule in
// §14.4 has no counterpart on the wire.
const DeviceFeatures uint64 = 1<<featureBlkSegMax |
	1<<featureBlkBlkSize |
	1<<featureBlkFlush |
	1<<featureRingIndirectDesc |
	1<<featureVersion1 |
	1<<featureProtocol

// ProtocolFeatures is the vhost-user protocol feature set this backend offers.
//
//   - CONFIG is mandatory: QEMU's vhost-user-blk reads the capacity out of
//     GET_CONFIG, and refuses to realize the device without it.
//   - REPLY_ACK makes configuration failures visible to the front-end instead of
//     silently producing a device that does not work.
//
// INFLIGHT_SHMFD is Increment 3.3 and is not offered here. Advertising it before
// the inflight region is implemented would make QEMU hand over a buffer this
// backend neither reads nor writes, and the guarantee the front-end would then
// believe in — requests survive a backend restart — would be false.
const ProtocolFeatures uint64 = 1<<protocolReplyAck |
	1<<protocolConfig
