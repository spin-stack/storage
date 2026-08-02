// Package blockdev is the guest-visible block device: the adapter that lets a
// virtio-blk request reach the WAL.
//
// # Why it is its own package
//
// internal/vhost owns the transport — the vhost-user handshake, the virtqueue, the
// virtio-blk request shapes — and defines vhost.Backend where it consumes it.
// internal/wal owns durability — records, segments, watermarks, the §14.4 ACK
// sequence. Neither should learn the other: a WAL that knew about descriptor chains
// could not be driven by DST, and a virtqueue that knew about epochs and object stores
// could not be tested without one. Everything that translates between them is a policy
// decision, and policy decisions want a name and a file. This is that file.
//
// It is also where the device grows: ReadAt today answers out of the WAL's read view
// alone, which is the whole truth for a volume this host wrote from empty. A volume
// restored from S3 also needs the materialized base image underneath, and that fetch
// belongs here — behind the same four methods — rather than inside either neighbour.
//
// # What a guest is promised, and what it is not (INV-18, §14.4)
//
// A WRITE completes when the record is in the local WAL. It is not durable, and it
// issues no object-store PUT: the device advertises a write-back cache
// (VIRTIO_BLK_F_FLUSH, §2) and the guarantee against losing the host arrives only with
// a FLUSH. So WriteAt maps to wal.Log.Write and nothing else, and Flush maps to
// wal.Log.Flush — which in `remote` mode returns only after every covering object is
// verified in S3 and the lease is confirmed valid on the monotonic clock. A Flush that
// cannot establish both returns an error, and the guest sees the request fail. Nothing
// here ever reports success for durability the object store cannot produce.
//
// # FUA
//
// wal.Log has WriteFUA, and nothing in this package calls it, because a virtio-blk
// request cannot ask for FUA. `struct virtio_blk_outhdr` carries a type, an ioprio and
// a sector, and the type space (virtio 1.2 §5.2.6) has no FUA bit — the only high bit
// ever defined there is the legacy VIRTIO_BLK_T_BARRIER, which this backend does not
// negotiate. A guest that wants force-unit-access gets it the way the Linux block layer
// produces it for any device without FUA support: the WRITE, then a FLUSH. That decomposition
// lands on Write + Flush here, which carry exactly the ACK contract WriteFUA does —
// wal.Log implements both through one durableStep — so the guarantee is the same and
// only the number of requests differs.
//
// Two things follow, and both are deliberate. WriteAt passes flags 0 and never
// format.FlagFUA: wal.Log.Write refuses that flag (ErrFUAOnWrite) precisely because it
// implements none of the contract, and a device that set it would be describing a
// durability it did not perform. And VIRTIO_BLK_F_CONFIG_WCE is not offered by
// internal/vhost, so a guest cannot switch the device to write-through and stop sending
// FLUSHes — which would leave it believing every WRITE was durable.
//
// # What the guest sees when the WAL says no
//
// virtio-blk's status byte has three values: OK, IOERR and UNSUPP (virtio 1.2 §5.2.6).
// Linux maps UNSUPP to ENOTSUPP and everything else to EIO; there is no ENOSPC on this
// wire and no way to invent one. So every refusal below completes as
// VIRTIO_BLK_S_IOERR and the guest reports EIO — which is the honest answer, and is
// the one thing a guest can act on. What must never happen instead is a completion
// that says OK, or no completion at all: a guest can retry an EIO and can remount
// read-only, and can do nothing whatsoever with a request that never comes back.
// internal/vhost completes every Backend error rather than propagating it
// (TestBackendFailuresBecomeIOErrorsNotHangs), and every call here returns without
// waiting on anything unbounded.
//
// The distinction the wire cannot carry is therefore carried in the error, for the
// operator and for the Agent, because the three have nothing in common but their
// status byte:
//
//   - wal.ErrSelfFenced (§12.2, §16) — this host's lease lapsed and it has lost the
//     authority to ACK. No local action clears it; the Control Plane promotes someone
//     else. Sticky by construction: every later FLUSH fails immediately.
//   - wal.ErrBackpressure (§5.7) — the unflushed or un-remote-durable backlog hit its
//     bound. Transient and self-clearing: a successful FLUSH resumes writes. This is
//     the error the design chose over silently filling the host's NVMe.
//   - ErrDeviceFull — the local device is out of space (wal.Degraded() reports
//     OUT_OF_SPACE). Local and recoverable — truncate after a checkpoint, grow the
//     device, restore S3 so the remote gap can close — and explicitly *not* a fencing
//     condition: handing a volume to another host because a disk filled would turn a
//     local problem into a failover.
//
// # DISCARD and WRITE_ZEROES
//
// Not offered. wal.Log has Discard and WriteZeroes, so the storage half exists, but
// VIRTIO_BLK_F_DISCARD and VIRTIO_BLK_F_WRITE_ZEROES are two wire features with their
// own request payload (struct virtio_blk_discard_write_zeroes), their own six
// configuration-space fields, and in WRITE_ZEROES' case a may_unmap flag whose two
// readings differ in whether the range is reclaimed. Offering a bit whose semantics are
// not implemented and tested is worse than not offering it, and internal/vhost
// currently answers both request types with UNSUPP, which is what the specification
// says to do and what a conforming driver never has to see. They arrive together with
// their negotiation and their tests, or not at all.
//
// # Concurrency
//
// wal.Log is not safe for concurrent use, and vhost.Backend does not promise its
// methods are called from one goroutine. Every method here therefore holds one mutex
// for the whole request. That serializes the guest's queue against itself, which is
// what §4's single queue does anyway — but it does *not* protect the Log from a
// caller that also holds it (a checkpointer, the Agent's reporter). Whoever owns the
// Log owns that coordination; this device does not take ownership of it, and does not
// close it.
package blockdev
