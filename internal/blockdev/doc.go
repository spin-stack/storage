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
// # What a guest is promised, and what it is not (INV-18, §14.8, ADR-0026)
//
// A WRITE completes when the record is in the local WAL. It is not durable, and it
// issues no object-store PUT: the device advertises a write-back cache
// (VIRTIO_BLK_F_FLUSH, §2), so the guest is told to send a FLUSH for anything it wants
// to keep. So WriteAt maps to wal.Log.Write and nothing else, and Flush maps to
// wal.Log.Flush.
//
// What that FLUSH buys is one fdatasync of the local WAL segments and nothing else.
// After it returns, the guest's writes survive this process, the Agent and QEMU dying;
// they do **not** survive the host dying. §14.8 and ADR-0026 chose that rather than
// paying for the difference on every commit, and the volume reaches the object store
// once, when it stops (agent.Volume.publish).
//
// This paragraph claimed the larger promise until 2026-08-03, and the claim is what
// makes it worth recording: it said Flush "in `remote` mode returns only after every
// covering object is verified in S3 and the lease is confirmed valid on the monotonic
// clock". Both halves — the upload-and-verify chain and the lease gate — were deleted
// in ADR-0026 increment 4.5, and a device whose doc still described them was telling a
// reader that a guest's fsync meant something the code underneath had stopped doing.
//
// A Flush that cannot complete returns an error and the guest sees the request fail.
// Nothing here ever reports success for a durability step that did not happen.
//
// # FUA
//
// wal.Log had a WriteFUA, and nothing ever called it, because a virtio-blk
// request cannot ask for FUA. `struct virtio_blk_outhdr` carries a type, an ioprio and
// a sector, and the type space (virtio 1.2 §5.2.6) has no FUA bit — the only high bit
// ever defined there is the legacy VIRTIO_BLK_T_BARRIER, which this backend does not
// negotiate. A guest that wants force-unit-access gets it the way the Linux block layer
// produces it for any device without FUA support: the WRITE, then a FLUSH. That decomposition
// lands on Write + Flush here, which carry exactly the ACK contract it did — so it was
// deleted with ADR-0026 increment 4.5 rather than kept as a second spelling of them —
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
//   - wal.ErrLogBroken — a failed rollback left the log unable to say what its tail
//     holds, so it will not confirm anything against it. No local action clears it;
//     the volume needs a new epoch. Sticky by construction: every later request fails
//     immediately.
//
//   - wal.ErrBackpressure (§5.7) — the volume has written its whole share of the local
//     device. This is the error the design chose over silently filling the host's NVMe.
//     **Nothing clears it while the volume runs.** The bound the Agent sets is
//     wal.Limits.MaxLocalBytes and only that one — agent.Budget.Limits explains why it
//     leaves MaxUnflushedBytes unset — and a FLUSH clears the unflushed counters, not
//     the retained segments. What gives the space back is stopping the volume, which
//     publishes its image and drops the WAL. This list said "transient and
//     self-clearing: a successful FLUSH resumes writes" until 2026-08-09, and so did
//     the sentence the guest's error carried, which sent an operator to do the one
//     thing that cannot work.
//
//   - wal.ErrViewBound — the read view's memory bound, which this list did not mention
//     at all until 2026-08-11 although the code has branched on it since the sentinel
//     existed. It wraps ErrBackpressure, so a guest still sees the I/O error it
//     understands, and its remedy is the *opposite* of the bound above: the memory is
//     the volume's live extents, a DISCARD gives it back with the volume still serving,
//     and integration/vhost's walking guest crosses the bound, trims and writes on.
//
//   - ErrDeviceFull — the local device is out of space (wal.Degraded() reports
//     OUT_OF_SPACE). Local and recoverable — truncate after a checkpoint or grow the
//     device — and explicitly *not* a fencing condition: handing a volume to another
//     host because a disk filled would turn a local problem into a failover.
//
// # What the host sees, and when it stops seeing it
//
// Nothing on the host observes any of the three otherwise: the refusal is produced on
// the guest's goroutine and handed to a virtqueue that completes with IOERR. So Device
// latches it, and RefusedForSpace is what cmd/volume-agent reports.
//
// It latches a Reason and not a sentence, because the consumer is a metric label and the
// three remedies contradict each other — an operator reading one gauge cannot tell a
// volume that needs an `fstrim` from one that needs a restart from a host that needs a
// bigger disk. It also *ends*: a latch is right for the transition (thousands of refusals
// a second, one report) and wrong for ever, and the walking guest above is the proof —
// after it recovered, the Agent went on reporting backpressure. Device.tookAnAppend
// carries the per-reason rule, including the one reason that has no end while the volume
// runs.
//
// The first bullet used to be `wal.ErrSelfFenced (§12.2, §16) — this host's lease
// lapsed and it has lost the authority to ACK. No local action clears it; the Control
// Plane promotes someone else.` That sentinel does not exist: it went with the
// lease-gated ACK in ADR-0026 increment 4.5, and `wal.Log`'s `fenced` flag became
// `broken` with one meaning left. `refuse` in blockdev.go has branched on
// wal.ErrLogBroken since; this list had not caught up.
//
// # DISCARD and WRITE_ZEROES
//
// Offered, and reaching the WAL: Discard and WriteZeroes below are what
// VIRTIO_BLK_T_DISCARD and VIRTIO_BLK_T_WRITE_ZEROES land on, and a real guest's
// `fstrim` drives them (integration/guestinit's discard mode). This section said "not
// offered" for as long as internal/vhost withheld the feature bits; it does not any
// more, and the mechanism was complete and unreachable in between.
//
// # Concurrency
//
// This package holds no lock. wal.Log is safe for concurrent use and owns the
// invariants behind its own two mutexes, so a mutex here could only re-serialize what
// is already serialized — and would put a guest's READ behind its FLUSH for nothing.
// See the Device type for the hazard that would have been the reason for one, and
// where it is actually handled. The device does not take ownership of the Log and does
// not close it.
package blockdev
