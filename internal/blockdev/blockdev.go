package blockdev

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// ErrDeviceFull says the local WAL device has no room left: wal.Degraded() reports
// OUT_OF_SPACE after the append the caller is being told about.
//
// It is a classification laid over the device's own error, never a replacement for it
// — the underlying disk error is still in the chain — because "the device is full" is
// the one I/O failure whose remedy (truncate after a checkpoint, grow the device,
// restore the object store so the remote gap can close) is different from every
// other's, and an operator reading a log line needs to be told which one they have.
//
// It is deliberately not a fencing condition. See the package doc.
var ErrDeviceFull = errors.New("the local WAL device is out of space")

// Reason names which bound turned a guest request away for want of space. It exists so a
// metric can carry the distinction as a label and an operator can act on it, which a
// sentence cannot do: the layer above this one flattened three bounds into one gauge and
// one WARN that always said "for want of space", so a volume needing an `fstrim` and a
// volume needing a restart looked identical from outside the process.
//
// The distinction is not invented here — it exists in the Go layer already
// (wal.ErrViewBound wraps wal.ErrBackpressure; ErrDeviceFull is its own sentinel) — it is
// only carried out to where it can be seen. The values are label values, so they are
// lower-case and stable: renaming one breaks a dashboard, not a compile.
type Reason string

const (
	// ReasonViewMemory is the read view's memory bound: this volume's live extents fill
	// the share of host memory it was given. The guest gets it back itself, with the
	// volume still running, by discarding what it no longer needs.
	ReasonViewMemory Reason = "view_memory"
	// ReasonWALShare is the volume's share of the local device (§5.7). V1 has no
	// mid-session reclaim, so it ends when the session does and not before.
	ReasonWALShare Reason = "wal_share"
	// ReasonDeviceENOSPC is the device itself out of space, under a volume that never
	// reached its own share. The remedy is on the host — truncate after a checkpoint,
	// grow the device — and it does not need the volume stopped.
	ReasonDeviceENOSPC Reason = "device_enospc"
)

// Reasons is every value a Refusal can carry. A caller reporting the reason as a metric
// label needs it: `volume_backpressure` has one series per (volume, reason), and the
// series that are *not* current have to be driven to 0 explicitly or they hold whatever
// they last held for ever — the same staleness this type exists to end, moved down a
// level. A function and not a package-level slice, so no caller can edit the vocabulary.
func Reasons() []Reason { return []Reason{ReasonViewMemory, ReasonWALShare, ReasonDeviceENOSPC} }

// rank decides which refusal a device keeps when two bounds are refusing at once.
//
// It is not severity in the abstract. It is what an operator would be made to *wait for*:
// ReasonViewMemory ends while the volume serves, ReasonDeviceENOSPC ends when someone
// gives the device room with the volume still running, and ReasonWALShare does not end
// until the session does. Reporting a clearable reason while an unclearable one is also
// refusing tells the operator to wait for a recovery that cannot come — and it can never
// be corrected afterwards, because what clears the clearable reason is a successful write
// and the unclearable bound has made those impossible.
func (r Reason) rank() int {
	switch r {
	case ReasonWALShare:
		return 3
	case ReasonDeviceENOSPC:
		return 2
	default:
		return 1
	}
}

// Refusal is what the host can see of a guest request turned away for want of space: the
// machine-readable bound, and the sentence naming the remedy for it.
//
// Both, and not one or the other. The Reason is what a time series can carry and what an
// alert can route on; the Remedy is the same sentence the guest's own error carried, and
// it is the thing an operator acts on. A latch holding only the sentence is what made the
// two bounds indistinguishable to everything outside this process.
type Refusal struct {
	Reason Reason
	Remedy string
}

// String renders a Refusal for a log line or a %s: the reason first, because that is the
// word an operator greps for, then what to do about it.
func (r Refusal) String() string { return string(r.Reason) + ": " + r.Remedy }

// Device serves one volume's guest-visible block device out of a wal.Log. It
// satisfies vhost.Backend; see the package doc for what each method promises.
//
// The zero value is not usable: a device with no log and no capacity has nothing to
// tell a guest. Use New.
//
// It holds no lock of its own. That is a statement about where the invariants live,
// not an omission: wal.Log owns them and is safe for concurrent use, behind two
// mutexes whose split is documented on the type. A mutex here could only re-serialize
// what the Log already serializes correctly, and it would serialize a guest's READ
// behind its FLUSH for no gain.
//
// The hazard a lock here would have been for is real and is handled in wal: a WRITE
// arriving in the middle of a FLUSH must not be ACKed by that FLUSH. Log.Flush captures
// its target sequence under the same mutex Log.Write appends under, so a WRITE that
// returns afterwards has a strictly higher sequence and the durable step cannot reach
// it. `TestConcurrentRequestsDoNotRaceTheLog` in this package drives the three request
// types at the device concurrently under -race; the sequence argument itself is wal's
// to prove.
type Device struct {
	log *wal.Log
	cap int64

	// refusal holds the bound that turned a guest request away for want of space, and
	// is nil while none has.
	//
	// It exists because *nothing on the host sees that refusal otherwise*. It is
	// produced here, on the guest's own goroutine, and handed to a virtqueue that
	// completes the request with IOERR and moves on; the guest gets `I/O error, dev
	// vda` and a failed fsync, and the Agent's log says nothing at all. A volume that
	// has hit a bound is the tenant-visible failure this Agent is most likely to have
	// and the one it was least able to report.
	//
	// Latched rather than live, and that is right for the *transition*: a poller at a
	// human cadence must not have to catch the device mid-refusal, and a guest that
	// keeps writing produces thousands of refusals a second, so anything that reported
	// per refusal would bury the log at the moment it is needed. Whoever reports it
	// therefore says it once (cmd/volume-agent's watchSpacePressure).
	//
	// **But a latch that never ends is a lie the moment the condition does.** This field
	// held one string, set once, for the life of the volume — and integration/vhost's
	// walking guest proves a guest can cross the read view's memory bound, BLKDISCARD
	// its way back under it and keep writing, after which the Agent went on reporting
	// backpressure and telling the operator to stop a volume that had recovered. So each
	// reason carries its own end, decided in tookAnAppend: ReasonWALShare has none while
	// the volume runs, which is V1's documented shape (§5.7) and stays.
	//
	// An atomic and not a mutex, so the claim above about this type holding no lock
	// stays true and a refused WRITE stays off any lock a READ could be waiting on.
	refusal atomic.Pointer[Refusal]
}

// New returns a Device of capacity bytes over l.
//
// The capacity must be a whole number of 512-byte sectors: that is the only unit a
// virtio-blk configuration space can express, so a partial trailing sector is either
// capacity the guest addresses and the device refuses, or capacity silently discarded
// — and which of the two it is should not depend on a rounding decision made here.
//
// The Device does not take ownership of l: the caller keeps it for checkpointing,
// truncation and reporting, and closes it.
func New(l *wal.Log, capacity int64) (*Device, error) {
	if l == nil {
		return nil, errors.New("blockdev: a device needs a WAL to serve")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("blockdev: a device of %d bytes has no capacity", capacity)
	}
	if capacity%vhost.SectorSize != 0 {
		return nil, fmt.Errorf("blockdev: a device of %d bytes is not a whole number of %d-byte sectors",
			capacity, vhost.SectorSize)
	}
	return &Device{log: l, cap: capacity}, nil
}

// Size implements vhost.Backend: the capacity the guest is told about.
func (d *Device) Size() int64 { return d.cap }

// ReadAt implements vhost.Backend. It answers out of the WAL's read view, which holds
// every write this log has taken whether or not it has been flushed — so a guest that
// writes a block and reads it back without a FLUSH sees its own bytes. Ranges nothing
// has written read as zero, which is what an empty volume is.
func (d *Device) ReadAt(p []byte, off int64) (int, error) {
	if err := d.inRange(len(p), off, "READ"); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := d.log.Read(uint64(off), p); err != nil {
		// A read that cannot be answered must fail, never return the zeros it happens
		// to hold: the guest cannot tell those from a range it never wrote.
		return 0, d.refuse("READ", len(p), off, err)
	}
	return len(p), nil
}

// WriteAt implements vhost.Backend: it appends a WAL record and returns. It makes no
// durability claim and issues no object-store PUT (§5.3, INV-18); that is Flush.
//
// The flags are 0 and never format.FlagFUA. wal.Log.Write refuses FUA on purpose
// because it implements none of the FUA ACK contract, and a virtio-blk request has no
// way to ask for it in any case — see the package doc.
func (d *Device) WriteAt(p []byte, off int64) (int, error) {
	if err := d.inRange(len(p), off, "WRITE"); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		// A zero-length WRITE is not a record. Appending one would consume a sequence
		// and a header to describe nothing, and replay would have to carry it forever.
		return 0, nil
	}
	if _, err := d.log.Write(uint64(off), p, 0); err != nil {
		return 0, d.refuse("WRITE", len(p), off, err)
	}
	// A WRITE the log took is the one piece of evidence the read view's bound can
	// produce; see tookAnAppend.
	d.tookAnAppend(true)
	return len(p), nil
}

// Discard implements vhost.Backend: the guest's VIRTIO_BLK_T_DISCARD, and what an
// `fstrim` or a `mount -o discard` produces. The range leaves the read view and is not
// published at stop, which is how the storage converges to the working set (§14.6)
// rather than growing monotonically with everything the guest ever deleted.
//
// What it does NOT do is give the local byte back. The record is appended like any
// other — a discard consumes a sequence and a header — so the WAL grows slightly while
// the object store shrinks. That asymmetry is the honest V1 shape: there is no
// mid-session reclaim of local space (§5.7), and a discard that pretended otherwise
// would be a guest's answer to backpressure that does not work.
func (d *Device) Discard(off, length int64) error {
	return d.clear("DISCARD", off, length, d.log.Discard)
}

// WriteZeroes implements vhost.Backend: VIRTIO_BLK_T_WRITE_ZEROES.
//
// unmap is accepted and deliberately not branched on. The guest's may_unmap flag
// permits releasing the range rather than requiring it, and this device always
// releases it, so both readings produce the observable the guest is entitled to — the
// range reads back as zeros. Branching would mean keeping a range of explicit zero
// bytes in the WAL and in the published image to preserve an allocation that this
// design does not track in the first place; the guest cannot observe the difference,
// and the bytes would be real.
func (d *Device) WriteZeroes(off, length int64, _ bool) error {
	return d.clear("WRITE_ZEROES", off, length, d.log.WriteZeroes)
}

// clear is the shared body of Discard and WriteZeroes: the same range check, the same
// refusal classification, and a different record type.
func (d *Device) clear(op string, off, length int64, append func(uint64, uint32) (uint64, error)) error {
	if err := d.inRange(int(length), off, op); err != nil {
		return err
	}
	if length == 0 {
		// Not a record, for the same reason a zero-length WRITE is not one: a
		// sequence and a header spent describing nothing, which replay would then
		// carry forever.
		return nil
	}
	if _, err := append(uint64(off), uint32(length)); err != nil {
		return d.refuse(op, int(length), off, err)
	}
	// growsView is false: a clear is exempt from the read view's bound (that exemption is
	// what lets a guest already over it escape), so its success says nothing about
	// whether the view came back under. It does say the device took bytes.
	d.tookAnAppend(false)
	return nil
}

// Flush implements vhost.Backend, and it is the guest's VIRTIO_BLK_T_FLUSH. It returns
// after one fdatasync of the local WAL segments and nothing else (§14.8, ADR-0026) —
// see the package doc for what that does and does not promise the guest.
//
// A failure is returned, never swallowed. The guest completes the request with IOERR
// and knows its writes are not safe; a Flush that reported success on a durable step
// that did not happen would be the one lie this whole design exists to prevent.
func (d *Device) Flush(ctx context.Context) error {
	if err := d.log.Flush(ctx); err != nil {
		return d.refuse("FLUSH", 0, 0, err)
	}
	return nil
}

// inRange refuses anything that does not fall wholly inside the device. The
// arithmetic avoids the overflow it is checking for: off+n can wrap, and a wrapped
// offset is not a large request, it is a request for somebody else's data.
func (d *Device) inRange(n int, off int64, op string) error {
	if off < 0 || n < 0 || off > d.cap || int64(n) > d.cap-off {
		return fmt.Errorf("blockdev: %s of %d bytes at %d is outside the %d-byte device: %w",
			op, n, off, d.cap, vhost.ErrOutOfRange)
	}
	return nil
}

// refuse turns a wal error into the one the guest's request fails with. All of them
// complete as VIRTIO_BLK_S_IOERR — the wire has nothing finer — so what this adds is
// for the host side: a sentinel the Agent can branch on and a sentence an operator can
// act on. See the package doc for why the three cases are not the same failure.
func (d *Device) refuse(op string, n int, off int64, err error) error {
	where := fmt.Sprintf("%s of %d bytes at %d", op, n, off)
	if op == "FLUSH" {
		where = "FLUSH"
	}
	switch {
	case errors.Is(err, wal.ErrLogBroken):
		// Not a loss of authority — nothing took the volume away. This log cannot say
		// what its tail holds after a failed rollback, so it will not confirm anything
		// against it. The lease-fencing case that used to be here went with the
		// lease-gated ACK (ADR-0026).
		return fmt.Errorf("blockdev: %s refused: this volume's log cannot describe its own tail: %w", where, err)
	case errors.Is(err, wal.ErrViewBound):
		// A different bound with the opposite remedy, and it must not inherit the
		// sentence below. This one is memory — the read view's live extents — and a
		// DISCARD gives it back while the volume keeps running, which is exactly what
		// the guest that hit it should do and what integration/vhost proves a real
		// kernel can. Telling that operator to stop and republish would cost them a
		// session's downtime for a condition an fstrim clears.
		d.latch(ReasonViewMemory, "this volume's read view is at its memory bound: a DISCARD "+
			"(fstrim, or mount -o discard) from inside the guest gives it back without stopping anything")
		return fmt.Errorf("blockdev: %s refused: this volume's read view is at its memory bound: "+
			"the guest can free it without stopping — DISCARD (fstrim, or mount -o discard) is the only "+
			"thing that shrinks a read view, and it is deliberately never refused by this bound: %w", where, err)
	case errors.Is(err, wal.ErrBackpressure):
		// The sentence used to end "a successful FLUSH clears it", and it named the
		// one remedy that cannot work. A FLUSH clears MaxUnflushedBytes/Age, which the
		// Agent does not set: agent.Budget.Limits sets MaxLocalBytes alone — the
		// volume's share of the device — and nothing clears *that* during a session,
		// because V1 has no mid-session reclaim (§5.7, ADR-0013 §1). What gives the
		// space back is stopping the volume, which publishes its image and drops the
		// local WAL. An operator told to FLUSH watches the writes keep failing.
		d.latch(ReasonWALShare, "this volume has written its whole share of the local device: "+
			"nothing reclaims it while the volume runs — stop the volume, which publishes its image and reclaims the WAL")
		return fmt.Errorf("blockdev: %s refused: this volume has written its whole share of the local device (§5.7): "+
			"a FLUSH does not clear this bound and nothing else does while the volume runs — "+
			"stop the volume, which publishes its image and reclaims the WAL: %w", where, err)
	}
	// The device state is read *after* the failed append, never before it: the
	// out-of-space latch is cleared only by an append the device took (see
	// wal.Degraded), so a caller that pre-checked it would refuse every write from the
	// first ENOSPC onwards and the volume would never come back.
	if d.log.Degraded() == wal.DegradedOutOfSpace {
		d.latch(ReasonDeviceENOSPC, "the device under this volume is out of space, below the share "+
			"this volume was bounded by: truncate after a checkpoint, grow the device, or restore the object store")
		return fmt.Errorf("blockdev: %s refused: %w — truncate after a checkpoint, grow the device, or restore the object store: %w",
			where, ErrDeviceFull, err)
	}
	return fmt.Errorf("blockdev: %s failed: %w", where, err)
}

// latch records a refusal for want of space. It keeps the first of a kind — the first is
// the transition and the rest are the same fact repeated once per guest request, so the
// sentence a reporter prints stays still under a storm — and it lets a higher-ranked
// reason displace a lower one, which is the case where keeping the first would be wrong.
// See Reason.rank.
func (d *Device) latch(reason Reason, remedy string) {
	next := Refusal{Reason: reason, Remedy: remedy}
	for {
		cur := d.refusal.Load()
		if cur != nil && next.Reason.rank() <= cur.Reason.rank() {
			return
		}
		if d.refusal.CompareAndSwap(cur, &next) {
			return
		}
	}
}

// tookAnAppend ends a latched refusal when the append that just succeeded is evidence
// that its condition ended. Called on the success path of every request that appends, so
// the common cost is one atomic load of a nil pointer.
//
// Each reason gets the evidence it can actually produce, and only that:
//
//   - ReasonViewMemory — grewView. wal charges a WRITE against the read view's bound
//     before appending, and only records that grow the view are charged, so a WRITE the
//     log took *is* the statement "the view is inside its ceiling". On this path only a
//     DISCARD or WRITE_ZEROES can have made that true, which is precisely the recovery a
//     real guest performs (integration/vhost's walking hold run). The clear itself is not
//     the evidence: clears are exempt from the bound, so one succeeds just as readily
//     from over it as from under it.
//
//   - ReasonDeviceENOSPC — wal.Degraded, asked rather than inferred. It is a live latch
//     over what the device just did, set by an append the device refused and cleared by
//     one it took, so it already answers this exact question and answers it for both
//     kinds of append.
//
//   - ReasonWALShare — nothing, while this Device exists. No mid-session reclaim exists
//     (§5.7): what gives the share back is stopping the volume, which publishes its image
//     and drops the WAL, and the volume that comes back has a new Log and a new Device.
//     So an accepted append after this bound fires — a small record squeezing into the
//     gap the refused one did not fit — is not recovery, and treating it as recovery
//     would tell the operator to stand down from the one condition that needs them.
func (d *Device) tookAnAppend(grewView bool) {
	cur := d.refusal.Load()
	if cur == nil {
		return
	}
	switch cur.Reason {
	case ReasonViewMemory:
		if !grewView {
			return
		}
	case ReasonDeviceENOSPC:
		if d.log.Degraded() == wal.DegradedOutOfSpace {
			return
		}
	default: // ReasonWALShare
		return
	}
	d.refusal.CompareAndSwap(cur, nil)
}

// RefusedForSpace reports whether this device is turning guest requests away for want of
// space, and which bound is doing it. It is the host's only view of a condition the guest
// experiences as EIO and a failed fsync.
//
// It clears when the condition does, per reason — see tookAnAppend for what counts as
// evidence for each, and why ReasonWALShare has none.
func (d *Device) RefusedForSpace() (Refusal, bool) {
	if r := d.refusal.Load(); r != nil {
		return *r, true
	}
	return Refusal{}, false
}

// blockdev.Device is the Backend a real guest is served from. hostio.RawFile still
// exists and is not what it replaced: it is the file-backed Backend the QEMU lane
// serves, so that a real kernel can be driven against the transport with no WAL
// underneath it.
var _ vhost.Backend = (*Device)(nil)
