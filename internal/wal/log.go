package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrBackpressure is returned when a WRITE would exceed the unflushed limits
// (§5.7): the guest gets an explicit error rather than the host silently filling
// NVMe.
var ErrLogBroken = errors.New("wal: the log's tail is unknown after a failed rollback")

var ErrBackpressure = errors.New("wal: backpressure (unflushed limit reached)")

// ErrWatermarkOrder is returned by an attempt to advance a watermark past a higher
// one, violating published <= durable <= local (§5.6).
var ErrWatermarkOrder = errors.New("wal: watermark ordering violation")

// ErrTruncateAboveDurable is returned by an attempt to truncate local WAL above the
// verified published point (§21.1, INV-13) — that would discard records not yet in a
// verified checkpoint.
var ErrTruncateAboveDurable = errors.New("wal: truncate above verified published point")

// ErrDirtyLog is returned by the first append to a log built over a WAL directory that
// already holds segments. Such a log did not replay them, so it does not know where
// the sequence space ends: it would re-issue sequences that are already on disk and
// (for an encrypted volume) already used as GCM nonces for this (volume, epoch).
var ErrDirtyLog = errors.New("wal: the WAL directory already holds segments this log did not replay")

// ErrFUAOnWrite is returned when a WRITE carries FlagFUA. A FUA write carries the
// FLUSH ACK contract (§14.3.1, §14.8) — fdatasync, verified PUT, valid lease — and
// Write implements none of it. Accepting the flag and appending anyway is worse than
// refusing it: it reads as "FUA is implemented" while the guest's write lives in the
// host page cache.
var ErrFUAOnWrite = errors.New("wal: Write does not implement the FUA durability contract")

// Watermarks are the three sequence watermarks (§5.6). In Phase 04 only Local
// advances (durable/published need remote durability, Phase 06+); the ordering
// invariant is enforced here regardless.
type Watermarks struct {
	Local     uint64
	Durable   uint64
	Published uint64
}

// Limits bound the local WAL (§5.7).
type Limits struct {
	// MaxUnflushedBytes/MaxUnflushedAge bound what fdatasync has not seen; both are
	// cleared by Sync.
	MaxUnflushedBytes int64
	MaxUnflushedAge   time.Duration
	// MaxLocalBytes bounds what this log may occupy on the device: every retained
	// segment, header included, minus what truncation has unlinked. Crossing it fails
	// the WRITE with ErrBackpressure — an error a guest understands — instead of
	// letting the device reach ENOSPC, which arrives as a partial append and after
	// which every WRITE fails with an I/O error nothing can act on. 0 means unbounded,
	// which is what a log with no device behind it (a unit test) wants; the Agent
	// always sets it, because it is the volume's share of the device budget
	// (agent.Budget, ADR-0013 §1 as amended 2026-08-03).
	//
	// It is a *second* bound and not a replacement for MaxUnflushedBytes, because the
	// two measure different things and only this one measures the device. Sync clears
	// the unflushed count on every guest fsync, so under any workload that fsyncs —
	// which is every workload that cares about its data — MaxUnflushedBytes is at zero
	// while the segments keep growing. It bounds a burst; it cannot bound a session,
	// and under ADR-0026 a session's whole WAL stays local until the volume stops.
	//
	// Nothing clears this one during a session, and that is the honest shape of V1: no
	// mid-session reclaim exists, so a volume that has written its share is a volume
	// that stays in backpressure until it stops and publishes. The alternative — let
	// it keep writing and take the device down for every other volume on the host — is
	// the failure ADR-0013 §1 exists for.
	MaxLocalBytes int64
	// SegmentBytes is the size at which a WAL segment is sealed and the next one
	// started. 0 means SegmentBytes, the 32 MiB default.
	//
	// It is configurable because ADR-0013 has it derived from the volume's share of the
	// device budget — agent.Budget.Limits does that now, an eighth of the share — and
	// because a test that has to write 32 MiB to cross one boundary tests the same code
	// more slowly. It is not a correctness knob: nothing below depends on the value,
	// only on it being the same for the life of a log.
	SegmentBytes int64
}

// Log is the append-only local WAL for one volume, with a read view over the
// not-yet-objectized extents. It never issues an object-store PUT on a normal
// WRITE (§5.3, INV-18) — it has no object store at all; durability is a later
// phase's concern.
//
// # Concurrency
//
// A Log is safe for concurrent use. It has to be: the guest's virtqueue loop writes
// and reads while the Agent's reconciliation flushes, checkpoints and truncates, and
// those are different goroutines. The lock lives here, on the type that owns the
// invariants, rather than in a rule the Agent has to remember.
//
// Two mutexes, and the split is the whole design:
//
//   - mu guards every field. It is held only for local work — appends, watermark
//     arithmetic, the read view, the segment set — and is **never held across an
//     object-store PUT**. That is not a performance preference: holding it across the
//     upload would put S3 latency in the guest's WRITE path (§5.3, INV-18) through
//     the back door, and would stop the Agent reading the watermarks during an S3
//     stall — exactly when the backlog they report is the RPO that is growing.
//   - flushMu serializes durable steps (Flush, WriteFUA). Releasing mu around the
//     upload means two flushes could otherwise read the same pending batches and each
//     account for having drained them; the pending list is consumed by position, so a
//     double removal discards records that exist on this host alone. Idempotent PUT
//     (§14.5) makes the duplicate upload harmless in the store, not the double
//     removal harmless in the batcher.
//
// Lock order is flushMu then mu, never the reverse. A method that takes mu must not
// call one that takes flushMu.
type Log struct {
	// mu guards every field below; flushMu serializes durable steps. See the type
	// comment for why they are separate and what mu may not be held across.
	mu      sync.Mutex
	flushMu sync.Mutex

	// broken is set when a failed rollback leaves the tail unknown: the log cannot
	// describe what it holds, so it must not ACK anything against it.
	//
	// It used to be `fenced`, and it meant two things — this, and "a durable step found
	// the lease invalid" (§16 SELF_FENCED). The second went with the lease-gated ACK
	// (ADR-0026): V1 ACKs on fdatasync and nothing consults a lease on this path. One
	// flag for one meaning is what stops a reader inferring the other.
	broken bool

	segs     *segments
	clk      clock.Clock
	volumeID [16]byte
	epoch    uint64

	start     uint64 // the sequence this log continues from (§12.5 boundary)
	local     uint64
	durable   uint64
	published uint64
	replayed  bool // this log rebuilt itself from the WAL file's contents

	// resumeTail holds the replayed records no verified object covers yet; they go
	// to the batcher as soon as EnableRemote provides one.
	resumeTail []resumedRecord

	view   *cow.IntervalMap
	limits Limits
	enc    *Encryption // nil = plaintext WAL

	// degraded is what the local device is refusing to do, independently of the
	// lease (see Degraded). outOfSpace classifies a disk error as ENOSPC.
	degraded   Degradation
	outOfSpace OutOfSpaceFunc

	// order decides the watermark and truncation rules (INV-03, INV-13). Nil is
	// strict; see OrderPolicy for why the rules sit behind an interface.
	order OrderPolicy

	// baseWait is non-nil on a log resumed with ResumeAwaitingBase: the read view's
	// base — everything up to the durable point, recovered from the object store — is
	// being fetched, and until it arrives this log's view holds only what the local
	// segments still had. Reads wait on it rather than answering out of a half-built
	// view, because the wrong answer is *zeros*, indistinguishable from a range nobody
	// wrote (BUILD-INVENTORY increment 5). Closed exactly once, by InstallBase or
	// FailBase; baseErr is set by the latter.
	baseWait chan struct{}
	baseErr  error

	unflushedBytes    int64
	oldestUnflushedAt clock.Instant
	hasUnflushed      bool

	discardedBytes int64
	truncatedUpTo  uint64 // local WAL discarded up to this sequence (§14.7)
	reclaimedBytes int64  // bytes given back to the device by unlinked segments

	rec      *obs.Recorder // nil = telemetry not wired (no-op)
	volLabel string
}

// TruncatedUpTo reports the sequence below which local WAL has been reclaimed.
func (l *Log) TruncatedUpTo() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncatedUpTo
}

// ReclaimedBytes reports the local bytes truncation has given back over this log's
// life. It is the number ADR-0013's device pressure is measured against, and the one
// the pre-segment WAL could not produce: it truncated the file only when the
// checkpoint had reached the very end of the log, so on a volume under continuous
// write it reclaimed nothing, ever.
func (l *Log) ReclaimedBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reclaimedBytes
}

// LocalBytes reports what this log occupies on the device right now, across every
// retained segment.
func (l *Log) LocalBytes() (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segs.bytes()
}

// SegmentNames returns the disk names of the retained segments, oldest first.
func (l *Log) SegmentNames() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segs.names()
}

// TruncateLocal reclaims local WAL up to and including upTo by unlinking the segments
// whose records are all at or below it. It refuses to truncate above the verified
// published point (§21.1, INV-13): records not yet in a published, verified checkpoint
// must never be discarded.
//
// The segment holding upTo is kept whole, so the floor of what a checkpoint can give
// back is one segment. Unlinking is the last step and runs oldest-first: the published
// watermark advanced before this was called, so a crash part-way through leaves
// segments a later truncation removes, where the reverse order would remove data the
// published point does not yet cover.
func (l *Log) TruncateLocal(upTo uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.orderPolicy().AllowTruncate(upTo, l.watermarksLocked()); err != nil {
		return err
	}
	l.truncatedUpTo = upTo
	freed, err := l.segs.reclaim(upTo)
	l.reclaimedBytes += freed
	return err
}

// SetRecorder wires the §26.2 metrics this log owns: the watermarks, the unflushed
// backlog, and the self-fencing counter. A nil recorder is a no-op, so the DST
// harness and unit tests run without an exporter (DEV-0010).
func (l *Log) SetRecorder(r *obs.Recorder, volumeLabel string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rec = r
	l.volLabel = volumeLabel
}

// recordWatermarks publishes the watermark trio and the unflushed backlog. These are
// the numbers an operator reads as the volume's RPO (§26.2), so they are recorded
// where they change rather than sampled by a background poller.
func (l *Log) recordWatermarks(ctx context.Context) {
	if l.rec == nil {
		return
	}
	vol := obs.String("volume", l.volLabel)
	l.rec.Gauge(ctx, "wal_local_sequence", float64(l.local), vol)
	l.rec.Gauge(ctx, "wal_durable_sequence", float64(l.durable), vol)
	// No wal_published_sequence: nothing publishes in V1 (ADR-0026), so it would be a
	// series permanently at 0 — and a gauge that can only read 0 is one an operator has
	// to learn to ignore. It returns with whatever advances the published point.
	l.rec.Gauge(ctx, "wal_unflushed_bytes", float64(l.unflushedBytes), vol)
	// The gap is what S3 cannot reproduce, not what fdatasync has not seen. A
	// `local` volume ACKs on fdatasync, so its unflushed count is 0 while its RPO
	// exposure is the whole backlog — reporting the former as the latter is the
	// operator's only RPO signal telling them the opposite of the truth.
	// Republished here so the series exists for a healthy volume too: an alert on
	// "the device is full" cannot fire on a metric that only appears once it is.
	l.recordDegraded(ctx)
}

// Broken reports whether a failed rollback left this log's tail unknown.
//
// It has no production caller. What it exists for is proof — the tests that plant a
// rollback failure read the transition through it rather than a flag they set themselves.
func (l *Log) Broken() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.broken
}

// EnableEncryption binds an Encryption context so subsequent WRITEs seal their
// payloads (§15). Must be set before the first WRITE.
func (l *Log) EnableEncryption(e *Encryption) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enc = e
}

// NewLog creates a log over the WAL directory <root>/<volume-id>/<epoch>, timed by
// clk. The directory is not touched until the first record: an attached volume that
// never writes leaves nothing behind.
func NewLog(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch uint64, limits Limits) *Log {
	return NewLogAfter(d, root, clk, volumeID, epoch, 0, limits)
}

// NewLogAfter creates a log whose first record continues the volume's sequence space
// after `boundary` — what a promoted writer does in epoch N+1 (§12.5). Sequences
// belong to the volume, not to the epoch: a log that restarted at 1 would write
// records that collide with the previous epoch's, and recovery, which chains the
// epochs together, would see two different records claiming the same sequence.
func NewLogAfter(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch, boundary uint64, limits Limits) *Log {
	return &Log{
		segs:       newSegments(d, root, clk, volumeID, epoch, limits.SegmentBytes),
		clk:        clk,
		volumeID:   volumeID,
		epoch:      epoch,
		start:      boundary,
		local:      boundary,
		durable:    boundary,
		view:       cow.NewIntervalMap(),
		limits:     limits,
		degraded:   DegradedNone,
		outOfSpace: DefaultOutOfSpace,
		order:      StrictOrder{},
	}
}

func (l *Log) backpressure(add int) error {
	// The device bound first, because it is the one whose breach costs the whole host:
	// a log at its share refuses one volume's WRITE, a device at ENOSPC refuses every
	// volume's. Measured against the retained segments, not against what this log has
	// ever appended — truncation gives bytes back, and a bound that ignored that would
	// throttle a volume whose data is no longer on the device.
	//
	// A segment header is charged on every WRITE although only a rotation writes one.
	// It costs a fixed 64 bytes of the share and never accumulates, where counting
	// headers only when they are written would mean the record that rotates a segment
	// is the one record the bound does not cover — which is precisely the record that
	// makes the log exceed it.
	if l.limits.MaxLocalBytes > 0 &&
		l.segs.retained()+int64(add)+int64(format.SegmentHeaderSize) > l.limits.MaxLocalBytes {
		return ErrBackpressure
	}
	if l.limits.MaxUnflushedBytes > 0 && l.unflushedBytes+int64(add) > l.limits.MaxUnflushedBytes {
		return ErrBackpressure
	}
	if l.hasUnflushed && l.limits.MaxUnflushedAge > 0 &&
		l.clk.Now().Sub(l.oldestUnflushedAt) > l.limits.MaxUnflushedAge {
		return ErrBackpressure
	}
	return nil
}

func (l *Log) appendEncoded(seq uint64, enc []byte, addView func()) (uint64, error) {
	if err := l.backpressure(len(enc)); err != nil {
		return 0, err
	}
	// Nothing has been appended through this log yet, but the WAL directory is not
	// empty: this log was built over segments whose records it never read (an agent
	// restart re-attaching at the same epoch, §16). Its sequence counter starts at the
	// boundary it was handed, so the next append would re-issue sequences that are
	// already on disk — duplicate sequences in the same (volume, epoch), reused GCM
	// nonces (§15.2), and two objects claiming one span (INV-21). Fail at the first
	// append rather than produce them.
	if !l.replayed && l.local == l.start && l.segs.empty() {
		existing, err := listSegments(l.segs.d, l.segs.dir)
		if err != nil {
			return 0, err
		}
		if len(existing) > 0 {
			return 0, fmt.Errorf("%w: %d segment(s) under %s, resuming at sequence %d",
				ErrDirtyLog, len(existing), l.segs.dir, l.start)
		}
	}
	// A failed append is not necessarily an append of nothing: a partial write
	// (ENOSPC, a torn write at a device boundary) leaves bytes that are not a record.
	// The segment is rolled back to its last intact record before the failure is
	// reported — otherwise the rejected write either replays as a record the guest was
	// told failed (with a sequence the next accepted write reuses) or truncates the
	// segment, making replay stop at the tear and silently drop everything after it.
	err, rollbackErr := l.segs.appendRecord(seq, enc)
	// Whether the device took the bytes is the only evidence there is about its
	// state, so it is read here, on the accepted path as well as the refused one: a
	// full device stays full until an append proves otherwise (see Degraded).
	l.noteAppendResult(err)
	if err != nil {
		if rollbackErr != nil {
			// The log's tail is now unknown. Refuse to serve it rather than ACK
			// anything against a segment we cannot describe.
			l.broken = true
			return 0, errors.Join(err, fmt.Errorf("wal: could not roll back a partial append: %w", rollbackErr))
		}
		return 0, err
	}
	l.local = seq
	addView()
	l.trackUnflushed(len(enc))
	return seq, nil
}

// Write appends a WRITE of data at offset and returns its sequence. It completes on
// local append; it does not sync or PUT (§5.3). When encryption is enabled the WAL
// bytes are ciphertext, but the read view keeps plaintext (it never leaves the host).
func (l *Log) Write(offset uint64, data []byte, flags uint32) (uint64, error) {
	if flags&format.FlagFUA != 0 {
		return 0, ErrFUAOnWrite
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.write(offset, data, flags)
}

func (l *Log) write(offset uint64, data []byte, flags uint32) (uint64, error) {
	seq := l.local + 1
	var (
		enc []byte
		err error
	)
	if l.enc != nil {
		enc, err = l.enc.encodeWrite(l.epoch, seq, offset, flags, data)
	} else {
		r := Record{Type: format.RecordWrite, VolumeID: l.volumeID, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: uint32(len(data)), Flags: flags, Payload: data}
		enc, err = r.Encode()
	}
	if err != nil {
		return 0, err
	}
	// The read view holds plaintext regardless of on-disk encryption.
	plaintext := append([]byte(nil), data...)
	got, err := l.appendEncoded(seq, enc, func() { l.view.Overwrite(offset, plaintext) })
	if err != nil {
		return 0, err
	}
	return got, nil
}

// Discard appends a DISCARD of [offset, offset+length); the range reads as zero.
func (l *Log) Discard(offset uint64, length uint32) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendClear(format.RecordDiscard, offset, length)
}

// WriteZeroes appends a WRITE_ZEROES of [offset, offset+length); reads as zero.
func (l *Log) WriteZeroes(offset uint64, length uint32) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appendClear(format.RecordWriteZeroes, offset, length)
}

// appendClear appends a header-only DISCARD/WRITE_ZEROES record: it clears the read
// view, feeds the remote batcher (these records must reach S3 so the working set
// converges, §14.6), and counts the reclaimed bytes.
func (l *Log) appendClear(t format.RecordType, offset uint64, length uint32) (uint64, error) {
	seq := l.local + 1
	enc, err := Record{Type: t, VolumeID: l.volumeID, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: length}.Encode()
	if err != nil {
		return 0, err
	}
	got, err := l.appendEncoded(seq, enc, func() { l.view.Clear(offset, uint64(length)) })
	if err != nil {
		return 0, err
	}
	l.discardedBytes += int64(length)
	return got, nil
}

// DiscardedBytes reports the cumulative bytes DISCARDed/zeroed (discarded_bytes_total).
func (l *Log) DiscardedBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.discardedBytes
}

// Read fills buf from the read view starting at offset (zero where unwritten). It
// takes the same lock the write path does: the interval map is one structure, and a
// read racing an Overwrite would see a partly-updated extent, not an older one.
// Read fills buf from the volume's read view. Ranges nothing has written read as zero.
//
// On a log resumed with ResumeAwaitingBase it blocks until the base has been installed
// or the attempt has failed, and returns ErrBaseUnavailable in the latter case. That is
// the whole point of the base: a read answered before it arrives would return zeros for
// every range whose local segments truncation reclaimed, and a guest cannot tell those
// zeros from a range it never wrote. Whoever resumes the log owes it exactly one call to
// InstallBase or FailBase — a log that gets neither leaves its reads waiting forever.
func (l *Log) Read(offset uint64, buf []byte) error {
	l.mu.Lock()
	wait := l.baseWait
	l.mu.Unlock()

	if wait != nil {
		<-wait
		l.mu.Lock()
		err := l.baseErr
		l.mu.Unlock()
		if err != nil {
			return fmt.Errorf("wal: %w: %w", ErrBaseUnavailable, err)
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.view.Read(offset, buf)
	return nil
}

// InstallBase adopts the read view recovered from the object store, under everything the
// local segments replayed. It is the seam BUILD-INVENTORY increment 5 exists to add:
// recovery.Recover and materialize.From* have always produced exactly this object and
// nothing could consume it.
// InstallBase adopts the read view recovered from the object store, under everything the
// local segments replayed, and with it the durable sequence that view covers.
//
// The watermarks it sets are the point of taking `durable` here rather than at resume:
//
//   - durable and published both become the recovered point. Those objects are verified
//     — that is what made truncating the local WAL legal — so published must say so, or
//     StrictOrder.AllowTruncate (INV-13) refuses to reclaim ranges the store already
//     holds and the first checkpoint after a restart republishes finished work.
//   - local is raised to at least that point. Not bookkeeping: the next append must not
//     reuse a sequence an object already carries, which is exactly what would happen on
//     a volume whose local segments were all reclaimed and whose replay therefore found
//     nothing.
func (l *Log) InstallBase(base *cow.IntervalMap, durable uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.baseWait == nil {
		return errors.New("wal: this log was not resumed awaiting a base")
	}
	select {
	case <-l.baseWait:
		return errors.New("wal: the base has already been resolved")
	default:
	}
	if err := l.view.SetBase(base); err != nil {
		return fmt.Errorf("wal: installing the base: %w", err)
	}
	if durable > l.local {
		l.local = durable
		l.start = durable
	}
	if durable > l.durable {
		l.durable = durable
	}
	if durable > l.published {
		l.published = durable
	}
	close(l.baseWait)
	return nil
}

// FailBase records that the base could not be recovered. Every subsequent read fails
// with ErrBaseUnavailable rather than answering zeros — the volume refuses to serve
// rather than quietly serving a hole where its data is.
func (l *Log) FailBase(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.baseWait == nil {
		return
	}
	select {
	case <-l.baseWait:
		return
	default:
	}
	l.baseErr = err
	close(l.baseWait)
}

// Sync makes prior appends durable locally (fdatasync) and clears the unflushed
// accounting. It does not advance the durable watermark — that requires remote
// durability via Flush.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.segs.sync(); err != nil {
		return err
	}
	l.clearUnflushed()
	return nil
}

// Close releases the segment this log holds open. It is not a durability step — Sync
// and Flush are — and it seals nothing: a log that is closed and rebuilt goes through
// Resume, which reopens the newest segment for append.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A log resumed awaiting a base may have reads parked on it. Resolving the base as
	// failed is what lets them leave: without it a shutdown that races the recovery
	// fetch strands every parked read for the life of the process, and the goroutine
	// that owed the log an InstallBase may itself be gone.
	if l.baseWait != nil {
		select {
		case <-l.baseWait:
		default:
			l.baseErr = errors.New("the log was closed before its base arrived")
			close(l.baseWait)
		}
	}
	return l.segs.close()
}

// Seal makes the newest segment durable and closes it for append; the next record
// starts a new file. It writes nothing to the segment being sealed — that is what
// keeps a sealed segment immutable and a crash mid-seal uninteresting — and it is a
// no-op when nothing is open.
//
// The data path seals through AdvancePublished, at the moment reclamation becomes
// possible, and through the size rotation inside the append path. This exposes the
// same act on its own so a caller can rotate deliberately.
func (l *Log) Seal() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segs.seal()
}

// Flush makes a FLUSH/FUA durable and ACKs it, following §14.4. Order: capture
// target, close the batch, fdatasync, then per durability mode (§14.8):
//
//   - remote (default): upload + verify every covering object, VERIFY the lease is
//     still valid on the monotonic clock, then advance durable_sequence and ACK.
//     durable advances ONLY after S3 verification (INV-07); and the ACK happens ONLY
//     while the lease is valid (§12.2, INV-06) — a PUT that landed in S3 after the
//     lease expired is NOT confirmed, and the log self-fences.
//   - local: ACK after the local fdatasync; the lease does not gate the FLUSH ACK
//     and S3 is asynchronous (§14.8 rule 3).
//
// A failed upload retains the un-uploaded batches for the next Flush and does not
// advance durable.
func (l *Log) Flush(ctx context.Context) error {
	l.mu.Lock()
	target := l.local
	l.mu.Unlock()
	return l.durableStep(ctx, target)
}

// durableStep is the ACK path shared by FLUSH and FUA — they carry the same contract, so
// they must not have two implementations of it.
//
// **One contract (§14.8, ADR-0026): fdatasync, then ACK.** No upload, no lease check. A
// FLUSH is durable against this process, this Agent and QEMU dying; it is not durable
// against the host dying, and §2 says so rather than paying for the difference on every
// commit. The volume reaches the object store when it stops (agent.Volume.publish).
//
// What was here was §14.4's six steps — close the batch, fdatasync, upload every covering
// object, verify the lease on the monotonic clock, advance durable_sequence, ACK — plus a
// `local` mode that skipped four of them. Both went with the remote durability chain.
func (l *Log) durableStep(ctx context.Context, target uint64) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken {
		return ErrLogBroken
	}
	if err := l.segs.sync(); err != nil {
		return err
	}
	if err := l.advanceDurableLocked(target); err != nil {
		return err
	}
	l.clearUnflushed()
	l.recordWatermarks(ctx)
	return nil
}

func (l *Log) clearUnflushed() {
	l.unflushedBytes = 0
	l.hasUnflushed = false
}

func (l *Log) trackUnflushed(n int) {
	if !l.hasUnflushed {
		l.oldestUnflushedAt = l.clk.Now()
		l.hasUnflushed = true
	}
	l.unflushedBytes += int64(n)
}

// Watermarks returns the current watermarks. The trio is read under one lock so a
// caller cannot observe a torn set — published above durable, say — that no single
// moment of this log ever had (INV-03).
func (l *Log) Watermarks() Watermarks {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watermarksLocked()
}

func (l *Log) watermarksLocked() Watermarks {
	return Watermarks{Local: l.local, Durable: l.durable, Published: l.published}
}

// UnflushedBytes reports the bytes appended since the last Sync.
func (l *Log) UnflushedBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.unflushedBytes
}

// Freeze seals the current read view under a sequence number and hands it back, leaving
// the volume writing into a fresh layer over it. It is §19's snapshot, and §19 is what
// makes it possible:
//
//  1. capture N = local_sequence under the volume's lock (µs)
//  2. keep accepting writes normally (sequences > N)
//     3b. seal a CoW view of the active map at sequence N
//
// The seal is one pointer. cow.NewIntervalMapOver layers a new map over the old one and
// never writes through to it — "the base is read, never written" — so the map handed back
// is immutable from this moment by construction rather than by a rule somebody has to
// remember. That is where §2's "pausa de I/O por snapshot ~0" comes from: there is no
// queue to drain and no quiesce, only a swap.
//
// The fdatasync comes first because the frozen view describes records the local WAL must
// still hold if this process dies before the copy is uploaded.
//
// The caller owns the returned view and must not mutate it; PublishSnapshot only reads.
func (l *Log) Freeze() (*cow.IntervalMap, uint64, error) {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.broken {
		return nil, 0, ErrLogBroken
	}
	if err := l.segs.sync(); err != nil {
		return nil, 0, err
	}
	frozen := l.view
	l.view = cow.NewIntervalMapOver(frozen)
	return frozen, l.local, nil
}

func (l *Log) ViewAtRest() (*cow.IntervalMap, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.view, l.local
}

// AdvanceDurable advances the durable watermark, enforcing durable <= local (§5.6).
func (l *Log) AdvanceDurable(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.advanceDurableLocked(seq)
}

func (l *Log) advanceDurableLocked(seq uint64) error {
	if err := l.orderPolicy().AllowDurable(seq, l.watermarksLocked()); err != nil {
		return err
	}
	l.durable = seq
	return nil
}

// AdvancePublished advances the published watermark, enforcing published <= durable,
// and seals the open segment.
//
// Sealing here rather than on a timer is the whole of the age policy: a segment is
// only worth sealing at the moment reclamation becomes possible, and that moment is
// the checkpoint that publishes. An idle volume holds at most one partly-filled
// segment either way, and a per-volume timer would buy nothing this does not.
//
// The watermark moves first and the seal follows. A seal that fails is a local
// durability failure worth reporting, but it does not un-publish the checkpoint that
// is already in S3 — and AllowPublished refuses to move the watermark backwards, so
// pretending the publication had not happened is not available even if it were right.
func (l *Log) AdvancePublished(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.orderPolicy().AllowPublished(seq, l.watermarksLocked()); err != nil {
		return err
	}
	l.published = seq
	return l.segs.seal()
}
