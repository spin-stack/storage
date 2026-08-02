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
	// MaxRemoteGapBytes bounds the bytes that no verified object covers yet — the
	// backlog a host loss destroys. It is a separate limit because fdatasync clears
	// the two above while leaving this one untouched, so on a `local` volume (or a
	// remote one riding out an S3 outage) nothing else stops the device from filling
	// with writes no other machine has. 0 disables it.
	MaxRemoteGapBytes int64
	// SegmentBytes is the size at which a WAL segment is sealed and the next one
	// started. 0 means SegmentBytes, the 32 MiB default.
	//
	// It is configurable because ADR-0013 wants it derived from the volume's share of
	// the device budget (clamp(share/8, 8 MiB, 64 MiB)) once the Agent computes one,
	// and because a test that has to write 32 MiB to cross one boundary tests the
	// same code more slowly. It is not a correctness knob: nothing below depends on
	// the value, only on it being the same for the life of a log.
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

	batcher  *Batcher  // nil = local-only (no remote WAL)
	uploader *Uploader // nil = local-only

	mode   DurabilityMode // remote (default) | local (§14.8)
	lease  LeaseChecker   // nil = no lease gate (dev/local without a CP)
	fenced bool           // set once a FLUSH finds the lease invalid (§16 SELF_FENCED)

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

	// Remote-durability backlog: the bytes appended that no verified S3 object
	// covers yet. Deliberately separate from the unflushed accounting above, which
	// fdatasync clears: fdatasync is host durability, and the host is exactly what
	// this backlog would be lost with (§14.8 RPO).
	gapBytes       int64
	oldestGapAt    clock.Instant
	hasGap         bool
	discardedBytes int64
	truncatedUpTo  uint64          // local WAL discarded up to this sequence (§14.7)
	reclaimedBytes int64           // bytes given back to the device by unlinked segments
	uploaded       []SummaryObject // durable objects, for the summary (§22.1)

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

// SetDurabilityMode selects the FLUSH/FUA ACK contract (§14.8). Default is remote.
func (l *Log) SetDurabilityMode(m DurabilityMode) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.mode = m
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
	l.rec.Gauge(ctx, "wal_published_sequence", float64(l.published), vol)
	l.rec.Gauge(ctx, "wal_unflushed_bytes", float64(l.unflushedBytes), vol)
	// The gap is what S3 cannot reproduce, not what fdatasync has not seen. A
	// `local` volume ACKs on fdatasync, so its unflushed count is 0 while its RPO
	// exposure is the whole backlog — reporting the former as the latter is the
	// operator's only RPO signal telling them the opposite of the truth.
	l.rec.Gauge(ctx, "wal_durable_gap_bytes", float64(l.gapBytes), vol)
	l.rec.Gauge(ctx, "wal_durable_gap_seconds", l.gapAge().Seconds(), vol)
	// Republished here so the series exists for a healthy volume too: an alert on
	// "the device is full" cannot fire on a metric that only appears once it is.
	l.recordDegraded(ctx)
}

// gapAge is how long the oldest un-remote-durable record has been waiting: the
// effective RPO in seconds (§14.8, §26.2).
func (l *Log) gapAge() time.Duration {
	if !l.hasGap {
		return 0
	}
	return l.clk.Now().Sub(l.oldestGapAt)
}

// RemoteGapBytes reports the bytes appended that no verified object covers yet.
func (l *Log) RemoteGapBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gapBytes
}

// Fenced reports whether the log has self-fenced (a FLUSH found the lease invalid).
func (l *Log) Fenced() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fenced
}

// EnableEncryption binds an Encryption context so subsequent WRITEs seal their
// payloads (§15). Must be set before the first WRITE.
func (l *Log) EnableEncryption(e *Encryption) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.enc = e
	l.alignKeyID()
}

// alignKeyID makes the DEK the single source of truth for the KeyID an assembled
// object announces (§15.1). The Batcher is constructed independently of the
// Encryption context — every call site in the tree passes 0 — and an object header
// naming a key version that did not seal its records points a recovering host at the
// wrong DEK. The two cannot disagree if only one of them is authoritative.
func (l *Log) alignKeyID() {
	if l.enc != nil && l.batcher != nil {
		l.batcher.keyID = l.enc.DEK.KeyID
	}
}

// EnableRemote wires the on-demand batcher and idempotent uploader so FLUSH/FUA
// make records durable in S3 (§14.3–14.5). Must be set before the first WRITE.
// EnableRemote wires the remote path. The lease checker is a parameter rather than a
// later setter so that every call site has to answer the question "what fences this
// writer?" — nil is legal only for `local` durability (§14.8 rule 3), and a remote
// FLUSH with a nil lease fails closed with ErrNoLease (DEV-0004).
func (l *Log) EnableRemote(b *Batcher, u *Uploader, lease LeaseChecker) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batcher = b
	l.uploader = u
	if lease != nil {
		l.lease = lease
	}
	l.alignKeyID()
	// A resumed log carries the records the crash left un-uploaded (Resume). They
	// are the writes that exist on this host only, so the batcher gets them the
	// moment there is one — forgetting them here would lose every write since the
	// last successful upload, silently.
	if b != nil {
		for _, r := range l.resumeTail {
			b.Append(r.seq, r.encoded, false)
		}
		l.resumeTail = nil
	}
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
	if l.limits.MaxUnflushedBytes > 0 && l.unflushedBytes+int64(add) > l.limits.MaxUnflushedBytes {
		return ErrBackpressure
	}
	if l.hasUnflushed && l.limits.MaxUnflushedAge > 0 &&
		l.clk.Now().Sub(l.oldestUnflushedAt) > l.limits.MaxUnflushedAge {
		return ErrBackpressure
	}
	if l.limits.MaxRemoteGapBytes > 0 && l.gapBytes+int64(add) > l.limits.MaxRemoteGapBytes {
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
			l.fenced = true
			return 0, errors.Join(err, fmt.Errorf("wal: could not roll back a partial append: %w", rollbackErr))
		}
		return 0, err
	}
	l.local = seq
	addView()
	l.trackUnflushed(len(enc))
	l.trackGap(len(enc))
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

// WriteFUA appends a WRITE carrying the FUA flag and makes it durable before it
// returns, under the same ACK contract as a FLUSH (§14.3.1, §14.8): in `remote` mode
// the record is in a verified object and the lease was valid at the instant of the
// ACK, in `local` mode it is on the host's stable media. On any failure the record
// stays in the local WAL — as an un-ACKed FLUSH's records do — and the caller gets
// the error instead of a completion the guest would trust.
// The append and the durable step are two critical sections, not one: the durable
// step uploads, and mu may not be held across a PUT. The FUA record's sequence is
// captured under the first, so a write that races in between raises `local` without
// widening what this ACK confirms.
func (l *Log) WriteFUA(ctx context.Context, offset uint64, data []byte) (uint64, error) {
	l.mu.Lock()
	seq, err := l.write(offset, data, format.FlagFUA)
	l.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if err := l.durableStep(ctx, seq); err != nil {
		return 0, err
	}
	return seq, nil
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
	if l.batcher != nil {
		l.batcher.Append(seq, enc, flags&format.FlagFUA != 0)
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
	if l.batcher != nil {
		l.batcher.Append(seq, enc, false)
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

// BasePending reports whether this log is still waiting for the read view's base, and
// with it the durable sequence that base covers.
//
// It exists for the durability scheduler. A resumed log reports durable = 0 until the
// base arrives, and a checkpoint taken in that window compares the object store's real
// durable point against 0 and concludes another writer is in the epoch — which under
// ADR-0023 fences a perfectly healthy host out of its own volume, on every restart.
// A log that does not yet know its own durable point has no business publishing one.
func (l *Log) BasePending() bool {
	l.mu.Lock()
	wait := l.baseWait
	l.mu.Unlock()
	if wait == nil {
		return false
	}
	select {
	case <-wait:
		return false
	default:
		return true
	}
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

// durableStep is the §14.4 sequence shared by FLUSH and FUA — they carry the same
// ACK contract, so they must not have two implementations of it. target is the
// sequence the ACK would confirm, captured before the batch is closed.
//
// It runs under flushMu, so only one durable step is in flight at a time, and it takes
// mu in short stretches around the upload rather than across it. See the Log type
// comment: mu held across a PUT would put the object store in the guest's write path
// and blind the Agent's reporting for the length of an S3 stall.
func (l *Log) durableStep(ctx context.Context, target uint64) error {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()

	l.mu.Lock()
	if l.fenced {
		l.mu.Unlock()
		return ErrSelfFenced
	}
	if l.batcher != nil {
		l.batcher.Flush() // step 2: close current batch
	}
	if err := l.segs.sync(); err != nil { // step 3: fdatasync local
		l.mu.Unlock()
		return err
	}

	if l.mode == ModeLocal {
		// §14.8: ACK on local durability; no lease gate, no synchronous S3. The
		// remote gap is untouched on purpose — it is exactly what this ACK does not
		// cover, and it is the RPO an operator reads.
		l.clearUnflushed()
		l.recordWatermarks(ctx)
		l.mu.Unlock()
		return nil
	}

	// Remote durability with nothing able to PUT is not "nothing to upload": it is a
	// durability claim with no backing. Refuse it here rather than let step 6 move
	// durable_sequence past what S3 can produce (INV-07).
	if l.batcher == nil || l.uploader == nil {
		l.mu.Unlock()
		return ErrNoUploader
	}

	// A snapshot, because the list is about to be read without the lock and a
	// concurrent WRITE may close a further batch onto the end of it. Those extra
	// batches are not this ACK's business — they are above target — and dropping the
	// first `done` entries still drops exactly the ones uploaded here.
	pending := append([]*ClosedBatch(nil), l.batcher.Pending()...)
	uploader := l.uploader
	l.mu.Unlock()

	done := 0
	for _, cb := range pending { // step 4: upload + verify (covering <= target)
		key, err := uploader.Upload(ctx, cb) // no lock held: this is the network
		l.mu.Lock()
		if err != nil {
			l.batcher.RemoveUploaded(done)
			l.recordWatermarks(ctx) // a growing gap is what an operator needs here
			l.mu.Unlock()
			return err // durable NOT advanced
		}
		l.uploaded = append(l.uploaded, SummaryObject{Key: key, First: cb.First, Last: cb.Last})
		l.closeGap(int64(len(cb.Records)))
		l.mu.Unlock()
		done++
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.batcher.RemoveUploaded(done)

	// The log is not re-checked for self-fencing here, and that is deliberate: the
	// only path that sets it while this one holds flushMu is a failed rollback in
	// appendEncoded, which leaves the tail *above* target unknown. The records this
	// step uploaded were appended and verified before that, so confirming them is
	// true. Fencing stops the next durable step, at the top.

	// step 5: verify the lease on the monotonic clock (§12.2, INV-06). If it is not
	// valid, do NOT advance durable and do NOT ACK — self-fence. In remote mode the
	// check is mandatory: no lease checker means nothing is fencing this writer, so
	// the ACK is refused rather than granted by default.
	if l.lease == nil {
		return ErrNoLease
	}
	if !l.lease.Valid() {
		l.fenced = true
		l.rec.Count(ctx, "self_fenced_total", 1, obs.String("volume", l.volLabel))
		return ErrSelfFenced
	}
	if err := l.advanceDurableLocked(target); err != nil { // step 6
		return err
	}
	l.clearUnflushed()
	l.recordWatermarks(ctx)
	return nil // step 7: ack
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

// trackGap records bytes that no object covers yet.
func (l *Log) trackGap(n int) {
	if !l.hasGap {
		l.oldestGapAt = l.clk.Now()
		l.hasGap = true
	}
	l.gapBytes += int64(n)
}

// closeGap discounts the bytes of an object that is now verified in the store. Only
// a verified PUT closes the gap — not fdatasync, not an ACK.
func (l *Log) closeGap(n int64) {
	l.gapBytes -= n
	if l.gapBytes <= 0 {
		l.gapBytes = 0
		l.hasGap = false
	}
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

// ViewBytes reports the read-view memory (active_map_bytes proxy for Phase 04).
func (l *Log) ViewBytes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.view.Bytes()
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
