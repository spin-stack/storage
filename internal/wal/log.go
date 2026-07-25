package wal

import (
	"context"
	"errors"
	"fmt"
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
	MaxUnflushedBytes int64
	MaxUnflushedAge   time.Duration
}

// Log is the append-only local WAL for one volume, with a read view over the
// not-yet-objectized extents. It never issues an object-store PUT on a normal
// WRITE (§5.3, INV-18) — it has no object store at all; durability is a later
// phase's concern.
type Log struct {
	file     disk.File
	clk      clock.Clock
	volumeID [16]byte
	epoch    uint64

	local     uint64
	durable   uint64
	published uint64

	view   *cow.IntervalMap
	limits Limits
	enc    *Encryption // nil = plaintext WAL

	batcher  *Batcher  // nil = local-only (no remote WAL)
	uploader *Uploader // nil = local-only

	mode   DurabilityMode // remote (default) | local (§14.8)
	lease  LeaseChecker   // nil = no lease gate (dev/local without a CP)
	fenced bool           // set once a FLUSH finds the lease invalid (§16 SELF_FENCED)

	unflushedBytes    int64
	oldestUnflushedAt clock.Instant
	hasUnflushed      bool
	discardedBytes    int64
	truncatedUpTo     uint64          // local WAL discarded up to this sequence (§14.7)
	uploaded          []SummaryObject // durable objects, for the summary (§22.1)

	rec      *obs.Recorder // nil = telemetry not wired (no-op)
	volLabel string
}

// TruncatedUpTo reports the sequence below which local WAL has been reclaimed.
func (l *Log) TruncatedUpTo() uint64 { return l.truncatedUpTo }

// TruncateLocal reclaims local WAL up to and including upTo. It refuses to truncate
// above the verified published point (§21.1, INV-13): records not yet in a published,
// verified checkpoint must never be discarded. When the whole log is objectized it
// resets the local file.
func (l *Log) TruncateLocal(upTo uint64) error {
	if upTo > l.published {
		return ErrTruncateAboveDurable
	}
	l.truncatedUpTo = upTo
	if upTo >= l.local {
		if err := l.file.Truncate(0); err != nil {
			return err
		}
	}
	return nil
}

// SetDurabilityMode selects the FLUSH/FUA ACK contract (§14.8). Default is remote.
func (l *Log) SetDurabilityMode(m DurabilityMode) { l.mode = m }

// SetRecorder wires the §26.2 metrics this log owns: the watermarks, the unflushed
// backlog, and the self-fencing counter. A nil recorder is a no-op, so the DST
// harness and unit tests run without an exporter (DEV-0010).
func (l *Log) SetRecorder(r *obs.Recorder, volumeLabel string) {
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
	l.rec.Gauge(ctx, "wal_durable_gap_bytes", float64(l.unflushedBytes), vol)
}

// SetLease wires the host lease checker used by the durable-ACK rule (§12.2).
func (l *Log) SetLease(c LeaseChecker) { l.lease = c }

// Fenced reports whether the log has self-fenced (a FLUSH found the lease invalid).
func (l *Log) Fenced() bool { return l.fenced }

// EnableEncryption binds an Encryption context so subsequent WRITEs seal their
// payloads (§15). Must be set before the first WRITE.
func (l *Log) EnableEncryption(e *Encryption) { l.enc = e }

// EnableRemote wires the on-demand batcher and idempotent uploader so FLUSH/FUA
// make records durable in S3 (§14.3–14.5). Must be set before the first WRITE.
// EnableRemote wires the remote path. The lease checker is a parameter rather than a
// later setter so that every call site has to answer the question "what fences this
// writer?" — nil is legal only for `local` durability (§14.8 rule 3), and a remote
// FLUSH with a nil lease fails closed with ErrNoLease (DEV-0004).
func (l *Log) EnableRemote(b *Batcher, u *Uploader, lease LeaseChecker) {
	l.batcher = b
	l.uploader = u
	if lease != nil {
		l.lease = lease
	}
}

// NewLog creates a log backed by file, timed by clk.
func NewLog(file disk.File, clk clock.Clock, volumeID [16]byte, epoch uint64, limits Limits) *Log {
	return &Log{
		file:     file,
		clk:      clk,
		volumeID: volumeID,
		epoch:    epoch,
		view:     cow.NewIntervalMap(),
		limits:   limits,
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
	return nil
}

func (l *Log) appendEncoded(seq uint64, enc []byte, addView func()) (uint64, error) {
	if err := l.backpressure(len(enc)); err != nil {
		return 0, err
	}
	// A failed append is not necessarily an append of nothing: a partial write
	// (ENOSPC, a torn write at a device boundary) leaves bytes that are not a record.
	// Roll the file back to the last intact record before reporting the failure —
	// otherwise the rejected write either replays as a record the guest was told
	// failed (with a sequence the next accepted write reuses) or truncates the log,
	// making replay stop at the tear and silently drop everything after it.
	before, err := l.file.Size()
	if err != nil {
		return 0, err
	}
	if _, err := l.file.Append(enc); err != nil {
		if terr := l.file.Truncate(before); terr != nil {
			// The log's tail is now unknown. Refuse to serve it rather than ACK
			// anything against a file we cannot describe.
			l.fenced = true
			return 0, errors.Join(err, fmt.Errorf("wal: could not roll back a partial append: %w", terr))
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
	seq := l.local + 1
	var (
		enc []byte
		err error
	)
	if l.enc != nil {
		enc, err = l.enc.encodeWrite(l.epoch, seq, offset, flags, data)
	} else {
		r := Record{Type: format.RecordWrite, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: uint32(len(data)), Flags: flags, Payload: data}
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
	return l.appendClear(format.RecordDiscard, offset, length)
}

// WriteZeroes appends a WRITE_ZEROES of [offset, offset+length); reads as zero.
func (l *Log) WriteZeroes(offset uint64, length uint32) (uint64, error) {
	return l.appendClear(format.RecordWriteZeroes, offset, length)
}

// appendClear appends a header-only DISCARD/WRITE_ZEROES record: it clears the read
// view, feeds the remote batcher (these records must reach S3 so the working set
// converges, §14.6), and counts the reclaimed bytes.
func (l *Log) appendClear(t format.RecordType, offset uint64, length uint32) (uint64, error) {
	seq := l.local + 1
	enc, err := Record{Type: t, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: length}.Encode()
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
func (l *Log) DiscardedBytes() int64 { return l.discardedBytes }

// Read fills buf from the read view starting at offset (zero where unwritten).
func (l *Log) Read(offset uint64, buf []byte) { l.view.Read(offset, buf) }

// Sync makes prior appends durable locally (fdatasync) and clears the unflushed
// accounting. It does not advance the durable watermark — that requires remote
// durability via Flush.
func (l *Log) Sync() error {
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.clearUnflushed()
	return nil
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
	if l.fenced {
		return ErrSelfFenced
	}
	target := l.local
	if l.batcher != nil {
		l.batcher.Flush() // step 2: close current batch
	}
	if err := l.file.Sync(); err != nil { // step 3: fdatasync local
		return err
	}

	if l.mode == ModeLocal {
		// §14.8: ACK on local durability; no lease gate, no synchronous S3.
		l.clearUnflushed()
		return nil
	}

	if l.batcher != nil && l.uploader != nil {
		pending := l.batcher.Pending()
		done := 0
		for _, cb := range pending { // step 4: upload + verify (covering <= target)
			key, err := l.uploader.Upload(ctx, cb)
			if err != nil {
				l.batcher.RemoveUploaded(done)
				return err // durable NOT advanced
			}
			l.uploaded = append(l.uploaded, SummaryObject{Key: key, First: cb.First, Last: cb.Last})
			done++
		}
		l.batcher.RemoveUploaded(done)
	}

	// step 5: verify the lease on the monotonic clock (§12.2, INV-06). If it is not
	// valid, do NOT advance durable and do NOT ACK — self-fence. In remote mode the
	// check is mandatory: no lease checker means nothing is fencing this writer, so
	// the ACK is refused rather than granted by default.
	if l.mode == ModeRemote {
		if l.lease == nil {
			return ErrNoLease
		}
		if !l.lease.Valid() {
			l.fenced = true
			l.rec.Count(ctx, "self_fenced_total", 1, obs.String("volume", l.volLabel))
			return ErrSelfFenced
		}
	}
	if err := l.AdvanceDurable(target); err != nil { // step 6
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

// Watermarks returns the current watermarks.
func (l *Log) Watermarks() Watermarks {
	return Watermarks{Local: l.local, Durable: l.durable, Published: l.published}
}

// UnflushedBytes reports the bytes appended since the last Sync.
func (l *Log) UnflushedBytes() int64 { return l.unflushedBytes }

// ViewBytes reports the read-view memory (active_map_bytes proxy for Phase 04).
func (l *Log) ViewBytes() int { return l.view.Bytes() }

// AdvanceDurable advances the durable watermark, enforcing durable <= local (§5.6).
func (l *Log) AdvanceDurable(seq uint64) error {
	if seq > l.local || seq < l.published {
		return ErrWatermarkOrder
	}
	l.durable = seq
	return nil
}

// AdvancePublished advances the published watermark, enforcing published <= durable.
func (l *Log) AdvancePublished(seq uint64) error {
	if seq > l.durable {
		return ErrWatermarkOrder
	}
	l.published = seq
	return nil
}
