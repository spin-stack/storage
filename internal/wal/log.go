package wal

import (
	"context"
	"errors"
	"time"

	"github.com/spin-stack/storage/internal/cow"
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

	unflushedBytes    int64
	oldestUnflushedAt clock.Instant
	hasUnflushed      bool
	discardedBytes    int64
	uploaded          []SummaryObject // durable objects, for the summary (§22.1)
}

// EnableEncryption binds an Encryption context so subsequent WRITEs seal their
// payloads (§15). Must be set before the first WRITE.
func (l *Log) EnableEncryption(e *Encryption) { l.enc = e }

// EnableRemote wires the on-demand batcher and idempotent uploader so FLUSH/FUA
// make records durable in S3 (§14.3–14.5). Must be set before the first WRITE.
func (l *Log) EnableRemote(b *Batcher, u *Uploader) {
	l.batcher = b
	l.uploader = u
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
	if _, err := l.file.Append(enc); err != nil {
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
	seq := l.local + 1
	r := Record{Type: format.RecordDiscard, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: length}
	enc, err := r.Encode()
	if err != nil {
		return 0, err
	}
	seq, err = l.appendEncoded(seq, enc, func() { l.view.Clear(offset, uint64(length)) })
	if err == nil {
		l.discardedBytes += int64(length)
	}
	return seq, err
}

// WriteZeroes appends a WRITE_ZEROES of [offset, offset+length); reads as zero.
func (l *Log) WriteZeroes(offset uint64, length uint32) (uint64, error) {
	seq := l.local + 1
	r := Record{Type: format.RecordWriteZeroes, Epoch: l.epoch, Sequence: seq, Offset: offset, Length: length}
	enc, err := r.Encode()
	if err != nil {
		return 0, err
	}
	seq, err = l.appendEncoded(seq, enc, func() { l.view.Clear(offset, uint64(length)) })
	if err == nil {
		l.discardedBytes += int64(length)
	}
	return seq, err
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
	l.unflushedBytes = 0
	l.hasUnflushed = false
	return nil
}

// Flush makes every record up to the current local sequence durable in S3 and
// advances durable_sequence (§14.4). Order: capture target, close the batch,
// fdatasync, upload+verify all covering objects, advance durable, ack. durable is
// advanced ONLY after the objects are verified (INV-07). If an upload fails, the
// un-uploaded batches are retained for the next Flush and durable is not advanced.
// The lease-verify step (§14.4 step 5) lands in Phase 07.
func (l *Log) Flush(ctx context.Context) error {
	target := l.local
	if l.batcher != nil {
		l.batcher.Flush() // step 2: close current batch
	}
	if err := l.file.Sync(); err != nil { // step 3: fdatasync local
		return err
	}
	if l.batcher != nil && l.uploader != nil {
		pending := l.batcher.Pending()
		done := 0
		for _, cb := range pending { // step 4: upload + verify (covering <= target)
			if err := l.uploader.Upload(ctx, cb); err != nil {
				l.batcher.RemoveUploaded(done)
				return err // durable NOT advanced
			}
			key, _, _ := cb.Object()
			l.uploaded = append(l.uploaded, SummaryObject{Key: key, First: cb.First, Last: cb.Last})
			done++
		}
		l.batcher.RemoveUploaded(done)
	}
	// step 5: lease verify — Phase 07.
	if err := l.AdvanceDurable(target); err != nil { // step 6
		return err
	}
	l.unflushedBytes = 0
	l.hasUnflushed = false
	return nil // step 7: ack
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
