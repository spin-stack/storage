package wal

import (
	"crypto/sha256"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/wal/format"
)

// BatchConfig controls on-demand batch closing (§14.3). Defaults per §10 config.
type BatchConfig struct {
	TargetBytes int           // close at/above this size (8 MiB)
	MaxBytes    int           // hard cap (16 MiB)
	MaxAge      time.Duration // close when the first record is this old (20 s)
}

// DefaultBatchConfig returns the doc's initial values (§10).
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{TargetBytes: 8 << 20, MaxBytes: 16 << 20, MaxAge: 20 * time.Second}
}

// CloseReason records why a batch was closed (for metrics / debugging).
type CloseReason string

const (
	CloseFUA       CloseReason = "fua"
	CloseFlush     CloseReason = "flush"
	CloseTarget    CloseReason = "target"
	CloseMax       CloseReason = "max"
	CloseAge       CloseReason = "age"
	CloseSnapshot  CloseReason = "snapshot"
	CloseUnflushed CloseReason = "unflushed"
	CloseShutdown  CloseReason = "shutdown"
)

// ClosedBatch is a sealed group of records ready to become a WAL object.
type ClosedBatch struct {
	VolumeID [16]byte
	Epoch    uint64
	KeyID    uint32
	First    uint64
	Last     uint64
	Count    uint32
	Records  []byte // concatenated encoded records (headers + ciphertext payloads)
	Reason   CloseReason
}

// Object assembles the on-S3 WAL object: ObjectHeader (§14.2) followed by the
// concatenated records. It returns the deterministic key, the object bytes, and
// the payload SHA-256.
func (b *ClosedBatch) Object() (key string, data []byte, sha [32]byte) {
	sha = sha256.Sum256(b.Records)
	h := format.ObjectHeader{
		VolumeID:      b.VolumeID,
		Epoch:         b.Epoch,
		FirstSequence: b.First,
		LastSequence:  b.Last,
		RecordCount:   b.Count,
		KeyID:         b.KeyID,
		PayloadLength: uint64(len(b.Records)),
		PayloadSHA256: sha,
	}
	hb, _ := h.MarshalBinary()
	data = make([]byte, 0, len(hb)+len(b.Records))
	data = append(data, hb...)
	data = append(data, b.Records...)
	key = format.WALObjectKey(b.VolumeID, b.Epoch, b.First, b.Last, sha)
	return key, data, sha
}

type openBatch struct {
	first   uint64
	last    uint64
	count   uint32
	records []byte
	firstAt clock.Instant
}

// Batcher accumulates encoded records and closes batches on the §14.3 rules only
// (never a short timer). It holds the closed-but-not-yet-uploaded batches.
type Batcher struct {
	cfg      BatchConfig
	clk      clock.Clock
	volumeID [16]byte
	epoch    uint64
	keyID    uint32

	cur     *openBatch
	pending []*ClosedBatch
}

// NewBatcher returns a batcher for one volume/epoch.
func NewBatcher(clk clock.Clock, volumeID [16]byte, epoch uint64, keyID uint32, cfg BatchConfig) *Batcher {
	return &Batcher{cfg: cfg, clk: clk, volumeID: volumeID, epoch: epoch, keyID: keyID}
}

// Append adds an encoded record. If fua is set (a FUA write), the batch closes
// immediately (§14.3.1). Size thresholds also trigger a close.
func (b *Batcher) Append(seq uint64, encoded []byte, fua bool) {
	if b.cur == nil {
		b.cur = &openBatch{first: seq, firstAt: b.clk.Now()}
	}
	b.cur.records = append(b.cur.records, encoded...)
	b.cur.last = seq
	b.cur.count++

	switch {
	case fua:
		b.Close(CloseFUA)
	case len(b.cur.records) >= b.cfg.MaxBytes:
		b.Close(CloseMax)
	case len(b.cur.records) >= b.cfg.TargetBytes:
		b.Close(CloseTarget)
	}
}

// Flush closes the current batch for a FLUSH request (§14.3.1); a no-op if empty.
func (b *Batcher) Flush() { b.Close(CloseFlush) }

// MaybeCloseForAge closes the current batch if its first record is older than
// MaxAge (§14.3.4). Called by a periodic tick, not a per-record timer.
func (b *Batcher) MaybeCloseForAge() {
	if b.cur != nil && b.clk.Now().Sub(b.cur.firstAt) >= b.cfg.MaxAge {
		b.Close(CloseAge)
	}
}

// Close seals the current batch with the given reason. No-op if there is no open
// batch with records.
func (b *Batcher) Close(reason CloseReason) {
	if b.cur == nil || b.cur.count == 0 {
		return
	}
	b.pending = append(b.pending, &ClosedBatch{
		VolumeID: b.volumeID,
		Epoch:    b.epoch,
		KeyID:    b.keyID,
		First:    b.cur.first,
		Last:     b.cur.last,
		Count:    b.cur.count,
		Records:  b.cur.records,
		Reason:   reason,
	})
	b.cur = nil
}

// Pending returns the closed batches awaiting upload.
func (b *Batcher) Pending() []*ClosedBatch { return b.pending }

// TakePending returns and clears the closed batches (the uploader drains these).
func (b *Batcher) TakePending() []*ClosedBatch {
	p := b.pending
	b.pending = nil
	return p
}

// RemoveUploaded drops the first n closed batches (those successfully uploaded),
// retaining the rest for a later retry.
func (b *Batcher) RemoveUploaded(n int) {
	if n >= len(b.pending) {
		b.pending = nil
		return
	}
	b.pending = b.pending[n:]
}

// OpenBytes reports the current open batch size (0 if none).
func (b *Batcher) OpenBytes() int {
	if b.cur == nil {
		return 0
	}
	return len(b.cur.records)
}
