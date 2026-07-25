package wal

import (
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrForeignEpoch means the WAL file holds records that do not belong to the epoch
// being resumed. A file replayed under the wrong identity — a restored backup, a
// reused volume directory, a path bug — would apply another writer's records to this
// volume's extents and count them in this epoch's sequence space.
var ErrForeignEpoch = errors.New("wal: the WAL file holds records from another epoch")

// resumedRecord is a record that was in the local WAL but is not yet covered by a
// verified object: it must be handed back to the batcher when the remote path is
// wired, or the writes since the last successful upload exist on this host only.
type resumedRecord struct {
	seq     uint64
	encoded []byte
}

// Resume rebuilds a Log from an existing WAL file: the ATTACHING→ACTIVE path of §16,
// where an agent restart re-attaches to a volume at the *same* epoch (§16 validates
// the epoch and takes the lease; it does not bump it).
//
// It is the only safe way to continue a non-empty WAL. A log built by NewLog over the
// same file starts its sequence counter at the boundary it was handed and serves an
// empty read view, so it re-issues sequences that are already on disk — duplicates in
// one (volume, epoch), reused GCM nonces (§15.2, INV-15), and two objects claiming
// one span (INV-21) — while reporting zeros for data the guest wrote. NewLog's first
// append refuses that; this is what to do instead.
//
// durableInS3 is the sequence the object store is known to reproduce (from
// recovery.DurablePoint). Records above it are queued for re-upload and counted in
// the remote gap; records at or below it are replayed into the view only, so no span
// the bucket already holds is re-issued.
//
// A torn tail is the normal crash case and is not an error: Replay returns the intact
// prefix (INV-05) and the log continues after the last whole record. Corruption
// *inside* the file is an error — the records after it cannot be read, and silently
// resuming at the corruption point would drop them without saying so.
func Resume(file disk.File, clk clock.Clock, volumeID [16]byte, epoch, durableInS3 uint64, limits Limits, enc *Encryption) (*Log, error) {
	size, err := file.Size()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := file.ReadAt(buf, 0); err != nil {
			return nil, fmt.Errorf("wal: read the WAL to resume it: %w", err)
		}
	}
	recs, err := Replay(buf)
	if err != nil {
		return nil, fmt.Errorf("wal: resume: %w", err)
	}

	l := NewLogAfter(file, clk, volumeID, epoch, durableInS3, limits)
	l.enc = enc
	l.replayed = true

	for _, rec := range recs {
		if rec.Epoch != epoch {
			return nil, fmt.Errorf("%w: sequence %d belongs to epoch %d, resuming %d",
				ErrForeignEpoch, rec.Sequence, rec.Epoch, epoch)
		}
		if err := l.replayRecord(rec); err != nil {
			return nil, err
		}
		if rec.Sequence > l.local {
			l.local = rec.Sequence
		}
		if rec.Sequence <= durableInS3 {
			continue // already in a verified object; replaying it into the view is enough
		}
		encoded, err := reencode(rec)
		if err != nil {
			return nil, fmt.Errorf("wal: resume sequence %d: %w", rec.Sequence, err)
		}
		l.resumeTail = append(l.resumeTail, resumedRecord{seq: rec.Sequence, encoded: encoded})
		l.trackGap(len(encoded))
	}
	return l, nil
}

// replayRecord folds one replayed record back into the read view, decrypting a WRITE
// when the volume is encrypted. The view is plaintext even for an encrypted volume:
// the ciphertext is what is on disk, never what a guest read returns (INV-15).
func (l *Log) replayRecord(rec Record) error {
	switch rec.Type {
	case format.RecordWrite:
		payload := rec.Payload
		if l.enc != nil {
			pt, err := l.enc.Decrypt(rec)
			if err != nil {
				return fmt.Errorf("wal: resume sequence %d: %w", rec.Sequence, err)
			}
			payload = pt
		}
		l.view.Overwrite(rec.Offset, payload)
	case format.RecordDiscard, format.RecordWriteZeroes:
		l.view.Clear(rec.Offset, uint64(rec.Length))
	}
	return nil
}

// reencode rebuilds the exact on-disk bytes of a replayed record. The header fields
// are carried through untouched — including the plaintext CRC and the GCM tag of an
// encrypted record, which cannot be recomputed without the DEK — so a record that is
// re-uploaded is byte-identical to the one already in the WAL.
func reencode(rec Record) ([]byte, error) {
	h := format.RecordHeader{
		RecordType:    rec.Type,
		Epoch:         rec.Epoch,
		Sequence:      rec.Sequence,
		Offset:        rec.Offset,
		Length:        rec.Length,
		Flags:         rec.Flags,
		KeyID:         rec.KeyID,
		PayloadCRC32C: rec.PayloadCRC,
		AuthTag:       rec.AuthTag,
	}
	return format.EncodeRecordRaw(h, rec.Payload)
}
