package wal

import (
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrForeignEpoch means the WAL holds records or segments that do not belong to the
// epoch being resumed. A WAL replayed under the wrong identity — a restored backup, a
// reused volume directory, a path bug — would apply another writer's records to this
// volume's extents and count them in this epoch's sequence space.
var ErrForeignEpoch = errors.New("wal: the WAL holds records from another epoch")

// ErrBaseUnavailable is returned by every Read on a log whose base — the read view
// recovered from the object store — could not be built. The volume refuses to answer
// rather than serving zeros for ranges truncation has reclaimed locally: a guest cannot
// tell those zeros from a range it never wrote, which is the failure BUILD-INVENTORY
// increment 5 exists to make impossible.
var ErrBaseUnavailable = errors.New("wal: the read view's base could not be recovered")

// ErrForeignVolume means the WAL holds records or segments belonging to another
// volume. It is the check that has no fallback: an encrypted volume's payloads are
// bound to their volume by the GCM AAD, but DISCARD and WRITE_ZEROES carry no payload
// and a plaintext volume carries no tag, so nothing else would notice.
var ErrForeignVolume = errors.New("wal: the WAL holds records from another volume")

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
// durableInS3 is the sequence the object store is known to reproduce. It is the floor
// the sequence space continues from, so a log whose local segments were all reclaimed
// does not re-issue sequences an object already carries; every record found on disk is
// replayed into the view regardless of where it sits relative to it.
//
// A torn tail in the newest segment is the normal crash case and is not an error:
// replay returns the intact prefix (INV-05), the torn bytes are cut off so the next
// record does not land behind a hole, and the log continues after the last whole
// record. Corruption anywhere else is an error — a tear in a segment nothing should
// have been appending to, a gap in the directory, or a bad CRC — because the records
// after it cannot be read, and silently resuming at that point would drop them without
// saying so.
func Resume(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch, durableInS3 uint64, limits Limits, enc *Encryption) (*Log, error) {
	return resume(d, root, clk, volumeID, epoch, durableInS3, limits, enc, false)
}

// ResumeAwaitingBase is Resume for a WAL whose local segments may have been truncated:
// the log's read view is layered and its reads block until InstallBase or FailBase.
//
// It takes no durable point, deliberately. That number lives in the object store, and
// asking for it here would mean a store round trip before the volume could be served —
// the eager shape this design rejected. It arrives with the base instead, in InstallBase,
// which is the only moment it is known and the only moment it can be trusted.
//
// Until then the log reports durable = 0. That understates what is durable, which is the
// safe direction for every rule that reads it, and it is true of *this process*: nothing
// has been verified by anyone here yet.
//
// The caller owes the returned log exactly one InstallBase or FailBase. Both are safe to
// call from another goroutine, which is what makes the fetch lazy: the volume is served
// immediately and only its *reads* wait. Close resolves the base too, so a shutdown
// never strands a read.
func ResumeAwaitingBase(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch uint64, limits Limits, enc *Encryption) (*Log, error) {
	return resume(d, root, clk, volumeID, epoch, 0, limits, enc, true)
}

func resume(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch, durableInS3 uint64, limits Limits, enc *Encryption, awaitBase bool) (*Log, error) {
	l := NewLogAfter(d, root, clk, volumeID, epoch, durableInS3, limits)
	if awaitBase {
		// Layered from the start, before a single record replays: a DISCARD replayed
		// into an unlayered view records no tombstone, and the base installed
		// afterwards would uncover exactly the range the guest discarded.
		l.view = cow.NewIntervalMapOver(nil)
		l.baseWait = make(chan struct{})
	}
	l.enc = enc
	l.replayed = true

	scan, err := scanSegments(d, l.segs.dir, volumeID, epoch)
	if err != nil {
		return nil, fmt.Errorf("wal: resume: %w", err)
	}
	if err := l.segs.adopt(scan); err != nil {
		return nil, fmt.Errorf("wal: resume: %w", err)
	}

	for _, rec := range scan.records {
		if rec.VolumeID != volumeID {
			return nil, fmt.Errorf("%w: sequence %d belongs to volume %s, resuming %s",
				ErrForeignVolume, rec.Sequence, format.UUIDString(rec.VolumeID), format.UUIDString(volumeID))
		}
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
