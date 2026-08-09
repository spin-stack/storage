package wal

import (
	"errors"
	"fmt"
	"log/slog"

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
// tell those zeros from a range it never wrote, which is the failure this
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

// ResumeReport is what replay found on the device, returned by Log.ResumeReport.
//
// RecoveredSequence is the whole point of the type: it is the highest sequence the local
// segments still held, and nothing else in the log reports it. The watermark that looks
// like it does — Watermarks().Local — is max(the floor the caller passed in, what replay
// found), so it reads the same whether the WAL held records up to N or held nothing at
// all above a floor of N.
//
// # What the caller owes this
//
// The Control Plane's catalog holds durable_sequence for the volume: the sequence *this
// host* ACKed, which is the promise a guest's fsync returned on. Resume cannot check it
// — the number lives in Postgres and this package has no catalog, deliberately — so the
// layer that has both must:
//
//	rep := log.ResumeReport()
//	if rep.Resumed && rep.RecoveredSequence < catalogDurableSequence {
//	        // refuse to serve; this device no longer holds writes a guest was told
//	        // were durable, and serving the volume answers reads with the survivors
//	}
//
// That comparison is the one that catches the reproduced failure: a segment whose tail
// was lost replays 14 of 15 records, the catalog still says 15, and the only thing that
// noticed was the tenant's guest failing a read-back. Below the durable point, "resume
// succeeded" is not the same statement as "the volume is intact", and only the caller
// can tell them apart.
type ResumeReport struct {
	// Resumed distinguishes a log rebuilt from a WAL directory from a fresh one, whose
	// zero RecoveredSequence must not be compared against anything.
	Resumed bool
	// Records is how many records replay folded back into the view.
	Records int
	// RecoveredSequence is the highest sequence found on the device, 0 if none.
	RecoveredSequence uint64

	// TornTail is set when replay stopped short of the newest segment's end: the bytes
	// after StoppedAtOffset were a partial record and have been cut off.
	TornTail        bool
	TornSegment     string
	StoppedAtOffset int64
	DiscardedBytes  int64
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
		return nil, fmt.Errorf("wal: resume: %w%s", err, repairHint(d, l.segs.dir))
	}
	if err := l.segs.adopt(scan); err != nil {
		return nil, fmt.Errorf("wal: resume: %w", err)
	}
	l.resume = describeScan(scan)

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
		if rec.Sequence > l.resume.RecoveredSequence {
			l.resume.RecoveredSequence = rec.Sequence
		}
	}
	l.announceDiscard(volumeID, epoch)
	return l, nil
}

// describeScan turns what the scan found into the report the caller reads back.
//
// The stub case — a file too short to hold a header, which adopt has just removed — is
// counted as discarded bytes too. It carried no records (a header is written and synced
// before the segment is used), so nothing was lost, but a resume that quietly unlinks a
// file is exactly the shape this whole change exists to stop being quiet about.
func describeScan(scan scanResult) ResumeReport {
	rep := ResumeReport{Resumed: true, Records: len(scan.records)}
	if len(scan.segs) == 0 {
		return rep
	}
	newest := scan.segs[len(scan.segs)-1]
	switch {
	case scan.torn:
		rep.TornTail = true
		rep.TornSegment = newest.name
		rep.StoppedAtOffset = scan.cleanLen
		rep.DiscardedBytes = newest.size - scan.cleanLen
	case scan.stub && newest.size > 0:
		rep.TornSegment = newest.name
		rep.DiscardedBytes = newest.size
	}
	return rep
}

// announceDiscard is the one moment this process knows records were thrown away.
//
// Cutting a torn tail is correct — a partial record was never durable, INV-05 makes
// replay return the intact prefix — and doing it in silence is not. Before this, a
// restart over a WAL whose last append was cut short replayed the prefix, trimmed the
// file, served the volume and logged nothing distinguishable from a clean start; the
// tenant's guest was the only component that noticed, by failing a read-back. The line
// carries the volume, the epoch, the segment, where parsing stopped and how much went,
// because those are what an operator needs to decide whether the lost record mattered,
// and recovered_sequence because that is the number to compare against the catalog's
// durable_sequence.
//
// It logs nothing when nothing was discarded. A warning printed on every restart is read
// on none of them.
func (l *Log) announceDiscard(volumeID [16]byte, epoch uint64) {
	if l.resume.DiscardedBytes == 0 {
		return
	}
	msg := "wal resume discarded a torn tail: the bytes after this offset were a partial record, were never durable, and are gone"
	if !l.resume.TornTail {
		msg = "wal resume discarded a segment stub: the crash landed between creating the file and writing its header, so it held no records"
	}
	slog.Warn(msg,
		"volume_id", format.UUIDString(volumeID),
		"epoch", epoch,
		"segment", l.resume.TornSegment,
		"stopped_at_offset", l.resume.StoppedAtOffset,
		"discarded_bytes", l.resume.DiscardedBytes,
		"recovered_sequence", l.resume.RecoveredSequence,
	)
}

// repairHint names the first byte of the WAL that cannot be decoded, so a scan failure
// tells an operator what to do rather than only that something is wrong.
//
// The failure it is for: bytes that are not a partial record sit in a segment — a stray
// append, a device that handed back garbage, a file restored over a live one. Replay
// stops there, every record after it is unreachable, and resume fails. That is the right
// outcome and this does not change it: a wedged volume an operator can fix is fine, a
// silently shortened one is not. What was missing was the exit. `wal/format: bad magic`,
// retried by the reconcile loop every five seconds forever, names no file, no offset and
// no action; truncating the named segment to the named offset resumes at the last whole
// record and discards exactly the bytes named.
//
// It re-reads the directory instead of taking the position from the scan, because the
// scan reports no position on its error path and that path lives in a file this change
// does not own. The cost is one extra pass over a WAL that has already failed to open,
// and nothing at all on a WAL that opens.
//
// It returns "" whenever the failure is not a truncation away from readable — an
// unreadable segment header, a foreign volume or epoch, a gap in the directory — so the
// error never suggests a repair that would destroy data.
func repairHint(d disk.Disk, dir string) string {
	names, err := listSegments(d, dir)
	if err != nil {
		return ""
	}
	for i, name := range names {
		last := i == len(names)-1
		buf, err := readWholeFile(d, name)
		if err != nil || len(buf) < format.SegmentHeaderSize {
			return ""
		}
		if _, err := format.UnmarshalSegmentHeader(buf); err != nil {
			return ""
		}
		body := buf[format.SegmentHeaderSize:]
		_, consumed, perr := replayPrefix(body)
		if perr == nil && (consumed == len(body) || last) {
			// A clean segment, or the newest one ending in a torn tail — which is
			// allowed and is not what the scan is failing on.
			continue
		}
		off := int64(format.SegmentHeaderSize + consumed)
		return fmt.Sprintf(" (repair: %s holds %d bytes and stops decoding at offset %d;"+
			" truncating it to %d bytes discards %d unreadable bytes and resumes at the last whole record)",
			name, len(buf), off, off, int64(len(buf))-off)
	}
	return ""
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
