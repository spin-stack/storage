package wal

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrCarryUnavailable is returned when this log cannot take up the records it still
// holds under an earlier epoch, because it has already appended in this session.
//
// It is refused rather than done anyway: the sequences on disk under the earlier epoch
// and the sequences this session has already issued are the same numbers, so appending
// the old ones now would put two different records under one sequence in one directory
// — duplicate GCM nonces for an encrypted volume, and a replay that applies whichever
// came last. Refusing costs a volume that will not serve until it is restarted with no
// guest attached; the alternative costs the guest's bytes with no error anywhere.
var ErrCarryUnavailable = errors.New("wal: this session has already appended, so the earlier epoch's records can no longer be taken up")

// ErrCarryHole is returned when the records this host holds below the granted epoch do
// not continue the volume's sequence space from where the object store leaves off. The
// records in the hole exist nowhere, and appending what is above it would put a later
// state on a volume that never reached the earlier one — the same rule ErrSegmentGap
// states inside one directory.
var ErrCarryHole = errors.New("wal: the records held under an earlier epoch do not continue the volume's sequence space")

// Carried is what CarryForward moved and what it unlinked.
type Carried struct {
	// Epochs are the epoch directories this host held below the granted one, ascending.
	// Non-empty is the whole signal that anything happened: an epoch appears here when
	// it was drained, whether its records were carried forward or were already in the
	// object store.
	Epochs []uint64
	// Records, First and Last describe what was appended under the granted epoch.
	// First and Last are 0 when the object store already covered everything found.
	Records     int
	First, Last uint64
	// Bytes is what the carried records occupy under the granted epoch, Reclaimed what
	// unlinking the drained directories gave back. They are close but not equal: an
	// encrypted record is resealed, a drained directory also gives back its segment
	// headers, and records the object store already covers are dropped rather than
	// rewritten.
	Bytes     int64
	Reclaimed int64
}

// Empty reports that this host held nothing under an earlier epoch — the common case,
// and the one that must stay silent.
func (c Carried) Empty() bool { return len(c.Epochs) == 0 }

// CarryForward takes up the records this host still holds for this volume under epochs
// below the one it has just been granted, and unlinks those directories once it has.
//
// # The failure it exists for
//
// A session's records are ACKed to a guest's fsync and not published — the Agent was
// killed, or a detach landed in the seconds before its next poll. They are on disk under
// `<data-dir>/wal/<vol>/<epoch>/`. The operator does the documented thing and attaches
// the volume again; every placement grants a *fresh* epoch, so the Agent opens
// `<epoch+1>/`, which does not exist, replays nothing, and the durable floor refuses the
// volume because it came back below the sequence a guest was told was durable. Every
// retry makes it worse: another attach is another epoch, and moving the directory by
// hand is refused because each segment header carries its own epoch. The bytes are on
// the local disk, intact, and nothing an operator can run reaches them.
//
// # Why this is allowed to cross an epoch at all
//
// **The epoch fences other hosts, not this host's own unpublished session.** A host
// holding `<vol>/2/` when it is granted epoch 3 is not a fencing hazard to itself: it is
// the same process, on the same device, holding records nobody else can have — the
// directory is under this host's data directory, which one process at a time owns
// (Disk.Lock). Nothing here reaches across volumes (only `<root>/<vol>/`) or across
// epochs at or above the granted one (a directory this host has not been granted yet, or
// has already moved past, is not its business).
//
// # `above` is the object store's sequence, and it is why this cannot run at attach
//
// Only records the object store does *not* already reproduce are carried. That is not an
// optimisation, it is the correctness condition, and it is worth stating why: sequences
// are per-volume, and a host promoted after this one continues the sequence space from
// the image it loaded. So a record of ours at sequence s with s <= the image's sequence
// is either the very record the image was built from — carrying it forward writes the
// same bytes twice — or a sequence another host reissued after we were fenced with it
// unpublished, in which case *their* record is the one in the image and it is newer.
// Replaying ours over the image would put a superseded write on top of the live one,
// with no error anywhere, which is the failure this whole file exists downstream of.
// Above the image's sequence there is no such doubt: nobody else can have those records.
//
// The image's sequence is known only once the manifest has been loaded, which is why
// this is called from the base fetch and not from the attach that opens the log.
//
// # Interruptible, because it runs in the window that created the problem
//
// Order: append every kept record under the granted epoch, fdatasync, then unlink the
// drained directories oldest first. A crash anywhere leaves a state the next attach
// completes rather than one it has to be rescued from:
//
//   - during the appends, the granted epoch holds a prefix and the sources are intact.
//     The next resume replays that prefix, so `l.local` covers it, and the records at or
//     below it are skipped here — the carry continues where it stopped. A torn last
//     append is cut off by the resume that adopts the directory, like any other.
//   - during the unlinks, everything is already under the granted epoch, so nothing is
//     carried on the retry and the unlinking simply finishes. Oldest first matters: a
//     partly unlinked directory is a *suffix* of the sequence space, which scans cleanly,
//     where the other order would leave a hole in the middle.
//
// # What it deliberately does not do
//
// It does not consult the device budget. The bytes are already on this device and the
// directory they came from is about to be unlinked, so the move is net zero; refusing it
// for want of space would refuse exactly the volume that cannot be recovered any other
// way. It does not rename: the segment header carries the epoch, and so does every
// record header, and an encrypted payload's GCM AAD binds it — so a carried record is
// re-encoded, and a sealed one is opened under its old epoch and resealed under the new.
// Its *sequence* never changes, which is what keeps the volume's sequence space one
// space, and which is also why resealing cannot reuse a nonce: the nonce is derived from
// (volume, epoch, sequence) and the granted epoch has never issued this sequence.
func (l *Log) CarryForward(above uint64) (Carried, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out Carried
	epochs, err := lowerEpochs(l.segs.d, l.segs.root, l.volumeID, l.epoch)
	if err != nil {
		return out, err
	}
	if len(epochs) == 0 {
		return out, nil
	}
	// Nothing may have been appended through this log yet. `local` moves only on an
	// accepted append and on the resume that set it, so this is that statement exactly.
	// See ErrCarryUnavailable for what accepting anyway would cost.
	if l.local != l.resume.RecoveredSequence {
		return out, fmt.Errorf("%w: volume %s is at sequence %d and resumed at %d",
			ErrCarryUnavailable, format.UUIDString(l.volumeID), l.local, l.resume.RecoveredSequence)
	}

	held, dirs, err := l.heldBelow(epochs)
	if err != nil {
		return out, err
	}

	// The floor: what the object store already reproduces, or what this directory
	// already holds, whichever is higher. The second half is what makes an interrupted
	// carry resumable — the prefix already under the granted epoch is not written twice.
	floor := max(above, l.local)
	kept := held
	for len(kept) > 0 && kept[0].Sequence <= floor {
		kept = kept[1:]
	}
	if len(kept) > 0 && kept[0].Sequence != floor+1 {
		return out, fmt.Errorf("%w: volume %s holds sequence %d under epoch %d, and %d is the last one the object store or this epoch can account for",
			ErrCarryHole, format.UUIDString(l.volumeID), kept[0].Sequence, epochs[len(epochs)-1], floor)
	}

	// The view and the watermark move with each accepted record rather than at the end,
	// so a carry that fails half way leaves this log describing exactly what is on the
	// device. The volume is refused either way; a log whose sequence counter disagreed
	// with its own directory would reissue sequences if anything ever wrote to it again.
	for _, rec := range kept {
		enc, err := l.reencode(rec)
		if err != nil {
			return out, err
		}
		if appendErr, rollbackErr := l.segs.appendRecord(rec.Sequence, enc); appendErr != nil {
			if rollbackErr != nil {
				l.broken = true
				return out, errors.Join(appendErr, fmt.Errorf("wal: could not roll back a partial carry-forward append: %w", rollbackErr))
			}
			return out, fmt.Errorf("wal: carrying sequence %d forward into epoch %d: %w", rec.Sequence, l.epoch, appendErr)
		}
		if err := l.replayRecord(rec); err != nil {
			return out, err
		}
		l.local = rec.Sequence
		l.resume.RecoveredSequence = rec.Sequence
		l.resume.Records++
		out.Records++
		out.Bytes += int64(len(enc))
		if out.First == 0 {
			out.First = rec.Sequence
		}
		out.Last = rec.Sequence
	}
	// Durable before a single directory is unlinked. Without this the records exist only
	// in the page cache while the files that held them are gone, and a host that lost
	// power here would have destroyed the thing it was rescuing.
	if out.Records > 0 {
		if err := l.segs.sync(); err != nil {
			return out, fmt.Errorf("wal: syncing the records carried into epoch %d: %w", l.epoch, err)
		}
	}

	// Redundant by construction, which is the justification for deleting anything at
	// all: every record under these directories is now either under the granted epoch
	// or at or below the sequence the object store reproduces. This is a different
	// moment from release(), which deliberately deletes nothing — there the session may
	// still have to be republished from those very segments, here they have been
	// superseded twice over.
	for _, dir := range dirs {
		freed, err := removeSegments(l.segs.d, dir)
		out.Reclaimed += freed
		if err != nil {
			return out, err
		}
	}
	out.Epochs = epochs
	return out, nil
}

// heldBelow reads every record this host holds for this volume under the given epochs,
// oldest epoch first, and returns the directories they came from.
//
// Each directory is scanned under *its own* epoch, so a segment filed under 2 whose
// header says 3 is still the foreign-epoch error it has always been. The concatenation
// has to be one contiguous run for the same reason a single directory does: a hole means
// records that exist nowhere, and carrying what is above one forward would leave a
// volume at a state it never reached.
//
// It holds every record at once, which is the same shape `scanSegments` already has and
// is bounded by the same number: Limits.MaxLocalBytes, one volume's share of the device.
// It is a *transient* second copy of what the read view is about to hold anyway — replay
// folds each payload into the view either way — so it costs a factor, not an order, and
// only on the attach of a volume that actually held an earlier epoch. Streaming it per
// directory would remove the factor and buy nothing at the sizes this runs at.
func (l *Log) heldBelow(epochs []uint64) ([]Record, []string, error) {
	var (
		held []Record
		dirs []string
	)
	for _, e := range epochs {
		dir := SegmentDir(l.segs.root, l.volumeID, e)
		scan, err := scanSegments(l.segs.d, dir, l.volumeID, e)
		if err != nil {
			return nil, nil, fmt.Errorf("wal: reading the records volume %s holds under epoch %d: %w",
				format.UUIDString(l.volumeID), e, err)
		}
		dirs = append(dirs, dir)
		for _, rec := range scan.records {
			if len(held) > 0 && rec.Sequence != held[len(held)-1].Sequence+1 {
				return nil, nil, fmt.Errorf("%w: sequence %d follows %d under epoch %d",
					ErrCarryHole, rec.Sequence, held[len(held)-1].Sequence, e)
			}
			held = append(held, rec)
		}
	}
	return held, dirs, nil
}

// reencode writes one held record's bytes as they must look under the granted epoch.
//
// Everything the record states is kept — type, sequence, offset, length, flags — and
// only the epoch changes, because that is the single field that made the file unusable
// where it lay. An encrypted WRITE is opened under the epoch it was sealed with (the
// record carries it) and resealed under the granted one, which is a re-encryption and
// not a copy: the nonce is derived from (volume, epoch, sequence).
//
// A WRITE whose KeyID is 0 is a cleartext record and stays one. Re-sealing it because
// this volume happens to hold a DEK would relabel a record the rest of the log describes
// as plaintext, and a volume with a DEK and cleartext records is a defect to report, not
// one to tidy away.
func (l *Log) reencode(rec Record) ([]byte, error) {
	out := Record{
		Type: rec.Type, VolumeID: l.volumeID, Epoch: l.epoch, Sequence: rec.Sequence,
		Offset: rec.Offset, Length: rec.Length, Flags: rec.Flags, Payload: rec.Payload,
	}
	if rec.Type != format.RecordWrite || rec.KeyID == 0 {
		return out.Encode()
	}
	if l.enc == nil {
		return nil, fmt.Errorf("wal: sequence %d is sealed with key version %d and this volume was opened without a key",
			rec.Sequence, rec.KeyID)
	}
	plaintext, err := l.enc.Decrypt(rec)
	if err != nil {
		return nil, fmt.Errorf("wal: opening sequence %d to reseal it under epoch %d: %w", rec.Sequence, l.epoch, err)
	}
	return l.enc.encodeWrite(l.epoch, rec.Sequence, rec.Offset, rec.Flags, plaintext)
}

// lowerEpochs lists the epochs under <root>/<volume>/ that hold segments and are below
// the granted one, ascending.
//
// The listing is deliberately narrow. It is rooted at this volume's directory, so no
// other volume's segments are even enumerated; and an epoch at or above the granted one
// is skipped, because a directory this host has not been granted — or has already moved
// past — is not its business, whatever it holds.
//
// Anything under this volume's directory that is not <digits>/<segment>.seg is an error
// rather than something to step over. This is our directory: a file in it we did not
// write is either a path bug or something else writing here, and both are worth failing
// on before a single byte is deleted.
func lowerEpochs(d disk.Disk, root string, volumeID [16]byte, epoch uint64) ([]uint64, error) {
	prefix := root + "/" + format.UUIDString(volumeID) + "/"
	names, err := d.List(prefix)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, name := range names {
		rest := strings.TrimPrefix(name, prefix)
		dir, file, ok := strings.Cut(rest, "/")
		if !ok {
			return nil, fmt.Errorf("%w: %s is not filed under an epoch", ErrCorruptSegment, name)
		}
		e, err := strconv.ParseUint(dir, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %s is not filed under an epoch: %w", ErrCorruptSegment, name, err)
		}
		if _, err := parseSegmentName(file); err != nil {
			return nil, err
		}
		if e >= epoch || slices.Contains(out, e) {
			continue
		}
		out = append(out, e)
	}
	slices.Sort(out)
	return out, nil
}

// removeSegments unlinks one epoch directory's segments, oldest first, and reports what
// that gave back.
//
// Oldest first is the interruptible order: what survives a crash part way through is a
// suffix of the sequence space, which the next scan reads cleanly, where the reverse
// order would leave a hole in the middle of a directory and fail it outright.
func removeSegments(d disk.Disk, dir string) (int64, error) {
	names, err := listSegments(d, dir)
	if err != nil {
		return 0, fmt.Errorf("wal: listing the drained epoch %s: %w", dir, err)
	}
	var freed int64
	for _, name := range names {
		size, err := segmentSize(d, name)
		if err != nil {
			return freed, err
		}
		if err := d.Remove(name); err != nil {
			return freed, fmt.Errorf("wal: unlink the drained segment %s: %w", name, err)
		}
		freed += size
	}
	// And the directory entry itself, best effort. Disk models files and not
	// directories — the simulator has no such thing and answers ErrNotExist — so this
	// cannot be checked here, and it does not need to be: the segments are what the
	// device budget measures and they are gone. A directory left behind costs an inode
	// and is removed by the next attach that drains this volume.
	_ = d.Remove(dir)
	return freed, nil
}
