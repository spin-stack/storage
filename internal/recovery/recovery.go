// Package recovery reconstructs volume state from S3, the authority for recovery
// (§5.8, §22.1): the durable point is the end of the longest contiguous prefix of
// sequences under wal/<vol>/<epoch>/. Recover replays the WAL objects up to that
// point (decrypting) to rebuild the read view; the summary object accelerates the
// scan (§22.1), and the recovery-point object fixes the epoch frontier (§12.5).
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func walPrefix(volumeID [16]byte, epoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/", format.UUIDString(volumeID), epoch)
}

// ErrObjectIntegrity means a stored WAL object does not match its own header, or
// does not belong to the volume/epoch being recovered. Such an object can never
// raise the durable point (DEV-0003): the prefix ends before it.
var ErrObjectIntegrity = errors.New("recovery: WAL object failed integrity validation")

// walObject is one WAL object with its parsed sequence span and raw bytes. Only
// objects that passed validate() are ever represented here.
type walObject struct {
	key         string
	first, last uint64
	body        []byte
}

// validate checks a stored object against its own header before it is allowed to
// contribute anything to the durable point. The header is self-describing but not
// self-proving: it is CRC-protected, so a *torn* header is caught by the decoder,
// while a torn or rewritten payload — and a header that simply claims more than it
// carries — is caught only here.
//
// The checks, in the order they can fail cheaply:
//
//  1. the object belongs to this volume and epoch (a mis-keyed or restored object
//     must not be read as ours);
//  2. the payload is exactly as long as the header says (truncation);
//  3. the payload digest matches (any corruption, including a rewritten tail);
//  4. the records replay, and their count and their first/last sequences are exactly
//     what the header claims (a header cannot claim sequences it does not carry).
func validate(volumeID [16]byte, epoch uint64, key string, h format.ObjectHeader, payload []byte) error {
	if h.VolumeID != volumeID {
		return fmt.Errorf("%w: %s belongs to volume %s", ErrObjectIntegrity, key, format.UUIDString(h.VolumeID))
	}
	if h.Epoch != epoch {
		return fmt.Errorf("%w: %s belongs to epoch %d, not %d", ErrObjectIntegrity, key, h.Epoch, epoch)
	}
	if uint64(len(payload)) != h.PayloadLength {
		return fmt.Errorf("%w: %s payload is %d bytes, header says %d",
			ErrObjectIntegrity, key, len(payload), h.PayloadLength)
	}
	if sha256.Sum256(payload) != h.PayloadSHA256 {
		return fmt.Errorf("%w: %s payload digest mismatch", ErrObjectIntegrity, key)
	}
	recs, err := wal.Replay(payload)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrObjectIntegrity, key, err)
	}
	if uint32(len(recs)) != h.RecordCount {
		return fmt.Errorf("%w: %s holds %d records, header says %d",
			ErrObjectIntegrity, key, len(recs), h.RecordCount)
	}
	if len(recs) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrObjectIntegrity, key)
	}
	if recs[0].Sequence != h.FirstSequence || recs[len(recs)-1].Sequence != h.LastSequence {
		return fmt.Errorf("%w: %s carries sequences %d..%d, header claims %d..%d",
			ErrObjectIntegrity, key, recs[0].Sequence, recs[len(recs)-1].Sequence,
			h.FirstSequence, h.LastSequence)
	}
	for i, rec := range recs {
		if rec.Sequence != h.FirstSequence+uint64(i) {
			return fmt.Errorf("%w: %s is not a contiguous run at record %d (sequence %d)",
				ErrObjectIntegrity, key, i, rec.Sequence)
		}
	}
	return nil
}

// listObjects fetches every WAL object for a volume/epoch, validates it, and returns
// the ones that passed, sorted by first sequence. An object that fails validation is
// dropped rather than fatal: it simply cannot be part of the durable prefix, and the
// contiguity walk stops where it is missing (§22.1). summary.json /
// recovery-point.json are skipped.
func listObjects(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) ([]walObject, error) {
	infos, err := store.List(ctx, walPrefix(volumeID, epoch))
	if err != nil {
		return nil, err
	}
	var objs []walObject
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, ".wal") {
			continue
		}
		body, err := store.Get(ctx, info.Key)
		if err != nil {
			return nil, err
		}
		if len(body) < format.ObjectHeaderSize {
			return nil, fmt.Errorf("recovery: short WAL object %s", info.Key)
		}
		h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
		if err != nil {
			// A torn header is exactly as untrustworthy as a torn payload.
			continue
		}
		if err := validate(volumeID, epoch, info.Key, h, body[format.ObjectHeaderSize:]); err != nil {
			continue
		}
		objs = append(objs, walObject{key: info.Key, first: h.FirstSequence, last: h.LastSequence, body: body})
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].first < objs[j].first })
	return objs, nil
}

// contiguousLast returns the last sequence of the longest contiguous run that starts
// exactly at floor. The floor matters as much as the contiguity: without it, a bucket
// whose first objects are missing (a partial restore, an aborted GC, a mis-scoped
// lifecycle rule) reads as a healthy prefix starting at whatever survived, and the
// durable point jumps forward over lost data. Objects past a gap are late/orphan
// (§22.1, §12.5).
func contiguousLast(objs []walObject, floor uint64) uint64 {
	expected := floor
	last := floor - 1
	for _, o := range objs {
		if o.first != expected {
			break
		}
		last = o.last
		expected = o.last + 1
	}
	return last
}

// prefixFloor is the first sequence this epoch's WAL must start at: 1 for a volume's
// first epoch, or one past what the previous epoch was recovered up to, which the
// epoch boundary records (§12.5).
func prefixFloor(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) uint64 {
	if rp, err := ReadRecoveryPoint(ctx, store, volumeID, epoch); err == nil {
		return rp.RecoveredUpTo + 1
	}
	return 1
}

// ObjectKeysUpTo returns the keys of the WAL objects whose last sequence is <= upTo,
// in order. Used to build a snapshot manifest (§19) covering a captured sequence.
func ObjectKeysUpTo(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch, upTo uint64) ([]string, error) {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, o := range objs {
		if o.last <= upTo {
			keys = append(keys, o.key)
		}
	}
	return keys, nil
}

// DurablePrefix returns the durable point for a volume/epoch computed from S3 alone
// (INV-08): the end of the longest contiguous run of *validated* objects starting at
// the epoch's floor.
func DurablePrefix(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, error) {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return 0, err
	}
	return contiguousLast(objs, prefixFloor(ctx, store, volumeID, epoch)), nil
}

// DurablePoint is DurablePrefix with a summary cross-check (§22.1): the summary
// object must never claim a durable sequence beyond what the contiguous prefix
// actually provides. In production the summary lets recovery start its LIST near the
// end instead of scanning everything; here it is a correctness guard.
func DurablePoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, error) {
	contiguous, err := DurablePrefix(ctx, store, volumeID, epoch)
	if err != nil {
		return 0, err
	}
	if sum, serr := wal.ReadSummary(ctx, store, volumeID, epoch); serr == nil {
		if sum.DurableSequence > contiguous {
			return 0, fmt.Errorf("recovery: summary claims durable=%d but contiguous prefix reaches only %d",
				sum.DurableSequence, contiguous)
		}
	}
	return contiguous, nil
}

// Recover reconstructs the read view (interval map) from S3 up to the durable point,
// decrypting each record with enc (nil for plaintext volumes). It returns the view
// and the durable sequence.
func Recover(ctx context.Context, store objectstore.Store, enc *wal.Encryption, volumeID [16]byte, epoch uint64) (*cow.IntervalMap, uint64, error) {
	durable, err := DurablePoint(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, 0, err
	}
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, 0, err
	}

	view := cow.NewIntervalMap()
	for _, o := range objs {
		if o.first > durable {
			break // past the durable prefix (sorted by first)
		}
		recs, err := wal.Replay(o.body[format.ObjectHeaderSize:])
		if err != nil {
			return nil, 0, fmt.Errorf("recovery: replay %s: %w", o.key, err)
		}
		for _, rec := range recs {
			if rec.Sequence > durable {
				break
			}
			if err := ApplyRecord(view, enc, rec); err != nil {
				return nil, 0, err
			}
		}
	}
	return view, durable, nil
}

// ApplyRecord folds one replayed record into the read view, decrypting WRITEs when
// the volume is encrypted (enc nil ⇒ plaintext). It is the single definition of
// "replaying a WAL record onto a view", shared by recovery and by cross-host
// materialization (§20, §22).
func ApplyRecord(view *cow.IntervalMap, enc *wal.Encryption, rec wal.Record) error {
	switch rec.Type {
	case format.RecordWrite:
		payload := rec.Payload
		if enc != nil {
			pt, err := enc.Decrypt(rec)
			if err != nil {
				return fmt.Errorf("recovery: decrypt seq %d: %w", rec.Sequence, err)
			}
			payload = pt
		}
		view.Overwrite(rec.Offset, payload)
	case format.RecordDiscard, format.RecordWriteZeroes:
		view.Clear(rec.Offset, uint64(rec.Length))
	}
	return nil
}

// RecoveryPoint is the immutable epoch frontier written by a promoted writer (§12.5).
type RecoveryPoint struct {
	PrevEpoch     uint64 `json:"prev_epoch"`
	RecoveredUpTo uint64 `json:"recovered_up_to"`
}

func recoveryPointKey(volumeID [16]byte, newEpoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/recovery-point.json", format.UUIDString(volumeID), newEpoch)
}

// WriteRecoveryPoint records the epoch boundary at the start of newEpoch (§12.5):
// which prior epoch was recovered and up to which sequence. Create-only.
func WriteRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch, prevEpoch, recoveredUpTo uint64) error {
	body, err := json.Marshal(RecoveryPoint{PrevEpoch: prevEpoch, RecoveredUpTo: recoveredUpTo})
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, recoveryPointKey(volumeID, newEpoch), body, objectstore.PutOptions{IfNoneMatch: true})
	return err
}

// ReadRecoveryPoint loads the epoch-boundary object for newEpoch.
func ReadRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch uint64) (RecoveryPoint, error) {
	var rp RecoveryPoint
	body, err := store.Get(ctx, recoveryPointKey(volumeID, newEpoch))
	if err != nil {
		return rp, err
	}
	if err := json.Unmarshal(body, &rp); err != nil {
		return rp, err
	}
	return rp, nil
}
