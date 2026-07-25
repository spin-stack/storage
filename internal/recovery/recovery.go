// Package recovery reconstructs volume state from S3, the authority for recovery
// (§5.8, §22.1): the durable point is the end of the longest contiguous prefix of
// sequences under wal/<vol>/<epoch>/. Recover replays the WAL objects up to that
// point (decrypting) to rebuild the read view; the summary object accelerates the
// scan (§22.1), and the recovery-point object fixes the epoch frontier (§12.5).
package recovery

import (
	"context"
	"encoding/json"
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

// walObject is one WAL object with its parsed sequence span and raw bytes.
type walObject struct {
	key         string
	first, last uint64
	body        []byte
}

// listObjects fetches every WAL object for a volume/epoch, parses its header, and
// returns them sorted by first sequence. summary.json / recovery-point.json are
// skipped.
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
			return nil, fmt.Errorf("recovery: decode %s: %w", info.Key, err)
		}
		objs = append(objs, walObject{key: info.Key, first: h.FirstSequence, last: h.LastSequence, body: body})
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].first < objs[j].first })
	return objs, nil
}

// contiguousLast returns the last sequence of the longest contiguous prefix (from
// the lowest first sequence). Objects past a gap are late/orphan (§22.1, §12.5).
func contiguousLast(objs []walObject) uint64 {
	if len(objs) == 0 {
		return 0
	}
	last := objs[0].last
	expected := objs[0].last + 1
	for _, o := range objs[1:] {
		if o.first != expected {
			break
		}
		last = o.last
		expected = o.last + 1
	}
	return last
}

// DurablePrefix returns the durable point for a volume/epoch computed from S3 alone
// (INV-08): the end of the longest contiguous WAL prefix.
func DurablePrefix(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, error) {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return 0, err
	}
	return contiguousLast(objs), nil
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
			if err := applyRecord(view, enc, rec); err != nil {
				return nil, 0, err
			}
		}
	}
	return view, durable, nil
}

func applyRecord(view *cow.IntervalMap, enc *wal.Encryption, rec wal.Record) error {
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
