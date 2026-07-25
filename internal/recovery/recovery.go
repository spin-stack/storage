// Package recovery determines the durable point of a volume from S3, which is the
// authority for recovery (§5.8, §22.1): the durable point is the end of the longest
// contiguous prefix of sequences under wal/<vol>/<epoch>/. It also writes the
// recovery-point object that fixes the immutable frontier between epochs (§12.5).
// This is the seed of Phase 08; here it is enough to prove that a promoted writer
// recovers every write its fenced predecessor ACKed as durable (INV-09).
package recovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// walPrefix is the object-store prefix for a volume/epoch's WAL objects.
func walPrefix(volumeID string, epoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/", volumeID, epoch)
}

// objRange is one WAL object's sequence span, parsed from its header.
type objRange struct {
	key         string
	first, last uint64
}

// DurablePrefix lists the WAL objects for a volume/epoch and returns the last
// sequence of the longest contiguous prefix (starting at the lowest first
// sequence). Objects beyond a gap are ignored — they are late/orphan PUTs (§22.1,
// §12.5). Returns 0 if there are no WAL objects.
func DurablePrefix(ctx context.Context, store objectstore.Store, volumeID string, epoch uint64) (uint64, error) {
	infos, err := store.List(ctx, walPrefix(volumeID, epoch))
	if err != nil {
		return 0, err
	}
	var ranges []objRange
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, ".wal") {
			continue // skip summary.json, recovery-point.json, ...
		}
		body, err := store.Get(ctx, info.Key)
		if err != nil {
			return 0, err
		}
		if len(body) < format.ObjectHeaderSize {
			return 0, fmt.Errorf("recovery: short WAL object %s", info.Key)
		}
		h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
		if err != nil {
			return 0, fmt.Errorf("recovery: decode %s: %w", info.Key, err)
		}
		ranges = append(ranges, objRange{key: info.Key, first: h.FirstSequence, last: h.LastSequence})
	}
	if len(ranges) == 0 {
		return 0, nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].first < ranges[j].first })

	// Walk the contiguous run from the lowest first sequence.
	last := ranges[0].last
	expected := ranges[0].last + 1
	for _, r := range ranges[1:] {
		if r.first != expected {
			break // gap: everything beyond is late/orphan
		}
		last = r.last
		expected = r.last + 1
	}
	return last, nil
}

// RecoveryPoint is the immutable epoch frontier written by a promoted writer (§12.5).
type RecoveryPoint struct {
	PrevEpoch     uint64 `json:"prev_epoch"`
	RecoveredUpTo uint64 `json:"recovered_up_to"`
}

// recoveryPointKey is the key of the epoch-boundary object (§12.5).
func recoveryPointKey(volumeID string, newEpoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/recovery-point.json", volumeID, newEpoch)
}

// WriteRecoveryPoint records the boundary between epochs at the start of newEpoch:
// which prior epoch was recovered and up to which sequence (§12.5). Create-only, so
// a promotion writes it exactly once.
func WriteRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID string, newEpoch, prevEpoch, recoveredUpTo uint64) error {
	body, err := json.Marshal(RecoveryPoint{PrevEpoch: prevEpoch, RecoveredUpTo: recoveredUpTo})
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, recoveryPointKey(volumeID, newEpoch), body, objectstore.PutOptions{IfNoneMatch: true})
	return err
}

// ReadRecoveryPoint loads the epoch-boundary object for newEpoch.
func ReadRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID string, newEpoch uint64) (RecoveryPoint, error) {
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
