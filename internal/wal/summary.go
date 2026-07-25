package wal

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// SummaryObject records one durable WAL object's key and sequence range.
type SummaryObject struct {
	Key   string `json:"key"`
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
}

// Summary is the small object written periodically (§22.1) so recovery is a couple
// of GETs plus a short LIST rather than a massive LIST. It records the last durable
// sequence and the object/range list for one (volume, epoch).
type Summary struct {
	VolumeID        string          `json:"volume_id"`
	Epoch           uint64          `json:"epoch"`
	DurableSequence uint64          `json:"durable_sequence"`
	Objects         []SummaryObject `json:"objects"`
}

// SummaryKey is the deterministic key of a volume/epoch summary (§22.1).
func SummaryKey(volumeID [16]byte, epoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/summary.json", format.UUIDString(volumeID), epoch)
}

// WriteSummary persists the current summary (last durable sequence + the objects
// uploaded so far). It overwrites the previous summary (latest wins).
func (l *Log) WriteSummary(ctx context.Context) error {
	if l.uploader == nil {
		return nil
	}
	s := Summary{
		VolumeID:        format.UUIDString(l.volumeID),
		Epoch:           l.epoch,
		DurableSequence: l.durable,
		Objects:         append([]SummaryObject(nil), l.uploaded...),
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = l.uploader.store.Put(ctx, SummaryKey(l.volumeID, l.epoch), data, objectstore.PutOptions{})
	return err
}

// ReadSummary loads and parses a summary from the store (used by recovery, §22.1).
func ReadSummary(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (Summary, error) {
	var s Summary
	data, err := store.Get(ctx, SummaryKey(volumeID, epoch))
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, err
	}
	return s, nil
}
