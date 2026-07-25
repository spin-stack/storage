// Package descriptor manages the self-describing per-volume object in S3,
// volumes/<vol>/descriptor.json (§22.5). Together with the epoch object,
// recovery-points, and manifests it makes the S3 layout autodescriptive so
// PostgreSQL can be rebuilt from the buckets (rebuild-metadata, INV-20). It holds
// no guest data — only structural metadata — so it may stay in the clear (§15.3).
package descriptor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Descriptor is the durable, self-describing metadata of a volume (§8, §22.5).
type Descriptor struct {
	VolumeID     string `json:"volume_id"`
	SizeBytes    int64  `json:"size_bytes"`
	BlockSize    int32  `json:"block_size"`
	Durability   string `json:"durability"`
	CurrentEpoch int64  `json:"current_epoch"` // last known; the epoch object is authoritative
	ChainDepth   int32  `json:"chain_depth"`
	KEKID        string `json:"kek_id"`
	DEKWrapped   []byte `json:"dek_wrapped"`
}

// Key is the deterministic descriptor key for a volume.
func Key(volumeID string) string { return "volumes/" + volumeID + "/descriptor.json" }

// Write persists (or overwrites) a volume descriptor. It is updated on resize, epoch
// change, and snapshot-lineage changes.
func Write(ctx context.Context, store objectstore.Store, d Descriptor) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, Key(d.VolumeID), body, objectstore.PutOptions{})
	return err
}

// Read loads a volume descriptor.
func Read(ctx context.Context, store objectstore.Store, volumeID string) (Descriptor, error) {
	var d Descriptor
	body, err := store.Get(ctx, Key(volumeID))
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(body, &d); err != nil {
		return d, fmt.Errorf("descriptor: decode %s: %w", Key(volumeID), err)
	}
	return d, nil
}

// ListVolumeIDs scans the buckets and returns every volume id that has a descriptor.
func ListVolumeIDs(ctx context.Context, store objectstore.Store) ([]string, error) {
	infos, err := store.List(ctx, "volumes/")
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, info := range infos {
		// keys look like volumes/<vol>/descriptor.json
		if !strings.HasSuffix(info.Key, "/descriptor.json") {
			continue
		}
		rest := strings.TrimPrefix(info.Key, "volumes/")
		id := strings.TrimSuffix(rest, "/descriptor.json")
		if id != "" && !strings.Contains(id, "/") {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
