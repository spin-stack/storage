// Package snapshot implements crash-consistent, pause-free snapshots (§19): a
// snapshot is a captured sequence number, not an event that drains queues. The
// capture is O(1) (the only guest pause, ≈ 0); sealing — flushing to the captured
// sequence and publishing an immutable manifest — happens in the background. Once
// PUBLISHED a snapshot never changes (§5.2, INV-16): the manifest is create-only and
// the WAL objects it references are append-only.
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Manifest is the immutable description of a published snapshot (§8, §19). It
// references the WAL objects covering sequences up to TargetSequence; segments are
// added by objectization (Phase 10).
type Manifest struct {
	SnapshotID       string   `json:"snapshot_id"`
	VolumeID         string   `json:"volume_id"`
	Epoch            uint64   `json:"epoch"`
	TargetSequence   uint64   `json:"target_sequence"`
	ParentSnapshotID string   `json:"parent_snapshot_id,omitempty"`
	RootDigest       string   `json:"root_digest"`
	Objects          []string `json:"objects"`
}

// ManifestKey is the deterministic manifest key for a snapshot.
func ManifestKey(volumeID, snapshotID string) string {
	return fmt.Sprintf("snapshots/%s/%s/manifest.json", volumeID, snapshotID)
}

// rootDigest is a content digest over the referenced state (target + object keys).
func rootDigest(targetSequence uint64, objects []string) string {
	buf := fmt.Appendf(nil, "seq=%d\n", targetSequence)
	for _, k := range objects {
		buf = append(buf, k...)
		buf = append(buf, '\n')
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// Publish writes the manifest create-only, so a published snapshot is immutable
// (INV-16): a second publish at the same key fails.
func Publish(ctx context.Context, store objectstore.Store, m Manifest) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, ManifestKey(m.VolumeID, m.SnapshotID), body, objectstore.PutOptions{IfNoneMatch: true})
	return err
}

// Read loads a snapshot manifest.
func Read(ctx context.Context, store objectstore.Store, volumeID, snapshotID string) (Manifest, error) {
	var m Manifest
	body, err := store.Get(ctx, ManifestKey(volumeID, snapshotID))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return m, err
	}
	return m, nil
}

// Snapshotter creates snapshots against an object store.
type Snapshotter struct {
	store objectstore.Store
	clk   clock.Clock
}

// NewSnapshotter returns a Snapshotter.
func NewSnapshotter(store objectstore.Store, clk clock.Clock) *Snapshotter {
	return &Snapshotter{store: store, clk: clk}
}

// Create takes a crash-consistent snapshot of log's volume at its current sequence.
// It returns the published manifest and the guest-perceived pause (the O(1) capture),
// which must be ≈ 0. Writes with sequence > TargetSequence are not in the snapshot.
func (s *Snapshotter) Create(ctx context.Context, log *wal.Log, volumeID [16]byte, epoch uint64, snapshotID, parentID string) (Manifest, time.Duration, error) {
	// The pause: capture the sequence atomically. No I/O, no clock advance.
	pauseStart := s.clk.Now()
	target := log.Watermarks().Local
	pause := s.clk.Now().Sub(pauseStart)

	// Sealing (background class): make the captured prefix durable, then build the
	// manifest from the objects covering it.
	if err := log.Flush(ctx); err != nil {
		return Manifest{}, pause, err
	}
	objects, err := recovery.ObjectKeysUpTo(ctx, s.store, volumeID, epoch, target)
	if err != nil {
		return Manifest{}, pause, err
	}
	m := Manifest{
		SnapshotID:       snapshotID,
		VolumeID:         format.UUIDString(volumeID),
		Epoch:            epoch,
		TargetSequence:   target,
		ParentSnapshotID: parentID,
		Objects:          objects,
		RootDigest:       rootDigest(target, objects),
	}
	if err := Publish(ctx, s.store, m); err != nil {
		return Manifest{}, pause, err
	}
	return m, pause, nil
}
