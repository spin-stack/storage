// Package snapshot implements crash-consistent, pause-free snapshots (§19): a
// snapshot is a captured sequence number, not an event that drains queues. The
// capture is O(1) (the only guest pause, ≈ 0); sealing — flushing to the captured
// sequence and publishing an immutable manifest — happens in the background. Once
// PUBLISHED a snapshot never changes (§5.2, INV-16): the manifest is create-only and
// the WAL objects it references are append-only.
//
// Publishing is also exclusive: a manifest names the WAL objects of one epoch, and that
// epoch belongs to the host it was *granted* to, not to every host that happens to hold
// the same epoch number (§12.4). Create gates on that with recovery.VerifyPublisher, so
// a Snapshotter has to say which host it speaks for — see HeldBy.
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

// Digest is the content digest over the referenced state (target + object keys).
// A consumer that materializes a snapshot elsewhere recomputes it to check the
// manifest is self-consistent before fetching a byte (§20, INV-16).
func Digest(targetSequence uint64, objects []string) string {
	buf := fmt.Appendf(nil, "seq=%d\n", targetSequence)
	for _, k := range objects {
		buf = append(buf, k...)
		buf = append(buf, '\n')
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// DigestMatches reports whether the manifest's RootDigest matches its own contents.
func (m Manifest) DigestMatches() bool {
	return m.RootDigest == Digest(m.TargetSequence, m.Objects)
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

// Snapshotter creates snapshots against an object store, on behalf of one host.
type Snapshotter struct {
	store objectstore.Store
	clk   clock.Clock
	// hostID is the host this snapshotter publishes for, checked against the epoch's
	// holder. Empty means "would not say", which cannot be reconciled with any grant
	// and is refused for every epoch that has one.
	hostID string
}

// NewSnapshotter returns a Snapshotter that does not name the host it publishes for.
// It can only snapshot an epoch nobody was granted, or a volume with no epoch object at
// all (§12.4, §22.5) — see recovery.VerifyPublisher. Anything that snapshots a live
// volume knows which host it is: use HeldBy.
func NewSnapshotter(store objectstore.Store, clk clock.Clock) *Snapshotter {
	return &Snapshotter{store: store, clk: clk}
}

// HeldBy returns a Snapshotter that publishes as hostID, which the epoch object must
// name as the holder of the epoch being snapshotted. The receiver is left alone, so a
// single Snapshotter can be scoped per volume without sharing mutable state.
func (s *Snapshotter) HeldBy(hostID string) *Snapshotter {
	scoped := *s
	scoped.hostID = hostID
	return &scoped
}

// Create takes a crash-consistent snapshot of log's volume at its current sequence.
// It returns the published manifest and the guest-perceived pause (the O(1) capture),
// which must be ≈ 0. Writes with sequence > TargetSequence are not in the snapshot.
//
// The epoch is verified once, before the publish, and deliberately not again after it.
// That is the difference from checkpoint.Create, where the second read is load-bearing:
// there the publish is followed by AdvancePublished, the step that authorises discarding
// the last local copy of the data (INV-13, §21.1), so a writer fenced mid-flight still
// has something left to refuse. Here Publish is the last thing Create does, and what it
// writes is create-only and immutable (INV-16). A second read could observe a fence and
// report it, but it could not unwrite the manifest or take back its status as a GC root
// (§21.3, ADR-0012) — it would only turn a completed publication into an error, which is
// worse than the truthful "this manifest exists". The gate has to be the pre-check.
//
// A snapshot never moves published and never authorises a truncation, which is why it
// read as the harmless publication and was left out of the wave-3 sweep. The damage is
// of a different shape: a host the epoch was never granted to publishes an immutable
// manifest naming another host's WAL objects, pinning its own view of that epoch
// forever, and every clone taken from it rebuilds a state the live volume never had.
func (s *Snapshotter) Create(ctx context.Context, log *wal.Log, volumeID [16]byte, epoch uint64, snapshotID, parentID string) (Manifest, time.Duration, error) {
	// The pause: capture the sequence atomically. No I/O, no clock advance.
	pauseStart := s.clk.Now()
	target := log.Watermarks().Local
	pause := s.clk.Now().Sub(pauseStart)

	vid := format.UUIDString(volumeID)
	if err := recovery.VerifyPublisher(ctx, s.store, vid, epoch, s.hostID); err != nil {
		return Manifest{}, pause, fmt.Errorf("snapshot: %s may not snapshot epoch %d: %w", vid, epoch, err)
	}

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
		VolumeID:         vid,
		Epoch:            epoch,
		TargetSequence:   target,
		ParentSnapshotID: parentID,
		Objects:          objects,
		RootDigest:       Digest(target, objects),
	}
	if err := Publish(ctx, s.store, m); err != nil {
		return Manifest{}, pause, err
	}
	return m, pause, nil
}
