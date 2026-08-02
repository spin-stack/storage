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
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/obs"
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

// ErrSnapshotConflict means this snapshot id already names a *different* manifest. It
// is not a retry of our own publish — a manifest is immutable (INV-16) — so it is two
// snapshots claiming one id, which nothing can reconcile.
var ErrSnapshotConflict = errors.New("snapshot: a different manifest already exists at this id")

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
	// rec records §19's two mandatory metrics. Nil is a working no-op.
	rec *obs.Recorder
}

// SetRecorder attaches the metric recorder. §19 makes two of them mandatory —
// snapshot_pause_duration_seconds, which must stay ~0, and
// snapshot_publish_duration_seconds — and internal/obs has registered both since Phase 01
// with nothing observing either.
func (s *Snapshotter) SetRecorder(r *obs.Recorder) { s.rec = r }

// Captured is a snapshot that has been *taken* but not yet sealed: the §19 pause has
// happened, the sequence is fixed, and everything durable is still ahead of it.
//
// It exists because §19 splits a snapshot in two — "capturar atómicamente N (µs)" and
// then, "en background", making N durable and publishing the manifest — and a single
// blocking call cannot express that. It carries the state the lifecycle vocabulary
// already had and nothing ever produced: a captured snapshot is CREATING until Seal
// publishes it (PUBLISHED) or the caller records the failure (FAILED).
type Captured struct {
	SnapshotID     string
	ParentID       string
	VolumeID       [16]byte
	Epoch          uint64
	TargetSequence uint64
	// Pause is what the capture cost the guest. §19's headline property is that this
	// stays ~0, and it is ~0 for a structural reason rather than a lucky one: the
	// capture reads a watermark and does no I/O.
	Pause time.Duration
	// State is CREATING. It is here so a caller recording the snapshot in the catalog
	// does not have to know which constant to reach for.
	State lifecycle.SnapshotState
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
	c := s.Capture(ctx, log, volumeID, epoch, snapshotID, parentID)
	m, err := s.Seal(ctx, log, c)
	return m, c.Pause, err
}

// Capture is §19 step 1: fix the sequence, and nothing else.
//
// It does no I/O at all — it reads a watermark — which is why the pause it reports is ~0
// for a structural reason rather than a lucky one, and why the guest keeps writing
// through a snapshot at sequences > TargetSequence. Everything expensive is Seal's.
func (s *Snapshotter) Capture(ctx context.Context, log *wal.Log, volumeID [16]byte, epoch uint64, snapshotID, parentID string) Captured {
	start := s.clk.Now()
	target := log.Watermarks().Local
	pause := s.clk.Now().Sub(start)

	vid := format.UUIDString(volumeID)
	s.rec.Observe(ctx, "snapshot_pause_duration_seconds", pause.Seconds(), obs.String("volume", vid))
	return Captured{
		SnapshotID: snapshotID, ParentID: parentID, VolumeID: volumeID,
		Epoch: epoch, TargetSequence: target, Pause: pause,
		State: lifecycle.SnapshotCreating,
	}
}

// Seal is §19 step 3: make the captured prefix durable and publish the manifest.
//
// It is the part that belongs in the background, and it is deliberately *not* run in one
// here. "In background" is a property of the caller — the Agent has io-class budgets to
// spend it against (INV-17), and spin's runner may own the lifecycle instead (ADR-0021) —
// so a `go` statement in this package would be a policy decision taken in a library.
//
// Safe to retry after a crash: the manifest is published create-only, so a second Seal
// either wins or finds its own manifest already there, and the objects it lists are
// derived from the same target sequence.
func (s *Snapshotter) Seal(ctx context.Context, log *wal.Log, c Captured) (Manifest, error) {
	started := s.clk.Now()
	vid := format.UUIDString(c.VolumeID)
	if err := recovery.VerifyPublisher(ctx, s.store, vid, c.Epoch, s.hostID); err != nil {
		return Manifest{}, fmt.Errorf("snapshot: %s may not snapshot epoch %d: %w", vid, c.Epoch, err)
	}
	if err := log.Flush(ctx); err != nil {
		return Manifest{}, err
	}
	objects, err := recovery.ObjectKeysUpTo(ctx, s.store, c.VolumeID, c.Epoch, c.TargetSequence)
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{
		SnapshotID:       c.SnapshotID,
		VolumeID:         vid,
		Epoch:            c.Epoch,
		TargetSequence:   c.TargetSequence,
		ParentSnapshotID: c.ParentID,
		Objects:          objects,
		RootDigest:       Digest(c.TargetSequence, objects),
	}
	if err := Publish(ctx, s.store, m); err != nil {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			return Manifest{}, err
		}
		// The manifest is already there. Sealing is meant to run in the background
		// (§19 step 3), so the process doing it can die between the PUT and whatever
		// records PUBLISHED — and the retry that follows must converge, not fail.
		// Failing would be the worst of the three outcomes: the manifest exists, is
		// immutable (INV-16) and is a GC root (§21.3), and the caller would write
		// FAILED next to it.
		//
		// Convergence is only safe if it is *our* manifest: a different one at this key
		// means two snapshots claiming one id, which no retry can reconcile.
		existing, rerr := Read(ctx, s.store, vid, c.SnapshotID)
		if rerr != nil {
			return Manifest{}, fmt.Errorf("snapshot: %s exists but could not be read: %w", c.SnapshotID, rerr)
		}
		if existing.RootDigest != m.RootDigest {
			return Manifest{}, fmt.Errorf("%w: snapshot %s already exists with a different root digest (%s, not %s)",
				ErrSnapshotConflict, c.SnapshotID, existing.RootDigest, m.RootDigest)
		}
		m = existing
	}
	s.rec.Observe(ctx, "snapshot_publish_duration_seconds",
		s.clk.Now().Sub(started).Seconds(), obs.String("volume", vid))
	return m, nil
}
