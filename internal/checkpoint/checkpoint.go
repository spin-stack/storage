// Package checkpoint publishes checkpoints — the durable, verified summary of a
// volume's state up to a sequence — and enforces the objectization order (§21.1):
// a checkpoint is published (create-only) only after its WAL objects are durable in
// S3, and only then may local WAL up to that sequence be truncated (INV-13). Local
// WAL is never truncated above a verified published point.
//
// Publishing is also exclusive: checkpoints/<vol>/<epoch>/ belongs to the host the
// epoch was *granted* to, not to every host that happens to hold the same epoch number
// (§12.4). Create gates on that with recovery.VerifyPublisher, so a Checkpointer has to
// say which host it speaks for — see HeldBy.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Checkpoint is the durable state summary at DurableSequence (§21.1). Objects are
// the WAL object keys it covers (segments are added by full objectization later).
type Checkpoint struct {
	VolumeID        string   `json:"volume_id"`
	Epoch           uint64   `json:"epoch"`
	DurableSequence uint64   `json:"durable_sequence"`
	RootDigest      string   `json:"root_digest"`
	Objects         []string `json:"objects"`
}

// ErrDurablePointMismatch means the object store holds a longer durable prefix than
// the log that is checkpointing ever ACKed — two writers in one epoch, or the wrong
// log. Publishing under that condition would put one writer's name on another's data.
var ErrDurablePointMismatch = errors.New("checkpoint: durable point disagrees with the log")

// ErrCheckpointConflict means a *different* checkpoint already occupies this
// (volume, epoch, sequence). A checkpoint at a sequence is immutable, so this is not
// a retry of our own publish: it is two writers claiming one epoch.
var ErrCheckpointConflict = errors.New("checkpoint: a different checkpoint already exists at this sequence")

// Key is the deterministic checkpoint key.
func Key(volumeID string, epoch, seq uint64) string {
	return fmt.Sprintf("checkpoints/%s/%d/%d.json", volumeID, epoch, seq)
}

// Digest is the content digest over the covered state (sequence + object keys). A
// consumer that rebuilds a volume from a checkpoint recomputes it to check the
// checkpoint is self-consistent before fetching a byte (§20, §22.3).
func Digest(seq uint64, objects []string) string {
	buf := fmt.Appendf(nil, "seq=%d\n", seq)
	for _, k := range objects {
		buf = append(buf, k...)
		buf = append(buf, '\n')
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// DigestMatches reports whether the checkpoint's RootDigest matches its contents.
func (c Checkpoint) DigestMatches() bool {
	return c.RootDigest == Digest(c.DurableSequence, c.Objects)
}

// Publish writes a checkpoint create-only (a checkpoint at a sequence is immutable).
func Publish(ctx context.Context, store objectstore.Store, cp Checkpoint) error {
	body, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, Key(cp.VolumeID, cp.Epoch, cp.DurableSequence), body, objectstore.PutOptions{IfNoneMatch: true})
	return err
}

// Read loads a checkpoint.
func Read(ctx context.Context, store objectstore.Store, volumeID string, epoch, seq uint64) (Checkpoint, error) {
	var cp Checkpoint
	body, err := store.Get(ctx, Key(volumeID, epoch, seq))
	if err != nil {
		return cp, err
	}
	if err := json.Unmarshal(body, &cp); err != nil {
		return cp, err
	}
	return cp, nil
}

// Checkpointer publishes checkpoints against an object store, on behalf of one host.
type Checkpointer struct {
	store objectstore.Store
	// hostID is the host this checkpointer publishes for, checked against the epoch's
	// holder. Empty means "would not say", which cannot be reconciled with any grant
	// and is refused for every epoch that has one.
	hostID string
}

// NewCheckpointer returns a Checkpointer that does not name the host it publishes for.
// It can only publish into an epoch nobody was granted, or into a volume with no epoch
// object at all (§12.4, §22.5) — see recovery.VerifyPublisher. Anything that publishes
// for a live volume knows which host it is: use HeldBy.
func NewCheckpointer(store objectstore.Store) *Checkpointer { return &Checkpointer{store: store} }

// HeldBy returns a Checkpointer that publishes as hostID, which the epoch object must
// name as the holder of the epoch being checkpointed. The receiver is left alone, so a
// single Checkpointer can be scoped per volume without sharing mutable state.
func (c *Checkpointer) HeldBy(hostID string) *Checkpointer {
	scoped := *c
	scoped.hostID = hostID
	return &scoped
}

// Create publishes a checkpoint and then advances published_sequence — the strict
// §21.1 order. After this the caller may TruncateLocal up to the published point,
// which is what makes the local copy disposable (INV-13).
//
// The sequence it publishes is the one S3 can *prove* right now, not the log's own
// durable watermark. The watermark was set when the Agent's PUTs returned; the prefix
// can have fallen behind it since (a lost object, a mis-scoped lifecycle rule, a
// corrupt upload). Publishing the higher number would authorise discarding local WAL
// whose only remaining copy was local — and the root digest cannot catch it, because
// it hashes key strings and cannot express a hole.
//
// The epoch is verified twice, and the two checks stop different things.
//
// Before: checkpoints/<vol>/<epoch>/ is a namespace exactly one host may claim, and
// two hosts can hold the same epoch *number* while only one was granted the epoch
// (§12.3–12.4). This is the check that keeps the second one out.
//
// After the publish and before AdvancePublished: the epoch object can be CASed while
// the PUT is in flight. The published object cannot be unwritten — it is create-only —
// but advancing published is a separate, later, and far more expensive act: it is what
// lets local WAL be truncated (INV-13, §21.1), and a writer fenced mid-publish would
// otherwise discard the last local copy of data on the strength of a publication it no
// longer owns. Refusing there leaves the checkpoint object in S3, which is harmless —
// it is byte-identical to what the epoch's real holder recomputes, and publishOrAdopt
// treats it as its own retry.
func (c *Checkpointer) Create(ctx context.Context, log *wal.Log, volumeID [16]byte, epoch uint64) (Checkpoint, error) {
	vid := format.UUIDString(volumeID)
	if err := recovery.VerifyPublisher(ctx, c.store, vid, epoch, c.hostID); err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: %s may not publish into epoch %d: %w", vid, epoch, err)
	}
	claimed := log.Watermarks().Durable
	durable, err := recovery.DurablePoint(ctx, c.store, volumeID, epoch)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("checkpoint: cannot establish the durable point: %w", err)
	}
	if durable > claimed {
		// S3 holds more than this writer ever ACKed: another writer is publishing
		// into the same epoch, or the log is not the one that wrote these objects.
		return Checkpoint{}, fmt.Errorf("%w: S3 proves %d but this log ACKed only %d",
			ErrDurablePointMismatch, durable, claimed)
	}
	objects, err := recovery.ObjectKeysUpTo(ctx, c.store, volumeID, epoch, durable)
	if err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{
		VolumeID:        vid,
		Epoch:           epoch,
		DurableSequence: durable,
		Objects:         objects,
		RootDigest:      Digest(durable, objects),
	}
	// Publish (verified: create-only) BEFORE advancing published (§21.1).
	if err := c.publishOrAdopt(ctx, cp); err != nil {
		return Checkpoint{}, err
	}
	if err := recovery.VerifyPublisher(ctx, c.store, vid, epoch, c.hostID); err != nil {
		return Checkpoint{}, fmt.Errorf(
			"checkpoint: %s/%d/%d was published but the epoch moved before it could authorise truncation: %w",
			vid, epoch, durable, err)
	}
	if err := log.AdvancePublished(durable); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}

// publishOrAdopt performs the create-only publish, treating a checkpoint that is
// already there and identical as done.
//
// The two steps of Create are separated by a crash boundary. If the PUT persists but
// its response is lost (§14.5, §23), or the process dies before AdvancePublished,
// every later attempt computes the same checkpoint and hits the create-only
// precondition. Reporting that as a failure leaves published stuck, so local WAL for
// the volume can never be truncated while it stays idle (INV-13) — the host's NVMe
// fills and stalls writes for that volume and every co-tenant on the disk. Retrying
// has to converge.
//
// A *different* checkpoint at the same sequence is the opposite case: a checkpoint at
// a sequence is immutable, so this is not our own retry but two writers claiming one
// epoch. It is reported as ErrCheckpointConflict rather than as an ordinary
// precondition failure, and published does not move.
func (c *Checkpointer) publishOrAdopt(ctx context.Context, cp Checkpoint) error {
	err := Publish(ctx, c.store, cp)
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return err
	}
	existing, rerr := Read(ctx, c.store, cp.VolumeID, cp.Epoch, cp.DurableSequence)
	if rerr != nil {
		return fmt.Errorf("checkpoint: %s exists but cannot be read: %w",
			Key(cp.VolumeID, cp.Epoch, cp.DurableSequence), rerr)
	}
	if existing.RootDigest != cp.RootDigest || existing.Epoch != cp.Epoch || existing.VolumeID != cp.VolumeID {
		return fmt.Errorf("%w: %s holds digest %s, this log computed %s",
			ErrCheckpointConflict, Key(cp.VolumeID, cp.Epoch, cp.DurableSequence),
			existing.RootDigest, cp.RootDigest)
	}
	return nil
}
