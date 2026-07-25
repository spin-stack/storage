// Package checkpoint publishes checkpoints — the durable, verified summary of a
// volume's state up to a sequence — and enforces the objectization order (§21.1):
// a checkpoint is published (create-only) only after its WAL objects are durable in
// S3, and only then may local WAL up to that sequence be truncated (INV-13). Local
// WAL is never truncated above a verified published point.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// Checkpointer publishes checkpoints against an object store.
type Checkpointer struct {
	store objectstore.Store
}

// NewCheckpointer returns a Checkpointer.
func NewCheckpointer(store objectstore.Store) *Checkpointer { return &Checkpointer{store: store} }

// Create publishes a checkpoint at the log's current durable sequence (already
// verified in S3, §14.4) and then advances published_sequence — the strict §21.1
// order. After this the caller may TruncateLocal up to the published point (INV-13).
func (c *Checkpointer) Create(ctx context.Context, log *wal.Log, volumeID [16]byte, epoch uint64) (Checkpoint, error) {
	durable := log.Watermarks().Durable
	objects, err := recovery.ObjectKeysUpTo(ctx, c.store, volumeID, epoch, durable)
	if err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{
		VolumeID:        format.UUIDString(volumeID),
		Epoch:           epoch,
		DurableSequence: durable,
		Objects:         objects,
		RootDigest:      Digest(durable, objects),
	}
	// Publish (verified: create-only) BEFORE advancing published (§21.1).
	if err := Publish(ctx, c.store, cp); err != nil {
		return Checkpoint{}, err
	}
	if err := log.AdvancePublished(durable); err != nil {
		return Checkpoint{}, err
	}
	return cp, nil
}
