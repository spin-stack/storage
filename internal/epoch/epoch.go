// Package epoch manages the small S3 "epoch object" that is the belt-and-suspenders
// fence of §12.4: volumes/<vol>/epoch holds the current fenced epoch and is advanced
// by CAS (If-Match on ETag). Low-frequency publications (checkpoint, manifest,
// compaction, promotion) validate it; it is NOT written on every WAL PUT, where the
// monotonic-clock lease (§12.2) already suffices.
//
// If the backend lacks conditional writes the protocol stays safe by §12.2 + the PG
// term (§7); the epoch object is defense in depth.
package epoch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Errors.
var (
	// ErrCASConflict means the epoch object changed under us (a concurrent advance).
	ErrCASConflict = errors.New("epoch: CAS conflict")
	// ErrEpochChanged means the stored epoch no longer matches the caller's — the
	// caller has been fenced (§12.5, §18 "old writer returns").
	ErrEpochChanged = errors.New("epoch: epoch changed (fenced)")
	// ErrEpochNotAdvancing means an advance would re-use or lower the stored epoch.
	// Epoch numbers are the namespace of the WAL objects (wal/<vol>/<epoch>/), so
	// re-issuing one puts two writers in one namespace and recovery can only read
	// the result as a truncated or spliced history (§12.4, INV-08/INV-10).
	ErrEpochNotAdvancing = errors.New("epoch: new epoch does not advance the stored one")
)

// record is the on-S3 epoch object body (self-describing for rebuild-metadata).
type record struct {
	VolumeID string `json:"volume_id"`
	Epoch    uint64 `json:"epoch"`
}

// Store reads and writes epoch objects.
type Store struct {
	obj objectstore.Store
}

// NewStore returns an epoch store over an object store.
func NewStore(obj objectstore.Store) *Store { return &Store{obj: obj} }

// Key is the deterministic epoch-object key for a volume.
func Key(volumeID string) string { return "volumes/" + volumeID + "/epoch" }

// Init create-only writes the initial epoch (§22.5). It fails if it already exists.
func (s *Store) Init(ctx context.Context, volumeID string, ep uint64) (etag string, err error) {
	body, err := json.Marshal(record{VolumeID: volumeID, Epoch: ep})
	if err != nil {
		return "", err
	}
	res, err := s.obj.Put(ctx, Key(volumeID), body, objectstore.PutOptions{IfNoneMatch: true})
	if err != nil {
		return "", err
	}
	return res.ETag, nil
}

// Current returns the stored epoch and the ETag a subsequent CompareAndAdvance must
// be made against.
//
// The pair has to come from one version of the object. The interface has no read
// that returns body and ETag together, so the body is read between two metadata
// reads and the ETags must agree: otherwise the caller would pair a *stale* epoch
// with the *current* ETag, and the CAS built from that pair wins — handing an epoch
// to a second host while the promoter that granted it is already recovering
// (§12.4, INV-10). A read that could not be made stable returns ErrCASConflict: the
// caller is racing another promoter and must re-read.
func (s *Store) Current(ctx context.Context, volumeID string) (ep uint64, etag string, err error) {
	key := Key(volumeID)
	before, err := s.obj.Head(ctx, key)
	if err != nil {
		return 0, "", err
	}
	body, err := s.obj.Get(ctx, key)
	if err != nil {
		return 0, "", err
	}
	after, err := s.obj.Head(ctx, key)
	if err != nil {
		return 0, "", err
	}
	if before.ETag != after.ETag {
		return 0, "", fmt.Errorf("%w: %s changed while it was being read", ErrCASConflict, key)
	}
	var r record
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, "", fmt.Errorf("epoch: decode %s: %w", key, err)
	}
	return r.Epoch, after.ETag, nil
}

// CompareAndAdvance CASes the epoch object from fromETag to newEpoch (§12.4). A
// concurrent advance (stale ETag) yields ErrCASConflict, which is how the protocol
// guarantees a single promoter wins (INV-10).
//
// The advance must be strictly forward. Re-issuing an epoch — after a bucket
// rollback or a restore, or from a caller that recomputed its target from state that
// moved under it — is refused here (ErrEpochNotAdvancing) rather than left to caller
// convention: nothing downstream can tell two writers apart once they share a
// wal/<vol>/<epoch>/ namespace.
func (s *Store) CompareAndAdvance(ctx context.Context, volumeID, fromETag string, newEpoch uint64) (etag string, err error) {
	stored, currentETag, err := s.Current(ctx, volumeID)
	if err != nil {
		return "", err
	}
	if currentETag != fromETag {
		return "", fmt.Errorf("%w: %s moved on", ErrCASConflict, Key(volumeID))
	}
	if newEpoch <= stored {
		return "", fmt.Errorf("%w: %d -> %d", ErrEpochNotAdvancing, stored, newEpoch)
	}
	body, err := json.Marshal(record{VolumeID: volumeID, Epoch: newEpoch})
	if err != nil {
		return "", err
	}
	res, err := s.obj.Put(ctx, Key(volumeID), body, objectstore.PutOptions{IfMatch: fromETag})
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return "", ErrCASConflict
	}
	if err != nil {
		return "", err
	}
	return res.ETag, nil
}

// Verify checks that the stored epoch still equals expected; otherwise the caller
// has been fenced and must not publish (§12.4, §18). A low-frequency publish calls
// this before writing a checkpoint/manifest.
func (s *Store) Verify(ctx context.Context, volumeID string, expected uint64) error {
	ep, _, err := s.Current(ctx, volumeID)
	if err != nil {
		return err
	}
	if ep != expected {
		return ErrEpochChanged
	}
	return nil
}
