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

// Current returns the stored epoch and its ETag (for a subsequent CAS).
func (s *Store) Current(ctx context.Context, volumeID string) (ep uint64, etag string, err error) {
	body, err := s.obj.Get(ctx, Key(volumeID))
	if err != nil {
		return 0, "", err
	}
	var r record
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, "", fmt.Errorf("epoch: decode %s: %w", Key(volumeID), err)
	}
	info, err := s.obj.Head(ctx, Key(volumeID))
	if err != nil {
		return 0, "", err
	}
	return r.Epoch, info.ETag, nil
}

// CompareAndAdvance CASes the epoch object from fromETag to newEpoch (§12.4). A
// concurrent advance (stale ETag) yields ErrCASConflict, which is how the protocol
// guarantees a single promoter wins (INV-10).
func (s *Store) CompareAndAdvance(ctx context.Context, volumeID, fromETag string, newEpoch uint64) (etag string, err error) {
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
