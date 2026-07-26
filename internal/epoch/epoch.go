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
	// ErrNotHolder means the epoch object is at the expected number but was granted
	// to a different host — or to nobody. The caller has not been fenced by a *newer*
	// epoch; it was never given this one, which a number-only check cannot tell apart
	// (§12.4).
	ErrNotHolder = errors.New("epoch: the epoch is held by another host")
)

// Record is the on-S3 epoch object body (self-describing for rebuild-metadata).
//
// HolderID is the host the epoch was granted to. The number alone says *when* a
// writer was fenced but never *who* holds the current epoch, and the two durable
// records of a promotion — volumes.primary_host_id in PostgreSQL and this object —
// are written by two different steps of §12.3. A promoter that crashed between them,
// or one that was overtaken, can leave them naming different hosts at the same
// epoch; both hosts then pass a number-only check and publish into the same
// wal/<vol>/<epoch>/ namespace. It is empty for an epoch nobody has been granted:
// a volume that was just created or rebuilt (§22.5).
type Record struct {
	VolumeID string `json:"volume_id"`
	Epoch    uint64 `json:"epoch"`
	HolderID string `json:"holder_id,omitempty"`
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
// The epoch it writes is held by nobody: a volume that has just been created or
// rebuilt has no writer, and the first holder is named by the first Grant.
func (s *Store) Init(ctx context.Context, volumeID string, ep uint64) (etag string, err error) {
	body, err := json.Marshal(Record{VolumeID: volumeID, Epoch: ep})
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
	r, etag, err := s.CurrentRecord(ctx, volumeID)
	return r.Epoch, etag, err
}

// CurrentRecord is Current with the holder: the whole object plus the ETag a
// subsequent Grant must be made against.
func (s *Store) CurrentRecord(ctx context.Context, volumeID string) (Record, string, error) {
	key := Key(volumeID)
	before, err := s.obj.Head(ctx, key)
	if err != nil {
		return Record{}, "", err
	}
	body, err := s.obj.Get(ctx, key)
	if err != nil {
		return Record{}, "", err
	}
	after, err := s.obj.Head(ctx, key)
	if err != nil {
		return Record{}, "", err
	}
	if before.ETag != after.ETag {
		return Record{}, "", fmt.Errorf("%w: %s changed while it was being read", ErrCASConflict, key)
	}
	var r Record
	if err := json.Unmarshal(body, &r); err != nil {
		return Record{}, "", fmt.Errorf("epoch: decode %s: %w", key, err)
	}
	return r, after.ETag, nil
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
	return s.Grant(ctx, volumeID, fromETag, newEpoch, "")
}

// Grant is CompareAndAdvance naming the host the new epoch is granted to (§12.3
// step 4b). Publishing is gated on VerifyHolder, so the name written here is what
// decides which of two hosts that both believe they are at this epoch may write.
// An empty holderID advances the number without naming an owner, which leaves
// nobody able to publish — correct for the epoch of a volume that has no writer.
func (s *Store) Grant(ctx context.Context, volumeID, fromETag string, newEpoch uint64, holderID string) (etag string, err error) {
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
	body, err := json.Marshal(Record{VolumeID: volumeID, Epoch: newEpoch, HolderID: holderID})
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
//
// It answers the number only. A caller that knows which host it is speaking for —
// a promoter, or an Agent about to publish — uses VerifyHolder instead: two hosts
// can believe they are at the same epoch, and only one of them was granted it.
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

// VerifyHolder is Verify plus "and it is yours": the stored epoch must equal
// expected *and* have been granted to holderID. An epoch that was never granted to
// anybody (a freshly created or rebuilt volume) is held by nobody, so this fails
// closed with ErrNotHolder — the record does not name you.
func (s *Store) VerifyHolder(ctx context.Context, volumeID string, expected uint64, holderID string) error {
	r, _, err := s.CurrentRecord(ctx, volumeID)
	if err != nil {
		return err
	}
	if r.Epoch != expected {
		return ErrEpochChanged
	}
	if r.HolderID == "" || r.HolderID != holderID {
		return fmt.Errorf("%w: epoch %d of %s is held by %q, not %q",
			ErrNotHolder, r.Epoch, volumeID, r.HolderID, holderID)
	}
	return nil
}
