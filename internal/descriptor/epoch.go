package descriptor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrNoEpoch means the bucket holds no epoch for this volume.
var ErrNoEpoch = errors.New("descriptor: this volume has no recorded epoch")

// Epoch is the fencing token a volume was last granted, recorded where it survives the
// catalog. A rebuild used to read `descriptor.CurrentEpoch`, which is written at create and
// at clone and updated by nothing, so a volume fenced up to epoch 4 came back at 1 and every
// host that had held it held a token the restored catalog would accept again.
//
// Not folded into descriptor.json: that file carries the wrapped DEK, and rewriting it on
// every attach would put key material on a path that runs whenever a volume moves, to record
// an integer. Losing this object costs an over-fence, which is the safe side.
//
// Not ADR-0026's withdrawn epoch object either: that one was a fence two writers raced at,
// replaced by the compare-and-set on HEAD. This is a record, read by one caller.
type Epoch struct {
	FormatVersion int    `json:"format_version"`
	VolumeID      string `json:"volume_id"`
	Epoch         int64  `json:"epoch"`
}

// EpochKey is where a volume's epoch is recorded.
func EpochKey(volumeID string) string { return Prefix + volumeID + "/epoch" }

// WriteEpoch records the epoch a volume has been granted.
//
// Written **before** the catalog is moved, and that order is the whole of its safety. A
// crash in between leaves the bucket holding a number the catalog has not reached, so a
// rebuild restores an epoch at or above the truth — which can over-fence a host that
// would have been allowed to write, and can never under-fence one that must not. The
// other order leaves the bucket behind the catalog, which is the direction that hands a
// predecessor a live token.
//
// Unconditional, with no precondition: two Control Planes racing to grant one volume are
// already decided by the catalog's own term-guarded compare-and-set, and a precondition
// here would only add a second way for that decision to be reported.
func WriteEpoch(ctx context.Context, store objectstore.Store, volumeID string, epoch int64) error {
	body, err := json.Marshal(Epoch{
		FormatVersion: framed.FormatVersion, VolumeID: volumeID, Epoch: epoch,
	})
	if err != nil {
		return err
	}
	if _, err := store.Put(ctx, EpochKey(volumeID), framed.Frame(body), objectstore.PutOptions{}); err != nil {
		return fmt.Errorf("descriptor: recording epoch %d for volume %s: %w", epoch, volumeID, err)
	}
	return nil
}

// ReadEpoch returns the recorded epoch, or ErrNoEpoch.
func ReadEpoch(ctx context.Context, store objectstore.Store, volumeID string) (int64, error) {
	body, err := store.Get(ctx, EpochKey(volumeID))
	if errors.Is(err, objectstore.ErrNotFound) {
		return 0, ErrNoEpoch
	}
	if err != nil {
		return 0, fmt.Errorf("descriptor: reading %s: %w", EpochKey(volumeID), err)
	}
	payload, err := framed.Unframe(body)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", EpochKey(volumeID), err)
	}
	var e Epoch
	if err := json.Unmarshal(payload, &e); err != nil {
		return 0, fmt.Errorf("descriptor: decode %s: %w", EpochKey(volumeID), err)
	}
	if err := framed.CheckVersion(e.FormatVersion); err != nil {
		return 0, fmt.Errorf("%s: %w", EpochKey(volumeID), err)
	}
	// The object must describe the volume it was asked for, for the reason descriptor.Read
	// states: the digest proves the bytes are the bytes that were written and says nothing
	// about where, and an epoch attributed to the wrong volume is a fence set from
	// somebody else's history.
	if e.VolumeID != volumeID {
		return 0, fmt.Errorf("descriptor: %s records volume %s", EpochKey(volumeID), e.VolumeID)
	}
	return e.Epoch, nil
}

// EpochWitness answers what epoch the object store records for a volume, as a method so its
// one caller — an Agent whose lease has lapsed, deciding whether anybody took its volumes —
// depends on an interface declared where it is consumed (agent.Witness). A missing object
// answers ErrNoEpoch and not zero: "no record" and "granted at epoch 0" are opposite facts,
// and the caller stops a guest on one of them.
type EpochWitness struct {
	Store objectstore.Store
}

// GrantedEpoch returns the epoch recorded for the volume.
func (w EpochWitness) GrantedEpoch(ctx context.Context, volumeID string) (int64, error) {
	return ReadEpoch(ctx, w.Store, volumeID)
}
