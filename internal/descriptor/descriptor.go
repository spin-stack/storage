// Package descriptor manages the self-describing per-volume object in S3,
// volumes/<vol>/descriptor.json (§22.5). Together with the epoch object,
// recovery-points, and manifests it makes the S3 layout autodescriptive so
// PostgreSQL can be rebuilt from the buckets (rebuild-metadata, INV-20). It holds
// no guest data — only structural metadata — so it may stay in the clear (§15.3).
package descriptor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Descriptor is the durable, self-describing metadata of a volume (§8, §22.5).
type Descriptor struct {
	VolumeID     string `json:"volume_id"`
	SizeBytes    int64  `json:"size_bytes"`
	BlockSize    int32  `json:"block_size"`
	CurrentEpoch int64  `json:"current_epoch"` // last known; the epoch object is authoritative
	ChainDepth   int32  `json:"chain_depth"`
	KEKID        string `json:"kek_id"`
	DEKWrapped   []byte `json:"dek_wrapped"`
	// DEKKeyID is the DEK's version (§15.1). It is here and not only in the catalog
	// because §22.5's rebuild-metadata reads this object to reconstruct a volume the
	// database no longer describes — and a volume rebuilt with its wrapped DEK but
	// without the version that names it is a volume nothing can open. The descriptor
	// stays cleartext (§15.3: manifests and descriptors carry no guest data); the
	// version is not a secret, it is which secret.
	DEKKeyID uint32 `json:"dek_key_id"`
	// ParentSnapshotID is the snapshot this volume was cloned from (§20), empty for a
	// volume that was created rather than cloned. It is here because §22.5's
	// rebuild-metadata reconstructs volumes from these objects: a clone rebuilt
	// without its parent link is a clone that reads zeros, with nothing to say why.
	ParentSnapshotID string `json:"parent_snapshot_id,omitempty"`
}

// ErrCorruptDescriptor means the stored bytes disagree with their own digest. It is
// not repaired and not guessed at: a descriptor nobody can state the true contents of
// is exactly what the recovery path must refuse to act on.
var ErrCorruptDescriptor = errors.New("descriptor: contents do not match the stored digest")

// Digest is SHA-256 over the descriptor's JSON with the digest field itself empty.
//
// Hashing the encoding rather than a hand-written list of fields is deliberate: a field
// added to this struct later is covered automatically, where a hand-written list would
// silently stop covering the struct the moment someone added to it — which is the exact
// failure this closes. encoding/json writes struct fields in declaration order and every
// field here is a scalar or a []byte, so the encoding is deterministic.

// Key is the deterministic descriptor key for a volume.
func Key(volumeID string) string { return "volumes/" + volumeID + "/descriptor.json" }

// Write persists (or overwrites) a volume descriptor.
//
// It is written at create (controlplane.Provision) and at clone (controlplane.Clone),
// and nothing else writes one. It said "updated on resize, epoch change, and
// snapshot-lineage changes" until 2026-08-06, which was true of none of the three: the
// only resize verb was a catalog UPDATE nothing called (now deleted, see metadata.Store),
// the epoch is bumped in the catalog by BumpVolumeEpoch without coming back here, and
// CurrentEpoch below already says the epoch object is the authority. The staleness that
// matters is therefore the epoch's, and it is stated where the field is; a sentence
// promising updates nobody makes is worse than no sentence, because -rebuild-metadata
// reads this object as the truth about a volume the database no longer describes.
func Write(ctx context.Context, store objectstore.Store, d Descriptor) error {
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	body = frame(body)
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
	payload, err := unframe(body)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%s: %w", Key(volumeID), err)
	}
	if err := json.Unmarshal(payload, &d); err != nil {
		return Descriptor{}, fmt.Errorf("descriptor: decode %s: %w", Key(volumeID), err)
	}
	return d, nil
}

// The stored object is `<64 hex chars>\n<json>`: a digest line, then the descriptor.
//
// The digest is over the **bytes as stored**, and that is the whole design rather than a
// detail. The obvious alternative — a `digest` field inside the JSON, recomputed from the
// decoded struct — cannot work, and the property test proved it in one run: flipping one
// bit of the `v` in `"volume_id"` yields `"Volume_id"`, Go's decoder matches field names
// case-insensitively, the struct comes out identical, and re-marshalling it reproduces
// the original digest exactly. A hash over a re-encoding can only ever see what the
// decoder did not normalise away — and unknown fields, duplicate keys, whitespace and
// numeric spellings are all normalised away too.
const digestLen = sha256.Size * 2

func frame(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	out := make([]byte, 0, digestLen+1+len(payload))
	out = append(out, hex.EncodeToString(sum[:])...)
	out = append(out, '\n')
	return append(out, payload...)
}

// unframe splits the digest line off and verifies it.
func unframe(body []byte) ([]byte, error) {
	// A descriptor written before DEV-0015 was closed has no digest line at all, and it
	// is refused with the same error as a corrupt one. Nothing is deployed, so there is
	// no such object anywhere to be lenient for, and a lenient branch would leave the
	// hole open permanently for the sake of a volume that does not exist.
	if len(body) < digestLen+1 || body[digestLen] != '\n' {
		return nil, fmt.Errorf("%w: no digest line", ErrCorruptDescriptor)
	}
	stored := string(body[:digestLen])
	payload := body[digestLen+1:]
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != stored {
		return nil, fmt.Errorf("%w: stored %s, contents hash to %s", ErrCorruptDescriptor, stored, got)
	}
	return payload, nil
}
