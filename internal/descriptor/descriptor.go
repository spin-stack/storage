// Package descriptor manages the self-describing per-volume object in S3,
// volumes/<vol>/descriptor.json (§22.5). Together with the epoch object,
// recovery-points, and manifests it makes the S3 layout autodescriptive so
// PostgreSQL can be rebuilt from the buckets (rebuild-metadata, INV-20). It holds
// no guest data — only structural metadata — so it may stay in the clear (§15.3).
package descriptor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Descriptor is the durable, self-describing metadata of a volume (§8, §22.5).
type Descriptor struct {
	// FormatVersion is framed.FormatVersion at the time this object was written. It is
	// first so that a human catting the object sees it before anything else, and so that
	// a future reader that has to sniff before decoding finds it in the first bytes.
	FormatVersion int    `json:"format_version"`
	VolumeID      string `json:"volume_id"`
	SizeBytes     int64  `json:"size_bytes"`
	BlockSize     int32  `json:"block_size"`
	CurrentEpoch  int64  `json:"current_epoch"` // last known; the epoch object is authoritative
	ChainDepth    int32  `json:"chain_depth"`
	KEKID         string `json:"kek_id"`
	DEKWrapped    []byte `json:"dek_wrapped"`
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
	// ParentVolumeID is the volume that snapshot belongs to, and without it the link
	// above is only half a link. A snapshot is not addressable on its own — it lives at
	// image/<volume>/snapshots/<id>.json (image.SnapshotKey) — so "which snapshot"
	// without "whose" names nothing a reader can open. Until this field existed the
	// other half lived only in the catalog (snapshots.volume_id), which meant a reader
	// holding the database could resolve a lineage and a reader holding only the bucket
	// could not, while every claim about these objects says the bucket is the authority
	// a rebuild trusts (§22.5, INV-20).
	//
	// The reader that made it necessary is agent.parentView, which walks a clone's
	// ancestry one descriptor at a time: the desired state names the first link (ADR-0021
	// — the Agent is told, it does not look things up), and every link above it comes
	// from here.
	//
	// Rejected: putting the whole ancestry in DesiredVolume. It would save this field and
	// one GET per link, and it makes the Control Plane responsible for bounding the
	// length of a list in a message, and it moves the authority for a volume's lineage
	// out of the object store — into the one component whose loss -rebuild-metadata
	// exists to survive.
	ParentVolumeID string `json:"parent_volume_id,omitempty"`
}

// ErrCorruptDescriptor means the stored bytes disagree with their own digest. It is
// not repaired and not guessed at: a descriptor nobody can state the true contents of
// is exactly what the recovery path must refuse to act on.
var ErrCorruptDescriptor = framed.ErrCorrupt

// Digest is SHA-256 over the descriptor's JSON with the digest field itself empty.
//
// Hashing the encoding rather than a hand-written list of fields is deliberate: a field
// added to this struct later is covered automatically, where a hand-written list would
// silently stop covering the struct the moment someone added to it — which is the exact
// failure this closes. encoding/json writes struct fields in declaration order and every
// field here is a scalar or a []byte, so the encoding is deterministic.

// Key is the deterministic descriptor key for a volume.
func Key(volumeID string) string { return "volumes/" + volumeID + "/descriptor.json" }

// Prefix is where every volume's descriptor lives. Two callers list it — the rebuild,
// to find the volumes a lost database no longer describes, and the lineage walk's
// inverse, to find what descends from a volume about to be deleted — and neither may
// spell it itself: a listing of the wrong prefix answers "nothing" rather than failing,
// and both callers read that answer as a fact about the fleet.
const Prefix = "volumes/"

// VolumeOfKey extracts the volume id from `volumes/<id>/descriptor.json`, reporting
// false for any other key under the prefix.
//
// It is the inverse of Key and lives beside it for the reason this repository has
// already paid for once with the KEK file: two components that parse the same string
// two ways disagree silently, and here the disagreement would be a volume that a
// deletion's descendant scan skips — which is the one question that scan exists to
// answer.
func VolumeOfKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, Prefix)
	if !ok {
		return "", false
	}
	id, ok := strings.CutSuffix(rest, "/descriptor.json")
	if !ok || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}

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
	// Stamped here rather than trusted from the caller: a Descriptor built by hand with a
	// zero version would be written as one, and the whole point of the field is that it
	// cannot be absent.
	d.FormatVersion = framed.FormatVersion
	body, err := json.Marshal(d)
	if err != nil {
		return err
	}
	body = framed.Frame(body)
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
	payload, err := framed.Unframe(body)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%s: %w", Key(volumeID), err)
	}
	if err := json.Unmarshal(payload, &d); err != nil {
		return Descriptor{}, fmt.Errorf("descriptor: decode %s: %w", Key(volumeID), err)
	}
	// Before anything is read out of the struct. json.Unmarshal silently discards fields
	// it does not know, so a descriptor from a newer format decodes without complaint into
	// whatever subset this binary happens to understand — a volume's geometry, wrapped key
	// and parent link, quietly missing whatever was added.
	if err := framed.CheckVersion(d.FormatVersion); err != nil {
		return Descriptor{}, fmt.Errorf("descriptor %s: %w", Key(volumeID), err)
	}
	// The object must describe the volume it was asked for. The digest above proves the
	// bytes are the bytes that were written; it says nothing about *where*, so a
	// descriptor copied or restored under another volume's prefix passes it intact — and
	// it is a whole volume's identity, geometry, wrapped key and parent link, every one
	// of which a reader then attributes to the wrong volume.
	//
	// The same check, for the same reason, is in image.readManifest ("what catches a
	// bucket copied under the wrong prefix, and every reader needs it"). It matters more
	// here since agent.parentChain started following these objects: a descriptor under
	// the wrong key sends the walk up a lineage that is not this volume's, and every
	// range it then layers is another volume's data served to this guest.
	if d.VolumeID != volumeID {
		return Descriptor{}, fmt.Errorf("descriptor: %s describes volume %s", Key(volumeID), d.VolumeID)
	}
	return d, nil
}

// The stored object is framed by internal/framed: a digest line over the bytes as
// stored, then the JSON. That package carries the reasoning, including why a digest
// field *inside* the JSON cannot work — this package's own property test is what proved
// it. It is the shape the new design's HEAD and commit manifest are built out of: a
// small mutable JSON object under compare-and-set, and an immutable one beside it.
