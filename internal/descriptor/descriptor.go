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
	"github.com/spin-stack/storage/internal/ids"
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
	// ParentVolumeID is the volume that snapshot belongs to; without it the link above is
	// half a link, since a snapshot lives at image/<volume>/snapshots/<id>.json and is not
	// addressable on its own. It was only in the catalog until agent.parentView started
	// walking a clone's ancestry one descriptor at a time — a reader holding the bucket
	// could not resolve a lineage the bucket is supposed to be the authority for (§22.5,
	// INV-20).
	//
	// Rejected: the whole ancestry in DesiredVolume — it moves the authority for a volume's
	// lineage into the one component -rebuild-metadata exists to survive losing.
	ParentVolumeID string `json:"parent_volume_id,omitempty"`
}

// ErrCorruptDescriptor means the stored bytes disagree with their own digest. It is
// not repaired and not guessed at: a descriptor nobody can state the true contents of
// is exactly what the recovery path must refuse to act on.
var ErrCorruptDescriptor = framed.ErrCorrupt

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
	// And it has to be a v7 UUID, not merely a path component. Every caller of this uses
	// the answer to decide that an object is a volume's descriptor and then *reads* it,
	// so a key anybody can create — `volumes/notes/descriptor.json` — becomes a volume
	// that a rebuild, a sweep or a delete has to account for, and the honest thing each
	// of them then does with an object it cannot parse is refuse the whole operation.
	// One object under a shared prefix could stop every one of them.
	//
	// INV-22 says every id in this system is v7, so this is not a new rule, it is the
	// existing rule applied where the string comes from outside.
	u, err := ids.Parse(id)
	if err != nil || !ids.IsV7(u) {
		return "", false
	}
	return id, true
}

// Write persists (or overwrites) a volume descriptor. Only create (controlplane.Provision)
// and clone (controlplane.Clone) write one; the epoch is bumped in the catalog and does not
// come back here, which is why the epoch object is the authority (see CurrentEpoch).
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
	// The object must describe the volume it was asked for. The digest proves the bytes are
	// the bytes that were written and says nothing about *where*, so a descriptor copied under
	// another volume's prefix passes it intact — and agent.parentChain follows these objects,
	// so a misplaced one sends the lineage walk up another volume's history and every range it
	// layers is another volume's data served to this guest.
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
