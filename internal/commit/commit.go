// Package commit is what a published commit is made of: the immutable manifest that
// describes one layer, and the single mutable object that says which manifest is
// current.
//
// # What a commit promises, and what these three objects do about it
//
// `Commit() → SUCCESS` promises that this state is reconstructible without the host that
// wrote it. Everything here serves that one sentence:
//
//   - the **layer** is the bytes, content-addressed by the digest of the object as
//     stored, so writing it again is the same object and never a second one;
//   - the **manifest** names that layer and its parent commit, is written create-only,
//     and can therefore be retried without a thought;
//   - **HEAD** is the only object in this design that is ever overwritten, and it is
//     overwritten only by compare-and-set. It is the whole of the mutual exclusion two
//     hosts get at the object store (INV-10), and a bit turned over in it is the entire
//     volume — which is why it carries a digest line like everything else here.
//
// The order is `PUT layer → PUT manifest → CAS HEAD`, never the reverse (v6 §9). What
// each reversal costs is in Publish.
//
// # Why there is no `created_at`
//
// v6 §12 drew one. A commit id is a v7 UUID (INV-22), which *is* a timestamp — a
// millisecond one, in the leading 48 bits, and the reason the ids are v7 at all is so
// that ordering and time are properties of the identifier rather than fields beside it.
// A second timestamp would have to come from a wall clock this Agent does not have
// (INV-01 gives it a monotonic instant, which cannot be rendered as a date) and would be
// a field that can disagree with the id it sits next to. §12 was corrected.
package commit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// The failures a caller branches on. Each is a different next move, and the two that
// look alike are the ones worth separating: a HEAD that moved under us is a fencing
// question for a human, while a manifest that is already there is the ordinary shape of
// a retry.
var (
	// ErrHeadMoved means the compare-and-set lost: something else published while this
	// commit was being assembled. **Nothing overwrites HEAD after this.** With a correct
	// single writer it cannot happen, so it is a fencing failure, a concurrent recovery,
	// or a bug (v6 §15) — and every one of those is made worse by taking the object.
	ErrHeadMoved = errors.New("commit: this host is not this volume's writer any more")
	// ErrManifestConflict means a *different* manifest already occupies this commit id.
	// A retry of our own is not this — that is byte-identical and reported as success.
	//
	// It wraps ErrHeadMoved, and that is not tidiness. With v7 ids two hosts cannot pick
	// one commit id by chance, so the only way here is a host publishing under an id
	// another host has already used for different content — which is the same statement
	// as a HEAD that moved: this host is not the volume's writer any more. It was its own
	// unrelated error until an adversary took a volume away from a host mid-commit and
	// watched it be told something nothing fences on, and keep the guest's disk.
	ErrManifestConflict = fmt.Errorf("%w: another manifest already exists at this commit id", ErrHeadMoved)
	// ErrNoHead means the volume has never published a commit.
	ErrNoHead = errors.New("commit: this volume has no HEAD")
	// ErrCorrupt is what a stored object that disagrees with its own digest gives back.
	ErrCorrupt = framed.ErrCorrupt
)

// Layer describes the one object a commit adds.
type Layer struct {
	// ObjectKey is where the sealed bytes are. Derived from SHA256 and stored anyway:
	// a reader must not have to recompute a key to find an object, and the day the
	// key scheme changes, old manifests still say where their layer went.
	ObjectKey string `json:"object_key"`
	// SizeBytes is the sealed object's length, not the qcow2's.
	SizeBytes int64 `json:"size"`
	// SHA256 is over the object **as stored** — sealed, framed, exactly the bytes a
	// GET returns. That is the half of integrity a recovery can check with no key
	// material at all, which is what v6 §10 asks of `rebuild-metadata`; the GCM tag on
	// every frame is the other half and needs the DEK.
	SHA256 string `json:"sha256"`
	// FrameBytes is the plaintext size of each sealed frame. It is recorded per layer
	// rather than fixed by the format so that changing it is not a migration: a layer
	// written before the change is opened with the number it carries.
	FrameBytes int32 `json:"frame_bytes"`
	// LayerID is the v7 UUID the Agent minted when it rotated this layer into being. It
	// is in the AAD of every frame, so it is not decoration: a layer moved under
	// another layer's manifest fails to open rather than decrypting into the wrong
	// chain.
	LayerID string `json:"layer_id"`
}

// Manifest is one published commit. Immutable: written create-only, never updated.
type Manifest struct {
	FormatVersion int    `json:"format_version"`
	VolumeID      string `json:"volume_id"`
	CommitID      string `json:"commit_id"`
	// ParentCommitID is the commit this one is layered on, empty for the first. A
	// recovery walks this chain to find every layer it must download (v6 §14); the
	// chain is not copied into each manifest, because a list that is rewritten on every
	// commit is a list that can be wrong, and walking costs one GET per layer that has
	// to be downloaded anyway.
	ParentCommitID string `json:"parent_commit_id,omitempty"`
	// Epoch is the fencing token the writer held (v6 §13). Recorded so that a published
	// history can be read back and audited: a commit whose epoch is below its parent's
	// was written by a host that had already been fenced.
	Epoch int64 `json:"epoch"`
	// VirtualSize is the guest-visible size of the volume this commit reconstructs. A
	// recovery creates the new active tip at this size, and it must come from the
	// commit rather than from a catalog row the recovery may be running without.
	VirtualSize int64 `json:"virtual_size"`
	Layer       Layer `json:"layer"`
}

// Head is the one mutable object: which commit is current.
type Head struct {
	FormatVersion int    `json:"format_version"`
	VolumeID      string `json:"volume_id"`
	CommitID      string `json:"commit_id"`
}

// LayerKey is where a sealed layer lives, addressed by the digest of the bytes as
// stored.
//
// Global, not under the volume's prefix, and that is deliberate against CLAUDE.md's
// usual rule that a volume's id names everything under its prefix. A lineage shares one
// DEK, so a clone's chain references layers its parent wrote; under the parent's prefix,
// deleting the parent would delete its clones' data. Content addressing also makes a
// re-upload a no-op instead of an orphan, which is what makes step 11 of v6 §9 safe to
// retry.
func LayerKey(sha256hex string) string {
	return "layers/sha256/" + sha256hex[0:2] + "/" + sha256hex[2:4] + "/" + sha256hex
}

// ManifestKey is where one commit's manifest lives.
func ManifestKey(volumeID, commitID string) string {
	return "volumes/" + volumeID + "/commits/" + commitID + ".json"
}

// HeadKey is the volume's one mutable object.
func HeadKey(volumeID string) string { return "volumes/" + volumeID + "/HEAD" }

// ErrBadIdentifier means an object carries an id that is not a v7 UUID where one is
// required.
//
// It is checked at both boundaries — on the way out and on the way in — because these
// strings do not stay strings. A commit id becomes an object key, a layer id becomes a
// *filesystem path* on whichever host rebuilds the volume, and a manifest is a document
// somebody else may have written into the bucket. Adversarial tests found both ends of
// that: a manifest whose layer_id was `../../<other volume>/layers/<id>` made a restore
// create and delete a file outside the volume's directory, and a Publish called with an
// empty commit id wrote `commits/.json` and pointed HEAD at "".
//
// The check is UUID-shaped rather than "no slashes" on purpose. INV-22 says every id in
// this system is a v7 UUID, so anything else is already wrong, and a rule that lists the
// characters an attacker may not use is a rule that is one encoding away from being
// wrong.
var ErrBadIdentifier = errors.New("commit: an identifier is not a UUID")

// checkID refuses anything that is not a UUID. An empty string is refused too, except
// where a caller has said it is allowed — a first commit has no parent.
func checkID(what, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return fmt.Errorf("%w: %s is %q", ErrBadIdentifier, what, id)
	}
	return nil
}

// validate checks every identifier in a manifest, at whichever boundary it is crossing.
func (m Manifest) validate() error {
	if err := checkID("volume_id", m.VolumeID); err != nil {
		return err
	}
	if err := checkID("commit_id", m.CommitID); err != nil {
		return err
	}
	if m.ParentCommitID != "" {
		if err := checkID("parent_commit_id", m.ParentCommitID); err != nil {
			return err
		}
	}
	if m.ParentCommitID == m.CommitID {
		// A commit that is its own parent is a chain a walk never leaves. It cannot be
		// produced by Publish any more, and it was produced by Publish once.
		return fmt.Errorf("%w: commit %s is its own parent", ErrBadIdentifier, m.CommitID)
	}
	if err := checkID("layer_id", m.Layer.LayerID); err != nil {
		return err
	}
	// And the layer's key must be the one its own digest names. The key is what a reader
	// GETs; a manifest that named some other object would send a recovery to bytes this
	// system never wrote, and the digest check afterwards would blame the object store.
	if want := LayerKey(m.Layer.SHA256); m.Layer.ObjectKey != want {
		return fmt.Errorf("%w: commit %s names layer object %q, and its digest names %q",
			ErrBadIdentifier, m.CommitID, m.Layer.ObjectKey, want)
	}
	return nil
}

// Digest is the SHA-256 of the bytes as stored, hex, which is what a layer's key and a
// manifest's `sha256` are both built from.
func Digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// WriteManifest publishes a commit manifest create-only, and treats a retry of the same
// manifest as the success it is.
//
// The idempotency is not a convenience. Step 12 of v6 §9 can be interrupted by anything
// — the process, the host, the network — and the recovery from that is to do the whole
// commit again. An object store that refused the second attempt would turn every
// interrupted commit into an operator's problem; one that overwrote would let a
// second writer replace a manifest a reader may already have followed.
//
// So: create-only, and if the key is taken, read what is there. Byte-identical is our
// own retry landing twice. Anything else is somebody having used this commit id for
// different content, which with v7 ids cannot happen by chance.
func WriteManifest(ctx context.Context, store objectstore.Store, m Manifest) error {
	// Stamped over whatever the caller put here rather than trusted, for the reason
	// descriptor.Write states: a struct built by hand with a zero version would be
	// written as one, and the whole point of the field is that it cannot be absent. `m`
	// is this function's own copy, so the caller's value is untouched.
	m.FormatVersion = framed.FormatVersion
	if err := m.validate(); err != nil {
		return err
	}
	body, err := marshal(m)
	if err != nil {
		return err
	}
	key := ManifestKey(m.VolumeID, m.CommitID)
	_, err = store.Put(ctx, key, body, objectstore.PutOptions{IfNoneMatch: true})
	if err == nil {
		return nil
	}
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("commit: publishing %s: %w", key, err)
	}
	existing, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("commit: reading the manifest already at %s: %w", key, err)
	}
	if string(existing) != string(body) {
		return fmt.Errorf("%w: %s", ErrManifestConflict, key)
	}
	return nil
}

// ReadManifest loads one commit.
func ReadManifest(ctx context.Context, store objectstore.Store, volumeID, commitID string) (Manifest, error) {
	key := ManifestKey(volumeID, commitID)
	body, err := store.Get(ctx, key)
	if err != nil {
		return Manifest{}, fmt.Errorf("commit: reading %s: %w", key, err)
	}
	var m Manifest
	if err := unmarshal(key, body, &m, &m.FormatVersion); err != nil {
		return Manifest{}, err
	}
	// The object must describe what it was asked for. The digest proves the bytes are
	// the bytes that were written and says nothing about *where*: a manifest copied or
	// restored under another volume's prefix passes it intact, and every layer it names
	// is then attributed to the wrong volume — which is another tenant's disk served to
	// this guest. The same check, for the same reason, is in descriptor.Read.
	if m.VolumeID != volumeID || m.CommitID != commitID {
		return Manifest{}, fmt.Errorf("commit: %s describes volume %s commit %s", key, m.VolumeID, m.CommitID)
	}
	// And every identifier in it, before the caller turns one into a path. The two checks
	// above say the object is the one that was asked for; this one says the object is
	// one this system could have written.
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", key, err)
	}
	return m, nil
}

// ReadHead returns the volume's current commit and the ETag to compare against when
// replacing it. The ETag is returned rather than looked up again at write time, because
// a CAS against an ETag read after the decision is a CAS against nothing.
func ReadHead(ctx context.Context, store objectstore.Store, volumeID string) (Head, string, error) {
	key := HeadKey(volumeID)
	info, err := store.Head(ctx, key)
	if errors.Is(err, objectstore.ErrNotFound) {
		return Head{}, "", ErrNoHead
	}
	if err != nil {
		return Head{}, "", fmt.Errorf("commit: heading %s: %w", key, err)
	}
	body, err := store.Get(ctx, key)
	if err != nil {
		return Head{}, "", fmt.Errorf("commit: reading %s: %w", key, err)
	}
	var h Head
	if err := unmarshal(key, body, &h, &h.FormatVersion); err != nil {
		return Head{}, "", err
	}
	if h.VolumeID != volumeID {
		return Head{}, "", fmt.Errorf("commit: %s names volume %s", key, h.VolumeID)
	}
	// The one field the whole chain walk is driven by, and the one nothing checked: a
	// HEAD naming "" read back cleanly and then panicked whoever followed it.
	if err := checkID("commit_id", h.CommitID); err != nil {
		return Head{}, "", fmt.Errorf("%s: %w", key, err)
	}
	return h, info.ETag, nil
}

// CASHead moves HEAD to commitID, and only if it still holds what the caller read.
//
// An empty etag means "there was no HEAD", written create-only. Both are the same
// promise from the object store: this write happens against the state I looked at, or it
// does not happen. It is the last line of mutual exclusion between two hosts that both
// believe they own the volume, and `task backend:conformance` is blocking per backend
// because a store that quietly ignores the precondition makes it silently absent.
//
// A precondition failure is not immediately a conflict. v6 §15 has the case: the write
// took effect and the answer was lost, and a retry then reads its own success as
// somebody else's. So HEAD is re-read, and if it already names this commit the publish
// is reported as the success it was. Only a HEAD naming something else is ErrHeadMoved,
// and nothing here overwrites that.
func CASHead(ctx context.Context, store objectstore.Store, volumeID, commitID, etag string) error {
	if err := checkID("volume_id", volumeID); err != nil {
		return err
	}
	if err := checkID("commit_id", commitID); err != nil {
		return err
	}
	h := Head{FormatVersion: framed.FormatVersion, VolumeID: volumeID, CommitID: commitID}
	body, err := marshal(h)
	if err != nil {
		return err
	}
	opts := objectstore.PutOptions{IfNoneMatch: etag == "", IfMatch: etag}
	_, err = store.Put(ctx, HeadKey(volumeID), body, opts)
	if err == nil {
		return nil
	}
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("commit: setting %s: %w", HeadKey(volumeID), err)
	}
	current, _, readErr := ReadHead(ctx, store, volumeID)
	if readErr == nil && current.CommitID == commitID {
		return nil
	}
	return fmt.Errorf("%w: it was %q and now names %q", ErrHeadMoved, etag, current.CommitID)
}

// marshal frames the JSON. Callers stamp the version into their own value first — see
// WriteManifest — because a helper that took a pointer to the field stamped the caller's
// struct and serialised the copy it had already been handed. The property test caught it
// on its first run, which is the whole reason that test draws the version rather than
// setting it correctly.
func marshal(v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return framed.Frame(body), nil
}

// unmarshal checks the digest, decodes, and checks the version *before* the caller reads
// a field.
//
// The order is the whole of it. json.Unmarshal silently discards fields it does not
// know, so an object from a newer format decodes without complaint into whatever subset
// this binary understands — a commit whose layer has a field this build never heard of,
// followed anyway. The version check is what turns that into a refusal.
func unmarshal(key string, body []byte, v any, version *int) error {
	payload, err := framed.Unframe(body)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return fmt.Errorf("commit: decode %s: %w", key, err)
	}
	if err := framed.CheckVersion(*version); err != nil {
		return fmt.Errorf("commit %s: %w", key, err)
	}
	return nil
}

// ReadHeadCommit is ReadHead for a caller that wants the commit and not the ETag.
func ReadHeadCommit(ctx context.Context, store objectstore.Store, volumeID string) (string, error) {
	h, _, err := ReadHead(ctx, store, volumeID)
	return h.CommitID, err
}
