package commit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrLayerKeyTaken means something occupies a layer's content-addressed key with content
// that is not this layer. It cannot happen from this code — the key *is* the digest of
// the bytes — so it is a bucket somebody else has written into, and publishing a manifest
// that points at it would name an object this Agent never wrote.
var ErrLayerKeyTaken = errors.New("commit: the layer's key holds an object of a different size")

// Request is everything one commit needs that is not the bytes.
type Request struct {
	VolumeID string
	// CommitID is minted by the caller and **reused across retries of the same commit**.
	// That is what makes a retry idempotent rather than a second commit: the manifest is
	// byte-identical, so create-only accepts it as the retry it is, and a CAS whose
	// answer was lost is recognised because HEAD already names this id. A fresh id per
	// attempt would publish the same layer twice and read its own success as somebody
	// else's conflict.
	CommitID string
	// LayerID is the layer being published. It is in the nonce of every sealed frame.
	LayerID string
	// Epoch is the fencing token this host holds (v6 §13).
	Epoch int64
	// VirtualSize is the guest-visible size a recovery must recreate the tip at.
	VirtualSize int64
	// PlainBytes is the sealed layer's plaintext length, used to size the buffer.
	PlainBytes int64
	// FrameBytes is the sealing frame; zero means crypto.LayerFrameBytes.
	FrameBytes int
}

// Publish performs steps 9 to 13 of v6 §9 for one sealed layer: digest it, upload it,
// publish an immutable manifest, and compare-and-set HEAD onto it.
//
//	PUT layer → PUT commit manifest → CAS HEAD
//
// Never the reverse. In this order an interruption leaves an object nothing points at,
// which is garbage a sweep collects; with the CAS anywhere but last it leaves HEAD naming
// a manifest that is not there — a commit that was acknowledged and cannot be
// reconstructed, the one thing `Commit() → SUCCESS` rules out.
//
// Only "the CAS is last" is load-bearing: planting the other swap, the manifest before
// its layer, turned nothing red. Layer-first is kept anyway so that an existing manifest
// is always complete.
//
// The whole sealed layer is held in memory, objectstore.Store taking a []byte. At the
// sizes rotation produces (32 MiB measured, and RotateAtBytes is a floor rather than a
// bound) that is a buffer and not a problem; the fix at an order of magnitude more is a
// streaming PUT on the store interface.
func Publish(ctx context.Context, store objectstore.Store, enc *crypto.Encryption, layer io.Reader, req Request) (Manifest, error) {
	layerID, err := boundLayerID(enc, req.VolumeID, req.LayerID)
	if err != nil {
		return Manifest{}, err
	}
	frameBytes := req.FrameBytes
	if frameBytes == 0 {
		frameBytes = crypto.LayerFrameBytes
	}

	// HEAD first: it says what this commit's parent is and whether this commit has already
	// happened. The second question has to be asked here rather than at the CAS — asked
	// late, a retry re-reads a HEAD that already names *this* commit and builds a manifest
	// whose parent is itself, a cycle this code shipped once.
	//
	// The wider window for another writer to move HEAD is not a cost: a CAS that fails is
	// precisely the signal wanted.
	parent, etag := "", ""
	current, currentETag, err := ReadHead(ctx, store, req.VolumeID)
	switch {
	case err == nil && current.CommitID == req.CommitID:
		// Already published, by an attempt whose answer never came back. The manifest in
		// the bucket is the truth about it; nothing is uploaded and nothing is moved.
		return ReadManifest(ctx, store, req.VolumeID, req.CommitID)
	case err == nil:
		parent, etag = current.CommitID, currentETag
	case errors.Is(err, ErrNoHead):
		// The volume's first commit. HEAD is written create-only, which is what stops
		// two hosts that both read "no HEAD" from both winning.
	default:
		return Manifest{}, err
	}

	sealed := bytes.NewBuffer(make([]byte, 0, sealedSize(req.PlainBytes, frameBytes)))
	if err := enc.SealLayer(layerID, frameBytes, layer, sealed); err != nil {
		return Manifest{}, fmt.Errorf("commit: sealing layer %s: %w", req.LayerID, err)
	}
	body := sealed.Bytes()
	digest := Digest(body)
	key := LayerKey(digest)

	if err := putLayer(ctx, store, key, body); err != nil {
		return Manifest{}, err
	}

	// The epoch fence (v6 §13). The CAS only asks "is HEAD what I last read", which is true
	// for a fenced Agent that read it a moment ago — so the parent's manifest is read and
	// its epoch compared. One GET per commit against a host appending a divergent chain onto
	// the volume it was fenced out of.
	if parent != "" {
		prev, err := ReadManifest(ctx, store, req.VolumeID, parent)
		if err != nil {
			return Manifest{}, fmt.Errorf("commit: reading the parent of %s: %w", req.CommitID, err)
		}
		if prev.Epoch > req.Epoch {
			return Manifest{}, fmt.Errorf("%w: this host holds epoch %d and the commit it would build on was written at %d",
				ErrHeadMoved, req.Epoch, prev.Epoch)
		}
	}

	m := Manifest{
		VolumeID: req.VolumeID, CommitID: req.CommitID, ParentCommitID: parent,
		Epoch: req.Epoch, VirtualSize: req.VirtualSize,
		Layer: Layer{
			ObjectKey: key, SizeBytes: int64(len(body)), SHA256: digest,
			FrameBytes: int32(frameBytes), LayerID: req.LayerID,
		},
	}
	if err := WriteManifest(ctx, store, m); err != nil {
		return Manifest{}, err
	}
	if err := CASHead(ctx, store, req.VolumeID, req.CommitID, etag); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// putLayer stores the sealed bytes at their content-addressed key.
//
// Create-only, and a key already taken is the ordinary shape of a retry: the key is the
// digest of the content, so what is there should be this object. "Should be" is why it is
// read back and hashed rather than measured — comparing the size alone let an object of
// the right length planted at the key make Publish report SUCCESS for a commit that could
// never be reconstructed.
//
// It costs a GET of a whole layer, and only on the retry path, where a wrong answer is
// permanent.
func putLayer(ctx context.Context, store objectstore.Store, key string, body []byte) error {
	_, err := store.Put(ctx, key, body, objectstore.PutOptions{IfNoneMatch: true})
	if err == nil {
		return nil
	}
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("commit: uploading %s: %w", key, err)
	}
	existing, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("commit: reading the object already at %s: %w", key, err)
	}
	if got := Digest(existing); got != Digest(body) {
		return fmt.Errorf("%w: %s holds an object that hashes to %s, this layer hashes to %s",
			ErrLayerKeyTaken, key, got, Digest(body))
	}
	return nil
}

// sealedSize is what SealLayer will produce, so the buffer is allocated once.
func sealedSize(plain int64, frameBytes int) int64 {
	frames := (plain + int64(frameBytes) - 1) / int64(frameBytes)
	if frames == 0 {
		frames = 1
	}
	return plain + frames*crypto.TagSize
}

// Fetch is Publish's inverse for one layer: download it, check the digest the manifest
// recorded, and write the plaintext out.
//
// The digest is checked before a byte is unsealed, and that ordering is the point. It is
// the check a recovery can make with no key material — v6 §10 asks exactly that of
// `rebuild-metadata` — and it is the one that tells "this object came back wrong" apart
// from "this object is not ours", which the authentication failure underneath cannot.
func Fetch(ctx context.Context, store objectstore.Store, enc *crypto.Encryption, m Manifest, w io.Writer) error {
	layerID, err := boundLayerID(enc, m.VolumeID, m.Layer.LayerID)
	if err != nil {
		return err
	}
	body, err := store.Get(ctx, m.Layer.ObjectKey)
	if err != nil {
		return fmt.Errorf("commit: downloading %s: %w", m.Layer.ObjectKey, err)
	}
	if got := Digest(body); got != m.Layer.SHA256 {
		return fmt.Errorf("%w: %s hashes to %s, the manifest says %s",
			ErrCorrupt, m.Layer.ObjectKey, got, m.Layer.SHA256)
	}
	if err := enc.OpenLayer(layerID, int(m.Layer.FrameBytes), bytes.NewReader(body), w); err != nil {
		return fmt.Errorf("commit: opening layer %s: %w", m.Layer.LayerID, err)
	}
	return nil
}

// boundLayerID parses a layer id and checks that the key being used belongs to the volume
// the caller named. Sealing with another volume's Encryption produces an object that is
// perfectly valid, uploads without complaint, and is found to be unopenable by whoever
// needs it — a recovery, on the day the host is gone.
func boundLayerID(enc *crypto.Encryption, volumeID, layerID string) ([16]byte, error) {
	var out [16]byte
	if enc == nil {
		return out, errors.New("commit: no key material for this volume")
	}
	named, err := uuid.Parse(volumeID)
	if err != nil {
		return out, fmt.Errorf("commit: volume id %q: %w", volumeID, err)
	}
	if named != uuid.UUID(enc.VolumeID) {
		return out, fmt.Errorf("commit: this commit is for volume %s and the key is volume %s's",
			volumeID, uuid.UUID(enc.VolumeID))
	}
	id, err := uuid.Parse(layerID)
	if err != nil {
		return out, fmt.Errorf("commit: layer id %q: %w", layerID, err)
	}
	return id, nil
}
