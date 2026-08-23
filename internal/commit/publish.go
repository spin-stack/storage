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
// # The order, and what each reversal costs
//
//	PUT layer → PUT commit manifest → CAS HEAD
//
// Never the reverse, and the reason is what a crash between two steps leaves behind. Done
// in this order, an interruption leaves an object nothing points at — a layer with no
// manifest, or a manifest no HEAD names — which is garbage a sweep collects and which no
// reader can reach. Done with the CAS anywhere but last, an interruption leaves HEAD
// pointing at a manifest that is not there: a commit that was acknowledged and cannot be
// reconstructed, which is the one thing `Commit() → SUCCESS` promises never happens.
//
// Of the three steps, only "the CAS is last" is load-bearing, and that is worth stating
// because the tests say so. Planting the other swap — the manifest published before its
// layer — turned nothing red, and it should not have: HEAD does not move until both are
// there, so no reader can reach the incomplete pair. The order is kept anyway, because a
// manifest that exists and names an absent layer is an object every sweep and every
// `rebuild-metadata` then has to reason about, and layer-first means an existing manifest
// is always complete.
//
// # The whole layer is held in memory
//
// objectstore.Store takes a []byte, so the sealed layer is assembled before it is sent.
// At the sizes rotation actually produces — 32 MiB measured, and the threshold is a floor
// rather than a bound (qcow.Config.RotateAtBytes) — that is a buffer, not a problem. It
// stops being one at a threshold or a write rate an order of magnitude larger, and the
// fix then is a streaming PUT on the store interface rather than anything here. Said out
// loud because the alternative is finding it as an OOM in an Agent that was holding
// somebody's disk.
func Publish(ctx context.Context, store objectstore.Store, enc *crypto.Encryption, layer io.Reader, req Request) (Manifest, error) {
	layerID, err := boundLayerID(enc, req.VolumeID, req.LayerID)
	if err != nil {
		return Manifest{}, err
	}
	frameBytes := req.FrameBytes
	if frameBytes == 0 {
		frameBytes = crypto.LayerFrameBytes
	}

	// HEAD first, and it answers two questions at once: what this commit's parent is,
	// and whether this commit has already happened.
	//
	// The second is v6 §15's "CAS exitoso, respuesta perdida" and it has to be asked
	// here rather than at the CAS. Asked late, the retry re-reads a HEAD that already
	// names *this* commit and builds a manifest whose parent is itself — which is a
	// cycle in the history, and it is what this code did until the idempotency test ran.
	//
	// Reading it before the upload widens the window in which another writer can move
	// HEAD, and that is not a cost. With a correct single writer nobody else publishes;
	// with a broken one, a CAS that fails is precisely the signal wanted, and a wider
	// window makes it more likely to be seen rather than less correct.
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
// Create-only, and a key that is already taken is the ordinary shape of a retry rather
// than a failure: the key is the digest of the content, so an object already there is
// this object. The size is checked anyway — it is one HEAD, and it is the difference
// between "this layer was already uploaded" and "a manifest of ours names something
// somebody else put in the bucket".
func putLayer(ctx context.Context, store objectstore.Store, key string, body []byte) error {
	_, err := store.Put(ctx, key, body, objectstore.PutOptions{IfNoneMatch: true})
	if err == nil {
		return nil
	}
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("commit: uploading %s: %w", key, err)
	}
	info, err := store.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("commit: examining the object already at %s: %w", key, err)
	}
	if info.Size != int64(len(body)) {
		return fmt.Errorf("%w: %s holds %d bytes, this layer is %d", ErrLayerKeyTaken, key, info.Size, len(body))
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
// the caller named.
//
// The mismatch it rules out is a wiring one, and the reason it is checked rather than
// assumed is what it would otherwise cost: sealing with another volume's Encryption
// produces an object that is perfectly valid, uploads without complaint, and is
// discovered to be unopenable by whoever needs it — which is a recovery, on the day the
// host is gone.
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
