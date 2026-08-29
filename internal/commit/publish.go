package commit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrLayerKeyTaken means something occupies a layer's content-addressed key with content
// that is not this layer. It cannot happen from this code — the key *is* the digest of
// the bytes — so it is a bucket somebody else has written into, and publishing a manifest
// that points at it would name an object this Agent never wrote.
var ErrLayerKeyTaken = errors.New("commit: the layer's key holds an object of a different size")

// ErrSealDrifted means the pass that uploaded the layer produced different bytes from the
// pass that hashed it. The object is content-addressed by the first digest, so anything
// but a refusal here puts an object at layers/sha256/<d> that does not hash to <d> — and
// Fetch checks the digest before it unseals, so that commit is unreadable forever while
// Commit() reported SUCCESS.
var ErrSealDrifted = errors.New("commit: the layer sealed differently on the upload pass than on the pass that hashed it")

// ErrRootSuperseded means a compacted root was offered for a HEAD that has moved on. The
// layer flattens one commit and only that commit, so publishing it over a descendant
// would drop the commits in between; the collapse is planned again over what the history
// is now. It is not ErrHeadMoved: nobody else took the volume, so this host is still its
// writer and fencing it would stop a guest for its own bookkeeping.
var ErrRootSuperseded = errors.New("commit: the commit this root flattens is not the one HEAD names")

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
	// FrameBytes is the sealing frame; zero means crypto.LayerFrameBytes.
	FrameBytes int
	// ReplacesCommitID, when set, publishes this commit as a root: the layer is a whole
	// image of the volume rather than a delta, so the manifest carries no parent and a
	// recovery stops here instead of walking and downloading the history it flattens.
	// That is what a chain collapse produces (v6 §19).
	//
	// It names the commit the flattened layer reconstructs, and HEAD has to still be that
	// commit or nothing is published. The check is here and not only in the caller
	// because of what it rules out: HEAD moves to a commit that is *not* a child of the
	// one it replaces, so a root landing on a HEAD that has moved on drops every commit
	// in between out of the history — each of which returned SUCCESS.
	//
	// Nothing else about the protocol changes: HEAD is still read first, the epoch of the
	// commit being replaced is still the fence this must clear, and the CAS is still
	// against the etag that read produced. A root is not a licence to overwrite a
	// history; it is a commit like any other that happens to carry the whole volume.
	ReplacesCommitID string
}

// Option carries what Publish measures itself with. It is optional because most callers
// of this package are a test or a simulation, where the recorder would be nil anyway;
// the Agent passes both.
type Option func(*options)

type options struct {
	clk clock.Clock
	rec *obs.Recorder
}

// WithTelemetry makes Publish record its §28 numbers. Without it nothing is recorded and
// the protocol is unchanged.
func WithTelemetry(clk clock.Clock, rec *obs.Recorder) Option {
	return func(o *options) { o.clk = clk; o.rec = rec }
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
// # Why the layer is read twice
//
// The key is the digest of the sealed object, so the digest has to exist before the
// upload can be addressed, and the sealed bytes are far too large to keep while that
// happens — which is what this code used to do. So the layer is sealed once to be
// measured and hashed, and sealed again straight into the upload; sealing is
// deterministic (crypto.SealLayer derives its nonces), so the two agree unless the source
// changed underneath. The upload pass is hashed as the store consumes it and compared
// before anything points at the object, because "the digest is over the bytes as stored"
// is the property every recovery leans on and a same-length difference would leave it
// silently false.
func Publish(ctx context.Context, store objectstore.Store, enc *crypto.Encryption, layer io.ReadSeeker, req Request, opts ...Option) (Manifest, error) {
	var o options
	for _, apply := range opts {
		apply(&o)
	}
	started := now(o.clk)

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
	// `replaces` is the commit HEAD names and `parent` is what the manifest will claim.
	// They are the same thing for every commit but a root, which carries no parent and
	// still has to clear the epoch of the commit it moves HEAD off: drop both and a host
	// that was fenced could publish a root over the history it was fenced out of.
	parent, replaces, etag := "", "", ""
	current, currentETag, err := ReadHead(ctx, store, req.VolumeID)
	switch {
	case err == nil && current.CommitID == req.CommitID:
		// Already published, by an attempt whose answer never came back. The manifest in
		// the bucket is the truth about it; nothing is uploaded and nothing is moved.
		return ReadManifest(ctx, store, req.VolumeID, req.CommitID)
	case err == nil && req.ReplacesCommitID != "" && current.CommitID != req.ReplacesCommitID:
		return Manifest{}, fmt.Errorf("%w: it flattens %s and HEAD is at %s",
			ErrRootSuperseded, req.ReplacesCommitID, current.CommitID)
	case err == nil:
		replaces, etag = current.CommitID, currentETag
		if req.ReplacesCommitID == "" {
			parent = replaces
		}
	case errors.Is(err, ErrNoHead) && req.ReplacesCommitID != "":
		// A root replaces a commit, and there is none. Whatever this host is holding, the
		// history it was flattening is not in this bucket.
		return Manifest{}, fmt.Errorf("%w: it flattens %s and the volume has no HEAD",
			ErrRootSuperseded, req.ReplacesCommitID)
	case errors.Is(err, ErrNoHead):
		// The volume's first commit. HEAD is written create-only, which is what stops
		// two hosts that both read "no HEAD" from both winning.
	default:
		return Manifest{}, err
	}

	upload := now(o.clk)
	digest, size, err := measureLayer(enc, layerID, frameBytes, layer)
	if err != nil {
		return Manifest{}, fmt.Errorf("commit: sealing layer %s: %w", req.LayerID, err)
	}
	key := LayerKey(digest)

	if err := putLayer(ctx, store, key, sealer{enc: enc, layerID: layerID, frameBytes: frameBytes, layer: layer}, digest, size); err != nil {
		return Manifest{}, err
	}
	// Recorded here rather than in internal/publisher, which owns the other layer_*
	// numbers: this is the only place that knows when the sealing began and when the last
	// byte was acknowledged. It covers both passes over the layer, because both are what
	// getting it into the bucket costs.
	o.rec.Observe(ctx, "layer_upload_duration_seconds", elapsed(o.clk, upload),
		obs.String("volume", req.VolumeID))

	// The epoch fence (v6 §13). The CAS only asks "is HEAD what I last read", which is true
	// for a fenced Agent that read it a moment ago — so the parent's manifest is read and
	// its epoch compared. One GET per commit against a host appending a divergent chain onto
	// the volume it was fenced out of.
	if replaces != "" {
		prev, err := ReadManifest(ctx, store, req.VolumeID, replaces)
		if err != nil {
			return Manifest{}, fmt.Errorf("commit: reading the commit %s is published onto: %w", req.CommitID, err)
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
			ObjectKey: key, SizeBytes: size, SHA256: digest,
			FrameBytes: int32(frameBytes), LayerID: req.LayerID,
		},
	}
	if err := WriteManifest(ctx, store, m); err != nil {
		return Manifest{}, err
	}
	if err := CASHead(ctx, store, req.VolumeID, req.CommitID, etag); err != nil {
		if errors.Is(err, ErrHeadMoved) {
			// Counted here and not inside CASHead: this is the CAS that publishes a
			// commit, and an operator reading the series wants the number of commits
			// that lost the volume under them, not every conditional write in the tree.
			o.rec.Count(ctx, "cas_failures_total", 1, obs.String("volume", req.VolumeID))
		}
		return Manifest{}, err
	}
	o.rec.Observe(ctx, "commit_publish_latency_seconds", elapsed(o.clk, started),
		obs.String("volume", req.VolumeID))
	return m, nil
}

// now and elapsed are the whole of this package's relationship with time: a nil clock is
// a caller that is not measuring, and every duration is then zero rather than a branch at
// each record site.
func now(clk clock.Clock) clock.Instant {
	if clk == nil {
		return 0
	}
	return clk.Now()
}

func elapsed(clk clock.Clock, since clock.Instant) float64 {
	if clk == nil {
		return 0
	}
	return clk.Now().Sub(since).Seconds()
}

// sealer produces the sealed form of one layer, as many times as it is asked to. Each
// call rewinds the plaintext, so the caller must not hold the two streams at once.
type sealer struct {
	enc        *crypto.Encryption
	layerID    [16]byte
	frameBytes int
	layer      io.ReadSeeker
}

// stream returns the sealed bytes as a reader, sealing them as they are consumed, and the
// hash of everything the consumer took. Closing the reader stops the sealing.
func (s sealer) stream() (io.ReadCloser, hash.Hash, error) {
	if _, err := s.layer.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("commit: rewinding the layer: %w", err)
	}
	pr, pw := io.Pipe()
	go func() {
		_ = pw.CloseWithError(s.enc.SealLayer(s.layerID, s.frameBytes, s.layer, pw))
	}()
	h := sha256.New()
	// Hashed on the reading side rather than the writing one, so what is hashed is
	// exactly what the store consumed — and so there is no race to read the digest the
	// moment the store stops reading.
	return sealedStream{Reader: io.TeeReader(pr, h), pr: pr}, h, nil
}

// sealedStream is the pipe's read half with the tee in front of it. Close closes the
// pipe, which is what makes the sealing goroutine return when the store stops reading —
// a store that refuses the request before touching the body is the ordinary case.
type sealedStream struct {
	io.Reader
	pr *io.PipeReader
}

func (s sealedStream) Close() error { return s.pr.Close() }

// measureLayer is the pass that names the object: it seals the layer and keeps only the
// digest and the length. Nothing is stored, so a layer of any size costs one frame of
// memory here.
func measureLayer(enc *crypto.Encryption, layerID [16]byte, frameBytes int, layer io.ReadSeeker) (string, int64, error) {
	if _, err := layer.Seek(0, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("rewinding the layer: %w", err)
	}
	h := sha256.New()
	c := &counter{w: h}
	if err := enc.SealLayer(layerID, frameBytes, layer, c); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), c.n, nil
}

// counter is a writer that keeps the length of what went through it.
type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// putLayer streams the sealed bytes to their content-addressed key and refuses to go on
// unless what the store consumed hashes to the name it was stored under.
//
// Create-only, and a key already taken is the ordinary shape of a retry: the key is the
// digest of the content, so what is there should be this object. "Should be" is why it is
// read back and hashed rather than measured — comparing the size alone let an object of
// the right length planted at the key make Publish report SUCCESS for a commit that could
// never be reconstructed.
//
// It costs a GET of a whole layer, and only on the retry path, where a wrong answer is
// permanent.
func putLayer(ctx context.Context, store objectstore.Store, key string, s sealer, digest string, size int64) error {
	body, h, err := s.stream()
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	_, err = store.PutStream(ctx, key, body, size, objectstore.PutOptions{IfNoneMatch: true})
	if err == nil {
		stored := hex.EncodeToString(h.Sum(nil))
		if stored == digest {
			return nil
		}
		// The object at this key is not the object this key names, and the key is
		// create-only from now on: leaving it there would refuse the same layer for
		// ever. Delete is a reversible mark on every implementation (INV-14), so the
		// bytes remain for whoever investigates.
		derr := store.Delete(ctx, key)
		return fmt.Errorf("%w: %s was uploaded as %s (the object was withdrawn: %v)",
			ErrSealDrifted, key, stored, derr)
	}
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("commit: uploading %s: %w", key, err)
	}
	existing, err := store.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("commit: reading the object already at %s: %w", key, err)
	}
	if got := Digest(existing); got != digest {
		return fmt.Errorf("%w: %s holds an object that hashes to %s, this layer hashes to %s",
			ErrLayerKeyTaken, key, got, digest)
	}
	return nil
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
