// Package image is a volume's durable state in the object store: everything the guest
// has written, as of the moment the volume stopped or a snapshot froze it.
//
// It is what ADR-0026 put in place of the remote WAL chain. Under the old contract a
// volume's state in S3 was N WAL objects that had to all be present and contiguous, and
// reading it meant replaying them — the machinery in `recovery`. Under this one it is a
// manifest plus a set of chunks, and reading it is a download.
//
// # Shape
//
//	<prefix>/manifest.json      the chunk list, CASed on ETag
//	<prefix>/chunks/<sha256>    the bytes, create-only and content-addressed
//
// Chunks are content-addressed on purpose, and it buys the two things the format needs:
// a stop that re-uploads an unchanged region is a no-op the store answers with
// ErrPreconditionFailed rather than a transfer, which is what makes a frequent snapshot
// of a running VM affordable (§2); and two writers producing the same bytes cannot
// corrupt each other, because the key *is* the content.
//
// The manifest is the only mutable object, which is what concentrates the whole fencing
// problem into one compare-and-set (§12, ADR-0026): two incarnations of a volume must
// not both publish, and one CAS on one key is the entirety of that.
//
// # NOT WIRED, AND NOT FINISHED — §15
//
// Nothing calls this yet, deliberately. **The chunks are plaintext**, and §15/INV-15 says
// nothing leaves the host in cleartext — so wiring it as it stands would put the guest's
// data in the bucket in the clear, which is the defect this package must not ship with.
//
// The open question is the nonce, and it is a review-zone decision rather than an
// oversight. `crypto.DEK.Seal` derives its nonce from (volumeID, epoch, sequence), which
// is unique per WAL record by construction; a chunk has no sequence. Deriving it from the
// chunk's plaintext digest would be unique per content and would keep content-addressing
// working — same bytes, same key, same ciphertext, so an unchanged region still costs
// nothing to re-publish. It also makes equality of plaintexts observable within a volume,
// which is a property to accept explicitly or reject, not to arrive at by accident.
// Reusing a nonce across two different plaintexts under one DEK is catastrophic in GCM,
// so this is the part to get right before anything calls Publish.
//
// Until that is decided: `cow.Ranges`, the format, the round-trip property test and the
// CAS are here and proven, and the Agent still boots through `recovery` (ADR-0026
// increment 2 is therefore half done, and STATUS.md says so).
package image

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// MaxChunkBytes bounds one chunk. It exists so a volume's image is never one object:
// a single-PUT is capped by the backend (5 GiB on S3), and a whole-volume object also
// means every stop re-uploads every byte. 64 MiB is large enough that a sequential
// writer produces few chunks and small enough that an unchanged region is skipped at a
// useful granularity.
const MaxChunkBytes = 64 << 20

// ErrNotPublished means the volume has no image: it has never stopped cleanly. It is a
// shape, not a failure — a volume being served for the first time has no image and must
// boot empty rather than refuse.
var ErrNotPublished = errors.New("image: this volume has no published image")

// ErrSuperseded means the manifest changed under this writer: another incarnation
// published while this one was uploading. It is the fencing failure, and it fails the
// stop rather than overwriting — two hosts both publishing is a silent lost update, and
// it is the one property ADR-0026 keeps from the old fencing protocol.
var ErrSuperseded = errors.New("image: the manifest was published by another writer")

// Chunk is one contiguous run of guest bytes, stored under the digest of its contents.
type Chunk struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
	Digest string `json:"digest"`
}

// Manifest is a volume's image: which regions hold data and where their bytes are.
type Manifest struct {
	VolumeID string  `json:"volume_id"`
	Chunks   []Chunk `json:"chunks"`
	// Sequence is the point the image was frozen at (§19: a snapshot is a number, not
	// an event). A boot resumes numbering above it.
	Sequence uint64 `json:"sequence"`
}

// Prefix is where a volume's image lives.
func Prefix(volumeID [16]byte) string { return "image/" + format.UUIDString(volumeID) + "/" }

// ManifestKey is the volume's manifest — the one mutable object, and the one the CAS is on.
func ManifestKey(volumeID [16]byte) string { return Prefix(volumeID) + "manifest.json" }

func chunkKey(volumeID [16]byte, digest string) string {
	return Prefix(volumeID) + "chunks/" + digest
}

// Publish writes the volume's state and CASes the manifest over it.
//
// Order matters and it is the same order every publish protocol in this repository uses:
// chunks first, manifest last. A manifest is only ever written once every chunk it names
// is readable, so a reader that sees a manifest can always resolve it — and a crash
// halfway leaves unreferenced chunks, which cost storage rather than correctness.
//
// prevETag is the manifest this writer believes it is replacing; empty means "there must
// be none". A mismatch is ErrSuperseded and the publish fails: another incarnation
// published while this one was uploading, and overwriting it is the silent lost update
// that ADR-0026 keeps fencing for.
func Publish(ctx context.Context, store objectstore.Store, volumeID [16]byte, view *cow.IntervalMap, seq uint64, prevETag string) (string, error) {
	man := Manifest{VolumeID: format.UUIDString(volumeID), Sequence: seq}

	for _, r := range view.Ranges() {
		for off := r.Offset; off < r.Offset+r.Length; {
			n := min64(uint64(MaxChunkBytes), r.Offset+r.Length-off)
			buf := make([]byte, n)
			view.Read(off, buf)

			sum := sha256.Sum256(buf)
			digest := hex.EncodeToString(sum[:])
			// Create-only: the key is the content, so a chunk that already exists holds
			// exactly these bytes and re-uploading it would transfer them for nothing.
			// That is what makes a second stop cheap when little changed.
			_, err := store.Put(ctx, chunkKey(volumeID, digest), buf, objectstore.PutOptions{IfNoneMatch: true})
			if err != nil && !errors.Is(err, objectstore.ErrPreconditionFailed) {
				return "", fmt.Errorf("image: uploading chunk at %d: %w", off, err)
			}
			man.Chunks = append(man.Chunks, Chunk{Offset: off, Length: n, Digest: digest})
			off += n
		}
	}

	body, err := json.Marshal(man)
	if err != nil {
		return "", err
	}
	opts := objectstore.PutOptions{IfNoneMatch: prevETag == ""}
	if prevETag != "" {
		opts = objectstore.PutOptions{IfMatch: prevETag}
	}
	res, err := store.Put(ctx, ManifestKey(volumeID), body, opts)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return "", fmt.Errorf("%w: volume %s", ErrSuperseded, format.UUIDString(volumeID))
	}
	if err != nil {
		return "", fmt.Errorf("image: publishing the manifest: %w", err)
	}
	return res.ETag, nil
}

// Load reads a volume's image into a read view, and returns the manifest's ETag so the
// caller can CAS against it when it publishes in turn.
//
// A volume with no manifest is ErrNotPublished, which callers treat as "boot empty":
// a volume being served for the first time has written nothing, and refusing it would
// make the first boot the one case that cannot work.
func Load(ctx context.Context, store objectstore.Store, volumeID [16]byte) (*cow.IntervalMap, Manifest, string, error) {
	head, err := store.Head(ctx, ManifestKey(volumeID))
	if errors.Is(err, objectstore.ErrNotFound) {
		return nil, Manifest{}, "", ErrNotPublished
	}
	if err != nil {
		return nil, Manifest{}, "", fmt.Errorf("image: reading the manifest: %w", err)
	}
	body, err := store.Get(ctx, ManifestKey(volumeID))
	if err != nil {
		return nil, Manifest{}, "", fmt.Errorf("image: reading the manifest: %w", err)
	}
	var man Manifest
	if err := json.Unmarshal(body, &man); err != nil {
		return nil, Manifest{}, "", fmt.Errorf("image: parsing the manifest: %w", err)
	}
	if man.VolumeID != format.UUIDString(volumeID) {
		return nil, Manifest{}, "", fmt.Errorf("image: manifest at %s describes volume %s",
			ManifestKey(volumeID), man.VolumeID)
	}

	view := cow.NewIntervalMap()
	for _, c := range man.Chunks {
		data, err := store.Get(ctx, chunkKey(volumeID, c.Digest))
		if err != nil {
			// A manifest naming a chunk that is not there is a broken image, not an
			// empty one. Failing closed matters: the alternative is a view with a hole
			// in it, which reads as zeros and is indistinguishable from a range the
			// guest never wrote.
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s at offset %d: %w", c.Digest, c.Offset, err)
		}
		// The digest is the key, so verifying it is checking the store kept its promise
		// rather than checking our own arithmetic — and a backend that returns the wrong
		// object for a key is exactly what §6.1's conformance suite exists to catch.
		if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != c.Digest {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s does not hash to its key", c.Digest)
		}
		if uint64(len(data)) != c.Length {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s is %d bytes, manifest says %d",
				c.Digest, len(data), c.Length)
		}
		view.Overwrite(c.Offset, data)
	}
	return view, man, head.ETag, nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
