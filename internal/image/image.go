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
//	image/<volume>/manifest.json         the chunk list, CASed on ETag
//	image/<volume>/snapshots/<id>.json   a frozen point, create-only
//	chunks/<lineage-root>/<sha256>       the bytes, create-only and content-addressed
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
// # A manifest states its own volume, not its ancestry
//
// This is the second half of CHUNK-ADDRESSING-SPEC's decision and it changes what reading
// an image means. A manifest used to be **flattened**: a publish serialised the volume's
// whole read view, so a clone's first stop wrote down every range its parent's snapshot
// held, under the clone's own name, and any one manifest could be read alone. It is now a
// **delta** — what this volume's own layers state — and reading a clone means composing
// its ancestry first and laying the delta over it (agent.parentChain walks the lineage,
// agent.fetchBase does the composing).
//
// Three things follow, and none of them is optional:
//
//   - **Absence changed meaning.** In a flattened manifest a range nobody named read as
//     zeros. In a delta it means "ask the layer below", so an erasure needs a word of its
//     own — Manifest.Discarded — or every DISCARD becomes data resurrection on the next
//     boot (§14.6).
//   - **A clone stopped being self-contained.** Its image cannot be read without its
//     ancestors' snapshots, so deleting a parent takes its descendants with it unless
//     they are flattened first (DELETION-AND-RECLAIM-SPEC's decision of 2026-08-07: a
//     delete of a volume with descendants flattens them).
//   - **The read path grows with depth.** Attaching a clone is one descriptor GET and one
//     snapshot manifest per link, plus the chunks each names. That is what the ceiling of
//     §20.1 bounds, and why step 4 makes it a refusal at create.
//
// What it buys is the cost this was all for: a clone's first stop writes down what the
// clone wrote, not what it inherited. Measured in this package's clone-cost tests.
//
// # The chunk store belongs to a lineage, not to a volume
//
// This is the one thing about the layout above that cannot be read off it, and it is the
// format change CHUNK-ADDRESSING-SPEC's decision of 2026-08-07 asked for. A clone
// inherits its parent's DEK, KEK id and DEK version (controlplane.Clone), so a lineage —
// the volume that was created, plus every clone that descends from it, however deep —
// encrypts under **one key**. The chunk store is scoped to exactly that set: the lineage
// root's id names it, every volume in the chain writes into it and reads from it, and a
// chunk a parent already stored is one its clone Heads, finds, and skips. That is what
// makes a clone's storage cost proportional to what the clone wrote rather than to what
// it inherited.
//
// Rejected: **one bucket-wide chunks/<digest>**, which is the shape everyone reaches for.
// It cannot work here and the reason is not the key space, it is the key: two lineages
// generally seal the same plaintext under different DEKs, so they produce different
// ciphertext for one content-addressed key. (Generally, not always — a flattened volume
// and the lineage it left share a DEK. That makes the rejection narrower than it reads,
// and not weaker: one pair of prefixes that happen to agree does not give a bucket-wide
// key space a key.) Whoever created the key owns it and nobody else can open
// what is under it. Making it work needs a bucket-wide DEK, which spends the design
// document's *"borrado de volumen = crypto-shred"* (§15) to buy dedup between volumes
// that have nothing to do with each other.
//
// Rejected: **keeping chunks under image/<volume>/**, which is where they were until
// 2026-08-08. It is why a clone's first stop re-uploaded its parent's entire dataset
// under its own prefix — measured at 8 MiB for a 512-byte write in this package's
// clone-cost tests — and it made "volume delete" and "the lineage's data" the same
// prefix, so deleting a parent took its clones' bytes with it.
//
// What that costs, stated where a reader will look for it: the blast radius of a chunk is
// now the lineage rather than the volume. Someone who can read the bucket can see that
// two volumes of one lineage hold equal plaintexts — as they could within a volume
// before — and a delete that reclaims bytes has to reason about the lineage's manifests
// rather than one volume's (DELETION-AND-RECLAIM-SPEC §3).
//
// # Encryption (§15, INV-15)
//
// Chunks are sealed with the lineage's DEK before they leave the host, and each one
// carries the nonce it was sealed under:
//
//	<nonce:12><ciphertext><tag:16>
//
// The AAD is "image-chunk" || lineage-root || digest. It binds the ciphertext to the
// lineage whose key it was sealed under and to the content it claims to be, so a chunk
// cannot be moved between lineages, and cannot be relabelled inside one. It deliberately
// no longer binds the *volume*: a chunk a parent wrote is one its clone must open, and
// binding the writer would forbid exactly the sharing this key space exists for. What
// replaces that protection is not the AAD — loadManifest verifies every chunk against
// the digest it is keyed by, so a chunk substituted for another inside a lineage is
// caught by its content and not by its label.
//
// ## Why no nonce is ever used twice, now that the key space spans volumes
//
// AES-GCM demands one thing: no (key, nonce) pair may ever seal two *different*
// plaintexts. Three facts give it here, and it is worth being exact about which one does
// the work, because the previous wording of this paragraph credited the wrong one.
//
//  1. The nonce is **drawn** from the caller's random source, never derived.
//  2. The key of a chunk is the SHA-256 of its plaintext, so a key names its content:
//     two seals that land on one key are two seals of the *same bytes*. There is no way
//     to reach one key with two plaintexts short of a SHA-256 collision.
//  3. A chunk whose key already exists is skipped rather than re-sealed, so the number of
//     nonces ever drawn under one DEK is the number of distinct chunk contents the
//     lineage has ever held — not the number of sessions, stops or volumes that held
//     them.
//
// (2) is what makes reuse-with-different-plaintexts impossible; (1) makes an actual nonce
// collision a 2^-96 event per pair rather than a scheme; (3) is what keeps the number of
// draws — and therefore the birthday bound (1) rests on — proportional to content instead
// of to activity. At 64 MiB a chunk, approaching NIST's 2^32-invocation guidance for
// random nonces means a lineage having held 2^32 distinct chunks, which is not a volume
// anything here can store.
//
// **The old argument was true at the wrong scope, and moving the key space is what fixed
// it.** It said a chunk is "encrypted exactly once, ever", and that held per *volume* —
// which was never the scope of the key. The DEK has always been per lineage, so before
// this change a clone re-sealed every byte it inherited under its parent's DEK with a
// fresh nonce, once per link: the same plaintext, N nonces, one key. That was not a reuse
// and it was never unsafe, but the sentence that claimed safety was not the sentence that
// provided it. Moving the key space is what put the two in the same scope.
//
// **"Sealed exactly once, ever" is still not literally true, and it does not need to be.**
// `lineage.Flatten` re-seals a volume's whole reachable content under its own new lineage
// root, with fresh nonces, while the DEK does not change — a flatten does not rotate
// anything. So one DEK spans two chunk prefixes and the same plaintext is sealed twice.
// That is the *converse* of the hazard this paragraph used to name, it arrived two
// increments after this text was written, and pretending otherwise would put the
// repository's oldest documentation defect inside its encryption argument.
//
// What matters is that the fact carrying the safety is (2), not (3). A key names its
// content, so two seals landing on one key are two seals of the *same bytes*: (key, nonce)
// reuse over *different* plaintexts is impossible whatever the prefix count. (3) bounds
// how many nonces are drawn for one plaintext, and a flatten costs one more per distinct
// content — a bounded factor against the 2^32 birthday bound that a 64 MiB chunk puts far
// out of reach.
//
// **The hazard is therefore two DEKs inside one chunk prefix, not one DEK across two.**
// That would be a rotation applied to one volume of a lineage rather than to the lineage.
// Nothing rotates a DEK today, and if a verb is added it must be per lineage. It fails
// closed rather than silently: the create-only PUT means the first ciphertext owns the
// key, and the other volume's load fails its authentication rather than returning
// anything.
//
// Deriving the nonce instead was considered and rejected in both available shapes, and
// both reasons survive this change — the first with more force than before. From a
// generation counter: chunks are uploaded *before* the compare-and-set that decides which
// writer wins, so two writers at the same generation with different content would seal
// two plaintexts under one nonce, which is the catastrophic case — and a lineage now has
// several volumes publishing into one key space, each with its own generation. From the
// plaintext digest: unique per content, but it makes the nonce a function of the secret.
package image

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// MaxChunkBytes bounds one chunk. It exists so a volume's image is never one object:
// a single-PUT is capped by the backend (5 GiB on S3), and a whole-volume object also
// means every stop re-uploads every byte. 64 MiB is large enough that a sequential
// writer produces few chunks and small enough that an unchanged region is skipped at a
// useful granularity.
//
// **It is a bound, not a granularity, and the difference is the whole of DEV-0024.** A
// chunk is one of this volume's own extents, cut only where an extent is longer than this
// — so a guest that writes 512 bytes produces a 512-byte chunk, not a padded 64 MiB one.
// Nothing anywhere aligns a chunk to a grid, and the design document's "granularidad de
// segmentos CoW de 64 KiB" describes a mechanism that has never existed in this tree:
// cow.IntervalMap holds arbitrary extents and merges adjacent ones.
//
// Two consequences, both pinned by TestChunksAreExtentSizedAndDedupByContentAlone:
//
//   - **Dedup is by content alone.** The key is the digest of the bytes; the offset lives
//     in the manifest. Two volumes of one lineage that wrote the same bytes share one
//     object whether or not they wrote them at the same offset.
//   - **And it therefore depends on extent boundaries coinciding.** Two volumes that wrote
//     the same megabyte, one as a single extent and one as two, produce different chunks
//     and share nothing. That costs nothing in the case V1 has — a parent holding the
//     image and clones writing small deltas over it, where the parent's chunks are
//     inherited rather than re-uploaded — and it is the weakness a content-defined
//     chunker (a rolling hash) would remove.
//
// Rejected, and it is the option that sounds right until it is examined: **cutting chunks
// on a fixed grid**, which would make boundaries independent of write history and dedup
// deterministic. A cell written only in part has to be filled from somewhere, and the only
// somewhere is the ancestry — so publishing would read through the chain to complete a
// cell, which is the flattening ADR-0026's chain decision removed, reappearing one layer
// down and per cell. Storing partial cells instead puts the boundaries back where they
// already are and buys nothing.
//
// The trigger for revisiting it is not a size, it is a workload: several volumes writing
// the *same* content with *different* extent boundaries. A golden image with clones does
// not do that; a fleet-wide dedup ambition would, and that also needs a bucket-wide key
// space, which the package doc above rejects for a separate reason.
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

// Span is a half-open range with no bytes behind it: [Offset, Offset+Length).
type Span struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// Manifest is a volume's image: which regions this volume holds, where their bytes are,
// and which regions it erased.
//
// **It states this volume and not its ancestry**, which is the change of 2026-08-08 and
// the thing to have in mind reading anything below. A manifest used to be flattened — a
// clone's first stop wrote down every range its parent's snapshot held, under the clone's
// own name — and now it is a delta: what this volume's own layers state, over whatever
// the chain it descends from says (agent.parentView composes that chain; cow.DeltaOver
// takes the delta). A reader that has one without the other has half a volume.
type Manifest struct {
	VolumeID string  `json:"volume_id"`
	Chunks   []Chunk `json:"chunks"`
	// Discarded is what makes the delta expressible, and it is the field a reader is most
	// likely to think is optional.
	//
	// In a flattened manifest, absence meant "zeros". In a delta it means "ask the layer
	// below", because that is what a delta is for — a clone that names nothing at an
	// offset is deferring to its parent there. So an erasure needs a word of its own: a
	// range the guest DISCARDed over something an ancestor holds is *here*, and a loader
	// lays it back over the ancestry as a tombstone (cow.IntervalMap.Clear).
	//
	// Without it, publishing a delta would turn every DISCARD into data resurrection —
	// the ancestor's older bytes reappearing at an offset the guest freed — which is
	// §14.6's failure and strictly worse than losing the erasure, since those blocks may
	// have been handed to something else.
	//
	// Empty for a volume that descends from nothing: with no layer below, absence and a
	// tombstone say the same thing, and cow.DeltaOver reports none rather than
	// accumulating them in every manifest for ever.
	Discarded []Span `json:"discarded,omitempty"`
	// Sequence is the point the image was frozen at (§19: a snapshot is a number, not
	// an event). A boot resumes numbering above it.
	Sequence uint64 `json:"sequence"`
}

// Ident is the pair of ids every operation in this package needs, and which are the same
// id for every volume that was never cloned.
//
// It is a struct rather than two parameters because they are both [16]byte and adjacent,
// and swapping them is silent: the chunks land under a prefix nothing else will look in,
// the manifest describes the wrong volume, and — for a root volume, which is most of
// them — the two are equal, so no test that does not involve a clone can tell. A field
// name at every call site is what makes that mistake impossible to write.
type Ident struct {
	// Volume owns the manifest and the snapshots. It is the id in the manifest body and
	// the one readManifest checks the object against.
	Volume [16]byte
	// Lineage is the root of the clone chain this volume belongs to — itself, for a
	// volume that descends from nothing. It names the chunk store and it is what the
	// chunk AAD binds, because it is the scope of the DEK: see the package doc.
	Lineage [16]byte
}

// OwnLineage is the Ident of a volume that descends from nothing, so its chunks are its
// own. Every non-clone is this, which is why it has a name.
func OwnLineage(volumeID [16]byte) Ident { return Ident{Volume: volumeID, Lineage: volumeID} }

// Prefix is where a volume's image lives: its manifest and its snapshots. Not its
// chunks — those belong to the lineage (ChunksPrefix), which is what makes a clone's
// bytes shareable with its parent's.
func Prefix(volumeID [16]byte) string { return "image/" + format.UUIDString(volumeID) + "/" }

// ManifestKey is the volume's manifest — the one mutable object, and the one the CAS is on.
func ManifestKey(volumeID [16]byte) string { return Prefix(volumeID) + "manifest.json" }

// ChunksPrefix is where a lineage's chunk objects live. It is a top-level prefix rather
// than something under the root volume's image/ prefix on purpose: these bytes outlive
// the volume that first wrote them — a clone still reads them after its parent's own
// manifest is gone — so a delete that clears image/<root>/ must not be able to take a
// descendant's data with it (DELETION-AND-RECLAIM-SPEC).
func ChunksPrefix(lineageRoot [16]byte) string {
	return "chunks/" + format.UUIDString(lineageRoot) + "/"
}

func chunkKey(lineageRoot [16]byte, digest string) string {
	return ChunksPrefix(lineageRoot) + digest
}

// SnapshotKey is where a named, frozen point of a volume lives. It sits under the
// volume's own prefix because it is a statement about that volume: which ranges it held
// at which sequence. What makes a snapshot cheap is not where the manifest is but that
// the chunks it names are already in the lineage's store — a snapshot of a volume that
// has barely changed transfers a manifest and no bytes, which is what makes §2's primary
// use case, frequent cloning from snapshots, affordable.
func SnapshotKey(volumeID [16]byte, snapshotID string) string {
	return Prefix(volumeID) + "snapshots/" + snapshotID + ".json"
}

// SnapshotKeyFor is SnapshotKey for a caller that holds the volume id as a string —
// the Control Plane, recording in the catalog where a host said it put the manifest.
// It computes the key rather than believing a reported one so the catalog and the
// writer cannot disagree about where a snapshot lives.
func SnapshotKeyFor(volumeID, snapshotID string) (string, error) {
	u, err := ids.Parse(volumeID)
	if err != nil {
		return "", fmt.Errorf("image: volume %q is not a uuid: %w", volumeID, err)
	}
	return SnapshotKey([16]byte(u), snapshotID), nil
}

// ErrSnapshotExists means a snapshot with that id was already published. §5.2/INV-16: a
// published snapshot never changes, so this is a refusal rather than an overwrite — and
// it is the difference between a snapshot and the volume's own manifest, which is CASed
// because it is *meant* to move.
var ErrSnapshotExists = errors.New("image: this snapshot is already published and snapshots are immutable")

// PublishSnapshot freezes a view under a name. The caller froze it (wal.Log.Freeze);
// this writes it down.
//
// Create-only, because §5.2 says a PUBLISHED snapshot is immutable and because two
// writers racing to publish the same id must not both think they won. The chunks are the
// volume's own, so nothing is copied that already exists.
//
// inherited is what Publish's is: the ancestry this volume's layers sit over, which a
// snapshot states no more than an image does. A snapshot of a clone is therefore only
// meaningful together with the chain it descends from — which is exactly what a clone of
// that snapshot walks (agent.parentChain).
func PublishSnapshot(ctx context.Context, store objectstore.Store, rnd io.Reader, enc *wal.Encryption, id Ident, view, inherited *cow.IntervalMap, seq uint64, snapshotID string) (string, error) {
	man, err := uploadChunks(ctx, store, rnd, enc, id, view, inherited, seq)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(man)
	if err != nil {
		return "", err
	}
	res, err := store.Put(ctx, SnapshotKey(id.Volume, snapshotID), body, objectstore.PutOptions{IfNoneMatch: true})
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return "", fmt.Errorf("%w: %s", ErrSnapshotExists, snapshotID)
	}
	if err != nil {
		return "", fmt.Errorf("image: publishing snapshot %s: %w", snapshotID, err)
	}
	return res.ETag, nil
}

// LoadSnapshot reads a named frozen point. It is what a clone reads: the parent's *live*
// image would carry writes the parent made after the snapshot, which is not what a clone
// descending from that snapshot is entitled to see.
func LoadSnapshot(ctx context.Context, store objectstore.Store, enc *wal.Encryption, id Ident, snapshotID string) (*cow.IntervalMap, Manifest, error) {
	return LoadSnapshotOver(ctx, store, enc, id, snapshotID, nil)
}

// LoadSnapshotOver reads a named frozen point as a layer *over* base: this snapshot's
// ranges win, and a range it does not name is answered by base.
//
// It exists for one caller, agent.parentView, which composes a clone's ancestry into one
// read view — the grandparent's snapshot at the bottom, each descendant's over it. cow
// already nests arbitrarily (NewIntervalMapOver, and Ranges/Read recurse through
// m.base), so this composes what is there rather than extending it.
//
// A nil base is exactly LoadSnapshot: the bottom of a chain is a link like any other,
// laid over nothing. It used to come back **unlayered**, so that cow.SetBase would refuse
// to slide anything underneath a manifest that had been flattened; since a manifest
// became a delta that refusal would forbid the composition this whole package now depends
// on, and what replaced it is the tombstone the manifest carries (Manifest.Discarded).
func LoadSnapshotOver(ctx context.Context, store objectstore.Store, enc *wal.Encryption, id Ident, snapshotID string, base *cow.IntervalMap) (*cow.IntervalMap, Manifest, error) {
	view, man, _, err := loadManifest(ctx, store, enc, id, SnapshotKey(id.Volume, snapshotID), base)
	return view, man, err
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
//
// # inherited is what this volume does not write down
//
// It is the ancestry the view sits over — the composed snapshots of every volume this one
// descends from — and everything below it is left out of the manifest, because the chain
// already states it and a reader walks the chain (agent.parentView). nil for a volume
// that descends from nothing, which writes down all of itself.
//
// Passing nil for a clone is not a corruption, it is the old behaviour: the manifest comes
// out flattened, naming every range the ancestry holds under this volume's name. It reads
// back correctly and costs a copy of the inherited dataset — which is what
// CHUNK-ADDRESSING-SPEC measured at 8 MiB for a 512-byte write. Passing the *wrong* map is
// refused rather than guessed at (cow.DeltaOver).
func Publish(ctx context.Context, store objectstore.Store, rnd io.Reader, enc *wal.Encryption, id Ident, view, inherited *cow.IntervalMap, seq uint64, prevETag string) (string, error) {
	man, err := uploadChunks(ctx, store, rnd, enc, id, view, inherited, seq)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(man)
	if err != nil {
		return "", err
	}
	opts := objectstore.PutOptions{IfNoneMatch: prevETag == ""}
	if prevETag != "" {
		opts = objectstore.PutOptions{IfMatch: prevETag}
	}
	res, err := store.Put(ctx, ManifestKey(id.Volume), body, opts)
	if errors.Is(err, objectstore.ErrPreconditionFailed) {
		return "", fmt.Errorf("%w: volume %s", ErrSuperseded, format.UUIDString(id.Volume))
	}
	if err != nil {
		return "", fmt.Errorf("image: publishing the manifest: %w", err)
	}
	return res.ETag, nil
}

// uploadChunks puts every region *this volume* holds in the store and returns the
// manifest describing them. It is shared by Publish and PublishSnapshot: the two differ
// only in which object the manifest is written to and under what precondition, and a
// second implementation of "put the bytes there" is a second place for the sealing rule
// to be forgotten.
//
// It walks `view.DeltaOver(inherited)` where it used to walk `view.Ranges()`, and that one
// substitution is the whole of step 3 of CHUNK-ADDRESSING-SPEC. Ranges flattens: it
// reports the base's ranges merged with this layer's, so a clone that wrote one sector
// wrote down a manifest naming every byte of its parent's dataset — and, with the chunk
// store now shared across a lineage, re-chunked and re-uploaded whichever of those chunks
// its own boundaries no longer matched. The delta reports what these layers state and
// leaves the ancestry to the chain.
//
// The bytes still come from `view.Read`, through the whole layering, and that is not an
// oversight: every offset the delta reports as data is held by a layer at or above
// `inherited`, so Read answers it out of exactly the layers the manifest is claiming.
func uploadChunks(ctx context.Context, store objectstore.Store, rnd io.Reader, enc *wal.Encryption, id Ident, view, inherited *cow.IntervalMap, seq uint64) (Manifest, error) {
	own, err := view.DeltaOver(inherited)
	if err != nil {
		return Manifest{}, fmt.Errorf("image: volume %s: %w", format.UUIDString(id.Volume), err)
	}

	man := Manifest{VolumeID: format.UUIDString(id.Volume), Sequence: seq}
	// The erasures first, and they are recorded even when this volume uploads nothing at
	// all: a stop whose only guest activity was a DISCARD still has something to say, and
	// a manifest that omitted it would let the ancestor's bytes back through on the next
	// boot.
	for _, r := range own.Discarded {
		man.Discarded = append(man.Discarded, Span{Offset: r.Offset, Length: r.Length})
	}

	for _, r := range own.Data {
		for off := r.Offset; off < r.Offset+r.Length; {
			n := min64(uint64(MaxChunkBytes), r.Offset+r.Length-off)
			buf := make([]byte, n)
			view.Read(off, buf)

			sum := sha256.Sum256(buf)
			digest := hex.EncodeToString(sum[:])

			// A chunk that already exists holds exactly these bytes — the key is their
			// digest — so it is skipped rather than re-uploaded. That is what makes a
			// second stop (or a second snapshot) cheap when little changed, and it is
			// also what makes the encryption safe: skipping means a chunk is sealed
			// exactly once, ever, so its nonce covers exactly one plaintext.
			//
			// **The skip now spans the lineage**, which is the whole of what the key
			// space change buys: a clone Heads the key its parent already created and
			// transfers nothing for every chunk it did not touch. It is also what makes
			// the sentence above true at the scope of the DEK rather than of the volume
			// — see the package doc, which used to claim it at the wrong scope.
			if _, err := store.Head(ctx, chunkKey(id.Lineage, digest)); err == nil {
				man.Chunks = append(man.Chunks, Chunk{Offset: off, Length: n, Digest: digest})
				off += n
				continue
			} else if !errors.Is(err, objectstore.ErrNotFound) {
				return Manifest{}, fmt.Errorf("image: checking chunk at %d: %w", off, err)
			}

			body, err := seal(rnd, enc, id.Lineage, digest, buf)
			if err != nil {
				return Manifest{}, fmt.Errorf("image: sealing chunk at %d: %w", off, err)
			}
			// Create-only: a race that puts the same bytes under the same key is
			// harmless — whichever ciphertext wins decrypts to the same plaintext, since
			// every volume of a lineage seals with the lineage's one DEK.
			_, err = store.Put(ctx, chunkKey(id.Lineage, digest), body, objectstore.PutOptions{IfNoneMatch: true})
			if err != nil && !errors.Is(err, objectstore.ErrPreconditionFailed) {
				return Manifest{}, fmt.Errorf("image: uploading chunk at %d: %w", off, err)
			}
			man.Chunks = append(man.Chunks, Chunk{Offset: off, Length: n, Digest: digest})
			off += n
		}
	}
	return man, nil
}

// Load reads a volume's image as a layer over base, and returns the manifest's ETag so
// the caller can CAS against it when it publishes in turn.
//
// A volume with no manifest is ErrNotPublished, which callers treat as "boot empty":
// a volume being served for the first time has written nothing, and refusing it would
// make the first boot the one case that cannot work.
//
// # base is the ancestry, and it is now required rather than forbidden
//
// This function used to return an **unlayered** map and take no base at all, and the
// reason was a real protection: a manifest was flattened, so it already held everything
// the volume could read, and sliding a parent underneath it would have uncovered every
// range the guest discarded — the discard having been written down as absence
// (agent.fetchBase, CHUNK-ADDRESSING-SPEC §2).
//
// Since a manifest became a delta, both halves of that reversed. The image no longer
// holds what the volume inherited, so it *must* be laid over the ancestry or the clone
// reads zeros for everything above it; and a discard is no longer absence, it is
// Manifest.Discarded, replayed here as a tombstone. The protection is not gone, it moved
// into the format — which is where it can also survive a restart.
//
// It takes the lineage as well as the volume, and a clone genuinely cannot be loaded
// without it: its own manifest names chunks that live under its lineage's prefix, so a
// caller that does not know the root cannot find one byte of its data. That is the price
// of the shared chunk store, and it is paid at the one place that can afford it —
// agent.fetchBase already resolves the lineage before it loads anything.
func Load(ctx context.Context, store objectstore.Store, enc *wal.Encryption, id Ident, base *cow.IntervalMap) (*cow.IntervalMap, Manifest, string, error) {
	return loadManifest(ctx, store, enc, id, ManifestKey(id.Volume), base)
}

// ReadSnapshotManifest returns a snapshot's manifest without materialising its data —
// what it covers and at which sequence, not the bytes. It needs no key: the manifest is
// structural metadata and carries no guest data (§15.3), so a catalog rebuilt from a
// bucket does not need the KEK, while reading the chunks would.
func ReadSnapshotManifest(ctx context.Context, store objectstore.Store, volumeID [16]byte, snapshotID string) (Manifest, error) {
	man, _, err := readManifest(ctx, store, volumeID, SnapshotKey(volumeID, snapshotID))
	return man, err
}

// readManifest fetches and validates a manifest object. The volume-id check is here
// rather than at the call sites because it is what catches a bucket copied under the
// wrong prefix, and every reader needs it.
func readManifest(ctx context.Context, store objectstore.Store, volumeID [16]byte, key string) (Manifest, string, error) {
	head, err := store.Head(ctx, key)
	if errors.Is(err, objectstore.ErrNotFound) {
		return Manifest{}, "", ErrNotPublished
	}
	if err != nil {
		return Manifest{}, "", fmt.Errorf("image: reading %s: %w", key, err)
	}
	body, err := store.Get(ctx, key)
	if err != nil {
		return Manifest{}, "", fmt.Errorf("image: reading %s: %w", key, err)
	}
	var man Manifest
	if err := json.Unmarshal(body, &man); err != nil {
		return Manifest{}, "", fmt.Errorf("image: parsing %s: %w", key, err)
	}
	if man.VolumeID != format.UUIDString(volumeID) {
		return Manifest{}, "", fmt.Errorf("image: manifest at %s describes volume %s", key, man.VolumeID)
	}
	return man, head.ETag, nil
}

// loadManifest reads one manifest and the chunks it names. Shared by Load and
// LoadSnapshot, which differ only in which key they read.
//
// Every failure is closed. A manifest that names a chunk which is not there is a *broken*
// image, not an empty one, and the difference matters: an empty view reads as zeros, and
// a guest cannot tell those from a range it never wrote.
func loadManifest(ctx context.Context, store objectstore.Store, enc *wal.Encryption, id Ident, key string, base *cow.IntervalMap) (*cow.IntervalMap, Manifest, string, error) {
	man, etag, err := readManifest(ctx, store, id.Volume, key)
	if err != nil {
		return nil, Manifest{}, "", err
	}

	// Layered whether or not there is a base, and a nil base is not the same thing as no
	// layering. A manifest is a delta: it may carry tombstones, and cow only records one
	// on a layered map (IntervalMap.Clear) — an unlayered map would silently drop exactly
	// the ranges the guest erased. The bottom of a chain therefore looks the same as any
	// other link, and reads identically: over a nil base, Read clears the buffer first,
	// so absence and a tombstone both answer zeros.
	view := cow.NewIntervalMapOver(base)
	// The erasures before the chunks. They are disjoint for a manifest this package
	// wrote — a delta reports a range as data or as discarded, never both — so the order
	// changes nothing here; it decides what a hand-edited or corrupted manifest does, and
	// a write winning over an erasure is the less destructive of the two answers.
	for _, s := range man.Discarded {
		view.Clear(s.Offset, s.Length)
	}
	for _, c := range man.Chunks {
		data, err := store.Get(ctx, chunkKey(id.Lineage, c.Digest))
		if err != nil {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s at offset %d: %w", c.Digest, c.Offset, err)
		}
		plain, err := open(enc, id.Lineage, c.Digest, data)
		if err != nil {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s at offset %d: %w", c.Digest, c.Offset, err)
		}
		// The digest is the key, so verifying it checks the store kept its promise
		// rather than checking our own arithmetic — and a backend that returns the wrong
		// object for a key is exactly what §6.1's conformance suite exists to catch. It
		// is checked on the *plaintext*, because that is what the key names.
		if sum := sha256.Sum256(plain); hex.EncodeToString(sum[:]) != c.Digest {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s does not hash to its key", c.Digest)
		}
		if uint64(len(plain)) != c.Length {
			return nil, Manifest{}, "", fmt.Errorf("image: chunk %s is %d bytes, manifest says %d",
				c.Digest, len(plain), c.Length)
		}
		view.Overwrite(c.Offset, plain)
	}
	return view, man, etag, nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// seal wraps one chunk. A nil enc is the unencrypted mode — no KMS on this Agent, which
// §15 allows only for dev — and the bytes go as they are.
//
// The AAD binds the ciphertext to the *lineage* and to the chunk's identity, so a chunk
// cannot be moved between lineages or relabelled inside one and still open. It does not
// bind the volume that wrote it, and that is the point of the whole key space: a chunk a
// parent sealed is one its clone opens, under the DEK they share. See the package doc for
// what protects a chunk from being substituted for another *inside* a lineage — the
// digest check in loadManifest, not this.
func seal(rnd io.Reader, enc *wal.Encryption, lineageRoot [16]byte, digest string, plain []byte) ([]byte, error) {
	if enc == nil {
		return plain, nil
	}
	nonce, sealed, err := enc.DEK.SealRandom(rnd, chunkAAD(lineageRoot, digest), plain)
	if err != nil {
		return nil, err
	}
	return append(nonce[:], sealed...), nil
}

// open is seal's inverse. It refuses a body too short to hold a nonce rather than
// slicing past the end of it.
func open(enc *wal.Encryption, lineageRoot [16]byte, digest string, body []byte) ([]byte, error) {
	if enc == nil {
		return body, nil
	}
	if len(body) < crypto.NonceSize {
		return nil, fmt.Errorf("image: chunk is %d bytes, too short to carry a nonce", len(body))
	}
	var nonce [crypto.NonceSize]byte
	copy(nonce[:], body[:crypto.NonceSize])
	return enc.DEK.OpenRandom(nonce, chunkAAD(lineageRoot, digest), body[crypto.NonceSize:])
}

func chunkAAD(lineageRoot [16]byte, digest string) []byte {
	return append(append([]byte("image-chunk"), lineageRoot[:]...), digest...)
}
