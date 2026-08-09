package lineage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
)

// ErrSelfContained means the volume already owes nothing to an ancestry: its descriptor
// names no parent. It is a shape rather than a failure — an operator re-running a flatten
// that already succeeded should be told it is done, not handed an error — which is why it
// is a sentinel the caller branches on.
var ErrSelfContained = errors.New("lineage: this volume descends from nothing, so there is nothing to flatten")

// ErrHasSnapshots means the volume has published snapshots of its own, and they are what
// makes it unflattenable. See Flatten.
var ErrHasSnapshots = errors.New("lineage: this volume has published snapshots, which are immutable deltas over the ancestry it is being asked to leave")

// ErrAlreadyStarted means a previous flatten of this volume wrote chunks under its own
// lineage. See Flatten's note on the one window that is not re-runnable.
var ErrAlreadyStarted = errors.New("lineage: a previous flatten of this volume left chunks under its own lineage")

// ErrImageMissing means the volume's own image is not in the object store although the
// catalog says it published one. See Flatten's note on the two questions that get the same
// answer.
var ErrImageMissing = errors.New("lineage: the catalog says this volume has published an image and the object store holds none")

// Result is what a flatten did, for the operator watching it.
type Result struct {
	// Ancestors is how many links the volume no longer reads through.
	Ancestors int
	// ParentSnapshotID is the link that was cleared, so the line that reports the flatten
	// names what the volume used to descend from.
	ParentSnapshotID string
	// ParentVolumeID is that snapshot's volume — the other half of the link.
	ParentVolumeID string
	// Runs, Erased and Bytes are what the flattened image states: the whole dataset and
	// every range the guest erased over it, not the delta, because that is what the volume
	// now states on its own account.
	Runs   int
	Erased int
	Bytes  int64
}

// Flatten makes a clone self-contained: it writes an image that owes nothing to its
// ancestors, and then stops the bucket saying it has any.
//
// It is the operation the chain-depth decision made load-bearing for
// two separate things. It is the only way back *under* the depth ceiling, now that a clone
// reads through its ancestry on every attach rather than through a flattened copy of it.
// And it is the only way to delete a parent that has clones:
// answer B, "a delete of a volume with descendants flattens them first" — because since
// publishing stopped flattening, a clone never becomes independent on its own.
//
// # It runs here, not on an Agent, and the volume being detached is the reason
//
// A flatten reads a chain and writes an image. The Agent has the chain-walking code and
// the DEK; the Control Plane has the catalog. Neither of those decides it. What decides it
// is that **the volume must not be served while this runs**: the flatten CASes the
// volume's manifest, and a host that is serving it holds the ETag it loaded and will CAS
// against it at its stop — so a flatten under a live Agent makes that Agent's publish fail
// with image.ErrSuperseded, which is one of the three failures its teardown deliberately
// does not retry, and the session is lost. So a flatten's
// precondition is a detached volume, and a detached volume is in no host's desired state:
// there is no Agent to ask. Giving one a flatten verb would mean handing it a volume it is
// not serving, with no device and no lock, driven by a Control Plane it is not allowed to
// know about (ADR-0021 §4, which keeps the host-side type usable by spin's runner without
// the reconciliation loop).
//
// Rejected: reviving a background operation. The `operations` table that would have
// scheduled it was retired with ADR-0017's second capacity term, and every other admin
// action in this repository is a one-shot flag. ADR-0021 §2 says these binaries are
// harnesses; a one-shot is the shape that fits.
//
// # What it costs, which is a full copy and is not an accident
//
// The volume becomes **its own lineage root**, so every chunk it reads is re-sealed and
// re-uploaded under `chunks/<this volume>/`. That is not tidiness: a volume's lineage root
// is derived from its chain (Root), so a volume with no chain is its own root by
// definition, and leaving its bytes under its parent's prefix would leave a manifest
// naming chunks nothing will ever look for. It also happens to be what makes the operation
// mean anything for a delete — a flattened clone whose bytes still lived in its parent's
// chunk store would keep every one of them alive, and deleting the parent would free
// nothing.
//
// The bill is therefore the whole dataset, once, at an operator's request. It is the same
// copy the eager flattening used to pay at every clone's first stop, moved to the one
// moment somebody asked for it.
//
// Rejected: naming the ancestors' chunk objects from the flattened manifest and moving no
// bytes at all. It is what the shared chunk store makes *almost* possible, and it fails on
// one thing: a Chunk names a whole object (offset, length, digest) and the format has no
// way to name a slice of one, so a run composed from a parent's 8 MiB chunk and a clone's
// 512-byte write cannot be expressed without re-chunking. Giving Chunk a `skip` field
// would fix that, and it is an on-S3 format change in a human-review zone with its own
// §25.2 obligations — not something to fold into the increment that first needs it.
//
// # The order, and the one window that is not re-runnable
//
// Chunks, then the manifest (image.Publish's own order), then the descriptor. Each earlier
// step is what the next one's meaning depends on, and the failure of any of them leaves
// the volume refusing reads rather than answering wrong ones:
//
//   - before the manifest CAS, nothing has changed: the volume is a delta over its
//     ancestry and reads exactly as it did;
//   - between the manifest and the descriptor, the volume's image is self-contained under
//     its own lineage while the bucket still says it has ancestors, so a reader resolves
//     the old root and finds none of the new chunks — a refusal, not a wrong byte.
//
// That middle state is the one thing here that a re-run does not repair: the flatten would
// try to load the volume's own image under the *old* root and fail on a missing chunk. It
// is refused by name (ErrAlreadyStarted) rather than met with a confusing "chunk not
// found", because the recovery is to rewrite the descriptor and the operator needs to be
// told that rather than left to infer it. The reverse order was considered and is worse:
// clearing the descriptor first destroys the only record of what the volume descends from,
// so a flatten that then failed could never be resumed at all, by anyone.
//
// # publishedSequence is a parameter, and it is a parameter on purpose
//
// It is the catalog's count of what this volume has published, and this package has no way
// to learn it: `lineage` reads the bucket, and the bucket cannot tell "this clone never
// owned a layer" from "this clone's layer was deleted". The Control Plane holds the catalog
// and is the only caller that can answer.
//
// Rejected: leaving the check in cmd/control-plane, which already has the row in hand. It
// would have to ask whether the manifest exists to know whether the check even applies —
// the same rule stated twice, in two packages, with a window between the caller's HEAD and
// this function's GET — and it is a check a second caller forgets by writing nothing. A
// parameter cannot be forgotten: every call site has to produce the number, and the two
// that existed when this was added were each made to say where theirs comes from. That is
// worth one breaking signature for an operation whose failure mode is silent data loss.
//
// # Why a volume with published snapshots is refused
//
// A snapshot is immutable (§5.2, INV-16) and every snapshot this volume published while it
// was a clone is a *delta* over the ancestry. Flattening the volume clears the link its own
// descriptors' walk follows, so a future clone of one of those snapshots would stop at this
// volume and read zeros for everything the ancestry held. There is no way to flatten a
// snapshot — rewriting it is exactly what create-only forbids — so the refusal is a
// statement of fact and not caution: the way to make such a volume flattenable is to delete
// those snapshots.
func Flatten(ctx context.Context, store objectstore.Store, rnd io.Reader, enc *wal.Encryption, volumeID string, publishedSequence int64) (Result, error) {
	u, err := ids.Parse(volumeID)
	if err != nil {
		return Result{}, fmt.Errorf("lineage: volume %q is not a uuid: %w", volumeID, err)
	}
	vol := [16]byte(u)

	self, err := descriptor.Read(ctx, store, volumeID)
	if err != nil {
		return Result{}, fmt.Errorf("lineage: volume %s: reading %s: %w", volumeID, descriptor.Key(volumeID), err)
	}
	if self.ParentSnapshotID == "" {
		return Result{}, fmt.Errorf("%w: %s", ErrSelfContained, volumeID)
	}

	snaps, err := store.List(ctx, image.Prefix(vol)+"snapshots/")
	if err != nil {
		return Result{}, fmt.Errorf("lineage: volume %s: listing its snapshots: %w", volumeID, err)
	}
	if len(snaps) > 0 {
		return Result{}, fmt.Errorf("%w: %s holds %d of them, the first being %s",
			ErrHasSnapshots, volumeID, len(snaps), snaps[0].Key)
	}
	// The volume's own chunk prefix is empty until a flatten writes into it: every publish
	// it has ever made went to its lineage's, which is its parent's. So a non-empty one is
	// the fingerprint of a flatten that did not finish, and it is the only cheap thing in
	// the bucket that says so.
	own, err := store.List(ctx, image.ChunksPrefix(vol))
	if err != nil {
		return Result{}, fmt.Errorf("lineage: volume %s: listing its own chunk store: %w", volumeID, err)
	}
	if len(own) > 0 {
		return Result{}, fmt.Errorf("%w: %s holds %d object(s) under %s. If %s is already flattened, what is left is to rewrite %s with no parent link; if it is not, the objects are unreferenced and cost storage only",
			ErrAlreadyStarted, volumeID, len(own), image.ChunksPrefix(vol), image.ManifestKey(vol), descriptor.Key(volumeID))
	}

	chain, err := Walk(ctx, store, volumeID)
	if err != nil {
		return Result{}, err
	}
	root := Root(vol, chain)

	// The foot of the world. It is an empty map rather than nil, and that one difference is
	// what makes the manifest below flattened: image.Publish writes down what its view
	// states *over the map it is handed*, so publishing over nil would write down this
	// volume's own layers again — the delta it already has — and publishing over the map at
	// the bottom of the composed chain writes down everything the chain answers with.
	bottom := cow.NewIntervalMap()

	ancestry, err := Compose(ctx, store, enc, root, chain, bottom)
	if err != nil {
		return Result{}, err
	}

	// The volume's own image, over its ancestry: exactly the view its Agent composes at
	// attach (agent.fetchBase), and therefore exactly what its guest reads. Anything else
	// here would be a flatten that wrote down a view nobody was reading.
	view, man, etag, err := image.Load(ctx, store, enc, image.Ident{Volume: vol, Lineage: root}, ancestry)
	switch {
	case errors.Is(err, image.ErrNotPublished):
		// A clone that has never stopped cleanly has no image of its own, and flattening it
		// is the useful case rather than an edge one: it is a volume that has read through
		// its parent since the moment it was created. Its view is the ancestry outright,
		// and its publish is create-only.
		//
		// **Only when the catalog agrees it never published**, which is why this function
		// takes a number it cannot look up. "There is no manifest for this volume" is the
		// object store's one answer to two questions, and until publishedSequence reached
		// here the answer was read as the first of them unconditionally: a stray delete, a
		// lifecycle expiry or a restore that missed one key produced a flatten that wrote
		// the *ancestry* down as the volume's whole content, published it over the missing
		// manifest (create-only, so it succeeds), and rewrote the descriptor so nothing
		// would ever look for the old layer again. Every byte the clone wrote, gone, with
		// no error anywhere and an operator running a documented command. It is the same
		// hole the attach path closed with agent.ErrImageMissing, on the one path that was
		// not the attach path.
		//
		// `> 0` rather than a comparison with the manifest's sequence, because there is no
		// manifest to compare with: what is being told apart is absence from never-existed,
		// and the catalog's count of what the volume published is the only fact that does
		// it.
		//
		// Refusing costs a flatten that will not run, which an operator can see and repair
		// by restoring the object — and a repair is possible precisely because nothing has
		// been overwritten yet. Carrying on costs the data.
		if publishedSequence > 0 {
			return Result{}, fmt.Errorf("%w: volume %s published up to sequence %d and %s does not exist, so a flatten now would write down its ancestry alone and drop everything the volume wrote; restore that object before flattening",
				ErrImageMissing, volumeID, publishedSequence, image.ManifestKey(vol))
		}
		view, man, etag = ancestry, image.Manifest{}, ""
	case err != nil:
		return Result{}, fmt.Errorf("lineage: volume %s: loading its own image: %w", volumeID, err)
	}

	// image.OwnLineage, which is the whole re-homing: the chunks go under this volume's own
	// id and their AAD binds it, because from the next write onwards that is what its
	// lineage root is.
	//
	// bottom as the inherited map, not nil, and what that buys is the erasures. cow.DeltaOver
	// drops tombstones when it is given nil, on the sound argument that a volume with nothing
	// underneath cannot tell "I hold nothing here" from "this was discarded" — but this
	// volume *has* something underneath until the descriptor below is written, and a reader
	// in that window lays this manifest over the chain.
	//
	// Only one class of erasure is actually at stake, and it is worth being exact because an
	// earlier draft of this comment claimed more and a planted bug disproved it: an erasure an
	// *ancestor* made is already stated by that ancestor's own manifest, so the composed
	// ancestry carries it whatever this manifest says. What is at stake is a range **this
	// volume** discarded over bytes an ancestor holds. Publish over nil and that range becomes
	// absence, absence in a layered read means "ask the layer below", and the guest gets back
	// the bytes it freed (§14.6) — reproduced by the plant, which reads 0xa1 where it must
	// read zeros.
	//
	// The CAS is against the manifest this flatten read, which is the same fence every
	// publish uses: an Agent that took the volume between the load and here loses one of the
	// two, loudly.
	if _, err := image.Publish(ctx, store, rnd, enc, image.OwnLineage(vol), view, bottom, man.Sequence, etag); err != nil {
		return Result{}, fmt.Errorf("lineage: volume %s: publishing its flattened image: %w", volumeID, err)
	}

	// What was written, measured from the same delta the publish serialised rather than by
	// reading the manifest back: it is the one number an operator asked for — how much this
	// volume now states on its own account — and a re-read would price a second GET to learn
	// what this process already computed. It is a run count, not a chunk count: image
	// re-chunks a run at MaxChunkBytes, so the two differ only for a volume past 64 MiB.
	res := Result{
		Ancestors:        len(chain),
		ParentSnapshotID: self.ParentSnapshotID,
		ParentVolumeID:   self.ParentVolumeID,
	}
	if own, derr := view.DeltaOver(bottom); derr == nil {
		res.Runs, res.Erased = len(own.Data), len(own.Discarded)
		for _, r := range own.Data {
			res.Bytes += int64(r.Length)
		}
	}

	// The link goes last, and with it the depth. ChainDepth is what §20.1's ceiling counts
	// and what controlplane.Clone refuses on, so a flatten that left it at N would have made
	// the image self-contained and left the volume just as unclonable as before.
	self.ParentSnapshotID, self.ParentVolumeID, self.ChainDepth = "", "", 0
	if err := descriptor.Write(ctx, store, self); err != nil {
		return res, fmt.Errorf("lineage: volume %s: its image is flattened under its own lineage but %s still names a parent, so it will refuse every read until that object is rewritten: %w",
			volumeID, descriptor.Key(volumeID), err)
	}
	return res, nil
}
