package controlplane

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/placement"
)

// MaxChainDepth is the deepest lineage this Control Plane will create. A volume that was
// created rather than cloned is at depth 0, so the ceiling admits five clone links above
// a root and refuses the sixth.
//
// **What one link costs, taken from the measurement rather than from the document.**
// `agent.TestWhatDepthCostsAtAttach` drives a real VolumeManager over lineages of one, two
// and three links and counts the object reads an attach issues. Each ancestor costs three
// fixed reads — its `descriptor.json`, then a Head and a Get for its snapshot manifest —
// plus one Get per chunk that manifest names. Only the three are depth's own cost: the
// chunk Gets are the dataset, which a volume downloads whatever shape its lineage has. So
// at this ceiling an attach pays fifteen fixed round trips where a root pays none, and
// that is nowhere near the wall — the wall is `agent.awaitBase`'s ShutdownGrace, which
// bounds one wait for a read view, and at any plausible per-request latency it sits an
// order of magnitude further out. That is what `agent.maxChainWalk`'s termination guard
// being far above this number means in practice, and it is why attach does not choose it.
//
// **What does choose it is the steady state, and the steady state has no knee.**
// `cow.IntervalMap.Read` recurses into its base unconditionally and scans every extent of
// every layer it crosses, so at depth D *every* guest read scans D+1 extent lists, and the
// host holds D+1 interval maps for each attached clone (`cow.Cost`, and the read_view_*
// series that report it). Both are linear, and nothing distinguishes four from five from
// six. There was therefore no cliff to derive a number from, and inventing one from a
// benchmark would have dressed a policy up as a measurement.
//
// So the number is a policy about how much read amplification a lineage may accumulate
// before an operator is made to FLATTEN — and it is the one §20.1, §10's `max_chain_depth`
// and the design document already tell an operator, which is worth more than a fresh
// number with the same justification. Two measurements would move it: a per-link constant
// that stops being small (links whose manifests each name many chunks make the attach the
// binding cost rather than the read), or an index in `cow` that stops a read from scanning
// every layer.
//
// Rejected: a flag on cmd/control-plane. A ceiling an operator can raise per invocation is
// one that gets raised during the incident it exists to prevent, and the number only means
// anything if every clone in the fleet was admitted against the same one.
const MaxChainDepth = 5

// ErrChainTooDeep is what a clone past MaxChainDepth is refused with.
//
// A sentinel rather than a bare error because this is the one refusal in Clone that an
// operator can act on: every other one says the fleet or the snapshot is wrong, and this
// one says the lineage is long and names the verb that shortens it. The caller that will
// branch on it is FLATTEN's one-shot, which has to tell "you are at the ceiling" apart
// from "that snapshot does not exist".
var ErrChainTooDeep = errors.New("controlplane: the lineage is at its depth ceiling")

// Clone creates a new volume from a parent snapshot (§20): it is pure metadata — a new
// active child at epoch 1 that reads *through* the parent snapshot's already-durable
// objects rather than copying them. The clone inherits the parent's size, block size, and
// DEK (so it can read the shared base), and takes the next chain depth — or is refused
// with ErrChainTooDeep if that would be past MaxChainDepth (§20.1). Returns the new
// volume's descriptor-shaped record.
//
// It is not "independent", and it stopped being so deliberately. Until publishing stopped
// flattening, a clone's first stop re-uploaded everything it had inherited under its own
// id, so every image was self-contained and `chain_depth` counted a lineage nobody walked;
// now a manifest states what its volume itself wrote and a read walks the ancestry
// (`image.uploadChunks`, `agent.parentView`). That is what makes the ceiling below a real
// bound rather than a number, and what makes deleting a parent take its descendants' data
// with it: deleting a parent whose clones still read through it is refused, not cascaded.
//
// **Where it lands is decided here, not passed in.** policy.Choose implements §20's
// three steps — source host, a host with the data cached, any host with capacity — and
// the snapshot's source_host_id is what makes step 1 expressible at all: it is a fact
// rather than a guess, because the host that took the snapshot stamped it (§19,
// increment 3b). Same-host is a *preference*, not a requirement: Choose falls through
// when that host is full, cordoned or gone, and making it mandatory would couple
// scheduling to a host with no obligation to be up.
//
// **What step 1 buys today is nothing, and this comment claimed otherwise for two waves.**
// It said a same-host clone "reads local NVMe" while a cross-host clone pays the download.
// No code path provides that: `agent.parentView` calls `image.LoadSnapshot` on every
// attach, which GETs every chunk the ancestry names, on the host that took the snapshot
// exactly as on any other; the only cache in `internal/agent` holds VolumeKeys, and
// nothing there reads another volume's local segments. The locality the sentence described
// was EROFS plus a checkpoint plus a cached WAL, which ADR-0026 deleted — the preference
// outlived the thing it was a preference for. It is left in place rather than removed
// because the preference costs nothing, is still the right destination if that cache is
// ever built, and deleting it would also delete the only reason source_host_id is carried
// into placement; what is removed is the claim. The chain-addressing decision
// records the same finding, which is where it was found.
//
// The capacity ceilings travel with the write rather than being checked here
// (ADR-0017): Choose is pure and advisory, so two callers reading the same fleet pick
// the same destination and both commit. policy.Bound hands the statement that places
// the bytes the same two numbers Choose admitted against — §28.2 on what the host has
// been promised, and ADR-0013's fill ceiling on what it measured itself using.
// KeyRewrapper is what a clone needs from the KMS: open the parent's wrapped DEK and
// seal the same key bytes again under the child's id.
//
// It is a second, narrow interface rather than a widening of KeyWrapper, whose comment
// — "provisioning has no business unwrapping anything" — stays true. Clone is the one
// verb that must do both, and it must, because a wrap is bound to the volume that
// carries it (crypto.wrapAAD): copying the parent's ciphertext into the child's row
// would produce a child nothing can open.
//
// No new secret reaches a new process: the Control Plane already handles a plaintext
// DEK at provision. What it does mean is that `-clone-snapshot` now needs `-kek-file`.
type KeyRewrapper interface {
	KEKID() string
	UnwrapDEK(wrapped []byte, keyID uint32, volumeID [16]byte) (crypto.DEK, error)
	WrapDEK(r io.Reader, dek crypto.DEK, volumeID [16]byte) ([]byte, error)
}

func Clone(ctx context.Context, md metadata.Store, store objectstore.Store, kms KeyRewrapper,
	rand io.Reader, policy placement.Policy,
	rec *obs.Recorder, term int64, parentSnapshotID, newVolumeID string,
) (metadata.Volume, error) {
	snap, err := md.GetSnapshot(ctx, parentSnapshotID)
	if err != nil {
		return metadata.Volume{}, err
	}
	if snap.State != lifecycle.SnapshotPublished {
		// A clone of a snapshot whose objects are not written yet reads zeros for
		// everything its parent wrote — DEV-0007's shape, reached through the catalog
		// instead of through a missing field.
		return metadata.Volume{}, fmt.Errorf("controlplane: snapshot %s is %s, not PUBLISHED: nothing has been written for a clone to read",
			parentSnapshotID, snap.State)
	}
	parent, err := md.GetVolume(ctx, snap.VolumeID)
	if err != nil {
		return metadata.Volume{}, err
	}
	// The ceiling refuses here — before a host is chosen, before a row exists, before a
	// byte is charged and before a descriptor is written — because a refusal that has
	// already written something is a refusal an operator has to clean up after.
	//
	// This is also the only place it *can* refuse. `agent.maxChainWalk` is a termination
	// guard on a walk, and refusing there would turn a volume the fleet created
	// successfully into one nothing can read: the guest is already booting, and the
	// operator's only remedy would be a FLATTEN of a volume that cannot be attached.
	// Refusing at create costs an operator one command they have not run yet.
	//
	// **The number it compares is the parent volume's, and that holds only while a
	// snapshot is at its volume's depth.** It is today: a snapshot is a delta published by
	// the volume it belongs to, so its ancestry is that volume's ancestry. FLATTEN is what
	// can break it — a flattened volume goes back to depth 0 while the snapshots it
	// published before are still deltas over the old lineage, and a clone of one of those
	// would be admitted at depth 1 while reading through as many ancestors as ever. So
	// whatever FLATTEN does about its volume's earlier snapshots, it must leave that
	// sentence true, or this comparison stops describing the read path it exists to bound.
	if parent.ChainDepth >= MaxChainDepth {
		return metadata.Volume{}, fmt.Errorf(
			"%w: volume %s is at depth %d, so a clone of snapshot %s would be depth %d and the ceiling is %d. "+
				"Every guest read on a clone scans one extent list per link and every attach reads one more ancestor, "+
				"which is what this refuses to grow further. FLATTEN volume %s — the operator one-shot that makes it "+
				"self-contained and returns it to depth 0 — then snapshot the flattened volume and clone that",
			ErrChainTooDeep, parent.VolumeID, parent.ChainDepth, parentSnapshotID,
			parent.ChainDepth+1, MaxChainDepth, parent.VolumeID)
	}
	hosts, err := md.ListHosts(ctx)
	if err != nil {
		return metadata.Volume{}, err
	}
	newHostID, err := policy.Choose(hosts, placement.Request{
		SizeBytes: parent.SizeBytes,
		// The host that took the snapshot still has its data on local NVMe.
		SourceHostID: snap.SourceHostID,
	})
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: placing a clone of snapshot %s: %w", parentSnapshotID, err)
	}
	// §26.2's clone_same_host_total, recorded where the placement decision is made
	// because that is the only place that knows both what was asked for and what was
	// chosen. Same-host is the case §20 exists to produce — the clone starts where its
	// data already is — so a fleet where this counter stays flat is one where
	// placement is not buying what the design says it buys.
	if rec != nil && newHostID == snap.SourceHostID && snap.SourceHostID != "" {
		rec.Count(ctx, "clone_same_host_total", 1)
	}

	var bound *metadata.CapacityBound
	for _, h := range hosts {
		if h.HostID == newHostID {
			bound = policy.Bound(h, parent.SizeBytes)
		}
	}
	if bound == nil {
		// Unreachable: Choose returns a host from this slice. Said out loud because
		// the failure mode of "leave the bound nil" is silence — an unbounded write
		// is how the catalog reads "not a placement decision", so a clone would be
		// placed with no ceiling at all rather than refused.
		return metadata.Volume{}, fmt.Errorf("controlplane: placing a clone of snapshot %s: chose host %s, which is not in the fleet it was chosen from",
			parentSnapshotID, newHostID)
	}
	// The shared DEK, re-wrapped under the child's id rather than copied.
	//
	// **The key bytes are the parent's and stay the parent's** (§10: a lineage shares one
	// DEK, because the clone reads layers the parent sealed). What changes is the
	// ciphertext: a wrap is bound to the volume that carries it, so the child's
	// descriptor must carry a wrap that names the child. Copying `parent.DEKWrapped`
	// verbatim — which is what this did until 2026-08-27 — would hand the child a blob
	// that fails to unwrap under its own id.
	//
	// It costs one unwrap and one 12-byte nonce per clone, and it buys the property that
	// makes the binding worth anything: *every* volume's wrap names that volume, root or
	// clone, so a descriptor swap has nowhere to hide.
	//
	// **Crypto-shred stays lineage-scoped, and this is the line that makes it so.** The
	// key *bytes* are shared, so deleting the parent destroys no secret the child does
	// not still hold, and the parent's layers stay openable by anyone holding those bytes
	// plus two public identifiers. A FLATTEN that re-uploads a clone's data must
	// therefore mint a *fresh* DEK while it does it; if it keeps the shared key, deleting
	// the flattened clone's parent shreds nothing and the delete verb's promise is false.
	// There is no flatten in the tree yet — this is the constraint it has to be built
	// under, written here because here is where the sharing happens.
	parentID, err := ids.Parse(snap.VolumeID)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: parent volume id %q: %w", snap.VolumeID, err)
	}
	// A clone mints a fresh id, and the catalog's re-create path is not a way to get one.
	// CreateVolume converges onto a row that already exists — deliberately, so that two
	// operators running rebuild-metadata at once do not undo each other — so a clone
	// pointed at a live volume's id was a *merge* into it: same size, same host, and a
	// descriptor rewritten under that id. Key material and geometry are protected there
	// now, and this is the other half: the id has to be free.
	if _, err := md.GetVolume(ctx, newVolumeID); err == nil {
		return metadata.Volume{}, fmt.Errorf("%w: volume %s already exists, and a clone mints a new id",
			metadata.ErrAlreadyPlaced, newVolumeID)
	} else if !errors.Is(err, metadata.ErrNotFound) {
		return metadata.Volume{}, fmt.Errorf("controlplane: checking that %s is free: %w", newVolumeID, err)
	}
	childID, err := ids.Parse(newVolumeID)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: new volume id %q: %w", newVolumeID, err)
	}
	dek, err := kms.UnwrapDEK(parent.DEKWrapped, parent.DEKKeyID, [16]byte(parentID))
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: opening the DEK of parent volume %s to clone it: %w", parent.VolumeID, err)
	}
	rewrapped, err := kms.WrapDEK(rand, dek, [16]byte(childID))
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: re-wrapping the lineage DEK for clone %s: %w", newVolumeID, err)
	}

	clone := metadata.Volume{
		VolumeID:      newVolumeID,
		SizeBytes:     parent.SizeBytes,
		BlockSize:     parent.BlockSize,
		CurrentEpoch:  1, // a fresh active child
		State:         lifecycle.VolumeActive,
		PrimaryHostID: newHostID,
		ChainDepth:    parent.ChainDepth + 1,
		DEKWrapped:    rewrapped,
		KEKID:         kms.KEKID(),
		// A clone shares the parent's DEK (§19: the chain's objects are the parent's
		// until the child writes), so it must share the *version* that names it —
		// crypto.DevKMS binds it as GCM AAD, and a clone carrying the key without the
		// version is a volume nobody can open.
		DEKKeyID: dek.KeyID,
		// And the link itself. ChainDepth above says a chain exists; these say what is
		// on the other end of it, which is what the clone's Agent needs to find the
		// objects it reads through. Without them the clone starts an empty WAL under
		// its own id, finds nothing under that id in the object store, and serves
		// zeros for everything the parent ever wrote (DEV-0007).
		ParentSnapshotID: parentSnapshotID,
		ParentVolumeID:   snap.VolumeID,
	}
	if err := md.CreateVolume(ctx, term, clone, bound); err != nil {
		return metadata.Volume{}, err
	}
	// `chain_depth` acquires the producer §26.2 declared it with and it has never had.
	//
	// **Here rather than on the Agent**, because it is the catalog's number: the one the
	// ceiling above refuses on and the one a flatten reduces. It used to have a sibling
	// on the Agent — `read_view_layers`, what a read actually walked — and comparing the
	// two was comparing a claim with what the object store made of it. That one went with
	// the read view; this one is the claim, and it is the half the Control Plane owns.
	//
	// Recorded at the change and not polled, because between a clone and a FLATTEN a
	// volume's depth cannot move: a poller would re-report, at fleet cardinality and from
	// a process that has no loop to hang it on, a number that is not allowed to have
	// changed. What that costs is stated rather than hidden: a series written only on
	// change goes quiet, so "which volumes are deep *now*" is answered by the catalog —
	// `-fleet-status` prints the column — and this answers "what did the Control Plane
	// create, and how deep was it when it did".
	//
	// After CreateVolume, deliberately. A depth nothing has committed may never exist: the
	// term guard can refuse this write, and a gauge that leads the catalog is one an
	// operator cannot reconcile with the row.
	rec.Gauge(ctx, "chain_depth", float64(clone.ChainDepth), obs.String("volume", clone.VolumeID))

	// The descriptor, for the same reason provisioning writes one: §22.5's
	// rebuild-metadata reconstructs volumes from these objects, and a clone with no
	// descriptor is a volume a restore silently loses — along with the chain link that
	// is the difference between reading its parent's data and reading zeros.
	//
	// Reported, not rolled back, exactly as provisioning does it: deleting the row here
	// would need a term-guarded delete that does not exist, and would turn one
	// repairable inconsistency into two writes that can each fail.
	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID:         clone.VolumeID,
		SizeBytes:        clone.SizeBytes,
		BlockSize:        clone.BlockSize,
		CurrentEpoch:     clone.CurrentEpoch,
		ChainDepth:       clone.ChainDepth,
		KEKID:            clone.KEKID,
		DEKWrapped:       clone.DEKWrapped,
		DEKKeyID:         clone.DEKKeyID,
		ParentSnapshotID: clone.ParentSnapshotID,
		// Both halves, because either alone is unusable. A snapshot id names an object
		// only together with the volume it lives under (image.SnapshotKey), so a
		// descriptor carrying the snapshot alone left the bucket unable to state a
		// lineage it claims to describe — the missing half was in the catalog, which is
		// the component -rebuild-metadata exists to survive the loss of. It is what
		// agent.parentView follows past the first link.
		ParentVolumeID: clone.ParentVolumeID,
	}); err != nil {
		return clone, fmt.Errorf("writing the descriptor for clone %s (the row exists; rebuild-metadata cannot see it until this succeeds): %w",
			clone.VolumeID, err)
	}
	return clone, nil
}
