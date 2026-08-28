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
// created rather than cloned is at depth 0, so five clone links are admitted above a root
// and the sixth is refused.
//
// It is a policy, not a measurement, and there is no measurement to make. Each ancestor
// costs three fixed object reads at attach (agent.TestWhatDepthCostsAtAttach: its
// descriptor, a Head and a Get for its snapshot manifest), so fifteen fixed round trips at
// this ceiling — an order of magnitude inside agent.awaitBase's ShutdownGrace. The steady
// state is what would choose the number and it has no knee: cow.IntervalMap.Read scans
// every extent list of every layer it crosses, so a guest read at depth D scans D+1, which
// is linear and does not distinguish four from five from six. So the number is §20.1's and
// §10's max_chain_depth, the one an operator has already been told, and FLATTEN is the verb
// that gets a lineage back under it. Two measurements would move it: a per-link constant
// that stops being small, or an index in `cow` that stops a read from scanning every layer.
//
// Rejected: a flag on cmd/control-plane. A ceiling an operator can raise per invocation is
// one that gets raised during the incident it exists to prevent, and the number only means
// anything if every clone in the fleet was admitted against the same one.
const MaxChainDepth = 5

// ErrChainTooDeep is what a clone past MaxChainDepth is refused with. A sentinel because
// FLATTEN's one-shot has to tell "you are at the ceiling" apart from "that snapshot does not
// exist"; every other refusal in Clone says the fleet or the snapshot is wrong.
var ErrChainTooDeep = errors.New("controlplane: the lineage is at its depth ceiling")

// Clone creates a new volume from a parent snapshot (§20): pure metadata — a new active
// child at epoch 1 that reads *through* the parent snapshot's already-durable objects
// rather than copying them, inheriting the parent's size, block size and DEK, at the
// parent's depth+1 or refused with ErrChainTooDeep. A clone is not independent: a manifest
// states only what its own volume wrote and a read walks the ancestry
// (image.uploadChunks, agent.parentView), which is why deleting a parent whose clones read
// through it is refused rather than cascaded.
//
// KeyRewrapper is what a clone needs from the KMS: open the parent's wrapped DEK and seal
// the same key bytes again under the child's id. A second, narrow interface rather than a
// widening of KeyWrapper because a wrap is bound to the volume that carries it
// (crypto.wrapAAD): copying the parent's ciphertext into the child's row would produce a
// child nothing can open. It is why `-clone-snapshot` now needs `-kek-file`.
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
	// Refused before a host is chosen, before a row exists and before a byte is charged: a
	// refusal that has already written something is one an operator has to clean up after.
	// It is also the only place it can refuse — agent.maxChainWalk is a termination guard on
	// a walk, and refusing there turns a volume the fleet created successfully into one
	// nothing can read, with a guest already booting.
	//
	// It compares the *parent volume's* depth, which holds only while a snapshot is at its
	// volume's depth. FLATTEN can break that — a flattened volume is back at depth 0 while
	// the snapshots it published before are still deltas over the old lineage — so whatever
	// FLATTEN does about them must leave that sentence true.
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
		// §20 rule 1: prefer the host that took the snapshot. A preference only — it buys
		// no locality today, since a clone reads its ancestry from the object store on any
		// host alike — and it is the destination a local cache would want.
		SourceHostID: snap.SourceHostID,
	})
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: placing a clone of snapshot %s: %w", parentSnapshotID, err)
	}
	// §26.2's clone_same_host_total, recorded here because this is the only place that knows
	// both what was asked for and what was chosen. Same-host is the case §20 exists to
	// produce, so a fleet where this counter stays flat is one where placement is not buying
	// what the design says it buys.
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
	// The shared DEK, re-wrapped under the child's id rather than copied. The key *bytes*
	// are the parent's (§10: a lineage shares one DEK, because the clone reads layers the
	// parent sealed); the ciphertext is not, because a wrap is bound to the volume that
	// carries it, so a child handed the parent's blob cannot unwrap it under its own id.
	//
	// Crypto-shred is therefore lineage-scoped, and this is the line that makes it so:
	// deleting the parent destroys no secret the child does not still hold. A FLATTEN that
	// re-uploads a clone's data must mint a *fresh* DEK while it does, or deleting the
	// flattened clone's parent shreds nothing and the delete verb's promise is false.
	parentID, err := ids.Parse(snap.VolumeID)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("controlplane: parent volume id %q: %w", snap.VolumeID, err)
	}
	// A clone mints a fresh id, and CreateVolume's converge-onto-an-existing-row path — which
	// exists so two operators running rebuild-metadata at once do not undo each other — is not
	// a way to get one: a clone pointed at a live volume's id was a merge into it. Key
	// material and geometry are protected there; this is the other half, the id being free.
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
		// And the link itself: without these the clone finds nothing under its own id in the
		// object store and serves zeros for everything the parent ever wrote (DEV-0007).
		ParentSnapshotID: parentSnapshotID,
		ParentVolumeID:   snap.VolumeID,
	}
	if err := md.CreateVolume(ctx, term, clone, bound); err != nil {
		return metadata.Volume{}, err
	}
	// `chain_depth` (§26.2), recorded here rather than on the Agent because it is the
	// catalog's number: the one the ceiling above refuses on and a FLATTEN reduces. Recorded
	// at the change and not polled, because between a clone and a FLATTEN a volume's depth
	// cannot move; the cost is that the series goes quiet, so "which volumes are deep now" is
	// answered by `-fleet-status` and this answers "how deep was it when created".
	//
	// After CreateVolume, deliberately: a depth nothing has committed may never exist — the
	// term guard can refuse this write, and a gauge that leads the catalog is one an operator
	// cannot reconcile with the row.
	rec.Gauge(ctx, "chain_depth", float64(clone.ChainDepth), obs.String("volume", clone.VolumeID))

	// The descriptor, for the same reason provisioning writes one: rebuild-metadata (§22.5)
	// reconstructs volumes from these objects, and a clone with no descriptor loses the chain
	// link that is the difference between reading its parent's data and reading zeros.
	// Reported, not rolled back, exactly as provisioning does it.
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
		// Both halves: a snapshot id names an object only together with the volume it
		// lives under (image.SnapshotKey), and agent.parentView follows both past the
		// first link.
		ParentVolumeID: clone.ParentVolumeID,
	}); err != nil {
		return clone, fmt.Errorf("writing the descriptor for clone %s (the row exists; rebuild-metadata cannot see it until this succeeds): %w",
			clone.VolumeID, err)
	}
	return clone, nil
}
