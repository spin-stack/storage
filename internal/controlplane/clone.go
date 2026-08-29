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
// created rather than cloned is at depth 0, so one clone link is admitted above a root and
// a clone of a clone is refused.
//
// What a link costs at attach, measured against the code that pays it and pinned by
// recovery.TestWhatOneChainLinkCostsAtAttach, which asserts the exact requests: an ancestor
// costs two object-store GETs per commit in *its own* history — one manifest, one layer —
// and no fixed per-link cost at all, because the parent is rebuilt to the commit the
// snapshot names and its HEAD is deliberately never read. Depth is therefore not the
// variable: one link over a forty-commit ancestor costs twenty times one link over a
// two-commit ancestor, and what would bound both is §19's compaction, which nothing in
// this tree implements yet — not this number.
//
// What QEMU pays is per backing *file* for the same reason, and recovery.maxRestoreDepth
// holds that measurement: 301 layers open in both qemu-img and qemu-system at one
// descriptor and ~140 KiB of RSS each. What a guest read costs per backing file it crosses
// is not measured anywhere and cannot be measured from here — it needs a real guest, so it
// belongs in the guest lane and is not guessed at.
//
// What sets it is not a cost. Recovery rebuilds exactly one ancestor: qcow.Lineage carries
// one parent, cpserver fills it from the snapshot row, RestoreFrom does not recurse, and a
// clone's own published commits are overlays over a base no manifest of theirs names. A
// depth-2 clone re-placed on a host that holds nothing therefore rebuilds a chain missing
// everything its grandparent wrote and reports success
// (recovery.TestARestoreReadsOneAncestorAndStops).
//
// So the ceiling is the depth this system can actually serve, and it stays there until the
// walk learns the whole ancestry — which needs the Control Plane to send it, since the
// Agent is told and cannot look a lineage up (ADR-0021). It was 5 while nothing had
// measured what recovery does; a ceiling that admits four links no restore can rebuild is
// worse than no ceiling, because it reads as a decision somebody made.
//
// Rejected: a flag on cmd/control-plane. A ceiling an operator can raise per invocation is
// one that gets raised during the incident it exists to prevent, and the number only means
// anything if every clone in the fleet was admitted against the same one.
const MaxChainDepth = 1

// ErrChainTooDeep is what a clone past MaxChainDepth is refused with. A sentinel because a
// caller has to tell "you are at the ceiling", which is about the lineage and is answered by
// cloning something shallower, apart from "that snapshot does not exist"; every other
// refusal in Clone says the fleet or the snapshot is wrong.
var ErrChainTooDeep = errors.New("controlplane: the lineage is at its depth ceiling")

// Clone creates a new volume from a parent snapshot (§20): pure metadata — a new active
// child at epoch 1 that reads *through* the parent snapshot's already-durable objects
// rather than copying them, inheriting the parent's size, block size and DEK, at the
// parent's depth+1 or refused with ErrChainTooDeep. A clone is not independent: its
// manifests state only what its own volume wrote, and an attach rebuilds the parent's
// layers underneath them (recovery.RestoreFrom), which is why deleting a parent whose
// clones read through it is refused rather than cascaded.
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
	// It is also the only place it can refuse — recovery.maxRestoreDepth is a termination
	// guard on a walk, and refusing there turns a volume the fleet created successfully into
	// one nothing can read, with a guest already booting.
	//
	// It compares the *parent volume's* depth, which holds only while a snapshot is at its
	// volume's depth. Republishing a volume as a new root — §19's compaction, which nothing
	// implements yet — breaks it: the volume is back at depth 0 while the snapshots it
	// published before are still deltas over the old lineage. Whatever lands must leave that
	// sentence true or move this check.
	if parent.ChainDepth >= MaxChainDepth {
		return metadata.Volume{}, fmt.Errorf(
			"%w: volume %s is at depth %d, so a clone of snapshot %s would be depth %d and the ceiling is %d. "+
				"Every attach of a clone rebuilds an ancestor's whole published history from the object store, "+
				"and every guest read crosses one more backing file, which is what this refuses to grow further. "+
				"Nothing reduces an existing lineage's depth today — §19's compaction, an offline qemu-img convert "+
				"republished as a new immutable root, is what would, and it has no verb — so clone a shallower "+
				"volume in this lineage instead",
			ErrChainTooDeep, parent.VolumeID, parent.ChainDepth, parentSnapshotID,
			parent.ChainDepth+1, MaxChainDepth)
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
	// deleting the parent destroys no secret the child does not still hold. The compaction
	// §19 asks for does not exist yet; when it re-uploads a clone's data it must mint a
	// *fresh* DEK while it does, or deleting the parent of the volume it rewrote shreds
	// nothing and the delete verb's promise is false.
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
	// catalog's number: the one the ceiling above refuses on. Recorded at the change and not
	// polled, because nothing else moves a volume's depth once it exists; the cost is that
	// the series goes quiet, so "which volumes are deep now" is answered by `-fleet-status`
	// and this answers "how deep was it when created".
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
		// Both halves: a snapshot names a commit only together with the volume it lives
		// under (commit.ManifestKey), and a rebuild is handed both.
		ParentVolumeID: clone.ParentVolumeID,
	}); err != nil {
		return clone, fmt.Errorf("writing the descriptor for clone %s (the row exists; rebuild-metadata cannot see it until this succeeds): %w",
			clone.VolumeID, err)
	}
	return clone, nil
}
