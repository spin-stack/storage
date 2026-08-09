package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// deleteVolume removes a volume: its objects, then its catalog rows.
//
// **What a delete means here is the owner's sentence, and it is short.** "Deleting a
// volume means you cannot create or start a VM from it: the data is not there any more.
// A soft delete for a couple of days to allow recovery, and after that it does not
// exist." (the deletion decision, 2026-08-07.)
//
// The couple of days is **not implemented here, and could not be**. Every object goes
// through objectstore.Store.Delete, which places a reversible marker; the interface has
// no permanent delete on it and must not grow one (INV-14), and real.NewS3Store refuses
// a bucket without versioning, so the reversibility is a property of the deployment
// rather than a promise of this code. What expires those markers is the bucket's
// lifecycle policy, which this repository cannot see and cannot enforce —
// docs/plan/RUNBOOK.md says what the deployment owes and what shortening it costs.
//
// # The order, and why the rows go last
//
// Objects first (lineage.Delete carries the order within them), rows second. The row is
// therefore the resume record: a run that marked half the objects and stopped leaves a
// catalog that still names the volume, so re-running this command finds it, walks the
// same deterministic key set and marks the rest. The reverse order would leave objects
// nothing in the catalog names, re-creating by hand exactly the orphan class there is no
// sweeper to collect.
//
// Rejected: a DELETING state with a timer, which is what §6.3 of the spec proposed
// before the owner answered it. It puts the retention window in two places — a column
// here and the bucket's lifecycle policy — and they drift, while only one of them
// controls the bytes. The undo inside the window is `-rebuild-metadata` against the
// restored objects, which is not a new mechanism: it is the one built for losing the
// database (INV-20).
//
// # Two preconditions, and each is answered by whoever can answer it
//
//   - **Detached**, from the catalog. A host learns it has lost a volume on its next
//     poll (controlplane.Place's reasoning for ErrAlreadyPlaced), so "is anyone serving
//     it" is not a question the bucket can answer and not one an Agent can be asked.
//     It also means capacity is already released before the first object is touched:
//     host_committed_bytes sums the volumes that name the host, so a detached volume
//     charges nobody (ADR-0017).
//   - **No descendants**, from the *bucket*. lineage.Delete carries why it is not the
//     catalog: `volumes.parent_snapshot_id` cannot be written back to NULL by a
//     converging create, so the catalog names parents that FLATTEN has already
//     dissolved.
//
// # Descendants are flattened first, and this command does not invent that
//
// the deletion decision's answer is B: a delete of a volume with descendants
// flattens them. Since publishing stopped flattening, a clone reads its ancestry on
// every attach and never becomes independent on its own, so option A — refuse until the
// clone stops — stopped being a temporary refusal and became a permanent one. The
// flatten is the existing one-shot, called rather than reimplemented: it re-homes the
// clone's dataset under its own lineage, rewrites its descriptor and clears its catalog
// link, and it carries its own preconditions (the clone must be detached too, and a
// clone with published snapshots of its own is refused by name — those snapshots are
// immutable deltas over the ancestry this delete is about to remove).
//
// So -kek-file is required only when there is something to flatten: a flatten opens
// every chunk the clone reads and re-seals it, and a delete on its own opens nothing.
//
// # What it does not reclaim, said here because an operator will read "deleted" as
// "the bytes are gone"
//
// Deleting a *clone* frees almost nothing: its writes live in its ancestry's chunk
// store (image.ChunksPrefix names a lineage root), and this marks only the objects the
// volume itself owns. The report below names the ancestor so that the number is not
// mistaken for the volume's size. The way to reclaim a clone's bytes is to flatten it
// first, which is the same one-shot, and then delete it.
//
// And the host's local NVMe is untouched: `<data-dir>/wal/<vol>/<epoch>/` stays on
// whichever host last served the volume. That is deliberate and it is not this
// increment's (spec §6.6): from the Agent's side "no longer in my desired state" is
// also what a detach looks like, and an Agent that deleted segments on absence would
// destroy the unpublished session of every volume that was merely moved.
func deleteVolume(ctx context.Context, md metadata.Store, store objectstore.Store, kekFile, volumeID string, term int64) error {
	switch vol, err := md.GetVolume(ctx, volumeID); {
	case err == nil:
		if vol.PrimaryHostID != "" {
			return fmt.Errorf("volume %s is placed on host %s: detach it first (-detach-volume %s) and let that host publish its session. Deleting it now would take the objects out from under a guest that is still writing, and the host would not find out until its next poll",
				volumeID, vol.PrimaryHostID, volumeID)
		}
	case errors.Is(err, metadata.ErrNotFound):
		// The catalog half is already done, or the row never came back from a rebuild.
		// Carrying on is what makes the command re-runnable in both directions, and it
		// is safe for the reason the placement check exists: a volume no row names is in
		// no host's desired state, so nothing is serving it.
		slog.Warn("no catalog row names this volume; deleting whatever the bucket still holds under its names",
			"volume_id", volumeID)
	default:
		return fmt.Errorf("reading volume %s: %w", volumeID, err)
	}

	// The descendants, before anything is marked. lineage.Delete asks the same question
	// again and refuses on it — this is not a duplicate of that check but the step that
	// *clears* it, and it is done here because a flatten needs a key file and a catalog
	// read, which are this binary's to hold.
	kids, err := lineage.Descendants(ctx, store, volumeID)
	if err != nil {
		return err
	}
	for _, kid := range kids {
		slog.Info("a volume descends from this one and is flattened before it is deleted",
			"volume_id", volumeID, "descendant", kid)
		if err := flatten(ctx, md, store, kekFile, kid, term); err != nil {
			return fmt.Errorf("volume %s cannot be deleted until %s owes nothing to it: %w", volumeID, kid, err)
		}
	}

	res, err := lineage.Delete(ctx, store, volumeID)
	if err != nil {
		return err
	}
	slog.Info("volume deleted from the object store; every key it owned now carries a delete marker, and the bucket's lifecycle policy is what expires them",
		"volume_id", volumeID, "descriptor", res.Descriptor, "manifest", res.Manifest,
		"snapshots", res.Snapshots, "chunk_objects", res.Chunks, "chunk_bytes", res.ChunkBytes,
		"flattened_descendants", len(kids))
	if res.InheritedFrom != "" {
		slog.Warn("this volume was a clone, so its data is in its ancestry's chunk store and is not reclaimed by deleting it; flatten a clone before deleting it to reclaim what it wrote",
			"volume_id", volumeID, "chunks_belong_to_lineage_of", res.InheritedFrom)
	}

	// The rows last. ErrNotFound is the already-done case the warning above described.
	switch err := md.DeleteVolume(ctx, term, volumeID); {
	case err == nil:
		slog.Info("volume and its snapshots removed from the catalog; recovering it means restoring the objects and running -rebuild-metadata",
			"volume_id", volumeID)
	case errors.Is(err, metadata.ErrNotFound):
	default:
		return fmt.Errorf("the objects of volume %s are deleted but its catalog rows are not (re-run this command to finish it): %w", volumeID, err)
	}
	return nil
}
