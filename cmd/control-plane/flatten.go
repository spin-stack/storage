package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
)

// flatten makes one volume self-contained: it writes an image that owes nothing to its
// ancestors, and stops the bucket saying it has any (lineage.Flatten carries the mechanism
// and the ordering).
//
// It is a one-shot flag like every other admin action here — `-seed-volume`,
// `-snapshot-volume`, `-clone-snapshot`, `-detach-volume`, `-rebuild-metadata`,
// `-cordon-host` — and CHUNK-ADDRESSING-SPEC's decision of 2026-08-07 says so in as many
// words: the `operations` table that would have scheduled it was retired with ADR-0017's
// second capacity term, and bringing it back for one verb is a schema change, a term guard
// and a reconciliation loop for something ADR-0021 §2 already says these binaries are for.
//
// # It takes a term now, and it did not before
//
// The first version of this command wrote no catalog row, and said so as a decision:
// there was nothing for §7's guard to guard, and demanding a leader would have made an
// operator start a Control Plane before they could repair a bucket. What made that true
// was a gap rather than a design — `volumes.parent_snapshot_id` is write-once by
// construction (CreateVolume COALESCEs it so a converging rebuild cannot drop a clone's
// link), so no verb on metadata.Store could say a lineage had ended, and this command
// left a `WARN` where the write belonged. metadata.Store.ClearVolumeParent is that verb,
// and with it a flatten is a mutation like every other admin one-shot here: it borrows
// the current term rather than acquiring one, because a repair is not a leader taking
// over and AcquireLeadership would leave the serving Control Plane's writes stale.
//
// The cost is real and worth naming: a flatten of a bucket whose Control Plane is down
// now fails at the first read instead of half-succeeding. That is the better failure. A
// flatten that rewrote the bucket and could not tell the catalog leaves §20.1's ceiling
// refusing clones of a volume that is back at depth 0 and a delete of the old parent
// still seeing a descendant — the two things the operation exists to unblock.
//
// # The order: the bucket, then the catalog
//
// lineage.Flatten first, ClearVolumeParent after, and never the reverse. The bucket is
// the authority a rebuild trusts (INV-20), so a catalog cleared before the objects were
// rewritten would state a self-containment nothing had produced. The failure in between
// is the resumable one: the descriptor already names no parent, so a re-run gets
// lineage.ErrSelfContained — which this command treats as "finish the catalog half"
// rather than as nothing to do.
//
// What protects the objects is the precondition below plus the manifest CAS: the flatten
// replaces the volume's manifest, so a host that is serving it — holding the ETag it
// loaded and about to CAS against it at its stop — would have its publish fail with
// image.ErrSuperseded, which its teardown deliberately does not retry, and that session
// would be lost. So the volume must be detached, and detached is a fact only the catalog
// holds (a host learns it has lost a volume on its next poll, which is
// controlplane.Place's reasoning for ErrAlreadyPlaced).
func flatten(ctx context.Context, md metadata.Store, store objectstore.Store, kekFile, volumeID string, term int64) error {
	vol, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("reading volume %s: %w", volumeID, err)
	}
	if vol.PrimaryHostID != "" {
		return fmt.Errorf("volume %s is placed on host %s: detach it first (-detach-volume %s) and let that host publish its session, or this would replace the manifest that host is about to compare against and lose everything it holds",
			volumeID, vol.PrimaryHostID, volumeID)
	}

	enc, err := volumeEncryption(kekFile, vol)
	if err != nil {
		return err
	}

	res, err := lineage.Flatten(ctx, store, rand.Reader, enc, volumeID)
	switch {
	case errors.Is(err, lineage.ErrSelfContained):
		// Idempotent on the happy path, which is what an operator re-running a command they
		// are not sure completed needs. It is not "nothing happened": it is the state the
		// command exists to reach — and it is also the resume path, because a run that
		// rewrote the descriptor and then failed to reach the database lands here. So the
		// catalog write below still happens, and re-running the command is what repairs it.
		slog.Info("volume already owes nothing to an ancestry; its image is what answers every read",
			"volume_id", volumeID)
	case err != nil:
		return err
	default:
		slog.Info("volume flattened: its image now states the whole dataset and its descriptor names no parent",
			"volume_id", volumeID, "ancestors_left", res.Ancestors,
			"was_cloned_from", res.ParentSnapshotID, "of_volume", res.ParentVolumeID,
			"image_bytes", res.Bytes, "runs", res.Runs, "erased_ranges", res.Erased)
	}

	// The catalog last, and it is what makes the operation mean anything to the rest of
	// the Control Plane: until this row says depth 0 and no parent, §20.1's ceiling keeps
	// refusing clones of a volume that has just paid for its independence, and
	// -delete-volume on the old parent keeps refusing on a descendant that no longer
	// reads through it (metadata.ErrHasDescendants).
	if err := md.ClearVolumeParent(ctx, term, volumeID); err != nil {
		return fmt.Errorf("volume %s is flattened in the object store but the catalog still records its lineage (re-run this command to finish it): %w", volumeID, err)
	}
	slog.Info("the catalog no longer records a lineage for this volume",
		"volume_id", volumeID, "was_parent_snapshot_id", vol.ParentSnapshotID,
		"was_chain_depth", vol.ChainDepth)
	return nil
}

// volumeEncryption unwraps the volume's DEK so the flatten can open the chunks it is about
// to re-seal under a new lineage.
//
// The KEK comes from a file for the reason readKEK gives, and the check against the
// volume's own kek_id is the Agent's (agent.encryptionFor): unwrapping under the wrong KEK
// fails on the AEAD with nothing to say which key was missing, and an operator holding two
// key files deserves better than "authentication failed".
func volumeEncryption(kekFile string, vol metadata.Volume) (*wal.Encryption, error) {
	if kekFile == "" {
		return nil, errors.New("-kek-file is required with -flatten-volume: a flatten opens every chunk the volume reads and re-seals it under the volume's own lineage, so it needs the key the Agent was given")
	}
	kek, err := readKEK(kekFile)
	if err != nil {
		return nil, err
	}
	kms := crypto.NewDevKMS(kek, crypto.KEKID(kek))
	if vol.KEKID != kms.KEKID() {
		return nil, fmt.Errorf("volume %s is wrapped under KEK %q and this file holds %q", vol.VolumeID, vol.KEKID, kms.KEKID())
	}
	dek, err := kms.UnwrapDEK(vol.DEKWrapped, vol.DEKKeyID)
	if err != nil {
		return nil, fmt.Errorf("unwrapping the DEK of volume %s (version %d): %w", vol.VolumeID, vol.DEKKeyID, err)
	}
	u, err := ids.Parse(vol.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("volume %q is not a uuid: %w", vol.VolumeID, err)
	}
	return wal.NewEncryption(dek, [16]byte(u))
}
