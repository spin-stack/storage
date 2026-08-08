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
// # It takes no term, and that is a decision rather than an omission
//
// Every other one-shot here borrows the leader's term because it writes a row, and §7's
// guard is what stops a zombie Control Plane from making that write. This one writes no
// row at all — the catalog write it *would* make is owed and named on the way out — so
// there is nothing for a term to guard. Adding one would be a lock over a mutation that
// does not exist, and it would make an operator start a Control Plane before they could
// repair a bucket, which is the opposite of what an admin command should demand.
//
// What actually protects the objects is the precondition below plus the manifest CAS: the
// flatten replaces the volume's manifest, so a host that is serving it — holding the ETag
// it loaded and about to CAS against it at its stop — would have its publish fail with
// image.ErrSuperseded, which its teardown deliberately does not retry, and that session
// would be lost. So the volume must be detached, and detached is a fact only the catalog
// holds (a host learns it has lost a volume on its next poll, which is
// controlplane.Place's reasoning for ErrAlreadyPlaced).
func flatten(ctx context.Context, md metadata.Store, store objectstore.Store, kekFile, volumeID string) error {
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
		// command exists to reach.
		slog.Info("volume already owes nothing to an ancestry; its image is what answers every read",
			"volume_id", volumeID)
		return nil
	case err != nil:
		return err
	}

	slog.Info("volume flattened: its image now states the whole dataset and its descriptor names no parent",
		"volume_id", volumeID, "ancestors_left", res.Ancestors,
		"was_cloned_from", res.ParentSnapshotID, "of_volume", res.ParentVolumeID,
		"image_bytes", res.Bytes, "runs", res.Runs, "erased_ranges", res.Erased)

	// The half this command cannot do, said every time rather than documented once. The
	// volume reads correctly without it — its Agent takes the link from the descriptor
	// (lineage.Walk) precisely because this column cannot be cleared — but everything that
	// *counts* lineage still counts this one: `chain_depth` still says what it said, so
	// controlplane.Clone's ceiling still refuses clones of this volume, and a delete of the
	// old parent still sees a descendant. `-rebuild-metadata` fixes both, because it
	// reconstructs the row from the descriptor this command just rewrote.
	slog.Warn("the catalog still records the lineage this volume no longer has: volumes.parent_snapshot_id is write-once by construction (CreateVolume COALESCEs it), so no verb here can clear it. Until metadata.Store grows one, -rebuild-metadata is what makes the catalog agree with the bucket",
		"volume_id", volumeID, "catalog_parent_snapshot_id", vol.ParentSnapshotID,
		"catalog_chain_depth", vol.ChainDepth)
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
