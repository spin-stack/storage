package qcow

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
)

// Witness is the object store's answer to "who holds this volume now": the epoch
// `volumes/<id>/epoch` records, written at every grant.
//
// Defined here because this is where it is consumed, and it is deliberately the same
// question agent.Witness asks. That is not duplication, it is the point: the loop stops a
// running guest on this fact, so a host that will take a VM's disk away on it can
// certainly delete a file on it. One signal, one meaning, two consequences.
type Witness interface {
	GrantedEpoch(ctx context.Context, volumeID string) (int64, error)
}

// reclaim frees the local disk of volumes the fleet can be shown to have moved elsewhere.
// Callers hold m.mu.
//
// A released volume keeps its layers on purpose — a volume leaves the desired state for
// reasons that reverse, and getting it back should be free — so what makes them
// reclaimable cannot be time passing. It is a fact: the object store recording a grant at
// an epoch this host does not hold. From that moment this host's chain is a fork nothing
// will ever accept, because the compare-and-set on HEAD and the epoch fence both refuse
// it, and the files are holding a disk for a history with nowhere to go.
//
// **Silence is never that fact.** A store that will not answer, an epoch object that is
// not there, a volume whose row somebody deleted — all of them look identical to a bucket
// pointed at the wrong place, and this is the one path where being wrong deletes a
// tenant's data rather than costing a restore. Only a *higher* number reclaims anything.
//
// What it does not cover, and deliberately: a volume detached and left detached is never
// granted anywhere, so its epoch never rises and its layers stay. That is the same
// judgement one step out — nobody has said the volume is gone, so nobody here will act as
// if it were.
func (m *Manager) reclaim(ctx context.Context) {
	if m.wit == nil {
		// No object store: nothing can be confirmed, so nothing goes.
		return
	}
	names, err := m.paths.List(volumesRoot(m.cfg.Root))
	if err != nil {
		// Not an error the caller should see: every volume this host is serving is
		// unaffected, and a directory that cannot be listed is a reason to reclaim
		// nothing rather than to fail a reconciliation.
		slog.Warn("could not list this host's volumes to reclaim disk", "root", m.cfg.Root, "error", err)
		return
	}
	for _, id := range names {
		if _, serving := m.vols[id]; serving {
			continue
		}
		m.reclaimOne(ctx, id)
	}
}

// reclaimOne releases one volume's claim on this host's disk, if the object store
// confirms the fleet granted the volume to somebody else.
func (m *Manager) reclaimOne(ctx context.Context, volumeID string) {
	st, err := ReadState(m.paths, m.cfg.Root, volumeID)
	if err != nil {
		// A directory with no readable record: a volume this host is in the middle of
		// preparing, or one whose state file is corrupt. Neither is a licence to act, and
		// the record is where the epoch to compare against lives.
		return
	}
	held := heldEpoch(st)
	if held == 0 || len(st.LayerIDs()) == 0 {
		// Nothing recorded, or a record that already claims nothing. Asking the object
		// store about it would be a request per cycle about a volume with no disk behind
		// it — and there would be nothing to release.
		return
	}
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	granted, err := m.wit.GrantedEpoch(ctx, volumeID)
	if err != nil || granted <= held {
		return
	}
	// The claim goes; the files are not touched here. What frees a layer file is the one
	// rule that frees any of them — nothing on this host names it — and routing through it
	// is what keeps a clone of this volume, or any volume sharing its published history,
	// from losing the files it reads through. Dropping the claim is precisely the act that
	// makes the files unnamed, and the sweep at the end of this same cycle collects
	// whatever that leaves with no other claimant.
	//
	// What replaces it keeps the fence, at the epoch that was observed: a desired state
	// that arrives late naming the epoch this host used to hold is then refused, rather
	// than served off a chain whose files are gone.
	next := State{
		FormatVersion: st.FormatVersion, VolumeID: volumeID, Epoch: granted,
		Fenced: &Fencing{
			Epoch:   granted,
			Refusal: int32(storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST),
			Detail:  "the fleet granted this volume elsewhere and this host released its layers",
		},
	}
	if err := WriteState(m.paths, m.cfg.Root, volumeID, next); err != nil {
		slog.Warn("could not release the disk of a volume the fleet moved elsewhere",
			"volume_id", volumeID, "error", err)
		return
	}
	// And the pointer, which is the contract with whoever launches the VM: left naming a
	// file the sweep is about to collect, it would send a launcher at a path that does not
	// resolve.
	if err := m.paths.Remove(ActivePointer(m.cfg.Root, volumeID)); err != nil && !strings.Contains(err.Error(), "not exist") {
		slog.Warn("released a volume's layers and could not drop its pointer",
			"volume_id", volumeID, "error", err)
		return
	}
	slog.Info("released the local disk of a volume the fleet granted elsewhere; its layers go with the next sweep unless another volume reads through them",
		"volume_id", volumeID, "held_epoch", held, "granted_epoch", granted,
		"layers", len(st.LayerIDs()))
}

// volumesRoot is the directory holding one subdirectory per volume this host has.
func volumesRoot(root string) string { return filepath.Join(root, volumesDir) }

// heldEpoch is the epoch this host last held the volume under, from its own record.
func heldEpoch(st State) int64 {
	if st.Fenced != nil {
		return st.Fenced.Epoch
	}
	return st.Epoch
}
