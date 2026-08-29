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

// reclaimOne frees one released volume's layers, if the object store confirms the fleet
// granted it to somebody else.
func (m *Manager) reclaimOne(ctx context.Context, volumeID string) {
	st, err := ReadState(m.paths, m.cfg.Root, volumeID)
	if err != nil {
		// A directory with no readable record: it may be a volume this host is in the
		// middle of preparing, or one whose state file is corrupt. Neither is a licence
		// to delete, and the record is where the epoch to compare against lives.
		return
	}
	held := heldEpoch(st)
	if held == 0 || !hasLayers(m.paths, m.cfg.Root, volumeID) {
		// Nothing recorded, or nothing left to free. Asking the object store about it
		// would be a request per cycle for a directory with no disk in it.
		return
	}
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	granted, err := m.wit.GrantedEpoch(ctx, volumeID)
	if err != nil || granted <= held {
		return
	}
	freed, err := dropLayers(m.paths, m.cfg.Root, volumeID)
	if err != nil {
		slog.Warn("could not reclaim the disk of a volume the fleet moved elsewhere",
			"volume_id", volumeID, "held_epoch", held, "granted_epoch", granted, "error", err)
		return
	}
	// The record is replaced rather than left behind: one that still vouched for layers
	// that are gone is what would make a later Open believe this host holds a history it
	// does not. What it keeps is the fence — at the epoch that was observed, so a desired
	// state that arrives late naming the old epoch is refused rather than served off a
	// chain that is not there.
	next := State{
		FormatVersion: st.FormatVersion, VolumeID: volumeID,
		Fenced: &Fencing{
			Epoch:   granted,
			Refusal: int32(storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST),
			Detail:  "the fleet granted this volume elsewhere and its local layers were reclaimed",
		},
	}
	if err := WriteState(m.paths, m.cfg.Root, volumeID, next); err != nil {
		slog.Warn("reclaimed a volume's layers and could not replace its record",
			"volume_id", volumeID, "error", err)
		return
	}
	slog.Info("reclaimed the local disk of a volume the fleet granted elsewhere",
		"volume_id", volumeID, "held_epoch", held, "granted_epoch", granted,
		"layers", freed.count, "bytes", freed.bytes)
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

// hasLayers reports whether there is any disk here to free.
func hasLayers(p Paths, root, volumeID string) bool {
	names, err := p.List(LayersDir(root, volumeID))
	return err == nil && len(names) > 0
}

// reclaimed is what one volume's cleanup freed, for the line that records it.
type reclaimed struct {
	count int
	bytes int64
}

// dropLayers removes every file in a volume's layers directory and the pointer that named
// one of them. Files only: no directory is removed and no path outside this volume's
// layers directory is touched, because the blast radius of a wrong path here is a data
// directory rather than a file.
func dropLayers(p Paths, root, volumeID string) (reclaimed, error) {
	var out reclaimed
	dir := LayersDir(root, volumeID)
	names, err := p.List(dir)
	if err != nil {
		return out, err
	}
	for _, name := range names {
		path := filepath.Join(dir, name)
		if n, serr := p.Size(path); serr == nil {
			out.bytes += n
		}
		if err := p.Remove(path); err != nil {
			return out, err
		}
		out.count++
	}
	// And the pointer, which is the contract with whoever launches the VM: left naming a
	// file that is gone, it would send a launcher at a path that does not resolve.
	if err := p.Remove(ActivePointer(root, volumeID)); err != nil && !strings.Contains(err.Error(), "not exist") {
		return out, err
	}
	return out, nil
}
