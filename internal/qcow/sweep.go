package qcow

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/ids"
)

// sweep removes the files in a volume's layers directory that this host can prove
// nothing needs, and returns the ones it removed.
//
// Two conditions, and both have to hold for a file to go, because being wrong once costs
// a guest its disk:
//
//   - No record names it. The tip, the sealed layer this host still owes, every layer it
//     has seen as a tip and every layer of a commit it holds are all chain: a qcow2 reads
//     through everything under it, so a layer whose commit has landed is at most
//     *eligible* for cleanup: its file is still the backing of the tip, and the chain is
//     what decides.
//   - Its layer id was minted after the tip's. Layer ids are v7 and a backing file is
//     always created before the layer above it, so an id later than the tip's cannot be
//     anything the chain reads through. This is what keeps a lost state.json costing work
//     rather than the volume: state.json is derived, and on the first condition alone a
//     file that is not there reads as "no record names anything" and the sweep would take
//     the whole chain. Only the 48-bit timestamp is compared, so the random tail never
//     decides an ordering, and a tie keeps the file.
//
// What survives both is the overlay an interrupted rotation leaves: a layer above the
// tip, which nothing backs onto and which no guest ever wrote to — the guest is on the
// tip, and the tip is QEMU's own answer. Anything whose name is not a v7 layer file is
// left alone, which covers a download that did not finish.
func sweep(p Paths, root, volumeID, tip string, st State, pending *SealedLayer) ([]string, error) {
	tipID, err := ids.Parse(LayerIDOfImage(tip))
	if err != nil {
		return nil, fmt.Errorf("qcow: the tip %s of volume %s is not named by a layer id, so nothing here can be shown to be garbage: %w", tip, volumeID, err)
	}
	keep := map[string]bool{LayerIDOfImage(tip): true}
	for _, id := range st.Layers {
		keep[id] = true
	}
	for _, c := range st.Commits {
		keep[c.LayerID] = true
	}
	if pending != nil {
		keep[pending.LayerID] = true
	}
	// And whatever `active/current` names, which is not always the tip. Rotate writes the
	// pointer *before* it tells QEMU to switch — deliberately, so a crash leaves it one
	// layer ahead rather than one behind — so a rotation whose snapshot was refused leaves
	// an overlay that the pointer names and no record vouches for. Unlinking it makes the
	// one path this package promises to whoever launches the VM name a file that is not
	// there, turning a window that heals itself into a refusal.
	pointed, err := readPointer(p, root, volumeID, ActivePointer(root, volumeID))
	if err != nil {
		return nil, err
	}
	if pointed != "" {
		keep[LayerIDOfImage(pointed)] = true
	}

	dir := LayersDir(root, volumeID)
	names, err := p.List(dir)
	if err != nil {
		return nil, fmt.Errorf("qcow: listing %s: %w", dir, err)
	}
	var removed []string
	for _, name := range names {
		if filepath.Ext(name) != layerSuffix {
			continue
		}
		id := LayerIDOfImage(name)
		if keep[id] {
			continue
		}
		u, err := ids.Parse(id)
		if err != nil || !ids.IsV7(u) || !mintedAfter(u, tipID) {
			continue
		}
		path := filepath.Join(dir, name)
		if err := p.Remove(path); err != nil {
			return removed, fmt.Errorf("qcow: removing %s: %w", path, err)
		}
		removed = append(removed, path)
	}
	return removed, nil
}

// mintedAfter compares the millisecond stamps two v7 ids carry in their first six bytes.
func mintedAfter(a, b uuid.UUID) bool { return bytes.Compare(a[:6], b[:6]) > 0 }
