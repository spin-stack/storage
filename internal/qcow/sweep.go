package qcow

import (
	"fmt"
	"path/filepath"
	"strings"
)

// sweep removes the layer files nothing on this host reads through any more, and returns
// what it removed.
//
// One rule, where there used to be three. Layers live in one directory for the whole host
// (LayersDir says why), so "is this file garbage" stopped being a question about one
// volume's directory and became a question about the fleet on this machine: **a layer file
// is removable when no volume here names it.** The keep-set is the union of every volume's
// record and pointer — the tip, every layer it has served, every layer of a commit it
// holds, the layer it owes, and a collapse's two.
//
// That single rule now covers what the per-volume sweep, the reclamation of a moved
// volume's disk, and a collapse's replaced prefix each used to decide separately. It also
// covers the case the per-volume sweep was written for: the overlay an interrupted
// rotation leaves, which no record names and which nothing backs onto.
//
// **It refuses to run rather than under-count.** If any volume's record cannot be read,
// the keep-set is incomplete, and an incomplete keep-set here deletes a running guest's
// chain. Refusing costs disk; guessing costs the volume. This replaces a heuristic — "the
// id was minted after the tip's, so nothing can back onto it" — that existed to make a
// lost state.json survivable, and that is a weaker thing than not acting on a keep-set you
// know is short.
//
// A name that is not a layer file is left alone, which covers a download's `.part` and a
// compaction's `.compacting`: both are files another step will rename into place, and
// neither is anything a chain reads through yet.
func sweep(p Paths, root string, volumes []string) ([]string, error) {
	keep := map[string]bool{}
	for _, id := range volumes {
		// A directory with no record at all is the shape that matters most, and
		// ReadState answers it with an empty record and no error — which would read as
		// "this volume claims nothing" and take its whole chain while a guest is writing
		// to it. With one layers directory per host, the record is the *only* thing that
		// says which files were this volume's, so having none is not an empty claim, it
		// is an unanswerable question.
		there, err := p.Exists(StateFile(root, id))
		if err != nil {
			return nil, fmt.Errorf("qcow: looking for volume %s's record: %w", id, err)
		}
		if !there {
			return nil, fmt.Errorf("qcow: volume %s has a directory here and no record, so which layers are its own cannot be answered and nothing is swept", id)
		}
		st, err := ReadState(p, root, id)
		if err != nil {
			return nil, fmt.Errorf("qcow: volume %s's record could not be read, so nothing here can be shown to be garbage: %w", id, err)
		}
		for _, layerID := range st.LayerIDs() {
			keep[layerID] = true
		}
		// And whatever `active/current` names, which is not always in the record. Rotate
		// writes the pointer *before* it tells QEMU to switch — deliberately, so a crash
		// leaves it one layer ahead rather than one behind — so a rotation whose snapshot
		// was refused leaves an overlay the pointer names and no record vouches for.
		// Unlinking it makes the one path this package promises to whoever launches the VM
		// name a file that is not there, turning a window that heals itself into a refusal.
		pointed, err := readPointer(p, root, id, ActivePointer(root, id))
		if err != nil {
			return nil, err
		}
		if pointed != "" {
			keep[LayerIDOfImage(pointed)] = true
		}
	}

	dir := LayersDir(root)
	names, err := p.List(dir)
	if err != nil {
		return nil, fmt.Errorf("qcow: listing %s: %w", dir, err)
	}
	var removed []string
	for _, name := range names {
		// A layer, or a compaction's half-converted root: `<layer-id>.qcow2.compacting` is
		// a file `qemu-img convert` was killed in the middle of, and it is a whole volume's
		// worth of disk that nothing will ever name again — a collapse planned a second
		// time converts under a new id. Everything else is left alone, which covers a
		// download's `.part`: another step renames those into place, and until it does
		// they are not layers.
		id, ok := strings.CutSuffix(name, layerSuffix)
		if !ok {
			if id, ok = strings.CutSuffix(name, layerSuffix+compactSuffix); !ok {
				continue
			}
		}
		if keep[id] {
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

// HeldCommits is every commit this host holds a layer for, as commit id to layer id,
// unioned over every volume's record.
//
// The union is the point. A layer file is named by its own id and shared by every volume
// that reads through it (LayersDir says why), so "does this host already hold commit C"
// stopped being a question about one volume's record the moment a clone could read its
// parent's layers — and answering it from the clone's own record alone is what used to
// make a clone download a chain that was already on the disk.
//
// It is a record and not a stat, and that has not changed: a file at the right path is not
// evidence that it is the layer the manifest names. `qemu-img rebase -u` rewrites a
// layer's header, so a repointed layer no longer hashes to the object it came from and
// nothing but this record can vouch for it afterwards.
func HeldCommits(p Paths, root string) (map[string]string, error) {
	ids, err := p.List(volumesRoot(root))
	if err != nil {
		return nil, fmt.Errorf("qcow: listing this host's volumes: %w", err)
	}
	held := make(map[string]string, len(ids))
	for _, id := range ids {
		st, err := ReadState(p, root, id)
		if err != nil {
			// One unreadable record costs the layers it would have vouched for a
			// download; it must not stop a restore that can still be made.
			continue
		}
		for _, c := range st.Commits {
			held[c.CommitID] = c.LayerID
		}
	}
	return held, nil
}
