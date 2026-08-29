package recovery_test

import (
	"bytes"
	"slices"
	"testing"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
)

// TestAdversaryALayerIdThisHostHoldsForAnotherVolumeIsNotStoodInForThisOne.
//
// A rebuild is allowed to skip a download for a layer this host already holds, and on the
// clone path that layer is read out of *another volume's* directory. So the question this
// attacks is what makes a local file eligible: the layer id, or the record that this
// commit's layer is that file.
//
// The id alone is not enough, and a same-host clone is the one place where two volumes'
// directories are both in reach. A layer id that another volume holds under a different
// commit — a fork restored here, a data directory copied between machines, a manifest the
// bucket serves for a history this host also has layers of — would then stand in for the
// commit being rebuilt, and nothing downstream can catch it: `rebase -u` rewrites the
// header, so the file's own backing check passes; the whole-chain walk is the right
// length; the digest that would have refused it was skipped precisely because the file was
// believed. A guest boots someone else's block on the clone's disk.
//
// The assertion is on the bytes that land, not on the refusal: a rebuild that consults the
// record and then copies the wrong file satisfies any assertion on the error.
func TestAdversaryALayerIdThisHostHoldsForAnotherVolumeIsNotStoodInForThisOne(t *testing.T) {
	t.Parallel()
	w := newWorld(t, 2)
	ctx := t.Context()

	// This host holds a file named by the parent's first layer id, and it is not that
	// commit's layer: the parent's own record vouches for it under a different commit.
	foreign := []byte("another volume's block, under a layer id this chain also uses")
	if err := w.files.MkdirAll(qcow.LayersDir(root, w.vol)); err != nil {
		t.Fatalf("making the parent's layer directory: %v", err)
	}
	at := qcow.LayerImage(root, w.vol, w.layers[0])
	f, err := w.files.Create(at)
	if err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	if _, err := f.Write(foreign); err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("planting the impostor: %v", err)
	}
	if err := qcow.WriteState(w.files, root, w.vol, qcow.State{
		Commits: []qcow.CommitLayer{{CommitID: ids.New().String(), LayerID: w.layers[0]}},
	}); err != nil {
		t.Fatalf("writing the parent's record: %v", err)
	}

	clone := ids.New().String()
	w.keys.keys[clone] = cloneKeys(t, w, clone)

	got, err := w.rec.RestoreFrom(ctx, qcow.Lineage{
		VolumeID: clone, Ancestry: []qcow.Ancestor{{VolumeID: w.vol, CommitID: w.commits[len(w.commits)-1]}},
	}, virtualSize)
	if err != nil {
		t.Fatalf("restoring the clone: %v", err)
	}
	if got.Base != qcow.LayerImage(root, clone, w.layers[1]) {
		t.Errorf("the clone would be built over %q, want %q", got.Base, qcow.LayerImage(root, clone, w.layers[1]))
	}
	for i := range w.layers {
		if !bytes.Equal(w.files.content(qcow.LayerImage(root, clone, w.layers[i])), w.plain[i]) {
			t.Errorf("the clone's layer %s is not the bytes commit %s published", w.layers[i], w.commits[i])
		}
	}
	if slices.Contains(w.files.opened, at) {
		t.Errorf("%s was read off the local disk to stand in for commit %s; the only thing that says"+
			" it is that commit's layer is a record naming another commit", at, w.commits[0])
	}
	// The parent's file is untouched: a clone rewrites the headers of the layers it holds,
	// so reaching into another volume's directory for anything but a copy is data loss in
	// the volume that was minding its own business.
	if !bytes.Equal(w.files.content(at), foreign) {
		t.Errorf("the file under volume %s was rewritten by a clone's rebuild", w.vol)
	}
}
