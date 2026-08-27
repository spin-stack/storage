package qcow_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/qcow"
)

// otherVol is a second volume placed on the same host, as two volumes on one Agent are.
const otherVol = "0198c0de-0000-7000-8000-00000000beef"

// TestAdversaryPointerNamingAnotherVolumesLayer aims at the one file in the local layout
// that carries neither a digest nor the id of the volume it belongs to: `active/current`.
//
// state.json, descriptor.json and every commit manifest all refuse an object that
// describes a different volume, each of them saying that a file restored or copied under
// another volume's directory passes every integrity check intact. The pointer is a bare
// absolute path, and readPointer's only test of it is filepath.IsAbs — so a data
// directory restored from a backup taken of another volume, or copied when a volume was
// re-placed, hands this volume a tip that belongs to a different one. Manager.Apply makes
// exactly this check on the image QEMU reports (ErrForeignImage, "which is not a layer of
// volume %s"); the colder path, where there is no QEMU to ask, does not.
func TestAdversaryPointerNamingAnotherVolumesLayer(t *testing.T) {
	t.Parallel()
	theirs := qcow.LayerImage(root, otherVol, baseID)
	r := &fakeRunner{info: infoJSON("qcow2", size, false)}
	p := newPathsAt(theirs)

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(nil))
	if err == nil {
		t.Fatalf("volume %s was opened on %s, which is volume %s's layer", vol, chain.Active, otherVol)
	}
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("opening a pointer into another volume: %v, want ErrChainMismatch", err)
	}
}
