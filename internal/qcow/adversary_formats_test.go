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
// another volume's name passes every integrity check intact. The pointer is a bare
// absolute path — and since layers live in one directory for the whole host, the path
// cannot even say whose layer it is. So a data directory restored from a backup taken of
// another volume, or copied when a volume was re-placed, hands this volume a tip that
// belongs to a different one.
//
// What refuses it is the record, and it can refuse absolutely because a layer is written
// into the record before the pointer names it: a target the record does not account for is
// not a window to be tolerated, it is this. Manager.Apply asks the same question of the
// image QEMU reports; this is the colder path, where there is no QEMU to ask.
func TestAdversaryPointerNamingAnotherVolumesLayer(t *testing.T) {
	t.Parallel()
	theirs := qcow.LayerImage(root, baseID)
	r := &fakeRunner{info: infoJSON("qcow2", size, false)}
	// The pointer alone, with no record behind it — which is the attack, and which is why
	// this cannot use newPathsAt: that helper writes the pairing production guarantees.
	p := newPaths(theirs)
	p.present[qcow.ActivePointer(root, vol)] = true
	p.files[qcow.ActivePointer(root, vol)] = theirs
	// And the layer is genuinely another volume's: its record accounts for it, so the file
	// is not garbage and the sweep would keep it. Only this volume has no claim on it.
	p.recordTip(otherVol, theirs)

	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(nil))
	if err == nil {
		t.Fatalf("volume %s was opened on %s, which is volume %s's layer", vol, chain.Active, otherVol)
	}
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("opening a pointer into another volume: %v, want ErrChainMismatch", err)
	}
}
