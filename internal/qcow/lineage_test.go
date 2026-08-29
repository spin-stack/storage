package qcow_test

import (
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
)

// Two generations of ancestry, spelled out the way the Control Plane sends them: oldest
// first, one pair per generation.
const (
	oldestVol    = "0198c0de-0000-7000-8000-00000000a11c"
	oldestCommit = "0198c0de-0000-7000-8000-00000000a11d"
	middleVol    = "0198c0de-0000-7000-8000-00000000b22c"
	middleCommit = "0198c0de-0000-7000-8000-00000000b22d"
)

// TestTheAncestryOnTheWireIsTheLineageTheChainIsBuiltFrom is the binary seam between the
// desired state and a rebuild, and nothing else in the gate looks at it.
//
// The order carries the whole meaning: a chain is rebuilt oldest first, each generation
// repointed at the one below it, so a lineage delivered the other way round builds a chain
// that resolves, opens and boots — with the older generation's writes sitting on top of the
// newer one's at every offset both wrote. Dropping a generation is the same shape with
// zeros instead. Neither is visible in the chain's depth, in an error, or in anything the
// Agent reports, so the assertion is on what the rebuild was actually handed.
//
// Until this existed the seam was covered only by hack/stage6-demo.sh, which needs KVM and
// the pinned QEMU and is therefore not in `task ci:full`.
func TestTheAncestryOnTheWireIsTheLineageTheChainIsBuiltFrom(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	// A clone whose ancestors' layers the recovery has just put on this disk, which is
	// the only case the lineage travels in: a rebuild is what reads it.
	base := qcow.LayerImage(root, baseID)
	h.paths.present[base] = true
	h.runner.info = overlayJSON(size, base)
	h.rec.res, h.rec.err = qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}, nil

	d := active(vol, 1)
	d.Ancestry = []*storagev1.Ancestor{
		{VolumeId: oldestVol, CommitId: oldestCommit},
		{VolumeId: middleVol, CommitId: middleCommit},
	}
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("applying a clone of a clone: %v", err)
	}

	got := h.rec.lineage
	want := []qcow.Ancestor{
		{VolumeID: oldestVol, CommitID: oldestCommit},
		{VolumeID: middleVol, CommitID: middleCommit},
	}
	if got.VolumeID != vol {
		t.Errorf("the rebuild was asked for volume %q, want %q", got.VolumeID, vol)
	}
	if len(got.Ancestry) != len(want) {
		t.Fatalf("the rebuild was handed %d generations, want %d: %v", len(got.Ancestry), len(want), got.Ancestry)
	}
	for i := range want {
		if got.Ancestry[i] != want[i] {
			t.Errorf("generation %d is %v, want %v (oldest first, as sent)", i, got.Ancestry[i], want[i])
		}
	}

	// And a volume nobody cloned carries none: an ancestry invented here would send a
	// rebuild looking for objects under another volume's id.
	h2 := newHarness(t)
	if err := h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying a root volume: %v", err)
	}
	if n := len(h2.rec.lineage.Ancestry); n != 0 {
		t.Errorf("a root volume was rebuilt from %d generations of ancestry", n)
	}
}
