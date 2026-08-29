package qcow_test

import (
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
)

// local_disk_bytes is what ONE volume's chain occupies, not what the host holds.
//
// The two were the same thing while every volume had its own layers directory, and the
// number was measured by listing that directory. Layers live in one directory for the whole
// host now (ADR-0027), so the same listing answers with the host's total — and a per-volume
// gauge where every volume on a machine reports the same number is worse than no gauge: an
// operator asking which volume is filling the disk gets each of them accused equally.
//
// Asserted with two volumes of deliberately different sizes, because a measurement that
// summed the directory would satisfy any assertion made against one volume alone.
func TestLocalDiskBytesIsThisVolumesChainAndNotTheHosts(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	const other = "0198c0de-0000-7000-8000-00000000beef"

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1), active(other, 1)}); err != nil {
		t.Fatalf("preparing two volumes: %v", err)
	}
	mine, theirs := "", ""
	for id, want := range map[string]*string{vol: &mine, other: &theirs} {
		p, err := h.paths.ReadFile(qcow.ActivePointer(root, id))
		if err != nil {
			t.Fatalf("reading %s's pointer: %v", id, err)
		}
		*want = string(p)
	}
	// The fake never learns about the layers `qemu-img create` makes, so the files this
	// volume's chain *is* have to be declared.
	h.paths.put(mine, []byte("mine"), 4<<20)
	h.paths.put(theirs, []byte("theirs"), 64<<20)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1), active(other, 1)}); err != nil {
		t.Fatalf("the cycle that measures: %v", err)
	}
	got := h.volumes(t)
	if got[vol].LocalDiskBytes != 4<<20 {
		t.Errorf("volume %s occupies %d bytes and reports %d", vol, 4<<20, got[vol].LocalDiskBytes)
	}
	if got[other].LocalDiskBytes != 64<<20 {
		t.Errorf("volume %s occupies %d bytes and reports %d", other, 64<<20, got[other].LocalDiskBytes)
	}
}
