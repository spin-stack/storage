package qcow_test

import (
	"errors"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
)

// attachedToCorrupt scripts a QEMU whose guest has corrupted its own image: the same
// query-block answer as attachedTo, with the qcow2 corrupt flag the running QEMU reports.
//
// The shape is the pinned 11.1.1's, measured: query-block's `inserted.image` is the same
// ImageInfo `qemu-img info` prints, format-specific and all.
func attachedToCorrupt(image string) []string {
	return []string{
		`{"QMP": {"version": {}, "capabilities": []}}`,
		`{"return": {}}`,
		`{"return": [{"device": "virtio0", "inserted": {"file": "` + image + `", "drv": "qcow2",
		   "image": {"virtual-size": 268435456, "format": "qcow2",
		     "format-specific": {"type": "qcow2", "data": {"corrupt": true}}}}}]}`,
		`{"return": {}}`,
	}
}

// TestAGuestCorruptingItsOwnImageIsNoticedWhileItRuns is v6 §29's last kill point. The
// consequence was already closed at both ends — a sealed layer with the flag is not
// published, and both readers refuse such a chain — but the fact itself was only learned
// at the next open of the image, which is after the next rotation at best and on another
// host after a rebuild at worst.
//
// The image cannot be inspected from outside while a guest holds it (measured: qemu-img
// is refused on the write lock, and v6 §5 forbids it anyway), so the running QEMU is the
// only witness. It is asked every cycle already.
func TestAGuestCorruptingItsOwnImageIsNoticedWhileItRuns(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	image := qcow.LayerImage(root, vol, layerID)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedToCorrupt(image)
	h.paths.present[image] = true

	err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)})
	if !errors.Is(err, qcow.ErrChainMismatch) {
		t.Fatalf("a guest writing to a corrupt image was accepted: %v", err)
	}
	v := h.volumes(t)[vol]
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST {
		t.Fatalf("refusal = %v %q, want DURABILITY_LOST", v.Refusal, v.RefusalDetail)
	}
	// What an operator acts on starts with which image, and with the fact that the
	// newest restorable point is the last commit and not what the guest is writing.
	if !strings.Contains(v.RefusalDetail, image) {
		t.Errorf("the refusal does not name the image: %q", v.RefusalDetail)
	}
	// Not with an offline tool. Doing that to a live image is what §5 forbids, and it
	// would fail on QEMU's write lock in any case.
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Errorf("a live image was handed to qemu-img: %v", cmds)
	}
	// And the guest keeps running. Nobody else owns this volume, so this is not
	// supersession: stopping the VM would take away the operator's only copy of the
	// newest bytes, which is a second loss on top of the first.
	if strings.Contains(h.dialer.sent(), `"stop"`) {
		t.Errorf("the guest was stopped over its own image's corruption: %s", h.dialer.sent())
	}
}

// TestAHealthyLiveImageIsNotRefused is the other half: the flag is read from an answer
// this Manager already asks for every heartbeat, so a mistake in reading it would refuse
// every attached volume in the fleet.
func TestAHealthyLiveImageIsNotRefused(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	image := qcow.LayerImage(root, vol, layerID)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(image)
	h.paths.present[image] = true

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a healthy live volume was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
}
