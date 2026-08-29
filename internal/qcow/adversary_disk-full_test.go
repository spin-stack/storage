package qcow_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// TestAdversaryTheDeviceFillsWhileALayerIsSealedAndNothingSealedIsForgotten is v6 §22's
// "disco lleno" at the moment it costs most: the device runs out between the QMP
// snapshot that seals a layer and the write of the record saying this host owes it.
// rotate's comment prices that window — "in memory first, then on disk, so a write that
// fails still leaves the layer pending here" — and nothing checked it. The device stays
// full for several cycles afterwards, because a full device is not a one-shot fault: it
// is sticky until somebody gives space back, and every cycle in between still has to
// serve a guest that is doing nothing wrong (§26).
//
// The failure being hunted is not the error return. It is a sealed layer with the
// guest's writes in it that no later cycle ever offers to the object store, and the
// twin of it: the same layer published twice under two commit ids because the note of
// the first one could not be written.
//
// The five questions v6 §22 asks:
//
//   - Which commit is visible? Every layer this host sealed, each under exactly one
//     commit — including the one sealed in the cycle whose record failed.
//   - What local state is left? The chain, the pointer naming the layer QEMU has open,
//     and a state.json that is behind: the commits published while the device was full
//     are missing from it. The bucket, not this file, is what a recovery believes.
//   - What remote objects are left? One commit per sealed layer, no duplicates.
//   - Does it recover by itself? Yes, with no operator beyond giving the device space:
//     cycles go green again and the host's own record starts naming the commits again.
//   - Could a confirmed commit be lost? No. A full device costs this host its notes,
//     never a layer: nothing is deleted and no commit is republished under a second id.
func TestAdversaryTheDeviceFillsWhileALayerIsSealedAndNothingSealedIsForgotten(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	a := newAdversary(t, 8<<20, pub)
	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	first := a.tip(t)
	a.guestWriting(first, 9<<20)

	// The device fills. Every write this Agent makes about itself — the record of what
	// it sealed, and later the record of what it published — meets ENOSPC.
	a.paths.failState = fmt.Errorf("qcow: writing state.json: %w", disk.ErrNoSpace)

	err := a.apply(t, 1)
	if !errors.Is(err, disk.ErrNoSpace) {
		t.Fatalf("the cycle whose record could not be written returned %v; an operator has to be sent to the device, so the error has to carry it", err)
	}
	// The rotation really happened under the guest — this test is about the window
	// *after* the snapshot, and a Manager that failed before telling QEMU to switch
	// would satisfy every assertion below without ever having been there.
	if !strings.Contains(a.dialer.sent(), "blockdev-snapshot-sync") {
		t.Fatalf("no snapshot was taken, so the device did not fill where this test says it did: %s", a.dialer.sent())
	}
	if a.tip(t) == first {
		t.Fatal("the guest is still writing to the layer that was sealed")
	}

	// Four more cycles with the device still full and the guest still writing.
	for i := range 4 {
		a.guestWriting(a.tip(t), int64(20+i)<<20)
		_ = a.apply(t, 1)

		v, ok := a.volumes(t)[vol]
		if !ok {
			t.Fatalf("cycle %d: the volume stopped being reported because the device is full", i)
		}
		// A full device says nothing about who owns this volume. Refusing it here would
		// take the disk away from a guest whose writes are all still there, and — for
		// every refusal but IMAGE_MISSING — latch it until the fleet raised the epoch.
		if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			t.Fatalf("cycle %d: the volume was refused over a full device: %v %q", i, v.Refusal, v.RefusalDetail)
		}
		if got := a.tip(t); got == "" {
			t.Fatalf("cycle %d: active/current names nothing, so nothing can be launched against this volume", i)
		}
	}
	// And the guest was never paused. Stopping a VM is what this Agent does when it is
	// not a volume's writer any more; a device with no room is a different sentence.
	if strings.Contains(a.dialer.sent(), `"stop"`) {
		t.Error("the guest was stopped because the device filled")
	}

	// The point of the whole test: every layer sealed while the device was full reached
	// the object store, each exactly once. Two commits for one layer would be the
	// duplicate history that minting an id per attempt produces, and a missing one would
	// be a hole in the published chain that nothing anywhere reports.
	if len(pub.got) == 0 {
		t.Fatal("nothing was published at all, so this test is not exercising the publish path")
	}
	perLayer := map[string][]string{}
	for _, l := range pub.got {
		perLayer[l.LayerID] = append(perLayer[l.LayerID], l.CommitID)
	}
	if !a.offered()[qcow.LayerIDOfImage(first)] {
		t.Errorf("the layer sealed in the cycle whose record failed (%s) was never offered for publishing; the layers that were: %v",
			qcow.LayerIDOfImage(first), a.offered())
	}
	for layerID, commits := range perLayer {
		if len(commits) > 1 {
			t.Errorf("layer %s was published under %d commit ids while the device was full: %v", layerID, len(commits), commits)
		}
	}

	// Space comes back, and nobody in the fleet has done anything.
	a.paths.failState = nil
	for i := range 2 {
		a.guestWriting(a.tip(t), int64(40+i)<<20)
		if err := a.apply(t, 1); err != nil {
			t.Fatalf("the cycle after the device got space back: %v", err)
		}
	}
	body, err := a.paths.ReadFile(qcow.StateFile(root, vol))
	if err != nil {
		t.Fatalf("reading state.json after the device recovered: %v", err)
	}
	st, err := qcow.UnmarshalState(vol, body)
	if err != nil {
		t.Fatalf("state.json cannot be believed after the device recovered: %v", err)
	}
	last := pub.got[len(pub.got)-1].CommitID
	recorded := false
	for _, c := range st.Commits {
		recorded = recorded || c.CommitID == last
	}
	if !recorded {
		t.Errorf("the host's own record still names %v and not the commit %s it just published: it did not catch up on its own",
			st.Commits, last)
	}
}
