package qcow_test

import (
	"errors"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/qcow"
)

// TestAdversaryTheBucketIsDownForManyCyclesAndTheGuestKeepsItsDisk is v6 §15's row "S3
// no disponible — la VM sigue" held for as long as an outage actually lasts. §11's
// "nothing rotates while a sealed layer is unpublished" already has its test; what that
// one does not ask is what the *fleet* sees meanwhile, and that is where the expensive
// mistake is. Every refusal but IMAGE_MISSING latches until the Control Plane raises the
// epoch, so a volume refused on cycle three of an outage is a volume this host will not
// serve again after the bucket comes back — a thirty-second blip turned into an outage
// for a guest whose writes are all still on its disk.
//
// So the assertions are on what the outside sees, cycle after cycle: the volume is
// reported and unrefused, `active/current` still names the layer QEMU has open, the VM
// is never paused, and this host's own record still says which commit it owes. Then the
// bucket answers and the commit lands under the id it was sealed with.
//
// The five questions v6 §22 asks:
//
//   - Which commit is visible? The one HEAD named before the outage: nothing this host
//     seals during it reaches the bucket, and it claims nothing that it has not.
//   - What local state is left? The sealed layer, the tip growing under the guest, and
//     state.json naming the pending commit — the same id for the whole outage.
//   - What remote objects are left? None: every attempt failed before it wrote.
//   - Does it recover by itself? Yes. The first cycle after the bucket answers publishes
//     the layer that was waiting, under the same commit id, and records it.
//   - Could a confirmed commit be lost? No. Nothing that HEAD named is touched, and the
//     retries are the same commit rather than a queue of near-identical ones.
func TestAdversaryTheBucketIsDownForManyCyclesAndTheGuestKeepsItsDisk(t *testing.T) {
	t.Parallel()
	down := errors.New("RequestTimeout: the object store did not answer")
	pub := &recordingPublisher{err: down}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)

	// The cycle that seals a layer and cannot publish it.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a publish against a bucket that is down was reported as a success")
	}
	sealed := h.tip(t)
	if sealed == tip {
		t.Fatal("nothing was sealed, so this test is not exercising the publish path")
	}
	owed := h.state(t).Pending
	if owed == nil {
		t.Fatal("this host did not write down the commit it owes, so a restart during the outage would not know what to publish")
	}

	// Six more cycles of an outage, with the guest writing all the way through.
	for i := range 6 {
		h.paths.sizes[sealed] = int64(20+i) << 20
		h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(sealed)
		err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
		if err == nil {
			t.Fatalf("cycle %d: the bucket is down and the cycle reported success", i)
		}

		v, ok := h.volumes(t)[vol]
		if !ok {
			t.Fatalf("cycle %d: the volume stopped being reported because the bucket is down", i)
		}
		if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			t.Fatalf("cycle %d: the volume was refused because the bucket is down: %v %q — every refusal but IMAGE_MISSING latches, so this host would not serve it again at this epoch",
				i, v.Refusal, v.RefusalDetail)
		}
		if got := h.tip(t); got != sealed {
			t.Fatalf("cycle %d: active/current names %q while the guest is writing to %q", i, got, sealed)
		}
		if got := h.state(t).Pending; got == nil || got.CommitID != owed.CommitID {
			t.Fatalf("cycle %d: the record of what this host owes is now %+v, want the commit it sealed, %s", i, got, owed.CommitID)
		}
	}
	// The VM was never paused. A bucket that does not answer is not a statement about
	// who owns this volume, and pausing is what this Agent does when it is not the
	// writer any more.
	if strings.Contains(h.dialer.sent(), `"stop"`) {
		t.Error("the guest was paused because the object store was down")
	}

	// The bucket answers again, and nobody in the fleet has done anything: the same
	// epoch, on purpose.
	pub.err = nil
	before := len(pub.got)
	h.paths.sizes[sealed] = 40 << 20
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(sealed)
	// What `qemu-img info` will say about the overlay this cycle's rotation creates:
	// the publish clears what this host owes, so §11's guard lets it rotate again.
	h.runner.info = overlayJSON(size, sealed)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first cycle after the bucket came back: %v", err)
	}
	if len(pub.got) == before {
		t.Fatal("the layer that was waiting was not published when the bucket came back")
	}
	landed := pub.got[before]
	if landed.CommitID != owed.CommitID {
		t.Errorf("the layer landed as commit %s; it was sealed as %s, and a fresh id publishes the same layer twice", landed.CommitID, owed.CommitID)
	}
	st := h.state(t)
	if st.Pending != nil {
		t.Errorf("this host still owes %+v after the commit landed", st.Pending)
	}
	recorded := false
	for _, c := range st.Commits {
		recorded = recorded || c.CommitID == owed.CommitID
	}
	if !recorded {
		t.Errorf("the commit that landed is not in this host's record %v, so the next recovery downloads a layer it already holds", st.Commits)
	}
}
