package qcow_test

import (
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/qcow"
)

// TestAdversaryAHostRebootReattachesTheChainItsGuestLetGoOf is the reboot kill-point: the
// machine went down, so QEMU died with it and nothing holds the volume's tip any more, and
// the Agent comes back over the same filesystem. Every restart test in this package scripts
// a live guest on the QMP socket — the case where the Agent's process died *under* a guest.
// A reboot is the other one, and it is the one where Open takes a different branch: with no
// live image, `active/current` is all there is to go on, and the branch that mishandles it
// hands the guest a blank disk (born()) or a chain the published history has moved past.
//
// The five questions v6 §22 asks, answered by the assertions below:
//
//   - which commit is visible: the last one this host published, which is what HEAD names;
//     the guest's writes since it are in the local tip, which is served unchanged.
//   - what local state is left: the layer directory, `active/current` naming the same tip,
//     and state.json's record of the commits whose layers this host holds.
//   - what remote objects are left: exactly what was there before the reboot. A reboot
//     publishes nothing, and must not re-publish what was already published — a second
//     commit id for a layer already in the history is an entry nothing can collapse.
//   - does it recover by itself: yes, in one cycle, with no operator and no re-download —
//     the chain is validated offline (`--backing-chain`, which only a reboot's dead guest
//     makes possible) and re-served.
//   - could a confirmed commit be lost: no. The commit this host published stays in
//     state.json and stays under the tip; nothing here creates, truncates or repoints a
//     layer.
func TestAdversaryAHostRebootReattachesTheChainItsGuestLetGoOf(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	sealed := h.rotating(t, 9<<20)

	// The cycle before the reboot: the tip is over the threshold, so it is sealed, a new
	// tip is put over it and the sealed layer is published.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that rotates and publishes: %v", err)
	}
	tip := h.tip(t)
	if tip == sealed {
		t.Fatalf("nothing rotated; the tip is still %s", sealed)
	}
	if len(pub.got) != 1 {
		t.Fatalf("%d layers were published before the reboot, want 1", len(pub.got))
	}
	published := pub.got[0].CommitID
	if before := h.state(t); len(before.Commits) != 1 || before.Commits[0].CommitID != published {
		t.Fatalf("this host's record of what it published is %+v, want the commit it just made", before.Commits)
	}

	// The reboot. QEMU is gone with the machine, so nothing is listening on the volume's
	// QMP socket; the object store still names the commit this host published, which is
	// what makes the local chain current rather than a fork.
	delete(h.dialer.scripts, qcow.QMPSocket(root, vol))
	h.rec.head = published
	h.runner.info = overlayJSON(size, sealed)
	h.runner.reset()
	h.dialer.reset()

	after := h.restart(t, 8<<20, pub)
	if err := after.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first cycle after the reboot: %v", err)
	}

	// The guest's disk. A rebooted host that created a new chain — or that believed a
	// pointer it did not check — would satisfy every assertion about being "healthy" and
	// hand the next guest an image with none of the volume's writes in it.
	if got := after.tip(t); got != tip {
		t.Errorf("after the reboot the pointer names %q; the chain the guest was writing to is at %q, so the next guest reads a disk this volume never had", got, tip)
	}
	for _, cmd := range after.runner.commands() {
		if strings.Contains(cmd, "create") {
			t.Errorf("a reboot created a layer for a volume that already has a chain: %q", cmd)
		}
	}
	// With no guest holding the tip this is the one moment the whole chain can be opened,
	// and it is the check that catches a backing file the reboot lost: plain `info` exits 0
	// on an image whose backing is gone.
	if !strings.Contains(strings.Join(after.runner.commands(), "\n"), "--backing-chain") {
		t.Errorf("the chain was re-served without being walked while nothing held it: %v", after.runner.commands())
	}

	// What this host published stays published, and stays this host's: the record is what
	// a later recovery reads to skip a download, and what checkNotStale compares HEAD
	// against. A reboot that dropped it would republish the same bytes under a second id.
	st := after.state(t)
	if len(st.Commits) != 1 || st.Commits[0].CommitID != published {
		t.Errorf("after the reboot this host's record of the commits it holds is %+v, want the commit it published before the reboot", st.Commits)
	}
	if st.Pending != nil {
		t.Errorf("the reboot left this host owing a layer it had already published: %+v", st.Pending)
	}
	if len(pub.got) != 1 {
		t.Errorf("the reboot published %d layers; the debt was discharged before it, so any of them is a second commit for a layer the history already has", len(pub.got)-1)
	}

	// And the volume is served again, without an operator: a reboot is not a refusal.
	v, ok := after.volumes(t)[vol]
	if !ok {
		t.Fatalf("volume %s is not served after the reboot", vol)
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("after the reboot the volume is refused: %v %q", v.Refusal, v.RefusalDetail)
	}
}
