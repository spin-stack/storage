package qcow_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/qcow"
)

// TestAdversaryAFencedHostResumesItsForkWhenTheGuestNeverStopped is the re-grant hole
// with the one thing the existing test leaves out: the VM is still running.
//
// Fencing a volume does not stop its VM. Manager.Fence drops a chain out of a map;
// nothing here or in the fleet kills QEMU, so the ordinary shape of a lapsed lease is a
// host that has given the volume up with a guest still writing into the tip. While that
// goes on the successor serves the volume and publishes, and HEAD names a history none
// of the local layers sit under.
//
// The fleet then grants the volume back at a higher epoch. checkFenced lets it through —
// that is what a higher epoch is for — and Open's first branch is `LiveImage != ""`,
// which returns the chain QEMU has open before state.json is read at all. So
// `local.Fenced` is never consulted, regrant never runs, the object store is never asked,
// and the first rotation after that publishes a layer of the fork as a child of the
// successor's HEAD: a commit whose bytes do not continue the chain it claims to continue.
//
// The two rows are the whole argument. They differ in one fact — whether QEMU still has
// the tip open when the volume comes back — and everything the fleet said is identical.
// The row where the guest died goes through regrant and is correct; the row where it
// lived skips it.
func TestAdversaryAFencedHostResumesItsForkWhenTheGuestNeverStopped(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// live says whether QEMU still has the fork's tip open at the re-grant.
		live bool
	}{
		{name: "the guest died with the host that lost the volume"},
		{name: "the guest kept writing through the fence", live: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &recordingPublisher{}
			h := newHarnessFull(t, 8<<20, pub)
			first := h.rotating(t, 9<<20)

			// The cycle that seals and publishes the first layer. After it the guest is
			// writing into a new tip and this host's record says it published `first`.
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("the cycle that rotates and publishes: %v", err)
			}
			fork := h.tip(t)
			if fork == first {
				t.Fatalf("nothing rotated; the tip is still %s", fork)
			}

			// The lease lapses. The volume is given up and the files stay.
			if err := h.m.Fence(t.Context(), []string{vol},
				storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
				t.Fatalf("fencing: %v", err)
			}

			// While this host was not the writer, the successor served the volume and
			// published. What the bucket holds is a history none of the local layers
			// sit under, rebuilt here on demand.
			base := qcow.LayerImage(root, vol, baseID)
			h.paths.present[base] = true
			h.rec.res, h.rec.err, h.rec.calls = qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}, nil, nil
			h.rec.head = headCommit
			h.runner.info = overlayJSON(size, base)
			pub.got = nil

			if tc.live {
				// QEMU never let go, and the tip it holds has grown past the threshold
				// while nobody was publishing. What qemu-img is asked about in this row
				// is the overlay a rotation creates over that tip.
				h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(fork)
				h.paths.sizes[fork] = 9 << 20
				h.runner.info = overlayJSON(size, fork)
			} else {
				delete(h.dialer.scripts, qcow.QMPSocket(root, vol))
			}

			// The fleet grants the volume back.
			err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)})
			if tc.live {
				// With a guest still writing into the fork, the volume cannot be served
				// this cycle: the file it holds cannot be replaced under it. A refusal is
				// the honest outcome, and the guest has to stop — every byte it writes
				// from here lands in a history nobody will ever publish, and it is being
				// told they succeeded. The rebuild happens on the next cycle, which finds
				// no live image.
				if err == nil {
					t.Fatal("this host resumed a volume whose guest is writing into a fork of the published history")
				}
				if !strings.Contains(h.dialer.sent(), `"execute":"stop"`) {
					t.Errorf("the guest was left writing to a chain this host does not own: %s", h.dialer.sent())
				}
			} else if err != nil {
				t.Fatalf("re-applying at a higher epoch: %v", err)
			}

			if len(h.rec.calls) == 0 {
				t.Errorf("this host resumed volume %s at epoch 2 without once asking the object store what the published history is: it was fenced at epoch 1, and Open returned the live image before state.json was read",
					vol)
			}
			if len(pub.got) == 0 {
				return
			}
			if got, want := pub.got[0].LayerID, qcow.LayerIDOfImage(fork); got != want {
				t.Fatalf("the layer published is %s, and the test is about %s", got, want)
			}
			t.Errorf("this host published layer %s (commit %s) out of the chain it held while it was not the volume's writer; the published history had moved to commit %s, so the commit claims a parent whose bytes it does not continue",
				pub.got[0].LayerID, pub.got[0].CommitID, headCommit)
		})
	}
}

// TestAdversaryTheForkLayerSurvivesInMemoryTheClearForkThatDroppedItFromDisk is the same
// fork seen from the other side, and it is the case a restart gets *right*.
//
// A host publishes, is told HEAD moved, records the fence and keeps the sealed layer it
// still owes — in `v.pending` and in `state.json`. The volume comes back at a higher
// epoch with no guest attached, so regrant runs properly: the store's chain is rebuilt
// here, a new tip is put over it, and clearFork deletes the pending commit from
// state.json for the reason its own comment gives — publishing it now would put a layer
// into the history whose bytes the chain being served does not contain.
//
// It deletes it from the file only. `v.pending` is untouched, so the next cycle in which
// a guest attaches publishes exactly the layer clearFork was written to drop.
//
// The two rows are the finding: they differ only in whether the Agent's process survives
// between the re-grant and the guest attaching, and the survivor is the one that loses.
func TestAdversaryTheForkLayerSurvivesInMemoryTheClearForkThatDroppedItFromDisk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// restart says whether the Agent process dies between the cycle that rebuilds
		// the chain and the cycle in which a guest attaches to it.
		restart bool
	}{
		{name: "the Agent is restarted after the re-grant", restart: true},
		{name: "the Agent stays up across the re-grant"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &recordingPublisher{err: fmt.Errorf("publishing: %w", commit.ErrHeadMoved)}
			h := newHarnessFull(t, 8<<20, pub)
			sealed := h.rotating(t, 9<<20)

			// The cycle that seals a layer and is told the volume is not this host's.
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
				t.Fatal("a volume whose HEAD moved was served without complaint")
			}
			if st := h.state(t); st.Pending == nil || st.Pending.LayerID != qcow.LayerIDOfImage(sealed) {
				t.Fatalf("the sealed layer was not recorded as owed: %+v", st.Pending)
			}

			// The guest is gone — the VM died with the host that lost the volume — so
			// the re-grant takes the branch that can rebuild.
			delete(h.dialer.scripts, qcow.QMPSocket(root, vol))
			base := qcow.LayerImage(root, vol, baseID)
			h.paths.present[base] = true
			h.rec.res, h.rec.err, h.rec.calls = qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}, nil, nil
			h.runner.info = overlayJSON(size, base)
			// The winner published once and is idle, so a retry from here is accepted
			// rather than caught by the compare-and-set. That is the case that loses
			// data quietly.
			pub.err, pub.got = nil, nil

			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)}); err != nil {
				t.Fatalf("re-applying at a higher epoch: %v", err)
			}
			if st := h.state(t); st.Pending != nil {
				t.Fatalf("clearFork did not drop the pending commit from state.json: %+v", st.Pending)
			}
			served := h.tip(t)

			if tc.restart {
				h = h.restart(t, 8<<20, pub)
			}
			// A guest is launched against the chain that was just rebuilt.
			h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(served)
			h.paths.sizes[served] = 1 << 20

			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)}); err != nil {
				t.Fatalf("the cycle in which the guest attaches: %v", err)
			}

			if len(pub.got) == 0 {
				return
			}
			if got, want := pub.got[0].LayerID, qcow.LayerIDOfImage(sealed); got != want {
				t.Fatalf("the layer published is %s, and the test is about %s", got, want)
			}
			t.Errorf("this host published layer %s, the sealed layer of the chain it held before it lost the volume, onto the history rebuilt from commit %s; state.json had already dropped it, so the same Agent one SIGKILL later publishes nothing",
				pub.got[0].LayerID, headCommit)
		})
	}
}

// TestAdversaryAHostFencedBeforeItHeldAChainIsRefusedAtEveryLaterEpoch is a refusal that
// outlives the epoch that was supposed to lift it.
//
// The two guards disagree about what a higher epoch means. checkFenced says a higher
// epoch is the fleet granting the volume back, and that a higher epoch is the only thing
// that clears the record. born says a fenced host asking about a volume the store has
// never heard of is a host pointed at the wrong bucket, and refuses — with no epoch in
// the comparison at all.
//
// For a host that was fenced while it *held* a chain the two agree, because the re-grant
// goes through regrant, which clears the record. For a host that was fenced before it
// ever created the volume's first layer they do not, and nothing clears it: there is no
// pointer, so Open goes to born, born refuses, no chain is ever opened, and the record
// stays. The volume is unserveable on this host at every later epoch and after any number
// of restarts — for a volume that has never published a byte and whose correct first
// layer is an empty qcow2.
//
// Reaching it takes nothing exotic, which the two rows show: a new volume is placed here
// and the object store is briefly unreachable, so the first cycle refuses. That alone is
// recovered from on the next cycle. The same outage with a lease lapsing inside it is
// terminal.
func TestAdversaryAHostFencedBeforeItHeldAChainIsRefusedAtEveryLaterEpoch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// fence says whether the lease lapses during the outage.
		fence bool
	}{
		{name: "the object store was briefly unreachable"},
		{name: "the lease lapsed during the outage", fence: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			h.rec.err = errors.New("dial tcp 10.0.0.7:443: connect: connection refused")

			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
				t.Fatal("a volume nobody could ask the bucket about was prepared without complaint")
			}
			if tc.fence {
				if err := h.m.Fence(t.Context(), []string{vol},
					storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
					t.Fatalf("fencing: %v", err)
				}
			}

			// The store is reachable again and answers the truth about this volume: it
			// has never published, because nobody has ever served it.
			h.rec.err, h.rec.calls = fmt.Errorf("recovery: volume %s: %w", vol, commit.ErrNoHead), nil

			// The fleet grants it, twice, at two higher epochs, with a restart in
			// between: everything the refusal rests on is on the disk, so nothing about
			// a fresh process changes it.
			_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)})
			after := h.restart(t, 0, nil)
			_ = after.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 3)})

			if v := after.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
				t.Errorf("at epoch %d — two grants after epoch 1 — this host still refuses volume %s (%v: %q); the volume has never published, its first layer is an empty qcow2, and no epoch clears the record because the branch that clears it is only reached by a host that already has a local chain",
					v.Epoch, vol, v.Refusal, v.RefusalDetail)
			}
			if exists, _ := after.paths.Exists(qcow.ActivePointer(root, vol)); !exists {
				t.Errorf("volume %s has no chain on this host after two grants at higher epochs", vol)
			}
		})
	}
}
