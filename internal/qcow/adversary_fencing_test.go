package qcow_test

import (
	"fmt"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/qcow"
)

// TestAdversaryAFencedHostPublishesAgainAfterARestart. PUBLISH_FENCED is the strongest
// statement this host ever makes about itself: another host moved HEAD, so this one is
// not the volume's writer. It lives in a map in memory, and the process that holds it
// dies for the ordinary reasons — OOM, a deploy, a panic, a SIGKILL like the one
// `task demo:stage1` performs under a running guest.
//
// Nothing on the Control Plane acts on the refusal either: applyRefusal writes it to a
// column, and no reconciler reads it. So the desired state the restarted Agent reads back
// is the one it read before — this volume, ACTIVE, at the same epoch — and the ensure()
// guard that keeps a given-up volume from resuming is keyed on a refusal this process no
// longer has.
//
// The restarted Agent therefore re-attaches to the volume, reads the sealed layer it owes
// out of state.json, and hands it to the object store again. The second publish is given
// a store that accepts it, because that is the real case: the winner published once and is
// idle, so HEAD has not moved since this host last read it and the compare-and-set has
// nothing to catch.
func TestAdversaryAFencedHostPublishesAgainAfterARestart(t *testing.T) {
	t.Parallel()
	fenced := &recordingPublisher{err: fmt.Errorf("publishing: %w", commit.ErrHeadMoved)}
	h := newHarnessFull(t, 8<<20, fenced)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a volume whose HEAD moved was served without complaint")
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED {
		t.Fatalf("the refusal is %v, want PUBLISH_FENCED", v.Refusal)
	}

	// The guest kept running through the rotation, so what QEMU has open is the new tip,
	// well under the threshold: the restarted Agent has exactly one thing to do about this
	// volume, which is the layer it still owes.
	live := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(live)
	h.paths.sizes[live] = 1 << 20

	// The Agent restarts. The fleet has not moved: the volume is still listed for this
	// host, ACTIVE, at epoch 1.
	next := &recordingPublisher{}
	after := h.restart(t, 8<<20, next)
	_ = after.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	if len(next.got) != 0 {
		t.Errorf("after a restart this host published %d layer(s) for a volume it had already been told it does not own; commit %s went to the object store",
			len(next.got), next.got[0].CommitID)
	}
	if v, ok := after.volumes(t)[vol]; ok && v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("after a restart this host reports the volume as served and healthy again (epoch %d), so the fleet reads two writers as one", v.Epoch)
	}
}

// TestAdversaryARegrantedHostServesItsStaleLocalChain is the other half of the same hole,
// and it does not need a restart.
//
// This host holds the volume, its lease lapses, and it gives the volume up — the files
// stay, which is deliberate. Another host takes the volume, serves it for as long as that
// takes, and publishes. Then the volume comes back here at a higher epoch: the successor
// died, or the fleet moved it back. That grant is exactly what the ensure() guard waits
// for, so this host resumes.
//
// It resumes on `active/current`. Open consults the object store only in born(), the
// branch for a volume with no local pointer, so a host that has one never asks whether the
// published history moved past it. The guest is handed a chain that is missing every
// commit the successor made, and the first rotation over it publishes that divergence onto
// HEAD.
//
// The observable is the recovery being asked at all: it is the one call that separates
// "my local chain is the volume" from "my local chain is a fork of it", and a guard that
// is not consulted is not a guard.
func TestAdversaryARegrantedHostServesItsStaleLocalChain(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	stale := h.tip(t)
	// The control for the assertion below: on the branch that does consult the bucket —
	// a volume with no local pointer — the call is recorded here. What the re-grant does
	// is measured against this.
	if len(h.rec.calls) != 1 {
		t.Fatalf("the first placement asked the object store %d times, want once", len(h.rec.calls))
	}
	if err := h.m.Fence(t.Context(), []string{vol},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
		t.Fatalf("fencing: %v", err)
	}

	// While this host was out, the volume's successor wrote and published. What the
	// bucket holds is a history this host has no layer of, rebuilt here on demand.
	base := qcow.LayerImage(root, baseID)
	h.paths.present[base] = true
	h.runner.info = overlayJSON(size, base)
	h.rec.res, h.rec.err, h.rec.calls = qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}, nil, nil

	// The fleet grants the volume back to this host.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 5)}); err != nil {
		t.Fatalf("re-applying at a higher epoch: %v", err)
	}

	if len(h.rec.calls) == 0 {
		t.Errorf("this host resumed volume %s without once asking the object store what the published history is", vol)
	}
	if got := h.tip(t); got == stale {
		t.Errorf("the pointer still names %q, the tip this host had before it lost the volume; a guest launched now reads none of the commits its successor published", got)
	}
}
