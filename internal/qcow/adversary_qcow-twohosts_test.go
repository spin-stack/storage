package qcow_test

import (
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
)

// successorCommit is what the object store's HEAD names after another host took this
// volume and published: a commit id this host has never written and holds no layer of.
const successorCommit = "0198c0de-0000-7000-8000-0000000505ec"

// overtaken leaves this host looking exactly like a writer that has been overtaken. It
// is the state TestAdversaryARegrantedHostServesItsStaleLocalChain produced the long way
// round, written directly here so that the one variable between the two subtests below
// is whether a guest is attached:
//
//   - a local chain, with `active/current` naming its tip;
//   - a state.json whose only commit is one this host published itself; and
//   - an object store whose HEAD names a later commit, written by somebody else.
func overtaken(t *testing.T, h *harness) string {
	t.Helper()
	image := qcow.LayerImage(root, vol, layerID)
	h.paths.present[image] = true
	pointer := qcow.ActivePointer(root, vol)
	h.paths.present[pointer], h.paths.files[pointer] = true, image
	body, err := qcow.MarshalState(qcow.State{
		VolumeID: vol,
		Commits:  []qcow.CommitLayer{{CommitID: headCommit, LayerID: layerID}},
		Layers:   []string{layerID},
	})
	if err != nil {
		t.Fatalf("building the state this host would have written: %v", err)
	}
	if err := h.paths.WriteAtomic(qcow.StateFile(root, vol), body); err != nil {
		t.Fatalf("writing state.json: %v", err)
	}
	// The successor's commit, which this host did not write and cannot reach.
	h.rec.head = successorCommit
	return image
}

// TestAdversaryALiveGuestSkipsTheStaleChainCheck.
//
// checkNotStale is the guard that separates "my local chain is this volume" from "my
// local chain is a fork of it": it reads HEAD, compares it with the commits state.json
// says this host published, and refuses when the published history has moved past them.
// It is the whole of what stops a host that lost a volume, and was handed it back, from
// serving a chain another host has already written over.
//
// Open reaches it only on the offline path. The first thing Open does is take QEMU's
// answer and return:
//
//	if req.LiveImage != "" { syncPointer(...); return &Chain{Active: req.LiveImage}, nil }
//
// so a volume whose guest is still running never asks the object store anything. The
// justification for the shortcut is v6 §5 — an image a VM holds must not be handed to an
// offline tool — and it is a real rule, but it does not cover this check: checkNotStale
// runs no qemu-img at all. It reads one object (HEAD) and one local file (state.json),
// neither of which the write lock has any opinion about.
//
// The reachable shape is the one this package is built around. The Agent is killed while
// the guest keeps writing (`task demo:stage1` performs exactly that SIGKILL); the volume
// is moved to another host, which serves it and publishes; the fleet hands it back here,
// or the restarted Agent simply reads the desired state it read before. QEMU on this host
// still has the old tip open, so the Agent adopts it, repairs `active/current` to point
// at it, and reports the volume healthy — and the guest attached to it is writing to a
// fork of a history that has moved on. The first rotation over that fork is what carries
// the divergence to HEAD.
//
// The two subtests differ in one field. Detached is the control: the same disk, the same
// state.json, the same HEAD, and the volume is refused. Attached is the defect.
func TestAdversaryALiveGuestSkipsTheStaleChainCheck(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		attached bool
	}{
		{"no guest is attached, and the fork is refused", false},
		{"a guest is attached to the fork", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			image := overtaken(t, h)
			if tc.attached {
				h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(image)
			}

			err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 5)})

			// The object store is the only thing that can tell this host its chain is a
			// fork, and asking it is the one observable that a guard was consulted at
			// all. A guard that is not consulted is not a guard.
			var asked bool
			for _, c := range h.rec.calls {
				asked = asked || strings.HasPrefix(c, "current/")
			}
			if !asked {
				t.Errorf("this host adopted volume %s without once asking the object store whether its chain is still the volume's; HEAD names commit %s, which this host did not write",
					vol, successorCommit)
			}
			v, held := h.volumes(t)[vol]
			if err == nil || (held && v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED) {
				t.Errorf("this host reports volume %s as served and healthy at epoch %d over the chain at %s; the published history is at commit %s, and the guest on this host is writing into a fork of it",
					vol, v.Epoch, image, successorCommit)
			}
		})
	}
}

// TestAdversaryAnEpochThatGoesBackwardsUnfencesTheVolume.
//
// The epoch is the fencing token (v6 §13): it is what commit.Publish compares a parent
// manifest against, what ensure()'s latch is keyed on, and the only thing that lifts the
// durable fence in state.json — "a *higher* epoch, which is the fleet granting the volume
// to this host again. Not a restart, not the object store answering again, not the guest
// coming back."
//
// Nothing makes it monotonic. ensure() takes whatever the desired state carries:
//
//	v.epoch = d.GetEpoch()
//
// with no comparison against the epoch this host already holds for the volume, and the
// latch above that line only guards a volume that is *already refused*. So a desired state
// carrying an epoch this host has already passed — a Control Plane replica behind the one
// that granted the volume, an answer served from a restored catalog, a retry that landed
// on a stale reader — silently moves this host's fencing token backwards while it goes on
// serving.
//
// What that costs is not the wrong number in a report. The epoch this host holds is what
// gets stamped into the fence when it gives the volume up (recordFenced writes v.epoch),
// and the fence is cleared by anything strictly greater. So a host whose token was walked
// back to 3, fenced, and then handed the very same desired state it was already serving
// under — epoch 9, unchanged, nothing granted, no fleet decision at all — reads 9 > 3,
// clears its own fence and resumes. It publishes at epoch 9 again, which is not greater
// than the successor's parent manifest, so commit.Publish's own epoch fence lets it
// through too.
//
// The subtests differ only in whether the stale epoch ever arrives. Without it the latch
// holds and the volume stays given up, which is the rule working; with it the same final
// Apply resumes the volume.
func TestAdversaryAnEpochThatGoesBackwardsUnfencesTheVolume(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stale bool
	}{
		{"the epoch only ever goes forward", false},
		{"a desired state arrives carrying an epoch this host has passed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 9)}); err != nil {
				t.Fatalf("serving the volume at epoch 9: %v", err)
			}
			if tc.stale {
				// Still ACTIVE, still this host's, and three epochs behind. Nothing in
				// the Manager treats that as anything but the current truth.
				if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 3)}); err != nil {
					t.Fatalf("applying a stale desired state: %v", err)
				}
				if got := h.volumes(t)[vol].Epoch; got != 9 {
					t.Errorf("this host now holds volume %s at epoch %d; it was granted epoch 9 and the fleet has raised nothing since, so its fencing token has gone backwards",
						vol, got)
				}
			}

			// The lease lapses and this host gives the volume up. The fence is written
			// with whatever epoch it believes it holds.
			if err := h.m.Fence(t.Context(), []string{vol},
				storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
				t.Fatalf("giving the volume up: %v", err)
			}

			// The same desired state this host was already serving under. The fleet has
			// granted nothing: epoch 9 is the epoch it held before it was fenced.
			_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 9)})

			if v, held := h.volumes(t)[vol]; held && v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
				t.Errorf("this host gave volume %s up and is serving it again at epoch %d, on a desired state carrying the epoch it already held; nothing in the fleet granted it back",
					vol, v.Epoch)
			}
		})
	}
}
