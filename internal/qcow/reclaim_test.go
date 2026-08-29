package qcow_test

import (
	"context"
	"errors"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
)

// fakeWitness is the object store's answer to "who holds this volume now": the epoch
// `volumes/<id>/epoch` records, which is written at the grant and is readable on a path
// that does not run through the Control Plane.
type fakeWitness struct {
	granted map[string]int64
	err     error
	asked   []string
}

func (w *fakeWitness) GrantedEpoch(_ context.Context, volumeID string) (int64, error) {
	w.asked = append(w.asked, volumeID)
	if w.err != nil {
		return 0, w.err
	}
	e, ok := w.granted[volumeID]
	if !ok {
		return 0, errors.New("no epoch object for " + volumeID)
	}
	return e, nil
}

// held prepares a volume, serves it, and then takes it out of the desired state, which is
// the state a host is left in after the fleet moves a volume away: the layers are still
// here and nothing is serving them.
func (h *harness) held(t *testing.T, epoch int64) string {
	t.Helper()
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, epoch)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	tip := h.tip(t)
	// The fake never learns about the layers `qemu-img create` makes, because the runner
	// is a fake too — so the file this volume's disk *is* has to be declared here for
	// there to be anything to reclaim.
	h.paths.put(tip, []byte("a layer"), 64<<20)
	if err := h.m.Apply(t.Context(), nil); err != nil {
		t.Fatalf("releasing: %v", err)
	}
	return tip
}

// The space comes back when — and only when — the fleet can be shown to have moved the
// volume somewhere else.
//
// A released volume keeps its layers on purpose: a volume leaves the desired state for
// reasons that reverse, and getting it back should be free. What makes them reclaimable is
// not time passing but a fact: `volumes/<id>/epoch` recording a grant this host does not
// hold. From that moment this host's chain is a fork that can never be published — the
// compare-and-set on HEAD and the epoch fence both refuse it — so the files are occupying
// a disk to hold a history nothing will ever accept.
//
// It is the same signal, read on the same path, that the loop stops a guest on. A host
// that will stop a running VM on this fact can certainly delete a file on it.
func TestALayerIsReclaimedOnceTheFleetHasMovedTheVolume(t *testing.T) {
	t.Parallel()
	wit := &fakeWitness{granted: map[string]int64{vol: 2}}
	h := newHarnessWitnessing(t, wit)
	// The release itself is the cycle that reclaims: by the time a volume leaves this
	// host's desired state the fleet has already granted it elsewhere, so the epoch
	// object is already ahead and there is nothing to wait for.
	tip := h.held(t, 1)

	if got, err := h.paths.Exists(tip); err != nil || got {
		t.Fatalf("the layers of a volume granted elsewhere are still here: %v", err)
	}
	// The record goes with them. A state file vouching for files that are not there is
	// what makes a later Open decide it holds a history it does not.
	st, err := qcow.ReadState(h.paths, root, vol)
	if err != nil {
		t.Fatalf("reading the record back: %v", err)
	}
	if len(st.Commits) != 0 || len(st.Layers) != 0 {
		t.Fatalf("the record still vouches for reclaimed layers: %+v", st)
	}
	// And it remembers why, at the epoch that was observed: without it a desired state
	// that arrives late, naming the epoch this host used to hold, would be served off a
	// chain that is no longer there.
	if st.Fenced == nil || st.Fenced.Epoch != 2 {
		t.Fatalf("the record does not say the volume was granted away: %+v", st.Fenced)
	}
}

// And the two ways this must not fire, which are the whole of its safety: the fleet has
// not moved the volume, and the fleet cannot be asked.
func TestNothingIsReclaimedWithoutAConfirmedGrantElsewhere(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		wit  *fakeWitness
	}{{
		// A volume detached by an operator and not placed anywhere: the epoch object
		// still names this host's grant. It may come back, and getting it back should
		// cost nothing.
		name: "the epoch is the one this host holds",
		wit:  &fakeWitness{granted: map[string]int64{vol: 1}},
	}, {
		// The object store is unreachable, or the epoch object is gone. Silence is not a
		// fact: it is the same shape as a bucket misconfiguration, and this must never be
		// the thing that turns one into deleted data.
		name: "the object store will not answer",
		wit:  &fakeWitness{err: errors.New("the bucket is unreachable")},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newHarnessWitnessing(t, tc.wit)
			tip := h.held(t, 1)

			if err := h.m.Apply(t.Context(), nil); err != nil {
				t.Fatalf("the cycle that must reclaim nothing: %v", err)
			}
			if got, err := h.paths.Exists(tip); err != nil || !got {
				t.Fatalf("a volume nothing confirmed was taken away had its layers deleted: %v", err)
			}
			if len(tc.wit.asked) == 0 {
				t.Fatal("the object store was never asked, so this proves nothing")
			}
		})
	}
}

// A volume this host is serving is never a candidate, whatever the object store says. The
// guest is holding the files and §5 forbids going around it; the loop's own teardown is
// what stops a superseded guest, and it runs before this ever sees the volume released.
func TestAServedVolumeIsNeverReclaimed(t *testing.T) {
	t.Parallel()
	h := newHarnessWitnessing(t, &fakeWitness{granted: map[string]int64{vol: 99}})

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	tip := h.tip(t)
	h.paths.put(tip, []byte("a layer"), 64<<20)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("a second cycle: %v", err)
	}
	if got, err := h.paths.Exists(tip); err != nil || !got {
		t.Fatalf("the tip of a volume this host is serving was deleted: %v", err)
	}
}

// Without a witness nothing is reclaimed and nothing is asked — an Agent with no object
// store cannot confirm anything, and reclaiming on what it cannot confirm is the one
// mistake this whole path is shaped around.
func TestWithNoObjectStoreNothingIsReclaimed(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	tip := h.held(t, 1)

	if err := h.m.Apply(t.Context(), nil); err != nil {
		t.Fatalf("the cycle that must reclaim nothing: %v", err)
	}
	if got, err := h.paths.Exists(tip); err != nil || !got {
		t.Fatalf("an Agent with no object store deleted a volume's layers: %v", err)
	}
}
