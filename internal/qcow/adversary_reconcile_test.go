package qcow_test

import (
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
)

// These two attack the reconciler through the one input it cannot check: a state.json
// that is framed, digest-sound, decodable — and wrong. Corruption is somebody else's
// problem (framed refuses it, and the state_test table proves that); what is left is a
// record that is intact and does not describe this host's disk. It arrives by restoring a
// data directory from a backup, by cloning a machine image, or by an operator copying a
// volume directory to "recover" it.
//
// The other attacks named for this lane already have proofs, and a second copy of one is
// a second thing to keep true: a restart between sealing and the record is
// TestAdversaryASealedLayerNobodyRecordedIsDroppedFromTheHistory; two rotations with no
// publish between them is TestAdversaryTheOldestSealedLayerIsPublishedFirstWhenAPublisherArrives;
// a layer under the tip that nothing recorded is that same crash-points test. A layer file
// that nothing recorded *and* that was never a tip is unreachable rather than untested:
// the derivation reads Layers and never the directory, and qcow.Paths has no List.

// TestAdversaryARecordedPendingLayerIsNotPublishedOverTheGuestsOwnTip.
//
// Pending is the one field of state.json that names a layer to *upload*, and nothing in
// the file says what that layer is: sealed, published, or the file QEMU has open this
// second. A record naming the tip is not exotic — the tip of one boot is the sealed layer
// of the next, so any state.json that outlived the chain it described names one — and
// publishing it is a commit whose bytes a running guest is still changing under the
// reader. The object lands, its digest is whatever the file happened to be halfway
// through, and the manifest swears to it.
//
// The guard is that the derivation decides *which* layer is owed and the record only
// decides what to call it. This asserts on the publisher, because a reconciler that
// derives the right layer and then hands over the wrong one satisfies any assertion on
// state.json.
func TestAdversaryARecordedPendingLayerIsNotPublishedOverTheGuestsOwnTip(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	first := h.rotating(t, 9<<20)

	// One rotation with no object store: `first` is sealed and owed, `second` is what the
	// guest writes to now.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that rotates: %v", err)
	}
	second := h.tip(t)
	if second == first {
		t.Fatal("no rotation happened")
	}
	h.paths.sizes[first], h.paths.sizes[second] = 9<<20, 1<<20

	// The record is restored from a copy taken one rotation later: intact, this volume's,
	// and naming as owed the layer the guest is at this moment writing into.
	st := h.state(t)
	st.Pending = &qcow.PendingCommit{
		CommitID: ids.New().String(), LayerID: qcow.LayerIDOfImage(second),
		Epoch: 1, PlainBytes: 1 << 20, VirtualSize: size,
	}
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatalf("planting the record: %v", err)
	}

	// An object store appears and the Agent is restarted under the running guest.
	pub := &recordingPublisher{}
	h2 := h.restart(t, 8<<20, pub)
	for range 2 {
		h2.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(second)
		if err := h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("a cycle that should publish what is sealed: %v", err)
		}
	}

	live := qcow.LayerIDOfImage(second)
	for _, l := range pub.got {
		if l.LayerID == live {
			t.Errorf("layer %s was published because state.json said it was owed; it is the file QEMU has open"+
				" (the pointer names %q), so the commit's bytes are whatever the guest had written by then", live, second)
		}
	}
	// The control, so a green run is not a reconciler that publishes nothing at all: the
	// layer that really was sealed is published, once.
	sealed := qcow.LayerIDOfImage(first)
	n := 0
	for _, l := range pub.got {
		if l.LayerID == sealed {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the sealed layer %s was published %d times across two cycles, want once; published: %v",
			sealed, n, pub.got)
	}
	if got := h2.tip(t); got != second {
		t.Errorf("the pointer names %q, want the layer the guest is writing to, %q", got, second)
	}
}

// TestAdversaryAStateFileFromAnotherVolumeIsNotCreditedToThisOne.
//
// The digest proves the bytes are the bytes that were written and says nothing about
// where they were written. A state.json restored under the wrong volume is intact by every
// check framing can make, and every commit it names is then credited to this volume: the
// commits it claims satisfy the staleness check, so a host whose chain the published
// history has moved past goes on serving it, and its next rotation seals a layer over a
// fork.
//
// ReadState refuses it. What this asserts is what the *Manager* does with that refusal —
// the volume is refused and reported, rather than carried on with the zero State, which is
// the branch that would republish and re-download.
func TestAdversaryAStateFileFromAnotherVolumeIsNotCreditedToThisOne(t *testing.T) {
	t.Parallel()
	const other = "0198c0de-0000-7000-8000-0000000000ff"
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 0, pub)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	tip := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)

	// Another host published, and this host's chain is a fork of that history: without a
	// believable record of what *this* host published, serving the local chain is the
	// stale-chain defect.
	h.rec.head = "0198c0de-0000-7000-8000-00000000c0m2"
	body, err := qcow.MarshalState(qcow.State{
		VolumeID: other,
		Commits:  []qcow.CommitLayer{{CommitID: h.rec.head, LayerID: qcow.LayerIDOfImage(tip)}},
	})
	if err != nil {
		t.Fatalf("marshalling the foreign record: %v", err)
	}
	// Written past WriteState, which stamps the directory's volume id over the value: the
	// two can only ever disagree if something other than this binary put the file there.
	if err := h.paths.WriteAtomic(qcow.StateFile(root, vol), body); err != nil {
		t.Fatalf("planting the foreign record: %v", err)
	}

	h2 := h.restart(t, 0, pub)
	h2.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	_ = h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	v, ok := h2.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume is not reported at all")
	}
	if v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("volume %s was served on a state.json that describes volume %s, whose commit %s"+
			" this host never wrote", vol, other, h.rec.head)
	}
	if !strings.Contains(v.RefusalDetail, other) {
		t.Errorf("the refusal does not say whose record it is, so nobody can act on it: %q", v.RefusalDetail)
	}
	if len(pub.got) != 0 {
		t.Errorf("a volume refused over its own record still published %v", pub.got)
	}
	// Refused and not repaired: the layers belong to somebody's volume and this system
	// does not yet know whose.
	if got := h2.tip(t); got != tip {
		t.Errorf("the pointer now names %q, want the chain that was already here, %q", got, tip)
	}
}
