package qcow_test

import (
	"crypto/rand"
	"errors"
	"slices"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
)

// rotateOnce runs the cycle that seals `tip` and returns the layer the guest moved on
// to, with the fake filesystem told the sealed file is there — the fake runner is what
// stands in for `qemu-img create`, so nothing else makes a layer exist here.
func (h *harness) rotateOnce(t *testing.T, tip string) string {
	t.Helper()
	h.paths.present[tip] = true
	h.paths.sizes[tip] = 9 << 20
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	h.runner.info = overlayJSON(size, tip)

	// The error is not asserted on: the row where the object store is down is one of the
	// cases this test is about, and the rotation happens either way.
	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	next := h.tip(t)
	if next == tip {
		t.Fatalf("nothing rotated: the tip is still %s", tip)
	}
	h.paths.present[next] = true
	return next
}

// shift returns a v7 layer id whose millisecond stamp is deltaMs away from the given
// layer's. The ids the Manager mints come from the wall clock, which a test in this tree
// may not read (INV-01), so where a planted file sits in the chain's history is said
// relative to a layer the Manager made itself.
func shift(t *testing.T, layerID string, deltaMs int64) string {
	t.Helper()
	u, err := ids.Parse(layerID)
	if err != nil {
		t.Fatalf("layer %s is not a uuid: %v", layerID, err)
	}
	var ms int64
	for _, b := range u[:6] {
		ms = ms<<8 | int64(b)
	}
	return ids.NewAt(ms+deltaMs, rand.Reader).String()
}

// layers is what is actually in this volume's layers directory.
func (h *harness) layers(t *testing.T) []string {
	t.Helper()
	names, err := h.paths.List(qcow.LayersDir(root, vol))
	if err != nil {
		t.Fatalf("listing the layers directory: %v", err)
	}
	return names
}

// TestTheSweepRemovesOnlyWhatNoChainReadsThrough is the safety half of reclaiming local
// disk, and it is the half that can lose a guest its disk.
//
// The chain here is four layers deep with a published prefix, which is the ordinary
// shape of a volume that has been committing: a qcow2 tip reads through every layer
// under it, so a published layer's file is still load-bearing and publishing it is not a
// licence to delete it. Beside the chain are the files a crash and another host's clock
// leave: an overlay above the tip that an interrupted rotation created and the guest
// never moved to, a half-finished download, a layer no record names, a downloaded layer
// whose id is later than the tip's, and the half-written image of a compaction that was
// killed. Exactly two of the eight may go.
func TestTheSweepRemovesOnlyWhatNoChainReadsThrough(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)

	l1 := h.rotating(t, 9<<20)
	l2 := h.rotateOnce(t, l1)
	l3 := h.rotateOnce(t, l2)
	// The object store stops answering, so l3 is sealed and stays owed. A pending layer
	// is nobody's garbage: its bytes exist only here.
	pub.err = errors.New("503 Service Unavailable")
	tip := h.rotateOnce(t, l3)

	if st := h.state(t); len(st.Commits) != 2 || st.Pending == nil {
		t.Fatalf("the chain this test needs is not there: %d published commits, pending=%v", len(st.Commits), st.Pending)
	}

	// What an interrupted rotation leaves: the overlay was created and the pointer may
	// even have moved, but QEMU is still on the tip below it, so nothing ever wrote to it
	// and nothing backs onto it.
	orphan := qcow.LayerImage(root, vol, shift(t, qcow.LayerIDOfImage(tip), 1))
	// A layer older than the tip that no record names — a fork this host abandoned, say.
	// It cannot be told apart from a backing file, so it stays.
	stray := qcow.LayerImage(root, vol, shift(t, qcow.LayerIDOfImage(l1), -1))
	// A download that did not finish. It is not a layer and the sweep does not touch it.
	part := orphan + ".part"
	// A compaction's flattened root that `qemu-img convert` was killed in the middle of. It
	// is a whole volume's worth of disk that nothing will ever name — a collapse that is
	// planned again converts under a new id — so it is the one non-layer file this sweep
	// does take.
	unfinished := qcow.LayerImage(root, vol, shift(t, qcow.LayerIDOfImage(tip), 2)) + ".compacting"
	// A layer this host downloaded from a commit another host published while its clock
	// ran ahead of this one's. Its id is later than the tip's and it is the base the
	// whole chain reads through, so the record is the only thing that can vouch for it.
	skewed := qcow.LayerImage(root, vol, shift(t, qcow.LayerIDOfImage(tip), 3_600_000))
	for _, path := range []string{orphan, stray, part, skewed, unfinished} {
		h.paths.present[path] = true
	}
	st := h.state(t)
	st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: ids.New().String(), LayerID: qcow.LayerIDOfImage(skewed)})
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatalf("recording the commit this host holds: %v", err)
	}

	pub.err = nil
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that publishes the sealed layer and sweeps: %v", err)
	}

	want := []string{base(l1), base(l2), base(l3), base(tip), base(stray), base(part), base(skewed)}
	got := h.layers(t)
	for _, name := range want {
		if !slices.Contains(got, name) {
			t.Errorf("%s was swept, and the chain reads through it (or it is not this sweep's to remove); the directory holds %v", name, got)
		}
	}
	if slices.Contains(got, base(unfinished)) {
		t.Errorf("the half-converted root %s is still on disk; the compaction that was killed will never name it again", unfinished)
	}
	if slices.Contains(got, base(orphan)) {
		t.Errorf("the overlay %s an interrupted rotation left is still on disk; nothing reads through it and nothing ever will", orphan)
	}
	if len(got) != len(want) {
		slices.Sort(want)
		t.Errorf("the layers directory holds %v, want exactly %v", got, want)
	}
}

func base(path string) string { return path[strings.LastIndex(path, "/")+1:] }

// TestTheSweepKeepsTheLayerActiveCurrentNames.
//
// Rotate writes `active/current` before it tells QEMU to switch — deliberately, so a crash
// between the two leaves the pointer one layer ahead rather than one behind. A rotation
// whose snapshot is refused therefore leaves an overlay the pointer names, above the tip,
// that no record vouches for: state.Layers is written from what QEMU has open, and QEMU
// never moved.
//
// While a guest is attached the next Open repairs the pointer from the live image. With
// the guest gone nobody repairs it, and the sweep must not unlink the file the pointer
// still names: `active/current` is the one path this package promises to whoever launches
// the VM, and pointing it at nothing turns a window that heals itself into a refusal.
func TestTheSweepKeepsTheLayerActiveCurrentNames(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	l1 := h.rotating(t, 9<<20)
	tip := h.rotateOnce(t, l1)

	// The overlay the refused rotation left, and the pointer moved onto it.
	orphan := qcow.LayerImage(root, vol, shift(t, qcow.LayerIDOfImage(tip), 1))
	h.paths.present[orphan] = true
	if err := h.paths.WriteAtomic(qcow.ActivePointer(root, vol), []byte(orphan)); err != nil {
		t.Fatal(err)
	}
	// The guest is gone, so nothing repairs the pointer from a live image.
	delete(h.dialer.scripts, qcow.QMPSocket(root, vol))

	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	there, err := h.paths.Exists(orphan)
	if err != nil {
		t.Fatal(err)
	}
	if !there {
		t.Fatalf("the sweep unlinked %s, and active/current names it: the next launch is handed a path with no file", orphan)
	}
}

// TestACorruptSealedLayerIsNeverPublished.
//
// A guest can corrupt its own image while it runs, and QEMU sets the qcow2 corrupt bit
// inside the file. The bit then rides through the object store intact: the sealed bytes
// hash to what the manifest says, so every integrity check on the way back passes and the
// commit looks perfect until somebody tries to open it.
//
// Both readers refuse such a chain — Open's local-chain branch and recovery's rebuild — so
// publishing one produces a commit that returned SUCCESS and that no host can restore.
// That is the sentence §32 is built on, failing on the one kill-point §29 could not
// otherwise reach.
//
// The five questions: the visible commit is the one before, the local state is a sealed
// layer that stays owed, no remote object is written, the volume is refused with a reason
// an operator can act on, and no confirmed commit is lost — this refuses before the
// publish rather than after it.
func TestACorruptSealedLayerIsNeverPublished(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20
	// Two cycles, because the fake answers every `qemu-img info` with one string and the
	// rotation's own check of the new overlay must see a clean one. The store is down for
	// the first, so the layer is sealed and stays owed.
	pub.err = errors.New("503 Service Unavailable")
	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
	if h.state(t).Pending == nil {
		t.Fatal("nothing was sealed, so this test is not standing where it means to")
	}

	// The store comes back, and what the guest left in that layer is corrupt. The count
	// is taken first because the publisher records the attempt that failed above.
	offered := len(pub.got)
	pub.err = nil
	h.runner.info = infoJSON("qcow2", size, true)
	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	if len(pub.got) != offered {
		t.Fatalf("a layer carrying the qcow2 corrupt flag was handed over as commit %s; no host could ever restore it",
			pub.got[len(pub.got)-1].CommitID)
	}
	got := h.volumes(t)[vol]
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST {
		t.Fatalf("refusal = %s, want DURABILITY_LOST: this volume cannot reach the bucket at all", got.Refusal)
	}
	if !strings.Contains(got.RefusalDetail, "corrupt") {
		t.Fatalf("the refusal does not say what is wrong: %q", got.RefusalDetail)
	}
}
