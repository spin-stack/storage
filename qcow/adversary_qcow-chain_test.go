package qcow_test

import (
	"path/filepath"
	"strings"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/qcow"
)

// TestAdversaryTheOldestSealedLayerIsPublishedFirstWhenAPublisherArrives attacks the one
// slot that holds a sealed layer while there is nobody to publish it.
//
// A host with no Publisher is a supported configuration — Deps.Publisher's own comment
// says so, and maybeRotate's only guard is `v.pending != nil`, which such a host never
// sets. So it rotates every time the tip crosses the threshold, and every rotation calls
// recordSealed, which *overwrites* the single Pending slot. After two rotations the disk
// holds two complete sealed layers and state.json names only the newer one.
//
// Then the operator configures the object store — the scenario recordSealed's own comment
// is written about. adopt reads Pending first and takes it as the answer, so the *newer*
// layer is published as the volume's first commit. It is an overlay over the older one,
// so the commit's bytes do not reconstruct the volume; and once it lands, recordCommit
// trims Layers at it, which puts the older layer permanently below a published floor
// where SealedBelow will never look again.
//
// The derivation exists to catch exactly this ("found a sealed layer nothing had
// recorded"); it is simply not consulted while a Pending record is present, and the
// record is not the oldest owed layer, only the last one written.
func TestAdversaryTheOldestSealedLayerIsPublishedFirstWhenAPublisherArrives(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	first := h.rotating(t, 9<<20)

	// Two rotations, with no object store anywhere. Each one seals a complete layer.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first rotation: %v", err)
	}
	second := h.tip(t)
	if second == first {
		t.Fatal("the first rotation did not happen")
	}
	h.paths.sizes[second] = 9 << 20
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(second)
	h.runner.info = overlayJSON(size, second)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the second rotation: %v", err)
	}
	third := h.tip(t)
	if third == second {
		t.Fatal("the second rotation did not happen")
	}
	h.paths.sizes[first], h.paths.sizes[second] = 9<<20, 9<<20

	// The object store is configured and the Agent is restarted. Nothing rotates now;
	// the only thing left to do is publish what is already sealed.
	pub := &recordingPublisher{}
	h2 := h.restart(t, 0, pub)
	for range 4 {
		h2.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(third)
		if err := h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("a cycle that should publish: %v", err)
		}
	}

	if len(pub.got) == 0 {
		t.Fatal("nothing was published at all")
	}
	// Oldest first: a commit's parent has to be in the history before it lands, and a
	// layer is an overlay over the one under it. Publishing the newer one first makes it
	// a root commit whose qcow2 header still names a backing file — every future restore
	// refuses the whole volume on it (recovery.repoint), and the older layer is orphaned.
	if got, want := pub.got[0].LayerID, qcow.LayerIDOfImage(first); got != want {
		t.Errorf("the first commit publishes layer %s (the file %s); the oldest owed layer is %s",
			got, pub.got[0].Path, want)
	}
	published := map[string]bool{}
	for _, l := range pub.got {
		published[l.LayerID] = true
	}
	for _, want := range []string{first, second} {
		if !published[qcow.LayerIDOfImage(want)] {
			t.Errorf("the sealed layer %s was never published: it is complete, it is under the tip, and no commit covers it", want)
		}
	}
}

// TestAdversaryThePointerCannotNameAnotherVolumesLayer attacks readPointer's one guard.
//
// `active/current` is the single local file with no identity of its own. Layers live in
// one directory for the whole host, so the path can only say "a layer of this machine",
// and which volume it belongs to is a question for the record — which is why a layer is
// written into the record before the pointer names it.
//
// This aims at both halves at once: a pointer that walks back out of the layers directory
// and returns to it satisfies IsAbs and, after cleaning, the prefix — and lands on a layer
// this volume has no claim to. That is the thing the guard is written against: another
// tenant's disk served under this volume's name, and every layer this host then seals over
// it is theirs too.
func TestAdversaryThePointerCannotNameAnotherVolumesLayer(t *testing.T) {
	t.Parallel()
	victim := qcow.LayerImage(root, baseID)
	escape := qcow.LayersDir(root) + "/../" + filepath.Base(qcow.LayersDir(root)) + "/" + baseID + ".qcow2"
	if filepath.Clean(escape) != victim {
		t.Fatalf("the crafted pointer resolves to %q, not to %q", filepath.Clean(escape), victim)
	}

	p := newPaths(victim, escape)
	p.present[qcow.ActivePointer(root, vol)] = true
	p.files[qcow.ActivePointer(root, vol)] = escape

	chain, err := qcow.Open(t.Context(), &fakeRunner{info: infoJSON("qcow2", size, false)},
		p, "/qemu-img", req(nil))
	if err == nil {
		t.Fatalf("volume %s was opened on %q, which nothing about it accounts for", vol, chain.Active)
	}
	if !strings.Contains(err.Error(), "accounts for it") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}
