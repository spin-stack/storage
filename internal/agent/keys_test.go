package agent_test

import (
	"bytes"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// An Agent cannot open a volume without its key material, and the desired state —
// which it polls every few seconds, for every volume on the host — is the wrong
// place to carry it. These tests pin the alternative: one call, per volume, on
// demand, and the answer held until the volume stops being this host's.

func setKeys(f *fakeCP, volumeID, kekID string, wrapped []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[volumeID] = &storagev1.GetVolumeKeysResponse{
		VolumeId: volumeID, DekWrapped: wrapped, KekId: kekID,
	}
}

// TestVolumeKeysAreFetchedOnceAndHeld: key material is immutable for as long as the
// volume is this host's, so fetching it per cycle would be a wrapped DEK crossing
// the wire every few seconds for every volume on the host, to no purpose. The Agent
// asks once.
func TestVolumeKeysAreFetchedOnceAndHeld(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	setKeys(h.cp, "vol-a", "kek-1", []byte("wrapped-a"))

	first, err := h.loop.VolumeKeys(t.Context(), "vol-a")
	if err != nil {
		t.Fatalf("VolumeKeys: %v", err)
	}
	if first.KEKID != "kek-1" || !bytes.Equal(first.DEKWrapped, []byte("wrapped-a")) {
		t.Fatalf("key material did not arrive: %+v", first)
	}
	if _, err := h.loop.VolumeKeys(t.Context(), "vol-a"); err != nil {
		t.Fatalf("second VolumeKeys: %v", err)
	}
	if n := h.cp.keyCallsFor("vol-a"); n != 1 {
		t.Fatalf("the Agent asked for the same keys %d times, want 1", n)
	}
}

// TestVolumeKeysAreForgottenWhenTheVolumeLeaves: a volume that disappears from the
// desired state is a volume this host must stop serving — promoted away, detached,
// or fenced. Keeping its key material would mean the Agent holds the means to open a
// volume the fleet has taken from it, and a stale entry is also what would let a
// re-attached volume be opened with the key it had before.
func TestVolumeKeysAreForgottenWhenTheVolumeLeaves(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	setKeys(h.cp, "vol-a", "kek-1", []byte("wrapped-a"))
	h.cp.setDesired([]*storagev1.DesiredVolume{{VolumeId: "vol-a", Epoch: 1}})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.loop.VolumeKeys(t.Context(), "vol-a"); err != nil {
		t.Fatal(err)
	}

	// The fleet takes the volume away.
	h.cp.setDesired(nil)
	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.loop.VolumeKeys(t.Context(), "vol-a"); err != nil {
		t.Fatal(err)
	}
	if n := h.cp.keyCallsFor("vol-a"); n != 2 {
		t.Fatalf("key calls = %d, want 2: the Agent kept the keys of a volume it lost", n)
	}
}

// TestVolumeKeysFailureIsReported: no key material, no volume. The error must reach
// the caller rather than be answered with a zero-valued key.
func TestVolumeKeysFailureIsReported(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	if _, err := h.loop.VolumeKeys(t.Context(), "vol-unknown"); err == nil {
		t.Fatal("VolumeKeys invented key material for a volume the Control Plane does not know")
	}
	if _, err := h.loop.VolumeKeys(t.Context(), ""); err == nil {
		t.Fatal("VolumeKeys accepted an empty volume id")
	}
}
