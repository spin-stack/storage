package agent_test

import (
	"bytes"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// One call, per volume, on demand, with the answer held until the volume stops being this
// host's — the alternative to carrying key material on the desired state the Agent polls.

func setKeys(f *fakeCP, volumeID, kekID string, wrapped []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys[volumeID] = &storagev1.GetVolumeKeysResponse{
		VolumeId: volumeID, DekWrapped: wrapped, KekId: kekID, DekKeyId: 1,
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

// TestVolumeKeysAreForgottenWhenTheVolumeLeaves: keeping the keys of a volume that left the
// desired state means the Agent holds the means to open a volume the fleet took from it,
// and a stale entry would let a re-attached volume be opened with the key it had before.
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
