package agent_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestDiskUsageAsksTheDevice is ADR-0013 §3's first sentence, at the one place the
// number enters this process.
//
// The Agent's device figure has to be the *device's* answer — a statfs in production —
// and not a sum of the files this Agent knows about. A sum says nothing about the space
// another tenant of the same filesystem occupies, and nothing this Agent can do will
// ever free it, so a threshold evaluated on the sum fires after the device is already
// full: the one moment it needed to have fired earlier.
func TestDiskUsageAsksTheDevice(t *testing.T) {
	d := sim.NewDisk()
	const total = 1 << 30
	d.SetDeviceBudget(total)

	got, err := agent.NewDiskUsage(d).Usage(t.Context())
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if got.TotalBytes != total {
		t.Fatalf("total = %d, want the device's %d", got.TotalBytes, total)
	}
}

// TestDiskUsageRefusesToSmoothAFailureIntoAZero. An unreadable device that reports
// itself empty is the worst answer available here, because every ADR-0013 threshold
// downstream reads a zero as headroom — so the host would cordon nothing, take every
// placement offered, and find out at ENOSPC.
func TestDiskUsageRefusesToSmoothAFailureIntoAZero(t *testing.T) {
	// A Disk with no budget declared is a device that will not say how big it is.
	if _, err := agent.NewDiskUsage(sim.NewDisk()).Usage(t.Context()); err == nil {
		t.Fatal("a device that cannot be measured reported a usage")
	}
}

// TestVolumeSetIsTheSetItWasTold. VolumeSet is what the Loop reports from while there
// is no volume manager behind it, so what it holds is literally what the fleet is told
// this host is serving.
//
// The ordering is not cosmetic: reports are compared across runs and the DST harness
// requires the same seed to produce the same trace (INV-02), so a set that answered in
// map order would make every such comparison a coin flip.
func TestVolumeSetIsTheSetItWasTold(t *testing.T) {
	s := agent.NewVolumeSet()
	for _, id := range []string{"vol-c", "vol-a", "vol-b"} {
		s.Set(agent.VolumeStatus{VolumeID: id, Epoch: 1})
	}
	s.Set(agent.VolumeStatus{VolumeID: "vol-a", Epoch: 2}) // replaces rather than duplicates
	s.Remove("vol-b")

	got, err := s.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].VolumeID != "vol-a" || got[1].VolumeID != "vol-c" {
		t.Fatalf("Volumes() = %+v, want vol-a then vol-c", got)
	}
	if got[0].Epoch != 2 {
		t.Fatalf("vol-a came back at epoch %d; Set must replace, not accumulate", got[0].Epoch)
	}
}
