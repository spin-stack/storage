package agent_test

import (
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// TestTheReportRecordsWhatItCarries. §21's four per-volume numbers reach an operator
// through the wire *and* through a scrape, and neither one alone is enough: the Control
// Plane sees a fleet's worth of them one report at a time, while an alert fires off a
// series. Two of the four reached the wire and no metric, and two reached only a slog
// line on the host that computed them.
//
// The values are the report's own, so a host that reports one thing and exports another
// cannot pass.
func TestTheReportRecordsWhatItCarries(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30})
	h.vols.Set(agent.VolumeStatus{
		VolumeID:              "vol-a",
		Epoch:                 3,
		LastCommitAge:         90 * time.Second,
		UnpublishedLocalBytes: 7 << 20,
		ChainDepth:            5,
		LocalDiskBytes:        11 << 20,
	})
	h.vols.Set(agent.VolumeStatus{
		VolumeID:              "vol-b",
		Epoch:                 1,
		LastCommitAge:         2 * time.Second,
		UnpublishedLocalBytes: 1 << 20,
		ChainDepth:            2,
		LocalDiskBytes:        3 << 20,
	})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// On the wire: chain_depth and local_disk_bytes were computed on the host and never
	// left it, so the Control Plane could not see a chain getting deep at all.
	for _, v := range h.cp.lastReport(t).GetVolumes() {
		want := map[string][2]int64{
			"vol-a": {5, 11 << 20},
			"vol-b": {2, 3 << 20},
		}[v.GetVolumeId()]
		if int64(v.GetChainDepth()) != want[0] || v.GetLocalDiskBytes() != want[1] {
			t.Errorf("%s reports chain_depth=%d local_disk_bytes=%d, want %d and %d",
				v.GetVolumeId(), v.GetChainDepth(), v.GetLocalDiskBytes(), want[0], want[1])
		}
	}

	// And in the scrape, one series per volume. `local_chain_depth` and not `chain_depth`:
	// that name is the Control Plane's, for the catalog's lineage depth under the same
	// `volume` label, and one name over two different numbers is a query nobody can read.
	for name, want := range map[string]map[string]float64{
		"last_successful_commit_age_seconds": {`{volume="vol-a"}`: 90, `{volume="vol-b"}`: 2},
		"unpublished_local_bytes":            {`{volume="vol-a"}`: 7 << 20, `{volume="vol-b"}`: 1 << 20},
		"local_chain_depth":                  {`{volume="vol-a"}`: 5, `{volume="vol-b"}`: 2},
		"local_disk_bytes":                   {`{volume="vol-a"}`: 11 << 20, `{volume="vol-b"}`: 3 << 20},
	} {
		got, err := h.prov.GaugeSeries(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		for labels, value := range want {
			if got[labels] != value {
				t.Errorf("%s%s = %v, want %v (collected: %v)", name, labels, got[labels], value, got)
			}
		}
	}
}

// TestTheNumbersAreRecordedEvenWhenTheControlPlaneDoesNot. A host that cannot reach the
// Control Plane is exactly the host whose commit age is growing, and the scrape is the
// only channel left: recording after the call would go silent at the moment the numbers
// start mattering.
func TestTheNumbersAreRecordedEvenWhenTheControlPlaneDoesNot(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30})
	h.vols.Set(agent.VolumeStatus{
		VolumeID:              "vol-a",
		Epoch:                 1,
		LastCommitAge:         30 * time.Minute,
		UnpublishedLocalBytes: 9 << 20,
		ChainDepth:            5,
		LocalDiskBytes:        11 << 20,
	})
	h.cp.setReportErr(errUnreachable)

	if err := h.loop.Reconcile(t.Context()); err == nil {
		t.Fatal("Reconcile should have failed: the report never landed")
	}

	for name, want := range map[string]float64{
		"last_successful_commit_age_seconds": 1800,
		"unpublished_local_bytes":            9 << 20,
		"local_chain_depth":                  5,
		"local_disk_bytes":                   11 << 20,
	} {
		got, err := h.prov.GaugeSeries(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		if got[`{volume="vol-a"}`] != want {
			t.Errorf("%s = %v with the Control Plane unreachable, want %v (collected: %v)", name, got[`{volume="vol-a"}`], want, got)
		}
	}
}

// TestARefusedVolumeStillHasAnRPOSeries. A host that stopped being a volume's writer keeps
// reporting it, with zeros for the four numbers, and the scrape has to say the same thing:
// a series that stops arriving reads as "no data" in every alerting system, which is the
// one answer an operator must not get about the volume whose host just fenced itself.
// Zero is a statement, absence is not — and the plausible edit here is the one that skips
// the volumes this host is refusing.
func TestARefusedVolumeStillHasAnRPOSeries(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30})
	h.vols.Set(agent.VolumeStatus{
		VolumeID:      "vol-fenced",
		Epoch:         4,
		Refusal:       storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST,
		RefusalDetail: "the lease lapsed",
	})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	for _, name := range []string{
		"last_successful_commit_age_seconds", "unpublished_local_bytes",
		"local_chain_depth", "local_disk_bytes",
	} {
		got, err := h.prov.GaugeSeries(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		value, ok := got[`{volume="vol-fenced"}`]
		if !ok {
			t.Errorf("%s has no series for the volume this host refuses (collected: %v)", name, got)
			continue
		}
		if value != 0 {
			t.Errorf("%s for the refused volume = %v, want 0: this host is not serving it", name, value)
		}
	}
}
