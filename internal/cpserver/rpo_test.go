package cpserver_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/metadata"
)

// The number v6 §11 calls the product reaches the catalog.
//
// It was measured, recorded as a gauge, and put on the wire, and the Control Plane threw
// it away — so "what is the RPO of volume X" had no answer anywhere in the fleet except by
// scraping the host that happened to be serving it. This is the seam that was missing, so
// it is asserted through the RPC and read back from the store rather than from the
// handler's argument.
func TestAReportsRPOReachesTheCatalog(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		DEKKeyID: 1, VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 3, PrimaryHostID: hostA,
	})

	resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
		HostId: hostA,
		Volumes: []*storagev1.VolumeReport{{
			VolumeId: "vol-a", Epoch: 3,
			LastSuccessfulCommitAgeMs: 90_000,
			UnpublishedLocalBytes:     7 << 20,
		}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Msg.GetResults()[0].GetOutcome(); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("outcome = %v, want ACCEPTED", got)
	}

	v, err := f.md.GetVolume(t.Context(), "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case v.Progress.CommitAge != 90*time.Second:
		t.Fatalf("the RPO the host measured is %s in the catalog", v.Progress.CommitAge)
	case v.Progress.UnpublishedLocalBytes != 7<<20:
		t.Fatalf("the backlog the host measured is %d in the catalog", v.Progress.UnpublishedLocalBytes)
	case v.Progress.ReportedAt.IsZero():
		// Without it there is no way to tell a fresh measurement from one a silent host
		// left behind an hour ago, and the second reads as perfect health.
		t.Fatal("the report is not stamped with when the catalog heard it")
	}
}

// A volume nobody has reported on carries no measurement, which is not the same as a
// measurement of zero. An RPO of "0s" against a volume no host has ever spoken about is
// the one wrong answer nobody investigates.
func TestAVolumeNobodyHasReportedOnCarriesNoRPO(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		DEKKeyID: 1, VolumeID: "vol-new", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA,
	})

	v, err := f.md.GetVolume(t.Context(), "vol-new")
	if err != nil {
		t.Fatal(err)
	}
	if (v.Progress != metadata.VolumeProgress{}) {
		t.Fatalf("a volume nothing has reported on carries %+v", v.Progress)
	}
}
