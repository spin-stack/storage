package cpserver_test

import (
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// snapshotWorld is a volume on hostA with one snapshot requested against it. The ids are
// real UUIDs because the manifest key is derived from them (INV-22), which the rest of
// this package's fixtures do not need.
type snapshotWorld struct {
	*fixture
	vol, snap string
}

func newSnapshotWorld(t *testing.T) snapshotWorld {
	t.Helper()
	w := snapshotWorld{fixture: newFixture(t), vol: ids.New().String(), snap: ids.New().String()}
	w.createVolume(t, metadata.Volume{
		VolumeID: w.vol, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 1, PrimaryHostID: hostA,
	})
	if err := w.md.CreateSnapshot(t.Context(), w.term, metadata.Snapshot{
		SnapshotID: w.snap, VolumeID: w.vol, Epoch: 1,
		State: lifecycle.SnapshotCreating, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}
	return w
}

func (w snapshotWorld) desired(t *testing.T, host string) []*storagev1.DesiredVolume {
	t.Helper()
	resp, err := w.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: host}))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg.GetVolumes()
}

func (w snapshotWorld) report(t *testing.T, host string, r *storagev1.VolumeReport) storagev1.ReportOutcome {
	t.Helper()
	resp, err := w.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
		HostId: host, Volumes: []*storagev1.VolumeReport{r},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg.GetResults()[0].GetOutcome()
}

// A snapshot request reaches the Agent as desired state and nothing else: there is no
// call to the host, so this field is the entire trigger (§19, ADR-0021).
func TestDesiredStateCarriesAPendingSnapshot(t *testing.T) {
	w := newSnapshotWorld(t)

	vols := w.desired(t, hostA)
	if len(vols) != 1 || vols[0].GetPendingSnapshotId() != w.snap {
		t.Fatalf("desired state does not carry the request: %+v", vols)
	}
	// It follows the volume's primary. A host that does not serve it cannot freeze it,
	// and telling it to try would produce a snapshot of a volume it has never opened.
	if vols := w.desired(t, hostB); len(vols) != 0 {
		t.Fatalf("another host was asked to snapshot: %+v", vols)
	}
}

// The loop closes: once the host reports the snapshot, the request stops being sent.
// Without this the Agent is asked forever and the catalog never learns the sequence.
func TestAReportedSnapshotStopsBeingAskedFor(t *testing.T) {
	w := newSnapshotWorld(t)
	snapCommit := ids.New().String()

	if got := w.report(t, hostA, &storagev1.VolumeReport{
		VolumeId: w.vol, Epoch: 1, SnapshotId: w.snap, SnapshotCommitId: snapCommit,
	}); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("outcome = %v", got)
	}

	snap, err := w.md.GetSnapshot(t.Context(), w.snap)
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != lifecycle.SnapshotPublished || snap.CommitID != snapCommit || snap.SourceHostID != hostA {
		t.Fatalf("snapshot = %+v, want PUBLISHED at commit %s on %s", snap, snapCommit, hostA)
	}
	// There is no manifest key column any more: it had become a string the Agent sent,
	// which is exactly the property the original computation existed to hold. The key is
	// derived from the commit id (commit.ManifestKey), so nothing can disagree about where
	// a snapshot lives.
	if vols := w.desired(t, hostA); vols[0].GetPendingSnapshotId() != "" {
		t.Fatalf("a published snapshot is still being asked for: %q", vols[0].GetPendingSnapshotId())
	}
}

// A host the fleet has moved past froze a view of a volume it no longer writes. Its
// sequence must not become the snapshot's, and the row must stay CREATING so the volume's
// current writer still takes it.
func TestAStaleWritersSnapshotReportIsNotRecorded(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		epoch   int64
		outcome storagev1.ReportOutcome
	}{
		{"stale epoch", hostA, 0, storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH},
		{"not the primary", hostB, 1, storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newSnapshotWorld(t)
			if got := w.report(t, tc.host, &storagev1.VolumeReport{
				VolumeId: w.vol, Epoch: tc.epoch, SnapshotId: w.snap, SnapshotCommitId: ids.New().String(),
			}); got != tc.outcome {
				t.Fatalf("outcome = %v, want %v", got, tc.outcome)
			}
			snap, err := w.md.GetSnapshot(t.Context(), w.snap)
			if err != nil {
				t.Fatal(err)
			}
			if snap.State != lifecycle.SnapshotCreating || snap.CommitID != "" {
				t.Fatalf("a refused report was recorded: %+v", snap)
			}
		})
	}
}

// A snapshot that fails on its host is FAILED, not left CREATING. The alternative is a
// row nothing ever collects and a request the Agent is asked to retry forever.
func TestAFailedSnapshotIsRecordedAsFailed(t *testing.T) {
	w := newSnapshotWorld(t)

	if got := w.report(t, hostA, &storagev1.VolumeReport{
		VolumeId: w.vol, Epoch: 1, SnapshotId: w.snap, SnapshotError: "the store refused the manifest",
	}); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("outcome = %v", got)
	}
	snap, err := w.md.GetSnapshot(t.Context(), w.snap)
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != lifecycle.SnapshotFailed {
		t.Fatalf("state = %q, want FAILED", snap.State)
	}
	if vols := w.desired(t, hostA); vols[0].GetPendingSnapshotId() != "" {
		t.Fatalf("a failed snapshot is still being asked for: %q", vols[0].GetPendingSnapshotId())
	}
}
