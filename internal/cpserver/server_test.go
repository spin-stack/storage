package cpserver_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const (
	hostA    = "host-a"
	hostB    = "host-b"
	leaseTTL = 30 * time.Second
)

type fixture struct {
	md   *metasim.Store
	srv  *cpserver.Server
	term int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{md: md, term: term}
	f.srv = cpserver.New(md, func() int64 { return f.term }, leaseTTL)
	return f
}

func (f *fixture) heartbeat(t *testing.T, req *storagev1.HeartbeatRequest) (*storagev1.HeartbeatResponse, error) {
	t.Helper()
	resp, err := f.srv.Heartbeat(t.Context(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (f *fixture) createVolume(t *testing.T, v metadata.Volume) {
	t.Helper()
	if v.State == "" {
		v.State = lifecycle.VolumeActive
	}
	if v.Durability == "" {
		v.Durability = lifecycle.DurabilityRemote
	}
	if v.DEKKeyID == 0 {
		// Every real volume has one (§15.1) and the store refuses a row without it.
		// The default is here rather than in twenty literals, but a case that cares
		// about the version still sets its own.
		v.DEKKeyID = 1
	}
	if err := f.md.CreateVolume(t.Context(), f.term, v, nil); err != nil {
		t.Fatal(err)
	}
}

// TestHeartbeatWritesTheDevicePicture is the reason this RPC exists: nvme_used_bytes
// has been in the schema since Phase 07 with nothing to write it (ADR-0013 §3).
func TestHeartbeatWritesTheDevicePicture(t *testing.T) {
	f := newFixture(t)
	resp, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
		HostId:           hostA,
		AgentVersion:     "0.1.0",
		MaxFormatVersion: 2,
		Device: &storagev1.DeviceStatus{
			TotalBytes:         1 << 40,
			UsedBytes:          700 << 30,
			RemoteBacklogBytes: 4 << 30,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetLeaseTtlSeconds() != int32(leaseTTL/time.Second) {
		t.Errorf("lease_ttl_seconds = %d, want %d", resp.GetLeaseTtlSeconds(), int32(leaseTTL/time.Second))
	}
	if resp.GetState() != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", resp.GetState())
	}
	if resp.GetTerm() != f.term {
		t.Errorf("term = %d, want %d", resp.GetTerm(), f.term)
	}

	h, err := f.md.GetHost(t.Context(), hostA)
	if err != nil {
		t.Fatal(err)
	}
	if h.NVMeTotalBytes != 1<<40 || h.NVMeUsedBytes != 700<<30 {
		t.Fatalf("device numbers not stored: %+v", h)
	}
	// The backlog is the one number of the three that no other reader can recompute:
	// it is the part of `used` that no verified object covers yet (INV-13), so no
	// amount of local truncation reclaims it, and per-volume MaxRemoteGapBytes never
	// sums to it. Dropping it left the fleet unable to tell a host that is merely
	// full from one whose object store has stopped answering.
	if h.RemoteBacklogBytes != 4<<30 {
		t.Fatalf("remote backlog not stored: %+v", h)
	}
	if h.AgentVersion != "0.1.0" || h.MaxFormatVersion != 2 {
		t.Fatalf("identity not stored: %+v", h)
	}
	if _, err := f.md.GetHostLease(t.Context(), hostA); err != nil {
		t.Fatalf("the heartbeat did not renew the host lease: %v", err)
	}
}

// TestHeartbeatDoesNotUncordon: the fleet state and the capacity ledger belong to
// the Control Plane, never to a routine heartbeat (§28.1/§28.2). The Agent is told
// the state instead, so it learns it was cordoned.
func TestHeartbeatDoesNotUncordon(t *testing.T) {
	f := newFixture(t)
	if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}}); err != nil {
		t.Fatal(err)
	}
	if err := f.md.SetHostState(t.Context(), f.term, hostA, lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}
	resp, err := f.heartbeat(t, &storagev1.HeartbeatRequest{HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetState() != storagev1.HostState_HOST_STATE_CORDONED {
		t.Fatalf("state = %v, want CORDONED", resp.GetState())
	}
	h, _ := f.md.GetHost(t.Context(), hostA)
	if h.State != lifecycle.HostCordoned {
		t.Fatalf("a heartbeat un-cordoned the host: %v", h.State)
	}
}

// TestHeartbeatOfADeadHostRefusesTheLease: marking a host DEAD is the Control Plane
// asserting its writer is gone — the same assertion promotion accepts as a reason to
// skip the fencing wait. The heartbeat must not re-arm it, and the Agent must be
// able to tell the difference between "no answer" and "no lease for you".
func TestHeartbeatOfADeadHostRefusesTheLease(t *testing.T) {
	f := newFixture(t)
	if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}}); err != nil {
		t.Fatal(err)
	}
	if err := f.md.SetHostState(t.Context(), f.term, hostA, lifecycle.HostDead); err != nil {
		t.Fatal(err)
	}
	resp, err := f.heartbeat(t, &storagev1.HeartbeatRequest{HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}})
	if err != nil {
		t.Fatalf("a DEAD host's heartbeat must answer, not fail: %v", err)
	}
	if resp.GetLeaseTtlSeconds() != 0 {
		t.Errorf("lease_ttl_seconds = %d, want 0 for a DEAD host", resp.GetLeaseTtlSeconds())
	}
	if resp.GetState() != storagev1.HostState_HOST_STATE_DEAD {
		t.Errorf("state = %v, want DEAD", resp.GetState())
	}
}

func TestHeartbeatRejectsAnEmptyHostID(t *testing.T) {
	f := newFixture(t)
	_, err := f.heartbeat(t, &storagev1.HeartbeatRequest{AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}})
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", got, err)
	}
}

// TestStaleTermIsAborted: a Control Plane that lost the term must learn it is a
// zombie rather than have its writes silently drop (§7).
func TestStaleTermIsAborted(t *testing.T) {
	f := newFixture(t)
	f.term = 0 // before any election: the fail-closed value
	_, err := f.heartbeat(t, &storagev1.HeartbeatRequest{HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{}})
	if got := connect.CodeOf(err); got != connect.CodeAborted {
		t.Fatalf("code = %v, want Aborted (err=%v)", got, err)
	}
}

// TestGetDesiredStateListsThisHostsVolumes, ordered by volume id (INV-02).
func TestGetDesiredStateListsThisHostsVolumes(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: "vol-b", SizeBytes: 2 << 30, BlockSize: 4096, CurrentEpoch: 5, PrimaryHostID: hostA})
	f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 512, CurrentEpoch: 1, PrimaryHostID: hostA, Durability: lifecycle.DurabilityLocal})
	f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: "vol-z", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 1, PrimaryHostID: hostB})

	resp, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: hostA}))
	if err != nil {
		t.Fatal(err)
	}
	vols := resp.Msg.GetVolumes()
	if len(vols) != 2 {
		t.Fatalf("got %d volumes, want 2 (only this host's)", len(vols))
	}
	if vols[0].GetVolumeId() != "vol-a" || vols[1].GetVolumeId() != "vol-b" {
		t.Fatalf("not ordered by volume id: %v, %v", vols[0].GetVolumeId(), vols[1].GetVolumeId())
	}
	if vols[0].GetBlockSize() != 512 || vols[0].GetSizeBytes() != 1<<30 || vols[0].GetEpoch() != 1 {
		t.Fatalf("volume geometry not carried: %+v", vols[0])
	}
	if vols[0].GetDurability() != storagev1.Durability_DURABILITY_LOCAL {
		t.Errorf("durability = %v, want LOCAL", vols[0].GetDurability())
	}
	if vols[1].GetState() != storagev1.VolumeState_VOLUME_STATE_ACTIVE {
		t.Errorf("state = %v, want ACTIVE", vols[1].GetState())
	}
}

// TestReportVolumeState is the epoch qualification: a report is applied only if it
// comes from the volume's current writer, under the volume's current epoch.
func TestReportVolumeState(t *testing.T) {
	const vol = "vol-a"
	tests := []struct {
		name    string
		report  *storagev1.VolumeReport
		host    string
		want    storagev1.ReportOutcome
		applied bool
	}{
		{
			name:    "accepted",
			report:  &storagev1.VolumeReport{VolumeId: vol, Epoch: 4, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10},
			host:    hostA,
			want:    storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED,
			applied: true,
		},
		{
			name:   "stale epoch",
			report: &storagev1.VolumeReport{VolumeId: vol, Epoch: 3, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10},
			host:   hostA,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH,
		},
		{
			name:   "epoch from the future",
			report: &storagev1.VolumeReport{VolumeId: vol, Epoch: 5, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10},
			host:   hostA,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH,
		},
		{
			name:   "not primary",
			report: &storagev1.VolumeReport{VolumeId: vol, Epoch: 4, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10},
			host:   hostB,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY,
		},
		{
			name:   "unknown volume",
			report: &storagev1.VolumeReport{VolumeId: "vol-nope", Epoch: 4},
			host:   hostA,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME,
		},
		{
			name:   "out of order",
			report: &storagev1.VolumeReport{VolumeId: vol, Epoch: 4, LocalSequence: 1, DurableSequence: 2, PublishedSequence: 3},
			host:   hostA,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_OUT_OF_ORDER,
		},
		{
			name:   "empty volume id",
			report: &storagev1.VolumeReport{Epoch: 4},
			host:   hostA,
			want:   storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: vol, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4, PrimaryHostID: hostA})

			resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
				HostId:  tc.host,
				Volumes: []*storagev1.VolumeReport{tc.report},
			}))
			if err != nil {
				t.Fatal(err)
			}
			results := resp.Msg.GetResults()
			if len(results) != 1 {
				t.Fatalf("got %d results, want 1", len(results))
			}
			if results[0].GetOutcome() != tc.want {
				t.Fatalf("outcome = %v, want %v", results[0].GetOutcome(), tc.want)
			}
			if results[0].GetVolumeId() != tc.report.GetVolumeId() {
				t.Fatalf("result names %q, want %q", results[0].GetVolumeId(), tc.report.GetVolumeId())
			}

			v, err := f.md.GetVolume(t.Context(), vol)
			if err != nil {
				t.Fatal(err)
			}
			written := v.LocalSequence != 0 || v.DurableSequence != 0 || v.PublishedSequence != 0
			if written != tc.applied {
				t.Fatalf("watermarks written = %v, want %v (%+v)", written, tc.applied, v)
			}
		})
	}
}

// TestReportVolumeStateAnswersEveryVolume, in the order they were reported, so a
// caller can pair results with what it sent without matching on ids.
func TestReportVolumeStateAnswersEveryVolume(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 1, PrimaryHostID: hostA})
	f.createVolume(t, metadata.Volume{DEKKeyID: 1, VolumeID: "vol-b", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 2, PrimaryHostID: hostA})

	resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
		HostId: hostA,
		Volumes: []*storagev1.VolumeReport{
			{VolumeId: "vol-b", Epoch: 2, LocalSequence: 5, DurableSequence: 5, PublishedSequence: 5},
			{VolumeId: "vol-a", Epoch: 99},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	results := resp.Msg.GetResults()
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].GetVolumeId() != "vol-b" || results[0].GetOutcome() != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("first result = %+v", results[0])
	}
	if results[1].GetVolumeId() != "vol-a" || results[1].GetOutcome() != storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH {
		t.Fatalf("second result = %+v", results[1])
	}
}

func TestReportVolumeStateRejectsAnEmptyHostID(t *testing.T) {
	f := newFixture(t)
	_, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", got, err)
	}
}

func TestGetDesiredStateRejectsAnEmptyHostID(t *testing.T) {
	f := newFixture(t)
	_, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", got, err)
	}
}

// TestServedOverConnect proves the handler is wired the way a real Agent reaches
// it: the generated client, over HTTP, against the generated handler.
func TestServedOverConnect(t *testing.T) {
	f := newFixture(t)
	client, stop := serveForTest(t, f.srv)
	defer stop()

	resp, err := client.Heartbeat(context.WithoutCancel(t.Context()), connect.NewRequest(&storagev1.HeartbeatRequest{
		HostId:           hostA,
		AgentVersion:     "0.1.0",
		MaxFormatVersion: 1,
		Device:           &storagev1.DeviceStatus{TotalBytes: 10, UsedBytes: 1},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetState() != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("state = %v", resp.Msg.GetState())
	}

	if _, err := client.GetDesiredState(context.WithoutCancel(t.Context()), connect.NewRequest(&storagev1.GetDesiredStateRequest{})); err == nil {
		t.Fatal("an empty host id should not cross the wire as success")
	} else if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", connect.CodeOf(err))
	}
}
