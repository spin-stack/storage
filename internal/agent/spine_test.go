package agent_test

import (
	"net/http/httptest"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestTheSpineEndToEnd runs the real Agent loop against the real handler over a
// real HTTP transport, with only the clock and the store simulated. It is the first
// test in this repository where a report leaves one component and lands in the
// other's authority: everything until now proved the halves separately.
//
// The two properties it is here for:
//
//   - a heartbeat writes nvme_used_bytes, which has existed in the schema since
//     Phase 07 with nothing to write it (ADR-0013 §3);
//   - a watermark report is applied only under the volume's current epoch, and the
//     Agent learns that the other one was refused.
func TestTheSpineEndToEnd(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}

	const leaseTTL = 30 * time.Second
	httpSrv := httptest.NewServer(cpserver.Handler(cpserver.New(md, func() int64 { return term }, leaseTTL)))
	defer httpSrv.Close()

	// mine is served by this host under epoch 4; stolen was promoted away.
	for _, v := range []metadata.Volume{
		{VolumeID: "vol-mine", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4, PrimaryHostID: testHost,
			State: lifecycle.VolumeActive, Durability: lifecycle.DurabilityRemote},
		{VolumeID: "vol-stolen", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 9, PrimaryHostID: testHost,
			State: lifecycle.VolumeActive, Durability: lifecycle.DurabilityRemote},
	} {
		if err := md.CreateVolume(t.Context(), term, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	vols := agent.NewVolumeSet()
	vols.Set(agent.VolumeStatus{VolumeID: "vol-mine", Epoch: 4, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10, RemoteGapBytes: 1 << 20})
	vols.Set(agent.VolumeStatus{VolumeID: "vol-stolen", Epoch: 8, LocalSequence: 7, DurableSequence: 7, PublishedSequence: 7})

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(httpSrv.Client(), httpSrv.URL),
		Device:       fakeDevice{usage: agent.DeviceUsage{TotalBytes: 1 << 40, UsedBytes: 512 << 30}},
		Volumes:      vols,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	host, err := md.GetHost(t.Context(), testHost)
	if err != nil {
		t.Fatalf("the heartbeat did not register the host: %v", err)
	}
	if host.NVMeTotalBytes != 1<<40 || host.NVMeUsedBytes != 512<<30 {
		t.Fatalf("device numbers did not cross the wire: %+v", host)
	}
	if host.AgentVersion != testVersion || host.MaxFormatVersion != 3 {
		t.Fatalf("identity did not cross the wire: %+v", host)
	}
	if !loop.LeaseValid() {
		t.Fatal("the Agent holds no lease after a successful heartbeat")
	}
	if got := loop.HostState(); got != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("host state = %v, want ACTIVE", got)
	}

	desired := loop.Desired()
	if len(desired) != 2 || desired[0].GetVolumeId() != "vol-mine" || desired[0].GetEpoch() != 4 {
		t.Fatalf("desired state = %+v", desired)
	}

	mine, err := md.GetVolume(t.Context(), "vol-mine")
	if err != nil {
		t.Fatal(err)
	}
	if mine.LocalSequence != 30 || mine.DurableSequence != 20 || mine.PublishedSequence != 10 {
		t.Fatalf("in-epoch watermarks were not applied: %+v", mine)
	}
	stolen, err := md.GetVolume(t.Context(), "vol-stolen")
	if err != nil {
		t.Fatal(err)
	}
	if stolen.LocalSequence != 0 {
		t.Fatalf("a report under a superseded epoch was applied: %+v", stolen)
	}
	fenced := loop.Fenced()
	if len(fenced) != 1 || fenced[0] != "vol-stolen" {
		t.Fatalf("the Agent did not learn it was fenced for vol-stolen: %v", fenced)
	}
}
