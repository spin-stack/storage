package qcow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// catalogHost is this host's fleet identity: a real v7 id, because Config.Validate
// refuses anything else and a rejected Agent proves nothing about an outage.
const catalogHost = "0198c0de-0000-7000-8000-0000000000a1"

// pgDown is the Control Plane with its database gone: every call comes back as the
// Connect error cpserver.rpcError produces for a store that will not answer. Nothing here
// is a refusal — the fleet has said nothing about this host or its volumes, and cannot.
type pgDown struct {
	down    bool
	desired []*storagev1.DesiredVolume
}

var _ storagev1connect.ControlPlaneServiceClient = (*pgDown)(nil)

func (c *pgDown) err() error {
	if !c.down {
		return nil
	}
	return connect.NewError(connect.CodeUnavailable,
		errors.New(`cpserver: upserting host: dial tcp 10.0.0.5:5432: connect: connection refused`))
}

func (c *pgDown) Heartbeat(_ context.Context, _ *connect.Request[storagev1.HeartbeatRequest]) (*connect.Response[storagev1.HeartbeatResponse], error) {
	if err := c.err(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&storagev1.HeartbeatResponse{
		LeaseTtlSeconds: 30, State: storagev1.HostState_HOST_STATE_ACTIVE, Term: 7,
	}), nil
}

func (c *pgDown) GetDesiredState(_ context.Context, _ *connect.Request[storagev1.GetDesiredStateRequest]) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	if err := c.err(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{Volumes: c.desired}), nil
}

func (c *pgDown) ReportVolumeState(_ context.Context, req *connect.Request[storagev1.ReportVolumeStateRequest]) (*connect.Response[storagev1.ReportVolumeStateResponse], error) {
	if err := c.err(); err != nil {
		return nil, err
	}
	results := make([]*storagev1.VolumeReportResult, 0, len(req.Msg.GetVolumes()))
	for _, v := range req.Msg.GetVolumes() {
		results = append(results, &storagev1.VolumeReportResult{
			VolumeId: v.GetVolumeId(), Outcome: storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED,
		})
	}
	return connect.NewResponse(&storagev1.ReportVolumeStateResponse{Results: results}), nil
}

func (c *pgDown) GetVolumeKeys(_ context.Context, _ *connect.Request[storagev1.GetVolumeKeysRequest]) (*connect.Response[storagev1.GetVolumeKeysResponse], error) {
	if err := c.err(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&storagev1.GetVolumeKeysResponse{}), nil
}

// steadyDevice is a device with room: the outage under test is the catalog's, and a
// device answer that varied would put a second reason in the picture.
type steadyDevice struct{}

func (steadyDevice) Usage(context.Context) (disk.Usage, error) {
	return disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}, nil
}

// TestAdversaryTheCatalogGoesAwayAndTheGuestKeepsItsDisk is v6 §15's PostgreSQL row
// driven across the seam it actually lives at: the real Agent loop over the real
// VolumeManager, with the Control Plane answering nothing because its database is gone.
// The rule is that local I/O continues and that no attach, ownership change or epoch bump
// happens — and the second half is not a separate mechanism, it is the same one: the only
// thing that can move a volume or its epoch is an answer from the Control Plane, so an
// outage must leave this host serving exactly what it was serving, at the epoch it held.
//
// The half that needs a test is the one that has a way to go wrong. A lapsed lease used
// to be taken as proof that somebody else had the volume, and stopping the guest on it
// turns a management-plane outage — the failure this system is designed to ride out —
// into a stopped VM per volume on the host. Nothing about the catalog being unreachable
// is evidence that another host was granted anything; the catalog is where a grant would
// have been written.
//
// The five questions v6 §22 asks:
//
//   - which commit is visible: the same one as before the outage. No commit is made and
//     none is lost — the object store is a separate path and is not part of this failure.
//   - what local state is left: the volume served at the epoch it was granted, the same
//     tip under the same pointer, and a lease that has lapsed and is left lapsed.
//   - what remote objects are left: unchanged. Nothing is published by a cycle that never
//     got past the heartbeat.
//   - does it recover by itself: yes — the loop backs off and retries, and the first
//     answered heartbeat re-arms the lease with nothing to repair.
//   - could a confirmed commit be lost: no. Nothing in this path writes a layer, a
//     manifest or HEAD.
func TestAdversaryTheCatalogGoesAwayAndTheGuestKeepsItsDisk(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	cp := &pgDown{desired: []*storagev1.DesiredVolume{active(vol, 4)}}
	loop, err := agent.New(agent.Config{
		HostID: catalogHost, AgentVersion: "test", MaxFormatVersion: 3,
		HeartbeatInterval: time.Second, RetryBackoff: 100 * time.Millisecond, LeaseTTL: 30 * time.Second,
	}, agent.Deps{Clock: h.clk, ControlPlane: cp, Device: steadyDevice{}, Volumes: h.m})
	if err != nil {
		t.Fatalf("building the Agent: %v", err)
	}

	// Two healthy cycles, with the catalog answering: the first prepares the chain, and
	// the guest attaches to it before the second.
	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("the first cycle, with the catalog up: %v", err)
	}
	tip := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("the cycle with a guest attached: %v", err)
	}
	if !loop.LeaseValid() {
		t.Fatal("the Agent holds no lease after two answered heartbeats")
	}
	h.dialer.reset()

	// PostgreSQL goes away, and stays away for longer than the lease.
	cp.down = true
	h.clk.Advance(90 * time.Second)
	for i := range 3 {
		if err := loop.Reconcile(t.Context()); err == nil {
			t.Fatalf("cycle %d reported success with no Control Plane to answer it", i+1)
		}
		h.clk.Advance(time.Second)
	}

	// The outage is real: the lease this host holds has lapsed, which is the input the
	// old rule acted on.
	if loop.LeaseValid() {
		t.Fatal("the lease is still valid, so this test is not about an expired one")
	}

	// What the guest observes. Its disk is the same file, and QEMU was never told to stop
	// — the one message that turns a database outage into a stopped VM.
	if got := h.tip(t); got != tip {
		t.Errorf("the pointer moved to %q while the catalog was unreachable; the guest was writing to %q", got, tip)
	}
	if sent := h.dialer.sent(); strings.Contains(sent, `"execute":"stop"`) {
		t.Errorf("the guest was stopped because the Control Plane could not be reached, and nothing said its volume had been granted elsewhere: %s", sent)
	}

	// And what the fleet would observe when it comes back: this host still serving the
	// volume, at the epoch it was granted, with nothing to explain.
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatalf("volume %s stopped being served during the outage", vol)
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("the volume is refused after the outage: %v %q", v.Refusal, v.RefusalDetail)
	}
	if v.Epoch != 4 {
		t.Errorf("the volume is served at epoch %d, want the 4 it was granted: no epoch can be bumped while the catalog that grants them is down", v.Epoch)
	}
}
