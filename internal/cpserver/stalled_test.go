package cpserver_test

import (
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// v6 §11's other half: a host that cannot get its layers into the object store stops
// taking new volumes.
//
// The failure it exists for is silent by design. When the object store is unreachable the
// volume goes on being served and the guest goes on writing — §15 promises exactly that —
// so the host reports a healthy device, a healthy lease and volumes in ACTIVE, and the only
// thing moving is a backlog on a row nobody is reading. The fleet's placement then keeps
// sending it *more* volumes, each of which inherits a host that cannot publish, until the
// filesystem fills and QEMU hands the guests ENOSPC. §11 names the reaction: alarm long
// before that, and stop placing here.
//
// Cordoned and not fenced, and not refused: nothing about the volumes on this host is
// wrong, and taking them away over an object store that may come back in a minute would
// cost more than it saves. The cordon only stops *new* placements, and it clears itself.
func TestAHostThatCannotPublishStopsTakingNewVolumes(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		DEKKeyID: 1, VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA,
	})
	report := func(stalled bool) {
		t.Helper()
		if _, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
			HostId: hostA,
			Volumes: []*storagev1.VolumeReport{{
				VolumeId: "vol-a", Epoch: 1, PublishStalled: stalled,
				UnpublishedLocalBytes: 512 << 20,
			}},
		})); err != nil {
			t.Fatal(err)
		}
	}
	heartbeat := func() storagev1.HostState {
		t.Helper()
		resp, err := f.srv.Heartbeat(t.Context(), connect.NewRequest(&storagev1.HeartbeatRequest{
			HostId: hostA,
			Device: &storagev1.DeviceStatus{TotalBytes: 1 << 40, UsedBytes: 1 << 30},
		}))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Msg.GetState()
	}

	if got := heartbeat(); got != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("a healthy host is %v", got)
	}

	report(true)
	if got := heartbeat(); got != storagev1.HostState_HOST_STATE_CORDONED {
		t.Fatalf("a host that cannot publish is %v, want CORDONED", got)
	}
	h, err := f.md.GetHost(t.Context(), hostA)
	if err != nil {
		t.Fatal(err)
	}
	// The reason matters as much as the state: an operator reading CORDONED with no reason
	// cannot tell this from a full disk or from a colleague's decision, and the reason is
	// also what stops the automatic loop clearing a cordon a human placed.
	if h.CordonReason != lifecycle.CordonStalledPublish {
		t.Fatalf("cordon reason = %q, want %q", h.CordonReason, lifecycle.CordonStalledPublish)
	}

	// And the object store comes back. Nothing sweeps and nobody intervenes: the next
	// report simply carries no stall, which is the whole of the clearing mechanism —
	// the same shape the refusal beside it has.
	report(false)
	if got := heartbeat(); got != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("a host whose object store came back is %v, want ACTIVE", got)
	}
}

// An operator's cordon outranks it, in both directions. The automatic loop must never
// clear a decision it cannot see the reason for (ADR-0013 §5), and it must not overwrite
// one either — a host a human cordoned for maintenance that then stalls would come back
// ACTIVE the moment its store recovered.
func TestAStalledPublishDoesNotTouchAnOperatorsCordon(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		DEKKeyID: 1, VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA,
	})
	if _, err := f.srv.Heartbeat(t.Context(), connect.NewRequest(&storagev1.HeartbeatRequest{
		HostId: hostA, Device: &storagev1.DeviceStatus{TotalBytes: 1 << 40, UsedBytes: 1 << 30},
	})); err != nil {
		t.Fatal(err)
	}
	if err := f.md.SetHostState(t.Context(), f.term, hostA, lifecycle.HostCordoned, lifecycle.CordonOperator); err != nil {
		t.Fatal(err)
	}

	for _, stalled := range []bool{true, false} {
		if _, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
			HostId:  hostA,
			Volumes: []*storagev1.VolumeReport{{VolumeId: "vol-a", Epoch: 1, PublishStalled: stalled}},
		})); err != nil {
			t.Fatal(err)
		}
		if _, err := f.srv.Heartbeat(t.Context(), connect.NewRequest(&storagev1.HeartbeatRequest{
			HostId: hostA, Device: &storagev1.DeviceStatus{TotalBytes: 1 << 40, UsedBytes: 1 << 30},
		})); err != nil {
			t.Fatal(err)
		}
		h, err := f.md.GetHost(t.Context(), hostA)
		if err != nil {
			t.Fatal(err)
		}
		if h.CordonReason != lifecycle.CordonOperator {
			t.Fatalf("with stalled=%v the operator's cordon reads %q", stalled, h.CordonReason)
		}
	}
}
