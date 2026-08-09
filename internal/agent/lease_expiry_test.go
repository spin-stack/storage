package agent_test

import (
	"context"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// leaseHarness is a real Loop driving a real VolumeManager, both on one simulated
// clock, against a Control Plane that can be made to stop answering.
//
// The two halves have to share the clock: the lease expires on the Loop's clock and
// the socket is closed by the manager's teardown, and a test that advanced only one of
// them would be asserting about a host that cannot exist.
type leaseHarness struct {
	clk  *sim.Clock
	cp   *fakeCP
	mgr  *agent.VolumeManager
	loop *agent.Loop
	lf   *listenerFactory
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	lf := newListenerFactory()

	mgr, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
		Budget:    testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   clk,
		Disk:    sim.NewDisk(),
		Listen:  lf.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Close(context.Background()) }) //nolint:usetesting // a cancelled context abandons the publish

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}},
		Volumes:      mgr,
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	return &leaseHarness{clk: clk, cp: cp, mgr: mgr, loop: loop, lf: lf}
}

// served is the set of volume ids the manager is serving right now.
func (h *leaseHarness) served(t *testing.T) []string {
	t.Helper()
	vols, err := h.mgr.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	out := make([]string, 0, len(vols))
	for _, v := range vols {
		out = append(out, v.VolumeID)
	}
	return out
}

func socketOf(volumeID string) string { return "/run/spin/" + volumeID + ".sock" }

// TestAnExpiredLeaseTakesTheSocketDown is the partitioned-Agent blocker.
//
// An Agent that cannot reach the Control Plane keeps running, and until this existed it
// kept *serving*: the socket stayed bound and the device stayed answering for as long as
// the process lived. The fleet, meanwhile, declared the host dead after one lease TTL and
// an operator moved the volume elsewhere — so two guests wrote one volume and both were
// told their flushes were durable.
//
// The assertion is on the listener, not on a flag: what fences a second writer here is
// the socket being gone, and a lease field that says "expired" next to a socket that is
// still bound is the bug this test exists to catch.
func TestAnExpiredLeaseTakesTheSocketDown(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := desiredVolume(t, 1)
	h.cp.setDesired([]*storagev1.DesiredVolume{vol})
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	ln := h.lf.listenerFor(socketOf(vol.GetVolumeId()))
	if ln == nil {
		t.Fatalf("nothing bound %s; the volume never started", socketOf(vol.GetVolumeId()))
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("served volumes = %v, want the one the Control Plane asked for", got)
	}

	// The partition. The Control Plane stops answering; the process keeps running and
	// keeps its cadence, which is exactly what makes this invisible from the outside.
	h.cp.setErr(errUnreachable)

	// Still inside the TTL: giving the volume up here would fence a healthy host on a
	// blip, which is the failure mode this must not have.
	h.clk.Advance(29 * time.Second)
	if err := h.loop.Reconcile(ctx); err == nil {
		t.Fatal("a cycle against an unreachable Control Plane reported success")
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("the volume was given up while the lease was still valid: served = %v", got)
	}
	if n := ln.closeCount(); n != 0 {
		t.Fatalf("the socket was closed %d times while the lease was still valid", n)
	}

	// Past the TTL. The Control Plane's own fencing deadline is computed from the same
	// TTL over a renewal it stamped at or after the instant this Agent anchored to, so
	// from here on the volume may already have another writer.
	h.clk.Advance(2 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 0 {
		t.Fatalf("the host is still serving %v after its lease expired", got)
	}
	if ln.closeCount() == 0 {
		t.Fatalf("%s is still bound after the lease expired: a second guest can be started against this volume while this one is still being served",
			socketOf(vol.GetVolumeId()))
	}
	if h.loop.LeaseValid() {
		t.Fatal("the loop still reports a valid lease after its TTL passed")
	}
}

// TestTheHostServesAgainOnceItsLeaseIsBack pins the other half: giving up on an expired
// lease must not wedge the host.
//
// Expiring revokes the lease, and a revoked lease cannot be renewed — only granted. An
// Agent that only ever renews would come back from a partition with LeaseValid() false
// for the life of the process, start whatever the Control Plane granted it next, and give
// it up again on the following cycle, forever. That is a worse outage than the one being
// fixed, and nothing about the socket assertion above can see it.
func TestTheHostServesAgainOnceItsLeaseIsBack(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := desiredVolume(t, 1)
	h.cp.setDesired([]*storagev1.DesiredVolume{vol})
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	h.cp.setErr(errUnreachable)
	h.clk.Advance(31 * time.Second)
	_ = h.loop.Reconcile(ctx)
	if got := h.served(t); len(got) != 0 {
		t.Fatalf("precondition: the host still serves %v after its lease expired", got)
	}

	// The partition heals. The volume comes back at a higher epoch, which is the
	// Control Plane granting it to this host again — the only way back for a volume
	// this host gave up (VolumeManager.fencedEpoch).
	h.cp.setErr(nil)
	h.cp.setDesired([]*storagev1.DesiredVolume{{
		VolumeId:  vol.GetVolumeId(),
		SizeBytes: vol.GetSizeBytes(),
		BlockSize: vol.GetBlockSize(),
		Epoch:     2,
		State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}})
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle after the partition healed failed: %v", err)
	}
	if !h.loop.LeaseValid() {
		t.Fatal("the lease never re-armed after the partition healed; this host can never serve anything again")
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("the volume was not served again: %v", got)
	}

	// And it stays. One more cycle is what catches a lease that re-armed in name only:
	// the volume would be started by Apply and given up by the next expiry check.
	h.clk.Advance(time.Second)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the following cycle failed: %v", err)
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("the volume was given up again on a healthy host: %v", got)
	}
}

// TestARefusedRenewalTakesTheSocketDown is the other way a host loses its claim: the
// Control Plane answers, and the answer is "you are dead" — a zero lease TTL. The round
// trip succeeded, so nothing about the cycle failed, and before this the host went on
// serving as if it had been renewed.
func TestARefusedRenewalTakesTheSocketDown(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := desiredVolume(t, 1)
	h.cp.setDesired([]*storagev1.DesiredVolume{vol})
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	ln := h.lf.listenerFor(socketOf(vol.GetVolumeId()))
	if ln == nil {
		t.Fatalf("nothing bound %s; the volume never started", socketOf(vol.GetVolumeId()))
	}

	h.cp.mu.Lock()
	h.cp.leaseTTL = 0
	h.cp.mu.Unlock()

	// The cycle that learns it, and the one that acts on it. The check runs at the top
	// of a cycle, on the lease the previous one left behind, so a host learns it is dead
	// and gives its volumes up one heartbeat interval later — orders of magnitude inside
	// the TTL, which Config.Validate requires to exceed the interval.
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the cycle that was refused a renewal failed: %v", err)
	}
	h.clk.Advance(time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 0 {
		t.Fatalf("the host is still serving %v after the Control Plane refused to renew its lease", got)
	}
	if ln.closeCount() == 0 {
		t.Fatalf("%s is still bound after the Control Plane refused to renew this host's lease", socketOf(vol.GetVolumeId()))
	}
}
