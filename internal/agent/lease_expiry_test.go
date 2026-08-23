package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// What a host does when it stops being able to confirm that it owns its volumes.
//
// # Why these drive a stand-in and not the real thing
//
// They used to drive a real Loop against a real VolumeManager on one simulated clock,
// and the assertion was the *socket*: an Agent that cannot reach the Control Plane keeps
// running, and until the loop learned to give up it kept serving — the socket stayed
// bound and the device stayed answering for as long as the process lived, while the fleet
// declared the host dead after one lease TTL and an operator moved the volume elsewhere.
// Two guests wrote one volume and both were told their flushes were durable.
//
// That manager is withdrawn with the local block engine, and with it the socket. The
// decisions being asserted are the loop's and are unchanged, so what they are asserted
// *on* moves to the two places the loop actually speaks: the reconciler it tells to stop
// serving, and the report the Control Plane receives. The second is the one that matters
// and is the one this lane can still observe from outside the process — everything else
// about a host giving up its volumes is invisible until it reaches the wire.
//
// fakeReconciler is deliberately not a re-implementation of a volume manager: it records
// what it was told, and reproduces exactly one rule, for the reason stated at that field.
// A stand-in that decided anything more would be a second implementation of the rules
// under test.
type fakeReconciler struct {
	mu sync.Mutex
	// vols is what this host would report, refusals included. A fenced volume stays in
	// it — a volume that vanishes from the report is a volume the fleet cannot see,
	// which is the silence half of the failure above.
	vols map[string]agent.VolumeStatus
	// applyErr makes Apply fail the way an ordinary operational failure does: a socket
	// that cannot be bound, key material that cannot be unwrapped.
	applyErr error
	// fenced is the epoch each given-up volume was fenced at. It is the one rule this
	// stand-in reproduces rather than records, and it has to: a volume given up comes
	// back only at a *higher* epoch — only when the Control Plane grants it to this host
	// again — and without that a fence is undone by the very next Apply, since the
	// desired state still lists the volume. Every assertion below about a host that
	// stopped serving would then be asserting on one cycle's worth of nothing.
	fenced map[string]int64
}

func newFakeReconciler() *fakeReconciler {
	return &fakeReconciler{vols: map[string]agent.VolumeStatus{}, fenced: map[string]int64{}}
}

var _ agent.VolumeReconciler = (*fakeReconciler)(nil)

func (r *fakeReconciler) Volumes(context.Context) ([]agent.VolumeStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]agent.VolumeStatus, 0, len(r.vols))
	for _, v := range r.vols {
		out = append(out, v)
	}
	return out, nil
}

func (r *fakeReconciler) Apply(_ context.Context, desired []*storagev1.DesiredVolume) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range desired {
		if e, ok := r.fenced[d.GetVolumeId()]; ok && d.GetEpoch() <= e {
			continue
		}
		delete(r.fenced, d.GetVolumeId())
		if r.applyErr != nil {
			// The shape the real manager has: a volume it could not start is still
			// reported, carrying why. Reporting nothing is how a stuck volume looked
			// ACTIVE to the whole fleet.
			r.vols[d.GetVolumeId()] = agent.VolumeStatus{
				VolumeID: d.GetVolumeId(), Epoch: d.GetEpoch(),
				Refusal:       storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED,
				RefusalDetail: r.applyErr.Error(),
			}
			continue
		}
		r.vols[d.GetVolumeId()] = agent.VolumeStatus{VolumeID: d.GetVolumeId(), Epoch: d.GetEpoch()}
	}
	return r.applyErr
}

func (r *fakeReconciler) Fence(_ context.Context, volumeIDs []string, why storagev1.VolumeRefusal, detail string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range volumeIDs {
		v := r.vols[id]
		v.VolumeID = id
		v.Refusal, v.RefusalDetail = why, detail
		r.vols[id] = v
		r.fenced[id] = v.Epoch
	}
	return nil
}

// leaseHarness is a real Loop on a simulated clock, against a Control Plane that can be
// made to stop answering and a reconciler that records what it was told.
type leaseHarness struct {
	clk  *sim.Clock
	cp   *fakeCP
	rec  *fakeReconciler
	loop *agent.Loop
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	rec := newFakeReconciler()

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}},
		Volumes:      rec,
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	return &leaseHarness{clk: clk, cp: cp, rec: rec, loop: loop}
}

// served is the set of volume ids this host would report as being served.
//
// The refused ones are excluded, and the distinction is the point rather than a detail
// of the helper: the report carries every volume this host was told to serve, including
// the ones it has given up, because a volume that vanishes from the report is a volume
// the fleet cannot see. "Serving" is the subset with no refusal on it.
func (h *leaseHarness) served(t *testing.T) []string {
	t.Helper()
	vols, err := h.rec.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	out := make([]string, 0, len(vols))
	for _, v := range vols {
		if v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			out = append(out, v.VolumeID)
		}
	}
	return out
}

func (h *leaseHarness) desire(epoch int64) *storagev1.DesiredVolume {
	v := &storagev1.DesiredVolume{
		VolumeId:  ids.New().String(),
		SizeBytes: 1 << 30,
		BlockSize: 4096,
		Epoch:     epoch,
		State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}
	h.cp.setDesired([]*storagev1.DesiredVolume{v})
	return v
}

// TestAnExpiredLeaseStopsTheHostServing is the partitioned-Agent blocker.
//
// The assertion is on what the host does and then on what the fleet is told, not on a
// flag: a lease field that says "expired" next to a volume this host is still serving is
// the bug this test exists to catch.
func TestAnExpiredLeaseStopsTheHostServing(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
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

	// Past the TTL. The Control Plane's own fencing deadline is computed from the same
	// TTL over a renewal it stamped at or after the instant this Agent anchored to, so
	// from here on the volume may already have another writer.
	h.clk.Advance(2 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 0 {
		t.Fatalf("the host is still serving %v after its lease expired", got)
	}
	if h.loop.LeaseValid() {
		t.Fatal("the loop still reports a valid lease after its TTL passed")
	}

	// And the fleet is told. Everything above is invisible from outside this process:
	// the Control Plane still names this host the volume's primary at this epoch, so
	// nothing moves and nothing else will notice — the volume simply stopped being
	// served, and an absence on the wire looks exactly like a volume that was never
	// placed here.
	h.cp.setErr(nil)
	h.clk.Advance(time.Second)
	_ = h.loop.Reconcile(ctx)
	got := lastReportOf(t, h.cp, vol.GetVolumeId())
	if got.GetRefusal() != storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST {
		t.Fatalf("the report for a volume this host gave up says refusal=%s, want LEASE_LOST", got.GetRefusal())
	}
	if !strings.Contains(got.GetRefusalDetail(), "lease") {
		t.Fatalf("refusal detail = %q, and an operator has to be able to read why", got.GetRefusalDetail())
	}
}

// TestACycleThatCouldNotStartAVolumeStillReportsIt is the hole the two real binaries
// found.
//
// The refusal is recorded, the report message has a field for it, and the Control Plane
// stores it — and none of that fired, because `Reconcile` returned at the first failure
// and the failure *is* `Apply`. So the one cycle with something to say about a volume
// that could not start was the one cycle that never got as far as saying it: an Agent
// stuck refusing a volume printed a WARN every second and the fleet showed the volume
// ACTIVE for ever.
//
// The assertion is on the report the Control Plane received, and on the cycle *still*
// returning its error — the backoff and the WARN line are what retry it, and a fix that
// swallowed the error to reach the report would trade one silence for another.
func TestACycleThatCouldNotStartAVolumeStillReportsIt(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	h.rec.applyErr = errors.New("address already in use")
	vol := h.desire(1)

	err := h.loop.Reconcile(ctx)
	if err == nil {
		t.Fatal("a cycle that could not start the volume it was told to serve reported success")
	}
	if !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("the cycle's error does not carry the failure: %v", err)
	}

	got := lastReportOf(t, h.cp, vol.GetVolumeId())
	if got.GetRefusal() != storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED {
		t.Fatalf("the report says refusal=%s, want ATTACH_FAILED: the fleet still cannot see that this volume is not being served",
			got.GetRefusal())
	}
	if got.GetEpoch() != vol.GetEpoch() {
		t.Fatalf("the refusal was reported under epoch %d, want %d", got.GetEpoch(), vol.GetEpoch())
	}
}

// lastReportOf returns the most recent VolumeReport the Control Plane received for one
// volume. It fails when there is none, because "the volume stopped being reported" is
// precisely the silence these assertions exist to detect.
func lastReportOf(t *testing.T, cp *fakeCP, volumeID string) *storagev1.VolumeReport {
	t.Helper()
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for i := len(cp.reports) - 1; i >= 0; i-- {
		for _, v := range cp.reports[i].GetVolumes() {
			if v.GetVolumeId() == volumeID {
				return v
			}
		}
	}
	t.Fatalf("no report ever named volume %s", volumeID)
	return nil
}

// TestTheHostServesAgainOnceItsLeaseIsBack pins the other half: giving up on an expired
// lease must not wedge the host.
//
// Expiring revokes the lease, and a revoked lease cannot be renewed — only granted. An
// Agent that only ever renews would come back from a partition with LeaseValid() false
// for the life of the process, start whatever the Control Plane granted it next, and give
// it up again on the following cycle, forever. That is a worse outage than the one being
// fixed, and nothing about the assertion above can see it.
func TestTheHostServesAgainOnceItsLeaseIsBack(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	h.cp.setErr(errUnreachable)
	h.clk.Advance(31 * time.Second)
	_ = h.loop.Reconcile(ctx)
	if got := h.served(t); len(got) != 0 {
		t.Fatalf("precondition: the host still serves %v after its lease expired", got)
	}

	// The partition heals. The volume comes back at a higher epoch, which is the Control
	// Plane granting it to this host again.
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

// TestARefusedRenewalStopsTheHostServing is the other way a host loses its claim: the
// Control Plane answers, and the answer is "you are dead" — a zero lease TTL. The round
// trip succeeded, so nothing about the cycle failed, and before this the host went on
// serving as if it had been renewed.
func TestARefusedRenewalStopsTheHostServing(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
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
}
