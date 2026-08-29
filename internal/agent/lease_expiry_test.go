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
// These used to drive a real VolumeManager and assert on the *socket*. That manager went
// with the local block engine, so the loop's decisions are now asserted on the two places
// it speaks: the reconciler it tells to stop serving, and the report the Control Plane
// receives — the second being the only one observable from outside the process.
//
// fakeReconciler records what it was told and reproduces exactly one rule, for the reason
// stated at that field; a stand-in that decided more would be a second implementation of
// the rules under test.
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

// fakeWitness is the second signal: the epoch the object store records for a volume,
// read over a path that does not run through the Control Plane.
//
// It records nothing and decides nothing. The two states worth setting are the two the
// design turns on — an epoch that has moved (somebody else was granted the volume) and a
// store that cannot be reached (this host cannot tell isolation from supersession).
type fakeWitness struct {
	mu     sync.Mutex
	epochs map[string]int64
	err    error
}

func newFakeWitness() *fakeWitness { return &fakeWitness{epochs: map[string]int64{}} }

func (w *fakeWitness) GrantedEpoch(_ context.Context, volumeID string) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	e, ok := w.epochs[volumeID]
	if !ok {
		return 0, errNoEpochRecorded
	}
	return e, nil
}

func (w *fakeWitness) grant(volumeID string, epoch int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.epochs[volumeID] = epoch
}

func (w *fakeWitness) setErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.err = err
}

var (
	errNoEpochRecorded  = errors.New("no epoch recorded for this volume")
	errStoreUnreachable = errors.New("the object store is unreachable")
)

// leaseHarness is a real Loop on a simulated clock, against a Control Plane that can be
// made to stop answering and a reconciler that records what it was told.
type leaseHarness struct {
	clk  *sim.Clock
	cp   *fakeCP
	rec  *fakeReconciler
	wit  *fakeWitness
	loop *agent.Loop
}

func newLeaseHarness(t *testing.T) *leaseHarness {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	rec := newFakeReconciler()
	wit := newFakeWitness()

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}},
		Volumes:      rec,
		Witness:      wit,
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	return &leaseHarness{clk: clk, cp: cp, rec: rec, wit: wit, loop: loop}
}

// served is the set of volume ids this host would report as being served — the refused
// ones excluded. The report carries every volume this host was told to serve, including
// the ones it gave up: a volume that vanishes from the report is one the fleet cannot see.
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
		Epoch:     epoch,
		State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}
	h.cp.setDesired([]*storagev1.DesiredVolume{v})
	return v
}

// A lapsed lease is one signal, and these three tests are the three things it can mean.
//
// A guest is stopped only on *confirmed* supersession: giving up on the lapse alone
// stopped a tenant's VM over a partition nobody else had acted on, and under v6 nothing a
// superseded host writes can enter the published history anyway. The confirmation comes
// from `volumes/<id>/epoch`, written at the grant and read over the object store — see
// agent.Superseded.
func TestAConfirmedSupersessionStopsTheHostServing(t *testing.T) {
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

	// Past the TTL, and the second signal agrees: the bucket records a higher epoch than
	// the one this host holds, which is the Control Plane having granted the volume to
	// somebody else. That is the fact — not the silence — that stops the guest.
	h.wit.grant(vol.GetVolumeId(), 2)
	h.clk.Advance(2 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 0 {
		t.Fatalf("the host is still serving %v after the bucket said the volume was granted elsewhere", got)
	}
	if h.loop.LeaseValid() {
		t.Fatal("the loop still reports a valid lease after its TTL passed")
	}

	// And the fleet is told. Everything above is invisible from outside this process — the
	// Control Plane still names this host the volume's primary, so an absence on the wire
	// looks exactly like a volume that was never placed here.
	h.cp.setErr(nil)
	h.clk.Advance(time.Second)
	_ = h.loop.Reconcile(ctx)
	got := lastReportOf(t, h.cp, vol.GetVolumeId())
	if got.GetRefusal() != storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST {
		t.Fatalf("the report for a volume this host gave up says refusal=%s, want LEASE_LOST", got.GetRefusal())
	}
	if !strings.Contains(got.GetRefusalDetail(), "epoch") {
		t.Fatalf("refusal detail = %q, and an operator has to be able to read which signal stopped the guest", got.GetRefusalDetail())
	}
}

// TestAnIsolatedHostKeepsServing is the tenant's side of the same partition.
//
// The lease has lapsed and the bucket says the volume is still this host's at the epoch
// it holds. Nobody took it. Stopping the guest here costs a VM for a partition of the
// management path alone, and buys nothing: this host is already unable to publish
// anything the successor would have to reconcile with, because there is no successor.
func TestAnIsolatedHostKeepsServing(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	// The bucket agrees with what this host holds: epoch 1, granted to nobody since.
	h.wit.grant(vol.GetVolumeId(), 1)

	h.cp.setErr(errUnreachable)
	h.clk.Advance(31 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if h.loop.LeaseValid() {
		t.Fatal("the lease is reported valid past its TTL: the host must know it is degraded")
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("an isolated host gave up %v; the bucket said nobody else was granted the volume", vol.GetVolumeId())
	}

	// And it keeps serving across cycles rather than surviving one and dying on the next.
	for range 5 {
		h.clk.Advance(time.Second)
		_ = h.loop.Reconcile(ctx)
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("the host gave the volume up after several cycles of isolation: served = %v", got)
	}

	// When the Control Plane comes back and the volume is still this host's, the lease
	// re-arms and nothing had to be re-granted at a higher epoch to get there — which is
	// the whole saving over giving up: a fence costs a promotion to undo.
	h.cp.setErr(nil)
	h.clk.Advance(time.Second)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the cycle after the partition healed failed: %v", err)
	}
	if !h.loop.LeaseValid() {
		t.Fatal("the lease did not re-arm once the Control Plane answered again")
	}
	if got := h.served(t); len(got) != 1 {
		t.Fatalf("served = %v after the partition healed, want the volume still being served", got)
	}
}

// TestAHostThatCannotTellKeepsServing is the case the design does not resolve, asserted
// so that it is a decision rather than an accident. Cut off from the Control Plane *and*
// the object store, this host cannot distinguish isolation from supersession; stopping is
// correct in one of the two cases and a tenant's VM killed on a guess in the other, while
// the data is protected in both by a compare-and-set it cannot win. What is genuinely lost
// is the successor's guest, and that is not made better by stopping the wrong one.
func TestAHostThatCannotTellKeepsServing(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}

	h.cp.setErr(errUnreachable)
	h.wit.setErr(errStoreUnreachable)
	h.clk.Advance(31 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 1 {
		t.Fatalf("a host that could reach neither the Control Plane nor the bucket gave up %v on a guess", got)
	}
}

// TestAVolumeWithNoRecordedEpochIsNotGivenUp guards the reading of a *missing* answer.
//
// `volumes/<id>/epoch` is written at the grant, so a volume placed before that object
// existed has none — and "no record" is not "granted to somebody else". Reading it as
// supersession would stop every guest whose volume predates the object, on a partition.
func TestAVolumeWithNoRecordedEpochIsNotGivenUp(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	h.desire(1) // and nothing is granted in the witness, so it answers "no record"
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}

	h.cp.setErr(errUnreachable)
	h.clk.Advance(31 * time.Second)
	_ = h.loop.Reconcile(ctx)

	if got := h.served(t); len(got) != 1 {
		t.Fatalf("a volume with no recorded epoch was read as granted elsewhere and given up: served = %v", got)
	}
}

// TestACycleThatCouldNotStartAVolumeStillReportsIt is the hole the two real binaries
// found: `Reconcile` returned at the first failure and the failure *is* `Apply`, so the
// one cycle with something to say about a volume that could not start never got as far as
// saying it — a WARN every second and a fleet showing the volume ACTIVE for ever.
//
// The assertion is on the report the Control Plane received and on the cycle still
// returning its error: swallowing it to reach the report would trade one silence for
// another.
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
// lease must not wedge the host. Expiring revokes the lease and a revoked lease can only
// be granted, never renewed — an Agent that only renews comes back from a partition with
// LeaseValid() false for the life of the process, starting each volume it is granted and
// giving it up again on the next cycle, forever.
func TestTheHostServesAgainOnceItsLeaseIsBack(t *testing.T) {
	t.Parallel()
	h := newLeaseHarness(t)
	ctx := t.Context()

	vol := h.desire(1)
	if err := h.loop.Reconcile(ctx); err != nil {
		t.Fatalf("the first cycle failed: %v", err)
	}
	h.cp.setErr(errUnreachable)
	// The precondition is a volume actually given up, which now takes both signals: the
	// lease lapsing, and the bucket recording that the volume was granted to somebody
	// else. The lapse alone leaves the host serving — that is TestAnIsolatedHostKeepsServing.
	h.wit.grant(vol.GetVolumeId(), 2)
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
