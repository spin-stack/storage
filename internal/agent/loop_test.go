package agent_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// errUnreachable stands in for "the Control Plane did not answer" — the condition
// the loop must survive without losing its cadence or keeping its lease alive.
var errUnreachable = errors.New("control plane unreachable")

// fakeCP is a scripted ControlPlaneServiceClient. Every call records the monotonic
// instant it was made at, which is what a cadence assertion is actually about: the
// loop's schedule, not the wall clock a test happens to run on.
type fakeCP struct {
	clk *sim.Clock

	mu        sync.Mutex
	err       error // when non-nil every call fails with it
	reportErr error // when non-nil only ReportVolumeState fails with it
	leaseTTL  int32
	state     storagev1.HostState
	term      int64
	desired   []*storagev1.DesiredVolume
	keys      map[string]*storagev1.GetVolumeKeysResponse
	keyCalls  map[string]int
	outcomes  map[string]storagev1.ReportOutcome
	beats     []*storagev1.HeartbeatRequest
	beatAt    []clock.Instant
	reports   []*storagev1.ReportVolumeStateRequest
	beatCh    chan struct{}
	beatCount int
	roundTrip time.Duration // time that elapses between the request and its answer
}

var _ storagev1connect.ControlPlaneServiceClient = (*fakeCP)(nil)

func newFakeCP(clk *sim.Clock) *fakeCP {
	return &fakeCP{
		clk:      clk,
		leaseTTL: 30,
		state:    storagev1.HostState_HOST_STATE_ACTIVE,
		term:     7,
		keys:     map[string]*storagev1.GetVolumeKeysResponse{},
		keyCalls: map[string]int{},
		outcomes: map[string]storagev1.ReportOutcome{},
		beatCh:   make(chan struct{}, 1024),
	}
}

func (f *fakeCP) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeCP) Heartbeat(_ context.Context, req *connect.Request[storagev1.HeartbeatRequest]) (*connect.Response[storagev1.HeartbeatResponse], error) {
	f.mu.Lock()
	f.beats = append(f.beats, req.Msg)
	f.beatAt = append(f.beatAt, f.clk.Now())
	f.beatCount++
	err, ttl, state, term, delay := f.err, f.leaseTTL, f.state, f.term, f.roundTrip
	f.mu.Unlock()
	if delay > 0 {
		f.clk.Advance(delay) // the answer comes back late
	}
	f.beatCh <- struct{}{}
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&storagev1.HeartbeatResponse{
		LeaseTtlSeconds: ttl,
		State:           state,
		Term:            term,
	}), nil
}

func (f *fakeCP) GetDesiredState(_ context.Context, _ *connect.Request[storagev1.GetDesiredStateRequest]) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{Volumes: f.desired}), nil
}

func (f *fakeCP) ReportVolumeState(_ context.Context, req *connect.Request[storagev1.ReportVolumeStateRequest]) (*connect.Response[storagev1.ReportVolumeStateResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if f.reportErr != nil {
		return nil, f.reportErr
	}
	f.reports = append(f.reports, req.Msg)
	results := make([]*storagev1.VolumeReportResult, 0, len(req.Msg.GetVolumes()))
	for _, v := range req.Msg.GetVolumes() {
		outcome, ok := f.outcomes[v.GetVolumeId()]
		if !ok {
			outcome = storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED
		}
		results = append(results, &storagev1.VolumeReportResult{VolumeId: v.GetVolumeId(), Outcome: outcome})
	}
	return connect.NewResponse(&storagev1.ReportVolumeStateResponse{Results: results}), nil
}

// GetVolumeKeys counts its calls: whether the Agent asks once per volume or once
// per cycle is the difference between a key fetch and a key broadcast.
func (f *fakeCP) GetVolumeKeys(_ context.Context, req *connect.Request[storagev1.GetVolumeKeysRequest]) (*connect.Response[storagev1.GetVolumeKeysResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	id := req.Msg.GetVolumeId()
	f.keyCalls[id]++
	keys, ok := f.keys[id]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no such volume"))
	}
	return connect.NewResponse(keys), nil
}

func (f *fakeCP) setReportErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reportErr = err
}

func (f *fakeCP) setDesired(vols []*storagev1.DesiredVolume) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.desired = vols
}

func (f *fakeCP) keyCallsFor(volumeID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyCalls[volumeID]
}

func (f *fakeCP) lastHeartbeat(t *testing.T) *storagev1.HeartbeatRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.beats) == 0 {
		t.Fatal("no heartbeat was sent")
	}
	return f.beats[len(f.beats)-1]
}

func (f *fakeCP) lastReport(t *testing.T) *storagev1.ReportVolumeStateRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reports) == 0 {
		t.Fatal("no volume report was sent")
	}
	return f.reports[len(f.reports)-1]
}

func (f *fakeCP) instants() []clock.Instant {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]clock.Instant(nil), f.beatAt...)
}

// fakeDevice reports a fixed device picture.
type fakeDevice struct {
	usage disk.Usage
	err   error
}

func (d fakeDevice) Usage(context.Context) (disk.Usage, error) { return d.usage, d.err }

const (
	testHost     = "0197b5c2-8f00-7a1b-9c3d-4e5f60718293"
	testVersion  = "0.1.0-test"
	testInterval = 5 * time.Second
	testBackoff  = time.Second
)

func testConfig() agent.Config {
	return agent.Config{
		HostID:            testHost,
		AgentVersion:      testVersion,
		MaxFormatVersion:  3,
		HeartbeatInterval: testInterval,
		RetryBackoff:      testBackoff,
		LeaseTTL:          30 * time.Second,
	}
}

type harness struct {
	clk  *sim.Clock
	cp   *fakeCP
	vols *agent.VolumeSet
	loop *agent.Loop
	prov *obs.Provider
}

func newHarness(t *testing.T, cfg agent.Config, usage disk.Usage) *harness {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	vols := agent.NewVolumeSet()
	prov, err := obs.NewTestProvider("agent-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prov.Shutdown(context.WithoutCancel(t.Context())) })

	loop, err := agent.New(cfg, agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{usage: usage},
		Volumes:      vols,
		Recorder:     obs.NewRecorder(prov.Metrics),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{clk: clk, cp: cp, vols: vols, loop: loop, prov: prov}
}

// waitSleeping spins until the loop has registered its next timer, i.e. it has
// finished a cycle and is waiting. Advancing the clock before that would move time
// past a timer nobody has armed yet, and the loop would sleep for real.
func (h *harness) waitSleeping(t *testing.T) {
	t.Helper()
	for range 5_000_000 {
		if h.clk.PendingTimers() > 0 {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("the loop never armed its next timer")
}

// awaitBeat waits for the n-th heartbeat (1-based) to have been made.
func (h *harness) awaitBeat(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-h.cp.beatCh:
		case <-t.Context().Done():
			t.Fatal("context cancelled while waiting for a heartbeat")
		}
	}
}

// TestReconcileReportsTheDevicePicture is the ADR-0013 contract: the heartbeat is
// what finally produces total, used, and the aggregate remote backlog — the last of
// which no per-volume limit ever sums.
func TestReconcileReportsTheDevicePicture(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 1 << 40, UsedBytes: 300 << 30})
	h.vols.Set(agent.VolumeStatus{VolumeID: "vol-b", Epoch: 2})
	h.vols.Set(agent.VolumeStatus{VolumeID: "vol-a", Epoch: 1})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	beat := h.cp.lastHeartbeat(t)
	if beat.GetHostId() != testHost || beat.GetAgentVersion() != testVersion || beat.GetMaxFormatVersion() != 3 {
		t.Fatalf("identity not reported: %+v", beat)
	}
	dev := beat.GetDevice()
	if dev.GetTotalBytes() != 1<<40 {
		t.Errorf("total_bytes = %d, want %d", dev.GetTotalBytes(), int64(1)<<40)
	}
	if dev.GetUsedBytes() != 300<<30 {
		t.Errorf("used_bytes = %d, want %d", dev.GetUsedBytes(), int64(300)<<30)
	}
	// The heartbeat used to carry the host's remote backlog. It went with the uploader
	// (ADR-0026 increment 4.5) and nothing measures what a host would lose mid-session
	// now — recorded in STATUS.md rather than left to be noticed.
	if dev.GetRemoteBacklogBytes() != 0 {
		t.Errorf("remote_backlog_bytes = %d, want 0: nothing reports a backlog", dev.GetRemoteBacklogBytes())
	}
}

// TestReconcileReportsEpochQualifiedWatermarks: every watermark the Agent reports
// carries the epoch it was produced under, in volume-id order (INV-02).
func TestReconcileReportsEpochQualifiedWatermarks(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 10})
	h.vols.Set(agent.VolumeStatus{VolumeID: "vol-b", Epoch: 9, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10})
	h.vols.Set(agent.VolumeStatus{VolumeID: "vol-a", Epoch: 4, LocalSequence: 3, DurableSequence: 2, PublishedSequence: 1})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := h.cp.lastReport(t).GetVolumes()
	if len(got) != 2 {
		t.Fatalf("reported %d volumes, want 2", len(got))
	}
	if got[0].GetVolumeId() != "vol-a" || got[1].GetVolumeId() != "vol-b" {
		t.Fatalf("reports are not ordered by volume id: %q, %q", got[0].GetVolumeId(), got[1].GetVolumeId())
	}
	if got[0].GetEpoch() != 4 || got[1].GetEpoch() != 9 {
		t.Fatalf("epochs not carried: %d, %d", got[0].GetEpoch(), got[1].GetEpoch())
	}
	if got[1].GetLocalSequence() != 30 || got[1].GetDurableSequence() != 20 || got[1].GetPublishedSequence() != 10 {
		t.Fatalf("watermarks not carried: %+v", got[1])
	}
}

// TestRefusedReportMarksTheVolumeFenced: a refusal is information. STALE_EPOCH or
// NOT_PRIMARY means this host is no longer the writer, and the loop must surface
// that rather than swallow it — it is what the data path will act on (§12.2).
func TestRefusedReportMarksTheVolumeFenced(t *testing.T) {
	tests := []struct {
		name    string
		outcome storagev1.ReportOutcome
		fenced  bool
	}{
		{"accepted", storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED, false},
		{"stale epoch", storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH, true},
		{"not primary", storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY, true},
		{"unknown volume", storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME, true},
		{"out of order", storagev1.ReportOutcome_REPORT_OUTCOME_OUT_OF_ORDER, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
			h.vols.Set(agent.VolumeStatus{VolumeID: "vol-a", Epoch: 1})
			h.cp.outcomes["vol-a"] = tc.outcome

			if err := h.loop.Reconcile(t.Context()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			fenced := h.loop.Fenced()
			if got := len(fenced) == 1 && fenced[0] == "vol-a"; got != tc.fenced {
				t.Fatalf("fenced = %v (%v), want %v", got, fenced, tc.fenced)
			}
		})
	}
}

// TestReconcileRecordsDesiredState: what the Agent learned is what a data path
// would reconcile against, so it has to be readable after the cycle.
func TestReconcileRecordsDesiredState(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	h.cp.desired = []*storagev1.DesiredVolume{
		{VolumeId: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096, Epoch: 3, State: storagev1.VolumeState_VOLUME_STATE_ACTIVE},
	}

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	desired := h.loop.Desired()
	if len(desired) != 1 || desired[0].GetVolumeId() != "vol-a" || desired[0].GetEpoch() != 3 {
		t.Fatalf("desired state not recorded: %+v", desired)
	}
	if state := h.loop.HostState(); state != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("host state = %v, want ACTIVE", state)
	}
}

// TestReconcileStopsAtTheFirstFailure: the calls are ordered, and a Control Plane
// that did not answer the heartbeat has nothing useful to say to the rest.
func TestReconcileStopsAtTheFirstFailure(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	h.cp.setErr(errUnreachable)

	err := h.loop.Reconcile(t.Context())
	if !errors.Is(err, errUnreachable) {
		t.Fatalf("Reconcile error = %v, want %v", err, errUnreachable)
	}
	h.cp.mu.Lock()
	defer h.cp.mu.Unlock()
	if len(h.cp.reports) != 0 {
		t.Fatal("the loop reported volume state after the heartbeat failed")
	}
}

// TestRunHoldsItsCadence: the loop runs immediately, then once per interval, on
// the injected clock. Nothing here waits on real time.
func TestRunHoldsItsCadence(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- h.loop.Run(ctx) }()

	start := h.clk.Now()
	h.awaitBeat(t, 1)
	for range 3 {
		h.waitSleeping(t)
		h.clk.Advance(testInterval)
		h.awaitBeat(t, 1)
	}
	h.waitSleeping(t)
	// Cancelling is enough to end the sleep: the loop's clock is the injected one,
	// and Sleep honours the context.
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	instants := h.cp.instants()
	if len(instants) != 4 {
		t.Fatalf("got %d heartbeats, want 4", len(instants))
	}
	for i, at := range instants {
		if want := start.Add(time.Duration(i) * testInterval); at != want {
			t.Errorf("heartbeat %d at %d, want %d (one interval apart)", i, at, want)
		}
	}
}

// TestRunBacksOffAfterAFailureAndRecovers: a failing cycle must not spin, must not
// stall, and must return to the normal cadence once the Control Plane answers —
// the retry delay doubles and is capped at one heartbeat interval.
func TestRunBacksOffAfterAFailureAndRecovers(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	h.cp.setErr(errUnreachable)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.loop.Run(ctx) }()

	start := h.clk.Now()
	h.awaitBeat(t, 1)

	// 1s, 2s, 4s, then capped at the 5s interval.
	for _, d := range []time.Duration{testBackoff, 2 * testBackoff, 4 * testBackoff, testInterval} {
		h.waitSleeping(t)
		h.clk.Advance(d)
		h.awaitBeat(t, 1)
	}
	// The Control Plane comes back: the next delay is the plain interval again.
	h.waitSleeping(t)
	h.cp.setErr(nil)
	h.clk.Advance(testInterval)
	h.awaitBeat(t, 1)
	h.waitSleeping(t)
	h.clk.Advance(testInterval)
	h.awaitBeat(t, 1)

	h.waitSleeping(t)
	cancel()
	<-done

	instants := h.cp.instants()
	want := []time.Duration{0, testBackoff, 3 * testBackoff, 7 * testBackoff, 7*testBackoff + testInterval,
		7*testBackoff + 2*testInterval, 7*testBackoff + 3*testInterval}
	if len(instants) != len(want) {
		t.Fatalf("got %d heartbeats, want %d", len(instants), len(want))
	}
	for i, at := range instants {
		if got, wantAt := at, start.Add(want[i]); got != wantAt {
			t.Errorf("heartbeat %d at %v, want %v after the start", i, got.Sub(start), want[i])
		}
	}
}

// TestLeaseLapsesWhenHeartbeatsFail is INV-06's Agent-side half: the lease is armed
// by a successful heartbeat and by nothing else, so a Control Plane that stops
// answering lets it run out on the monotonic clock (§12.2) — the Agent does not need
// to be told it was fenced.
func TestLeaseLapsesWhenHeartbeatsFail(t *testing.T) {
	cfg := testConfig()
	h := newHarness(t, cfg, disk.Usage{TotalBytes: 100, UsedBytes: 1})

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !h.loop.LeaseValid() {
		t.Fatal("the lease is not valid after a successful heartbeat")
	}

	h.cp.setErr(errUnreachable)
	h.clk.Advance(29 * time.Second)
	if err := h.loop.Reconcile(t.Context()); err == nil {
		t.Fatal("Reconcile should have failed")
	}
	if !h.loop.LeaseValid() {
		t.Fatal("the lease expired before its TTL")
	}
	h.clk.Advance(2 * time.Second) // past the 30s TTL the Control Plane granted
	if h.loop.LeaseValid() {
		t.Fatal("the lease is still valid past its TTL with no successful renewal")
	}

	values, err := h.prov.CollectedMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !values["lease_renewal_failures_total"] {
		t.Error("a failed heartbeat did not record lease_renewal_failures_total")
	}
	if !values["lease_remaining_seconds"] {
		t.Error("a successful heartbeat did not record lease_remaining_seconds")
	}
}

// TestLeaseIsAnchoredToTheRequest: §12.2 — a renewal is anchored to the instant the
// request left the host, never to the instant the answer arrived. A slow round trip
// must shorten the Agent's window, not extend it.
func TestLeaseIsAnchoredToTheRequest(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	h.cp.roundTrip = 25 * time.Second // a GC pause, a PG failover retry, a healing partition

	sent := h.clk.Now()
	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The answer arrived at sent+25s, but the 30s TTL runs from `sent`.
	h.clk.Advance(4 * time.Second) // now sent+29s
	if !h.loop.LeaseValid() {
		t.Fatalf("lease expired before its TTL (requested at %d)", sent)
	}
	h.clk.Advance(2 * time.Second) // now sent+31s
	if h.loop.LeaseValid() {
		t.Fatal("lease anchored to the answer instead of the request: it outlived the Control Plane's window")
	}
}

// TestDeadHostStopsTheLease: a Control Plane that has declared the host DEAD answers
// with no lease at all. The Agent must not keep an armed lease on the strength of a
// successful HTTP round trip.
func TestDeadHostStopsTheLease(t *testing.T) {
	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 100, UsedBytes: 1})
	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	h.cp.mu.Lock()
	h.cp.leaseTTL = 0
	h.cp.state = storagev1.HostState_HOST_STATE_DEAD
	h.cp.mu.Unlock()

	if err := h.loop.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if h.loop.LeaseValid() {
		t.Fatal("the Agent kept its lease after the Control Plane refused to renew it")
	}
	if h.loop.HostState() != storagev1.HostState_HOST_STATE_DEAD {
		t.Fatal("the Agent did not record the fleet state it was told")
	}
}

// TestDeviceReadFailureFailsTheCycle: reporting a device picture the Agent could not
// read would be worse than reporting nothing — every ADR-0013 threshold is evaluated
// on these numbers.
func TestDeviceReadFailureFailsTheCycle(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{err: errUnreachable},
		Volumes:      agent.NewVolumeSet(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := loop.Reconcile(t.Context()); !errors.Is(err, errUnreachable) {
		t.Fatalf("Reconcile error = %v, want the device error", err)
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if len(cp.beats) != 0 {
		t.Fatal("a heartbeat was sent with a device picture that could not be read")
	}
}
