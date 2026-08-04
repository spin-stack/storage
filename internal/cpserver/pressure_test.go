package cpserver_test

import (
	"context"
	"math"
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// deviceTotal is the device every case here reports: 1 TiB, a size at which one byte
// is 10^-12 of the whole, so stepping across a threshold by a single byte is the
// sharpest statement of "nothing about this device changed" the API can make.
const deviceTotal = int64(1) << 40

// used is the smallest byte count whose share of the device is at or above ratio, so
// used(r) is on the far side of the r line and used(r)-1 is on the near side. It
// rounds up rather than truncating because truncation lands *below* the ratio it
// names, which would make "at the cordon line" quietly mean "just under it".
func used(ratio float64) int64 { return int64(math.Ceil(ratio * float64(deviceTotal))) }

// beat sends one heartbeat reporting `usedBytes` of deviceTotal and returns what the
// store says about the host afterwards — the state and the reason, which is what an
// operator would read, not a field the handler set.
func (f *fixture) beat(t *testing.T, usedBytes int64) (lifecycle.HostState, lifecycle.CordonReason) {
	t.Helper()
	if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
		HostId:       hostA,
		AgentVersion: "0.1.0",
		Device:       &storagev1.DeviceStatus{TotalBytes: deviceTotal, UsedBytes: usedBytes},
	}); err != nil {
		t.Fatal(err)
	}
	h, err := f.md.GetHost(t.Context(), hostA)
	if err != nil {
		t.Fatal(err)
	}
	return h.State, h.CordonReason
}

// TestHeartbeatCordonsAndUncordonsAcrossTheBand is ADR-0013 §3's first row with the
// hysteresis that makes it usable: the Control Plane cordons a host at 70% used and
// gives it back only below 65%, so the whole band between the two is a host that
// stays where the last crossing put it.
//
// The sequence is driven through real heartbeats in both directions and asserts the
// host row after each, because the cordon is only worth anything if it is what the
// fleet's next placement decision reads.
func TestHeartbeatCordonsAndUncordonsAcrossTheBand(t *testing.T) {
	f := newFixture(t)

	steps := []struct {
		name       string
		used       int64
		wantState  lifecycle.HostState
		wantReason lifecycle.CordonReason
	}{
		{"an empty device is active", used(0.10), lifecycle.HostActive, lifecycle.CordonNone},
		{"one byte below the cordon line changes nothing", used(cpserver.CordonUsedRatio) - 1, lifecycle.HostActive, lifecycle.CordonNone},
		{"at the cordon line the fleet cordons it", used(cpserver.CordonUsedRatio), lifecycle.HostCordoned, lifecycle.CordonPressure},
		// The next three are the hysteresis. Each is a heartbeat that would have
		// un-cordoned the host under a single threshold, and each leaves it cordoned.
		{"falling one byte back below the cordon line does not release it", used(cpserver.CordonUsedRatio) - 1, lifecycle.HostCordoned, lifecycle.CordonPressure},
		{"crossing the line again does not re-cordon it", used(cpserver.CordonUsedRatio), lifecycle.HostCordoned, lifecycle.CordonPressure},
		{"inside the band it stays cordoned", used(0.67), lifecycle.HostCordoned, lifecycle.CordonPressure},
		{"one byte above the release line still holds", used(cpserver.UncordonUsedRatio), lifecycle.HostCordoned, lifecycle.CordonPressure},
		{"below the release line it comes back", used(cpserver.UncordonUsedRatio) - 1, lifecycle.HostActive, lifecycle.CordonNone},
		{"and it can be cordoned again", used(0.80), lifecycle.HostCordoned, lifecycle.CordonPressure},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			state, reason := f.beat(t, step.used)
			if state != step.wantState || reason != step.wantReason {
				t.Fatalf("after %d/%d used: state = %q reason = %q, want %q / %q",
					step.used, deviceTotal, state, reason, step.wantState, step.wantReason)
			}
		})
	}
}

// TestHeartbeatTellsTheAgentItWasCordonedImmediately: the Agent has to learn about
// the cordon in the answer to the heartbeat that caused it, not in the next one. A
// round trip of lag is a round trip in which the fleet has stopped placing on the
// host and the host does not know.
func TestHeartbeatTellsTheAgentItWasCordonedImmediately(t *testing.T) {
	f := newFixture(t)
	resp, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
		HostId:       hostA,
		AgentVersion: "0.1.0",
		Device:       &storagev1.DeviceStatus{TotalBytes: deviceTotal, UsedBytes: used(0.90)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetState() != storagev1.HostState_HOST_STATE_CORDONED {
		t.Fatalf("state in the answer = %v, want CORDONED", resp.GetState())
	}
	// A cordoned host is still serving what it holds, so it keeps its lease (§12.6).
	// Cordoning it and stopping its ACKs in the same heartbeat would turn a capacity
	// signal into an availability event.
	if resp.GetLeaseTtlSeconds() == 0 {
		t.Fatal("cordoning the host also took its lease away")
	}
}

// TestPressureNeverTouchesAnOperatorsCordon is ADR-0013 §5: the Control Plane's
// automatic reaction has authority over its own cordons and none over a human's. A
// human cordons a host for a cause the fleet cannot see — a NIC about to be
// replaced, a kernel about to be rebooted — and a device that happens to empty must
// not put it back into service.
func TestPressureNeverTouchesAnOperatorsCordon(t *testing.T) {
	f := newFixture(t)
	if state, _ := f.beat(t, used(0.10)); state != lifecycle.HostActive {
		t.Fatalf("state = %q, want ACTIVE", state)
	}
	if err := f.md.SetHostState(t.Context(), f.term, hostA, lifecycle.HostCordoned, lifecycle.CordonOperator); err != nil {
		t.Fatal(err)
	}

	// A heartbeat far below the release line: under a pressure cordon this is
	// exactly the report that returns the host to ACTIVE.
	state, reason := f.beat(t, used(0.01))
	if state != lifecycle.HostCordoned || reason != lifecycle.CordonOperator {
		t.Fatalf("a heartbeat cleared an operator's cordon: state = %q reason = %q", state, reason)
	}

	// And it stays refused across a full pressure cycle: rising past the cordon line
	// and falling back must not launder the operator's cordon into a pressure one
	// that the next quiet heartbeat is then free to clear.
	if state, reason := f.beat(t, used(0.90)); state != lifecycle.HostCordoned || reason != lifecycle.CordonOperator {
		t.Fatalf("pressure re-stamped an operator's cordon: state = %q reason = %q", state, reason)
	}
	if state, reason := f.beat(t, used(0.01)); state != lifecycle.HostCordoned || reason != lifecycle.CordonOperator {
		t.Fatalf("an operator's cordon was cleared after a pressure cycle: state = %q reason = %q", state, reason)
	}
}

// TestPressureLeavesDrainingAndDeadHostsAlone: a host being evacuated or asserted
// gone is already refusing placement, and cordoning it would overwrite a decision
// the Control Plane took for a stronger reason than fill — in the DEAD case, one
// that promotion reads as "the writer is gone" and skips the fencing wait for.
func TestPressureLeavesDrainingAndDeadHostsAlone(t *testing.T) {
	for _, state := range []lifecycle.HostState{lifecycle.HostDraining, lifecycle.HostDead} {
		t.Run(state.String(), func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
				HostId: hostA, AgentVersion: "0.1.0",
				Device: &storagev1.DeviceStatus{TotalBytes: deviceTotal, UsedBytes: used(0.10)},
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.md.SetHostState(t.Context(), f.term, hostA, state, lifecycle.CordonOperator); err != nil {
				t.Fatal(err)
			}
			if got, reason := f.beat(t, used(0.99)); got != state || reason != lifecycle.CordonNone {
				t.Fatalf("a %s host under pressure: state = %q reason = %q", state, got, reason)
			}
		})
	}
}

// refusingCordon is a store whose SetHostState always fails. Everything else is the
// real sim store, so the heartbeat is exercised end to end and only the one write
// the device measurement asks for is broken.
type refusingCordon struct {
	metadata.Store
	err error
}

func (r refusingCordon) SetHostState(context.Context, int64, string, lifecycle.HostState, lifecycle.CordonReason) error {
	return r.err
}

// TestACordonThatCannotBeWrittenDoesNotCostTheHostItsHeartbeat: by the time the
// pressure reaction runs, the heartbeat's own job — recording the device picture,
// renewing the lease — has already succeeded. Failing the RPC over a cordon the host
// can do nothing about would trade a capacity signal for an availability one, so a
// refused cordon is reported as the state that is actually stored.
//
// A stale term is the exception and is deliberately fatal: it means this process is
// a zombie and every write it believes it made is void (§7), which is exactly what
// the Agent must not be told the opposite of.
func TestACordonThatCannotBeWrittenDoesNotCostTheHostItsHeartbeat(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode connect.Code
		wantLive bool // the RPC answers, and the answer still renews the lease
	}{
		{"an operator's cordon won the race", lifecycle.ErrCordonHeld, 0, true},
		{"the host moved under us", lifecycle.ErrInvalidTransition, 0, true},
		{"this Control Plane is a zombie", metadata.ErrStaleTerm, connect.CodeAborted, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			// Registered through the real store first: the refusal is about the
			// cordon, not about a host the Control Plane has never seen.
			if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
				HostId: hostA, AgentVersion: "0.1.0",
				Device: &storagev1.DeviceStatus{TotalBytes: deviceTotal, UsedBytes: used(0.10)},
			}); err != nil {
				t.Fatal(err)
			}
			f.srv = cpserver.New(refusingCordon{Store: f.md, err: tc.err}, func() int64 { return f.term }, leaseTTL)

			resp, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
				HostId: hostA, AgentVersion: "0.1.0",
				Device: &storagev1.DeviceStatus{TotalBytes: deviceTotal, UsedBytes: used(0.90)},
			})
			if got := connect.CodeOf(err); tc.wantCode != 0 && got != tc.wantCode {
				t.Fatalf("code = %v, want %v (err=%v)", got, tc.wantCode, err)
			}
			if !tc.wantLive {
				return
			}
			if err != nil {
				t.Fatalf("the heartbeat failed over a cordon it could not write: %v", err)
			}
			if resp.GetLeaseTtlSeconds() == 0 {
				t.Fatal("a refused cordon took the host's lease renewal with it")
			}
			// The answer must be what the store holds, not what the handler wanted:
			// an Agent told CORDONED by a Control Plane that failed to cordon it
			// believes a fence that does not exist.
			if resp.GetState() != storagev1.HostState_HOST_STATE_ACTIVE {
				t.Fatalf("state = %v, want the ACTIVE the store still holds", resp.GetState())
			}
		})
	}
}

// TestPressureIgnoresAHostThatHasNotMeasuredItsDevice: total and used come from one
// statfs in one heartbeat, so a zero total is "no measurement", not "a full device".
// Reading 0/0 as 100% would cordon every host at registration.
func TestPressureIgnoresAHostThatHasNotMeasuredItsDevice(t *testing.T) {
	f := newFixture(t)
	if _, err := f.heartbeat(t, &storagev1.HeartbeatRequest{
		HostId: hostA, AgentVersion: "0.1.0", Device: &storagev1.DeviceStatus{},
	}); err != nil {
		t.Fatal(err)
	}
	h, err := f.md.GetHost(t.Context(), hostA)
	if err != nil {
		t.Fatal(err)
	}
	if h.State != lifecycle.HostActive {
		t.Fatalf("an unmeasured host was cordoned: state = %q", h.State)
	}
}
