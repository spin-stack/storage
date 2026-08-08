package cpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// The ADR-0013 §3 cordon band, as fractions of the device the host reports.
//
// CordonUsedRatio is the ADR's number: at 70% used the Control Plane stops giving
// the host new volumes. UncordonUsedRatio is not in the ADR, because the ADR names
// one threshold and one threshold is a flapping cordon — a host sitting on the line
// crosses it in both directions on consecutive heartbeats, and every crossing is a
// write, a state change every placement decision in the fleet reads, and a line in
// whatever an operator is watching. A cordon that flaps is worse than no cordon:
// placement becomes non-deterministic for reasons nothing records, and the signal
// stops being readable exactly when somebody is reading it.
//
// So the band is a Schmitt trigger: cordon at 70%, return to ACTIVE only below 65%.
// Coming back therefore requires the host to free 5% of its device — on the smallest
// device this would run on, gigabytes — which is not something heartbeat-to-heartbeat
// jitter produces. It is deliberately the *smallest* gap that is unambiguously real
// rather than a comfortable one: everything between 65% and 70% is a host that stays
// cordoned while it could still legally take work (admission's ceiling is 85%,
// placement.DefaultMaxUsedRatio), and that dead zone is the price of the hysteresis.
//
// **A dwell was rejected**, on its own and in addition to the band. A dwell — "stay
// cordoned until the pressure has been clear for N heartbeats" — needs state this
// does not: when the pressure last cleared, per host. That is either a new column
// written on every heartbeat of every host, on the busiest RPC there is, or memory
// in the Control Plane, which a leader change discards — and a fleet whose CP is
// failing over would then hold hosts cordoned indefinitely, each new leader
// restarting the dwell. It also puts a clock into a decision that is otherwise a
// pure function of two numbers already in the row, and under INV-01 every clock is
// an injected dependency. What a dwell catches that the band does not is a device
// that frees 5% of itself and refills it between two heartbeats; that is not noise,
// it is a host doing precisely what the cordon exists for, and cordoning it again is
// the right answer.
//
// These were constants, with the reasoning that ADR-0013 chose 70%, nobody had asked
// to tune it, and a knob added before a caller needs it is a knob whose default is
// the only value ever used. **That reasoning was right and CI is the caller.** The
// first run this repository ever had on a GitHub runner failed every placement in the
// e2e lane: the runner's disk is 87% full, the Agent measures the filesystem holding
// --data-dir *including other tenants* — deliberately, see agent.NewDiskUsage — so
// every host cordoned itself on its first heartbeat and nothing could be placed.
//
// The product was right and the lane was wrong, which is the useful half: a fleet that
// refuses to put new volumes on an 87%-full host is doing its job. What was wrong is
// that a lane testing placement was at the mercy of the runner's disk, and had been
// passing here only because this developer's /tmp is a roomy tmpfs.
//
// So they are a band the caller states, defaulting to the ADR's numbers. They sit
// beside -max-used-ratio and -max-oversubscription, which are the same kind of policy
// and were flags already.
type Band struct {
	// Cordon is the used ratio at which the Control Plane stops giving a host new
	// volumes; Uncordon is where it starts again. Uncordon is strictly lower, and the
	// gap is the hysteresis: one number would flap a host between ACTIVE and CORDONED
	// on a device sitting at the line.
	Cordon, Uncordon float64
}

// DefaultBand is ADR-0013 §3's cordon threshold, with the uncordon line the ADR does
// not name — it names one number, and one number is a flapping cordon.
func DefaultBand() Band { return Band{Cordon: 0.70, Uncordon: 0.65} }

// Validate refuses a band that would flap or that could never fire. A zero-width or
// inverted band is not a tuning choice, it is a typo that would show up as a host
// changing state on every heartbeat.
func (b Band) Validate() error {
	switch {
	case b.Cordon <= 0 || b.Cordon > 1:
		return fmt.Errorf("cpserver: cordon ratio %v is not in (0,1]", b.Cordon)
	case b.Uncordon <= 0 || b.Uncordon >= b.Cordon:
		return fmt.Errorf("cpserver: uncordon ratio %v must be above 0 and below the cordon ratio %v, or a host flaps", b.Uncordon, b.Cordon)
	}
	return nil
}

// pressureTarget is the state the device measurement asks for, or ok=false when it
// asks for nothing. It is a pure function of the host row, which is what makes the
// hysteresis free: the previous decision is not remembered anywhere, it is *read
// back* from the state and the reason the previous decision wrote.
//
// Only two moves exist, and the asymmetry between them is the band:
//
//   - ACTIVE and at or above the cordon ratio → CORDONED. A DRAINING or DEAD host is
//     left alone: both already refuse placement, and re-cordoning a host in the
//     middle of an evacuation would undo a decision the Control Plane took for a
//     stronger reason than fill.
//   - CORDONED *for pressure* and below the uncordon ratio → ACTIVE. The reason
//     check is what keeps a human's cordon from being cleared by a device that
//     happened to empty (ADR-0013 §5); the store refuses that write anyway, and
//     checking here means the refusal is never reached in the ordinary case.
//
// A host that has not measured its device (total 0) asks for nothing. That is the
// whole of the "no measurement yet" case: total and used come from one statfs in one
// heartbeat, so there is no state where one is known and the other is not.
func pressureTarget(h metadata.Host, band Band) (lifecycle.HostState, bool) {
	if h.NVMeTotalBytes <= 0 {
		return "", false
	}
	used := float64(h.NVMeUsedBytes) / float64(h.NVMeTotalBytes)
	switch {
	case h.State == lifecycle.HostActive && used >= band.Cordon:
		return lifecycle.HostCordoned, true
	case h.State == lifecycle.HostCordoned && h.CordonReason == lifecycle.CordonPressure && used < band.Uncordon:
		return lifecycle.HostActive, true
	default:
		return "", false
	}
}

// applyPressure runs the §3 reaction for one heartbeat and returns the host's state
// as the Agent should be told it — this heartbeat, not the next one, or an Agent
// learns it was cordoned a round trip after the fleet stopped placing on it.
//
// Only a stale term fails the RPC. The heartbeat's own job — recording the device
// picture and renewing the lease — has already succeeded by the time this runs, and
// failing the call would cost the host its renewal over a cordon it can do nothing
// about. A stale term is different in kind: it means this process is a zombie and
// every write it believes it made is void (§7), which the Agent must learn.
//
// The other refusals are races with an operator writing the same row between the
// read above and this write — ErrCordonHeld, or an illegal transition because the
// state moved. In both the operator won, which is the outcome ADR-0013 §5 wants, so
// they are logged and the host's state is reported as it was read.
func (s *Server) applyPressure(ctx context.Context, term int64, h metadata.Host) (lifecycle.HostState, error) {
	target, ok := pressureTarget(h, s.band)
	if !ok {
		return h.State, nil
	}
	err := s.md.SetHostState(ctx, term, h.HostID, target, lifecycle.CordonPressure)
	switch {
	case err == nil:
		slog.InfoContext(ctx, "device pressure changed a host's fleet state",
			"host_id", h.HostID, "from", h.State, "to", target,
			"nvme_used_bytes", h.NVMeUsedBytes, "nvme_total_bytes", h.NVMeTotalBytes)
		return target, nil
	case errors.Is(err, metadata.ErrStaleTerm):
		return "", fmt.Errorf("cpserver: cordoning host %q on device pressure: %w", h.HostID, err)
	default:
		slog.WarnContext(ctx, "device pressure could not change a host's fleet state",
			"host_id", h.HostID, "from", h.State, "to", target, "error", err)
		return h.State, nil
	}
}
