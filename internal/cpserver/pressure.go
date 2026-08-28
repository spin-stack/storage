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
// The ADR names one threshold — 70% used — and one threshold is a flapping cordon: a host
// sitting on the line crosses it in both directions on consecutive heartbeats, and every
// crossing is a write that every placement decision in the fleet reads. So the band is a
// Schmitt trigger: cordon at 70%, return to ACTIVE only below 65%. Coming back requires
// freeing 5% of the device, which heartbeat jitter does not produce; the price is a dead
// zone where a host stays cordoned while admission would still allow it (85%,
// placement.DefaultMaxUsedRatio).
//
// **A dwell was rejected**: "clear for N heartbeats" needs per-host state — a column
// written on the busiest RPC there is, or Control Plane memory a leader change discards —
// and puts a clock into what is otherwise a pure function of two numbers already in the row.
//
// They are a caller-stated band rather than constants because CI is the caller: a GitHub
// runner's disk is 87% full and the Agent measures the whole filesystem holding
// --data-dir including other tenants (agent.NewDiskUsage), so every host in the e2e lane
// cordoned itself on its first heartbeat and nothing could be placed. The product was
// right and the lane was wrong.
type Band struct {
	// Cordon is the used ratio at which the Control Plane stops giving a host new
	// volumes; Uncordon is where it starts again, and is strictly lower.
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

// pressureTarget is the state the device measurement asks for, or ok=false when it asks
// for nothing. It is a pure function of the host row, which is what makes the hysteresis
// free: the previous decision is read back from the state and reason it wrote.
//
// Only two moves exist. ACTIVE at or above the cordon ratio → CORDONED; a DRAINING or DEAD
// host is left alone, since both already refuse placement and re-cordoning one
// mid-evacuation would undo a decision taken for a stronger reason than fill. CORDONED
// *for pressure* and below the uncordon ratio → ACTIVE; the reason check is what keeps a
// human's cordon from being cleared by a device that happened to empty (ADR-0013 §5).
//
// A host that has not measured its device (total 0) asks for nothing: total and used come
// from one statfs, so there is no state where one is known and the other is not.
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

// applyPressure runs the §3 reaction for one heartbeat and returns the host's state as the
// Agent should be told it — this heartbeat, or an Agent learns it was cordoned a round trip
// after the fleet stopped placing on it.
//
// Only a stale term fails the RPC: the heartbeat's own job has already succeeded, so
// failing over a cordon the host can do nothing about would cost it its renewal, while a
// stale term means this process is a zombie and every write it believes it made is void
// (§7). The other refusals are races with an operator writing the same row, and the
// operator winning is the outcome ADR-0013 §5 wants.
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
