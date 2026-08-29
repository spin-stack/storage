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

// stalledPublish reports whether any volume this host holds has a sealed layer it tried
// and failed to get into the object store.
//
// A listing per heartbeat, and it is the same listing applyPressure's decision would need
// if capacity were not derived: the host's own volumes, which is a single indexed read on
// primary_host_id. Cheaper than a column on hosts, and truthful for a reason a column
// would not be — it is derived from the reports, so a volume that moves away takes its
// stall with it and no sweep has to notice.
func (s *Server) stalledPublish(ctx context.Context, hostID string) (bool, string, error) {
	vols, err := s.md.ListVolumesByHost(ctx, hostID)
	if err != nil {
		return false, "", fmt.Errorf("cpserver: reading the volumes of host %q: %w", hostID, err)
	}
	for _, v := range vols {
		if v.Progress.PublishStalled {
			return true, v.VolumeID, nil
		}
	}
	return false, "", nil
}

// applyStall is v6 §11's other half: a host that cannot get its layers into the object
// store stops taking new volumes.
//
// The failure is silent by construction. §15 promises the VM keeps running when the object
// store is unreachable, so the host reports a healthy device, a healthy lease and volumes
// in ACTIVE — and placement goes on sending it more, each of which inherits a host that
// cannot publish, until the filesystem fills and QEMU hands every guest ENOSPC.
//
// Cordoned rather than drained or fenced. Nothing about the volumes here is wrong and the
// store may come back in a minute; moving them would cost more than it saves, and the
// cordon stops only what is about to become a problem, which is the *next* volume. It
// clears itself on the report that carries no stall — no sweep, nothing to notice a
// recovery, the same shape the volume refusal beside it has.
//
// A threshold would have been the other design and there is nothing to set one from: §11
// says to alarm on the backlog, and how many bytes are too many is a property of the disk,
// the guest and the store. "The host tried and it did not work" needs no number.
func (s *Server) applyStall(ctx context.Context, term int64, h metadata.Host, state lifecycle.HostState) (lifecycle.HostState, error) {
	stalled, volumeID, err := s.stalledPublish(ctx, h.HostID)
	if err != nil {
		return state, err
	}
	target, ok := stallTarget(state, h.CordonReason, stalled)
	if !ok {
		return state, nil
	}
	if err := s.md.SetHostState(ctx, term, h.HostID, target, lifecycle.CordonStalledPublish); err != nil {
		if errors.Is(err, metadata.ErrStaleTerm) {
			return "", fmt.Errorf("cpserver: cordoning host %q for a stalled publish: %w", h.HostID, err)
		}
		// A race with an operator writing the same row, and the operator winning is the
		// outcome ADR-0013 §5 wants.
		slog.WarnContext(ctx, "a stalled publish could not change a host's fleet state",
			"host_id", h.HostID, "from", state, "to", target, "error", err)
		return state, nil
	}
	slog.WarnContext(ctx, "a stalled publish changed a host's fleet state",
		"host_id", h.HostID, "from", state, "to", target, "volume_id", volumeID)
	return target, nil
}

// stallTarget is the move, as a pure function of the row and the fact — the same shape
// pressureTarget has, and for the same reason: the previous decision is read back from the
// state and reason it wrote, so there is no per-host memory anywhere.
//
// No hysteresis, and it is not an oversight. The device band needs one because a host
// sitting on a fill line crosses it in both directions on consecutive heartbeats; this is
// not a line, it is an attempt that failed, and a host that alternates between publishing
// and not is a host whose object store is alternating — which is a thing to see rather
// than to smooth away.
func stallTarget(state lifecycle.HostState, reason lifecycle.CordonReason, stalled bool) (lifecycle.HostState, bool) {
	switch {
	case stalled && state == lifecycle.HostActive:
		return lifecycle.HostCordoned, true
	case !stalled && state == lifecycle.HostCordoned && reason == lifecycle.CordonStalledPublish:
		return lifecycle.HostActive, true
	default:
		// Everything else is left alone: a DRAINING or DEAD host already refuses
		// placement, and a cordon placed for another reason is not this one's to clear.
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
