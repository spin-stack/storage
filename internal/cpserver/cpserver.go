// Package cpserver serves the Agent-facing RPC surface (api/spin/storage/v1) over
// the Control Plane's existing libraries. It is a translation layer and deliberately
// nothing more: every rule it appears to enforce — the term guard, the lifecycle
// transitions, the watermark ordering — is enforced inside metadata.Store's writes,
// which is where a second Control Plane cannot get between a read and a write.
//
// What lives here that lives nowhere else is the *epoch qualification* of a volume
// report (§12.3): metadata.UpdateWatermarks is monotonic per column but has no
// notion of who is reporting, so refusing a fenced writer's report is done here, by
// comparing what the report claims against what the volume says.
package cpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Server implements the ControlPlaneService handler over a metadata store.
type Server struct {
	md       metadata.Store
	term     func() int64
	leaseTTL time.Duration
}

var _ storagev1connect.ControlPlaneServiceHandler = (*Server)(nil)

// New returns a Server. term is read per call rather than captured once: the term a
// process holds is the one its Elector granted (ADR-0011), and a process that loses
// it must start failing immediately, not from its next restart.
func New(md metadata.Store, term func() int64, leaseTTL time.Duration) *Server {
	return &Server{md: md, term: term, leaseTTL: leaseTTL}
}

// Handler returns the mounted Connect handler: the path prefix the generated client
// expects, and the http.Handler that serves it.
func Handler(s *Server, opts ...connect.HandlerOption) http.Handler {
	path, h := storagev1connect.NewControlPlaneServiceHandler(s, opts...)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	return mux
}

// Heartbeat records what the host reports about itself and renews its lease.
func (s *Server) Heartbeat(ctx context.Context, req *connect.Request[storagev1.HeartbeatRequest]) (*connect.Response[storagev1.HeartbeatResponse], error) {
	msg := req.Msg
	if msg.GetHostId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cpserver: host_id is required"))
	}
	term := s.term()

	// UpsertHost deliberately does not carry the fleet state or the capacity ledger
	// (§28.1/§28.2). ACTIVE is only read when the row is new — the first time a host
	// introduces itself — and is ignored on every heartbeat after that, so this
	// cannot un-cordon a draining host.
	dev := msg.GetDevice()
	err := s.md.UpsertHost(ctx, term, metadata.Host{
		HostID:           msg.GetHostId(),
		State:            lifecycle.HostActive,
		AgentVersion:     msg.GetAgentVersion(),
		MaxFormatVersion: msg.GetMaxFormatVersion(),
		NVMeTotalBytes:   dev.GetTotalBytes(),
		NVMeUsedBytes:    dev.GetUsedBytes(),
		// dev.RemoteBacklogBytes has nowhere to go yet: the hosts table has no
		// column for it. It is the one number ADR-0013 needs that the schema does
		// not carry, and adding it belongs to whoever owns internal/schema.
	})
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: upserting host %q: %w", msg.GetHostId(), err))
	}

	host, err := s.md.GetHost(ctx, msg.GetHostId())
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: reading host %q: %w", msg.GetHostId(), err))
	}

	// A DEAD host is refused a lease rather than an answer. The Agent needs to tell
	// "the Control Plane is unreachable" from "the Control Plane will not renew me":
	// the first is a network problem it retries through, the second is a fence it
	// must respect, and an RPC error would look like the first.
	ttl := s.leaseTTL
	err = s.md.RenewHostLease(ctx, term, msg.GetHostId(), int(s.leaseTTL/time.Second))
	switch {
	case errors.Is(err, metadata.ErrHostNotServing):
		ttl = 0
	case err != nil:
		return nil, rpcError(fmt.Errorf("cpserver: renewing the lease of %q: %w", msg.GetHostId(), err))
	}

	return connect.NewResponse(&storagev1.HeartbeatResponse{
		LeaseTtlSeconds: int32(ttl / time.Second),
		State:           hostState(host.State),
		Term:            term,
	}), nil
}

// GetDesiredState returns the volumes whose primary is this host.
func (s *Server) GetDesiredState(ctx context.Context, req *connect.Request[storagev1.GetDesiredStateRequest]) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	hostID := req.Msg.GetHostId()
	if hostID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cpserver: host_id is required"))
	}
	vols, err := s.md.ListVolumesByHost(ctx, hostID)
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: listing the volumes of %q: %w", hostID, err))
	}
	out := make([]*storagev1.DesiredVolume, 0, len(vols))
	for _, v := range vols {
		out = append(out, &storagev1.DesiredVolume{
			VolumeId:   v.VolumeID,
			SizeBytes:  v.SizeBytes,
			BlockSize:  v.BlockSize,
			Epoch:      v.CurrentEpoch,
			State:      volumeState(v.State),
			Durability: durability(v.Durability),
		})
	}
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{Volumes: out}), nil
}

// ReportVolumeState applies epoch-qualified watermarks, one answer per report.
func (s *Server) ReportVolumeState(ctx context.Context, req *connect.Request[storagev1.ReportVolumeStateRequest]) (*connect.Response[storagev1.ReportVolumeStateResponse], error) {
	hostID := req.Msg.GetHostId()
	if hostID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cpserver: host_id is required"))
	}
	term := s.term()

	results := make([]*storagev1.VolumeReportResult, 0, len(req.Msg.GetVolumes()))
	for _, r := range req.Msg.GetVolumes() {
		outcome, err := s.applyReport(ctx, term, hostID, r)
		if err != nil {
			return nil, rpcError(err)
		}
		results = append(results, &storagev1.VolumeReportResult{VolumeId: r.GetVolumeId(), Outcome: outcome})
	}
	return connect.NewResponse(&storagev1.ReportVolumeStateResponse{Results: results}), nil
}

// applyReport decides one report's fate. A returned error is an infrastructure
// failure — a stale term, an unreachable store — and fails the whole RPC; anything
// the report itself got wrong comes back as an outcome, because the Agent has to
// learn it (a refusal is how it discovers it was fenced).
func (s *Server) applyReport(ctx context.Context, term int64, hostID string, r *storagev1.VolumeReport) (storagev1.ReportOutcome, error) {
	if r.GetVolumeId() == "" {
		return storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME, nil
	}
	v, err := s.md.GetVolume(ctx, r.GetVolumeId())
	switch {
	case errors.Is(err, metadata.ErrNotFound), errors.Is(err, metadata.ErrInvalidID):
		return storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME, nil
	case err != nil:
		return 0, fmt.Errorf("cpserver: reading volume %q: %w", r.GetVolumeId(), err)
	}
	if v.PrimaryHostID != hostID {
		return storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY, nil
	}
	// The epoch is the qualification the watermarks never had: a report from a
	// writer the fleet has moved past names an epoch that is no longer current, and
	// applying it would let a fenced host's numbers stand as the volume's own
	// (§12.3). An epoch *ahead* of the volume's is equally refused — nothing has
	// granted it, so it names a volume this Control Plane does not know about.
	if v.CurrentEpoch != r.GetEpoch() {
		return storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH, nil
	}

	err = s.md.UpdateWatermarks(ctx, term, r.GetVolumeId(), r.GetLocalSequence(), r.GetDurableSequence(), r.GetPublishedSequence())
	switch {
	case errors.Is(err, metadata.ErrWatermarkOrder):
		return storagev1.ReportOutcome_REPORT_OUTCOME_OUT_OF_ORDER, nil
	case err != nil:
		return 0, fmt.Errorf("cpserver: updating the watermarks of %q: %w", r.GetVolumeId(), err)
	}
	return storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED, nil
}

// rpcError maps a store error onto a Connect code. The mapping matters at 3am: an
// Aborted answer says "this Control Plane is a zombie, its term is gone", which is
// a different incident from an Internal one.
func rpcError(err error) error {
	switch {
	case errors.Is(err, metadata.ErrStaleTerm):
		return connect.NewError(connect.CodeAborted, err)
	case errors.Is(err, metadata.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, metadata.ErrInvalidID), errors.Is(err, lifecycle.ErrUnknownState):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, metadata.ErrHostNotServing), errors.Is(err, lifecycle.ErrInvalidTransition):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func hostState(s lifecycle.HostState) storagev1.HostState {
	switch s {
	case lifecycle.HostActive:
		return storagev1.HostState_HOST_STATE_ACTIVE
	case lifecycle.HostCordoned:
		return storagev1.HostState_HOST_STATE_CORDONED
	case lifecycle.HostDraining:
		return storagev1.HostState_HOST_STATE_DRAINING
	case lifecycle.HostDead:
		return storagev1.HostState_HOST_STATE_DEAD
	default:
		return storagev1.HostState_HOST_STATE_UNSPECIFIED
	}
}

func volumeState(s lifecycle.VolumeState) storagev1.VolumeState {
	switch s {
	case lifecycle.VolumeActive:
		return storagev1.VolumeState_VOLUME_STATE_ACTIVE
	case lifecycle.VolumePrimarySuspected:
		return storagev1.VolumeState_VOLUME_STATE_PRIMARY_SUSPECTED
	case lifecycle.VolumeFencingWait:
		return storagev1.VolumeState_VOLUME_STATE_FENCING_WAIT
	case lifecycle.VolumeRecoveryRequired:
		return storagev1.VolumeState_VOLUME_STATE_RECOVERY_REQUIRED
	case lifecycle.VolumeRecovering:
		return storagev1.VolumeState_VOLUME_STATE_RECOVERING
	case lifecycle.VolumeDetached:
		return storagev1.VolumeState_VOLUME_STATE_DETACHED
	default:
		return storagev1.VolumeState_VOLUME_STATE_UNSPECIFIED
	}
}

func durability(d lifecycle.Durability) storagev1.Durability {
	switch d {
	case lifecycle.DurabilityRemote:
		return storagev1.Durability_DURABILITY_REMOTE
	case lifecycle.DurabilityLocal:
		return storagev1.Durability_DURABILITY_LOCAL
	default:
		return storagev1.Durability_DURABILITY_UNSPECIFIED
	}
}
