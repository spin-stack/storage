// Package cpserver serves the Agent-facing RPC surface (api/spin/storage/v1) over
// the Control Plane's existing libraries. It is a translation layer and deliberately
// nothing more: every rule it appears to enforce — the term guard, the lifecycle
// transitions, the watermark ordering — is enforced inside metadata.Store's writes,
// which is where a second Control Plane cannot get between a read and a write.
//
// Two things live here that live nowhere else, and both are reactions to what a host
// reports rather than rules about a write:
//
//   - the *epoch qualification* of a volume report (§12.3): metadata.UpdateWatermarks
//     is monotonic per column but has no notion of who is reporting, so refusing a
//     fenced writer's report is done here, by comparing what the report claims
//     against what the volume says;
//   - the *device-pressure cordon* (ADR-0013 §3): the heartbeat carries the only
//     measurement of the device there is, and the fleet's answer to it — cordon at
//     70% used — is the Control Plane's alone (ADR-0013 §5). See pressure.go.
package cpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Server implements the ControlPlaneService handler over a metadata store.
type Server struct {
	md       metadata.Store
	term     func() int64
	leaseTTL time.Duration
	// band is the device-pressure cordon policy (ADR-0013 §3). It is stated by the
	// caller rather than compiled in — see cpserver.Band for why the constants stopped
	// being constants, which is a story about CI rather than about tuning.
	band Band
}

var _ storagev1connect.ControlPlaneServiceHandler = (*Server)(nil)

// New returns a Server. term is read per call rather than captured once: the term a
// process holds is the one its Elector granted (ADR-0011), and a process that loses
// it must start failing immediately, not from its next restart.
func New(md metadata.Store, term func() int64, leaseTTL time.Duration, band Band) *Server {
	return &Server{md: md, term: term, leaseTTL: leaseTTL, band: band}
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
		// RemoteBacklogBytes is carried through from the wire without being read.
		// Every Agent sends 0 and no decision here branches on it; metadata.Host
		// says what the field is and what deleting it would cost.
		RemoteBacklogBytes: dev.GetRemoteBacklogBytes(),
	})
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: upserting host %q: %w", msg.GetHostId(), err))
	}

	host, err := s.md.GetHost(ctx, msg.GetHostId())
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: reading host %q: %w", msg.GetHostId(), err))
	}

	// The device measurement the host just reported is the only one the fleet has,
	// and cordoning on it is the Control Plane's job alone (ADR-0013 §5): the Agent
	// gets local defensive powers — backpressure, refusing attaches — and moving
	// volumes stays here, because two actors evacuating one host is the bug two
	// earlier waves spent their effort closing. See pressure.go for the band.
	state, err := s.applyPressure(ctx, term, host)
	if err != nil {
		return nil, rpcError(err)
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
		State:           hostState(state),
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
	// One query for the whole host rather than one per volume: a poll that is mostly
	// "nothing to do" should cost one round trip, and it is on the Agent's hot loop.
	pending, err := s.md.ListPendingSnapshots(ctx, hostID)
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: listing the pending snapshots of %q: %w", hostID, err))
	}
	oldestPending := make(map[string]string, len(pending))
	for _, snap := range pending {
		// Oldest first, and the list is ordered, so the first id seen for a volume wins.
		if _, ok := oldestPending[snap.VolumeID]; !ok {
			oldestPending[snap.VolumeID] = snap.SnapshotID
		}
	}

	out := make([]*storagev1.DesiredVolume, 0, len(vols))
	for _, v := range vols {
		d := &storagev1.DesiredVolume{
			VolumeId:  v.VolumeID,
			SizeBytes: v.SizeBytes,
			BlockSize: v.BlockSize,
			Epoch:     v.CurrentEpoch,
			State:     volumeState(v.State),
			// The two watermarks the catalog holds, sent back to the host that will
			// serve the volume. They are the only facts on this message the Agent does
			// not otherwise have, and they are what lets it tell "this volume is new"
			// from "this volume's data is missing": an attach that finds no image, or
			// replays below the sequence a guest's fsync already returned on, is a
			// volume that has lost data rather than one that never had any.
			//
			// Copied verbatim rather than derived. metadata.Volume calls them
			// informative (§5.8) and that is still true of what the Control Plane does
			// with them — it takes no decision on either — but "informative" was read as
			// "not worth sending", and the Agent was then left deciding with the one
			// authority that cannot distinguish the two cases, the bucket.
			PublishedSequence: v.PublishedSequence,
			DurableSequence:   v.DurableSequence,
		}
		// A clone reads through its parent's objects (§20), and the Agent cannot look
		// the chain up itself (ADR-0021). The parent's *volume* id lives on the
		// snapshot row, so it is read here — one lookup per clone, on a path that
		// already reads the volume — rather than duplicated into volumes, where it
		// could disagree with the snapshot it names.
		if v.ParentSnapshotID != "" {
			snap, serr := s.md.GetSnapshot(ctx, v.ParentSnapshotID)
			if serr != nil {
				// Refused, not degraded: a clone served without its chain reads zeros,
				// and zeros are indistinguishable from a volume nobody wrote to.
				return nil, rpcError(fmt.Errorf("cpserver: volume %s names parent snapshot %s: %w",
					v.VolumeID, v.ParentSnapshotID, serr))
			}
			d.ParentSnapshotId = v.ParentSnapshotID
			d.ParentVolumeId = snap.VolumeID
		}
		d.PendingSnapshotId = oldestPending[v.VolumeID]
		out = append(out, d)
	}
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{Volumes: out}), nil
}

// GetVolumeKeys hands one volume's wrapped DEK to the host that writes it.
//
// It is a call of its own rather than a field of the desired state (see the proto
// for the full reasoning), and this handler is why: the answer is authorised per
// request, against the volume's current primary. A host that has been fenced, or
// that never held the volume, is refused here — a check with no equivalent inside a
// list answer, whose only unit is the whole list.
//
// The refusal is PermissionDenied rather than NotFound. Hiding the volume's
// existence buys nothing from a caller that already had to authenticate as a host in
// this fleet, and it costs the operator the one message that explains what happened:
// "you are not this volume's writer any more" is the fencing story, and it is what
// an Agent's log should say at 3am.
func (s *Server) GetVolumeKeys(ctx context.Context, req *connect.Request[storagev1.GetVolumeKeysRequest]) (*connect.Response[storagev1.GetVolumeKeysResponse], error) {
	hostID, volumeID := req.Msg.GetHostId(), req.Msg.GetVolumeId()
	switch {
	case hostID == "":
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cpserver: host_id is required"))
	case volumeID == "":
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cpserver: volume_id is required"))
	}

	v, err := s.md.GetVolume(ctx, volumeID)
	if err != nil {
		return nil, rpcError(fmt.Errorf("cpserver: reading volume %q: %w", volumeID, err))
	}
	if v.PrimaryHostID != hostID {
		return nil, connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("cpserver: host %q is not the writer of volume %q", hostID, volumeID))
	}

	return connect.NewResponse(&storagev1.GetVolumeKeysResponse{
		VolumeId:   v.VolumeID,
		DekWrapped: v.DEKWrapped,
		KekId:      v.KEKID,
		DekKeyId:   v.DEKKeyID,
	}), nil
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
	if err := s.applyRefusal(ctx, term, hostID, r); err != nil {
		return 0, err
	}
	if err := s.applySnapshotReport(ctx, term, hostID, r); err != nil {
		return 0, err
	}
	return storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED, nil
}

// applyRefusal records whether the reporting host is serving this volume, and why it is
// not. It runs on every accepted report, including the ones that say nothing is wrong —
// that is what clears a refusal when the volume comes back, and it is why there is no
// sweep and nothing that has to notice a recovery.
//
// It is a separate write from the watermarks above rather than three more columns on
// that statement, because the two facts want opposite storage. A watermark is monotonic
// and merged with GREATEST, so applying a late report is harmless. A refusal is a state,
// so it is last-report-wins — and "last" has to exclude a writer the fleet has moved
// past, which is what SetVolumeRefusal's host-and-epoch predicate does. Folding them
// together would force one of the two rules onto the other: GREATEST over a refusal has
// no meaning, and a host-and-epoch predicate on the watermarks would drop exactly the
// late reports §12.3 wants merged.
//
// The epoch check above already refused a stale report, so the predicate looks redundant
// from here. It is not: between that read and this write the volume can be promoted, and
// the window is the whole failure — a fenced host marking a volume NOT SERVED while its
// successor is serving it perfectly well, with the term guard passing because promotion
// does not move the CP term.
func (s *Server) applyRefusal(ctx context.Context, term int64, hostID string, r *storagev1.VolumeReport) error {
	refusal, err := refusalOf(r.GetRefusal())
	if err != nil {
		// An Agent from the future naming a refusal this Control Plane has never heard
		// of. Refused rather than stored as unknown: the column's CHECK would refuse it
		// anyway, and failing the RPC is what makes a fleet running two versions visible
		// instead of quietly losing one host's answers.
		return fmt.Errorf("cpserver: volume %q: %w", r.GetVolumeId(), err)
	}
	detail := r.GetRefusalDetail()
	if refusal == lifecycle.RefusalNone {
		// A sentence with nothing to explain outlives its cause, which is the one way a
		// reason column misleads. Cleared here as well as in the statement, so the two
		// stores answer the same way rather than one relying on a CASE the other lacks.
		detail = ""
	}
	if err := s.md.SetVolumeRefusal(ctx, term, r.GetVolumeId(), hostID, r.GetEpoch(), refusal, detail); err != nil {
		return fmt.Errorf("cpserver: recording that %q is not being served: %w", r.GetVolumeId(), err)
	}
	if refusal.Refused() {
		// One line per refused report, not one per transition: this repeats every few
		// seconds for as long as the condition lasts, which is what an operator who
		// arrives an hour later needs — a line on the transition alone is a line they
		// have to go looking for. The same reasoning as the Agent's own holding lines.
		slog.WarnContext(ctx, "a host is refusing to serve a volume the fleet placed on it",
			"volume_id", r.GetVolumeId(), "host_id", hostID, "epoch", r.GetEpoch(),
			"refusal", refusal.String(), "detail", detail)
	}
	return nil
}

// refusalOf maps the wire enum onto the stored vocabulary. It is a switch and not a
// string conversion of the enum's name so that the wire and the column can be renamed
// independently, and so an unknown value is an error rather than a row Postgres rejects
// three layers further down with no volume id in the message.
func refusalOf(r storagev1.VolumeRefusal) (lifecycle.Refusal, error) {
	switch r {
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED:
		return lifecycle.RefusalNone, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING:
		return lifecycle.RefusalImageMissing, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST:
		return lifecycle.RefusalDurabilityLost, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_NO_READ_VIEW:
		return lifecycle.RefusalNoReadView, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_NO_KEY:
		return lifecycle.RefusalNoKey, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST:
		return lifecycle.RefusalLeaseLost, nil
	case storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED:
		return lifecycle.RefusalAttachFailed, nil
	default:
		return "", fmt.Errorf("%w: volume refusal %d", lifecycle.ErrUnknownState, r)
	}
}

// applySnapshotReport records the outcome of a snapshot this host was asked to take.
//
// It runs after the epoch check, and that is the point: a host the fleet has moved past
// froze a view of a volume it no longer writes, and stamping its sequence into the
// catalog would publish a snapshot of a state that was superseded. A stale report leaves
// the row CREATING, which is the right answer — the volume's current writer still has
// the request in its desired state.
func (s *Server) applySnapshotReport(ctx context.Context, term int64, hostID string, r *storagev1.VolumeReport) error {
	snapID := r.GetSnapshotId()
	if snapID == "" {
		return nil
	}
	if msg := r.GetSnapshotError(); msg != "" {
		// FAILED is terminal, and it is what stops the request being re-sent. Leaving it
		// CREATING so the Agent retries would loop forever on the failures that do not
		// heal — a key that cannot open the parent, a store that refuses the write — and
		// the ones that do heal are already covered, because the Agent retries within
		// the session before it reports anything at all.
		if err := s.md.SetSnapshotState(ctx, term, snapID, lifecycle.SnapshotFailed); err != nil {
			return fmt.Errorf("cpserver: recording snapshot %q as failed: %w", snapID, err)
		}
		slog.WarnContext(ctx, "snapshot failed on its host",
			"snapshot_id", snapID, "volume_id", r.GetVolumeId(), "host_id", hostID, "error", msg)
		return nil
	}
	// Computed, not believed: the key is a function of the two ids, and taking the
	// Agent's word for it would let the catalog point somewhere the reader does not look.
	key, err := image.SnapshotKeyFor(r.GetVolumeId(), snapID)
	if err != nil {
		return fmt.Errorf("cpserver: snapshot %q: %w", snapID, err)
	}
	if err := s.md.PublishSnapshot(ctx, term, snapID, r.GetSnapshotSequence(), hostID, key); err != nil {
		return fmt.Errorf("cpserver: publishing snapshot %q: %w", snapID, err)
	}
	return nil
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
