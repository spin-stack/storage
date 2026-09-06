package dst

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// isolationScenarios drive the real agent.Loop against the real epoch object in the
// simulated store, under the one fault that decides the isolation response: whether the
// object store *answers*.
//
// The Agent's own tests cover the same rule with a witness of their own, and the seam
// between the two is exactly what this is for. That fake reports "no epoch recorded" with
// a sentinel it invents; production counts an answer as confirmation only when it is
// descriptor.ErrNoEpoch, and the two agree because a person kept them agreeing. Here the
// answer comes out of the store itself, so a volume whose epoch object has never been
// written — every volume placed before its first grant — is confirmed or paused by what
// descriptor really returns. Get that wrong and a partition of the Control Plane alone
// stops every guest on the host after the grace, which is the failure the whole file is
// arranged against and which no unit test with its own witness can see.
func isolationScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "an-unconfirmed-host-pauses-its-guest", Run: scenarioIsolationResponse},
	}
}

// errCPSilent is what a partitioned Control Plane looks like to the loop: every call
// fails, the process keeps its cadence, and nothing else changes.
var errCPSilent = errors.New("the control plane did not answer")

// scenarioIsolationResponse walks one host through a partition of both its paths.
//
//  1. The Control Plane goes silent and the bucket has no epoch object at all. The store
//     answering "nothing is recorded" is this host's claim standing, so the guest keeps
//     running however long the silence lasts.
//  2. The bucket goes silent too, and past the grace the guest is paused — and the volume
//     is *not* given up: no fence, nothing recorded, the chain still attached.
//  3. Both come back and the guest is resumed, which is the property that makes pausing
//     the right move rather than a slower way to lose a VM.
func scenarioIsolationResponse(s *Sim) error {
	ctx := context.Background()
	volumeID := ids.NewAt(1<<40, s.Rand).String()
	hostID := ids.NewAt(1<<40+1, s.Rand).String()

	cp := &isolationCP{leaseTTL: 30, desired: []*storagev1.DesiredVolume{{
		VolumeId:  volumeID,
		SizeBytes: 1 << 30,
		Epoch:     1,
		State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}}
	rec := &isolationReconciler{s: s, vols: map[string]agent.VolumeStatus{}}
	loop, err := agent.New(agent.Config{
		HostID:            hostID,
		AgentVersion:      "dst",
		MaxFormatVersion:  3,
		HeartbeatInterval: 10 * time.Second,
		RetryBackoff:      time.Second,
		LeaseTTL:          30 * time.Second,
	}, agent.Deps{
		Clock:        s.Clock,
		ControlPlane: cp,
		Device:       isolationDevice{},
		Volumes:      rec,
		Witness:      descriptor.EpochWitness{Store: s.Store},
	})
	if err != nil {
		return fmt.Errorf("building the agent: %w", err)
	}
	if err := loop.Reconcile(ctx); err != nil {
		return fmt.Errorf("the first cycle failed: %w", err)
	}
	if !slices.Contains(rec.served(), volumeID) {
		return errors.New("the host is not serving the volume the control plane asked for")
	}

	// Arm 1. The Control Plane stops answering, and the bucket holds no epoch for this
	// volume — which it answers, and which is not silence.
	cp.err = errCPSilent
	s.Emit(Event{Kind: EventFault, Msg: "the control plane stops answering"})
	for range 20 {
		s.Tick(10 * time.Second)
		_ = loop.Reconcile(ctx)
	}
	if paused := rec.paused(); len(paused) != 0 {
		return fmt.Errorf("guests paused while the object store was answering: %v — 'no epoch recorded' is the volume still being this host's", paused)
	}

	// Arm 2. The bucket goes silent as well. Now nothing confirms anything, and past the
	// grace this host cannot tell a partition from having been replaced.
	s.Store.InjectThrottleKey(descriptor.EpochKey(volumeID), 1_000)
	s.Emit(Event{Kind: EventFault, Msg: "the epoch object cannot be read either"})
	s.Tick(90 * time.Second)
	_ = loop.Reconcile(ctx)
	if paused := rec.paused(); !slices.Contains(paused, volumeID) {
		return fmt.Errorf("nothing confirmed this host's claim past the grace and its guest is still running: paused=%v", paused)
	}
	if rec.fenced[volumeID] {
		return errors.New("an isolated host gave the volume up; a pause must leave the chain attached and record no fork")
	}

	// Arm 3. The partition heals on both paths at once, and the bucket still records the
	// epoch this host holds. Both facts, the same two whose silence paused the guest.
	cp.err = nil
	s.Store.InjectThrottleKey(descriptor.EpochKey(volumeID), 0)
	if err := descriptor.WriteEpoch(ctx, s.Store, volumeID, 1); err != nil {
		return fmt.Errorf("recording the epoch this host holds: %w", err)
	}
	s.Emit(Event{Kind: EventNote, Msg: "both paths answer again"})
	s.Tick(time.Second)
	if err := loop.Reconcile(ctx); err != nil {
		return fmt.Errorf("the healing cycle failed: %w", err)
	}
	if paused := rec.paused(); len(paused) != 0 {
		return fmt.Errorf("still paused after both paths came back: %v", paused)
	}
	return nil
}

// isolationCP is a Control Plane that answers or does not. It holds no state worth
// asserting on — what this scenario reads is what the host did to its guest — so it
// scripts the two answers the loop needs and nothing else.
type isolationCP struct {
	err      error
	leaseTTL int32
	desired  []*storagev1.DesiredVolume
}

var _ storagev1connect.ControlPlaneServiceClient = (*isolationCP)(nil)

func (c *isolationCP) Heartbeat(_ context.Context, _ *connect.Request[storagev1.HeartbeatRequest]) (*connect.Response[storagev1.HeartbeatResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&storagev1.HeartbeatResponse{
		LeaseTtlSeconds: c.leaseTTL,
		State:           storagev1.HostState_HOST_STATE_ACTIVE,
		Term:            7,
	}), nil
}

func (c *isolationCP) GetDesiredState(_ context.Context, _ *connect.Request[storagev1.GetDesiredStateRequest]) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{Volumes: c.desired}), nil
}

func (c *isolationCP) ReportVolumeState(_ context.Context, _ *connect.Request[storagev1.ReportVolumeStateRequest]) (*connect.Response[storagev1.ReportVolumeStateResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&storagev1.ReportVolumeStateResponse{}), nil
}

func (c *isolationCP) GetVolumeKeys(_ context.Context, _ *connect.Request[storagev1.GetVolumeKeysRequest]) (*connect.Response[storagev1.GetVolumeKeysResponse], error) {
	if c.err != nil {
		return nil, c.err
	}
	return connect.NewResponse(&storagev1.GetVolumeKeysResponse{KekId: "dst", DekKeyId: 1}), nil
}

// isolationReconciler records what the loop told it to do with the guests. It reproduces
// one rule — a fenced volume is not started again at the same epoch — because without it
// the next Apply would undo every give-up and the arms above would assert on one cycle's
// worth of nothing.
type isolationReconciler struct {
	s      *Sim
	vols   map[string]agent.VolumeStatus
	fenced map[string]bool
}

var _ agent.VolumeReconciler = (*isolationReconciler)(nil)

func (r *isolationReconciler) Volumes(context.Context) ([]agent.VolumeStatus, error) {
	out := make([]agent.VolumeStatus, 0, len(r.vols))
	for _, v := range r.vols {
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b agent.VolumeStatus) int { return cmpString(a.VolumeID, b.VolumeID) })
	return out, nil
}

func (r *isolationReconciler) Apply(_ context.Context, desired []*storagev1.DesiredVolume) error {
	for _, d := range desired {
		if r.fenced[d.GetVolumeId()] {
			continue
		}
		v := r.vols[d.GetVolumeId()]
		v.VolumeID, v.Epoch = d.GetVolumeId(), d.GetEpoch()
		r.vols[d.GetVolumeId()] = v
	}
	return nil
}

func (r *isolationReconciler) Fence(_ context.Context, volumeIDs []string, why storagev1.VolumeRefusal, detail string) error {
	if r.fenced == nil {
		r.fenced = map[string]bool{}
	}
	for _, id := range volumeIDs {
		v := r.vols[id]
		v.VolumeID, v.Refusal, v.RefusalDetail = id, why, detail
		r.vols[id] = v
		r.fenced[id] = true
		r.s.Emit(Event{Kind: EventNote, VolumeID: id, Msg: "fenced: " + why.String()})
	}
	return nil
}

func (r *isolationReconciler) Isolate(_ context.Context, volumeIDs []string, paused bool) error {
	for _, id := range volumeIDs {
		v := r.vols[id]
		v.VolumeID = id
		v.Refusal = storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED
		msg := "guest resumed"
		if paused {
			v.Refusal = storagev1.VolumeRefusal_VOLUME_REFUSAL_ISOLATED
			msg = "guest paused: nothing confirms this host's claim"
		}
		r.vols[id] = v
		r.s.Emit(Event{Kind: EventNote, VolumeID: id, Msg: msg})
	}
	return nil
}

// served is the volumes this host reports itself as serving, refusals excluded.
func (r *isolationReconciler) served() []string {
	var out []string
	for id, v := range r.vols {
		if v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// paused is the volumes whose guests were stopped for isolation.
func (r *isolationReconciler) paused() []string {
	var out []string
	for id, v := range r.vols {
		if v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_ISOLATED {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// isolationDevice is a host whose disk never moves: the isolation response reads nothing
// from it, and a device that varied would be one more thing to explain in a failing trace.
type isolationDevice struct{}

func (isolationDevice) Usage(context.Context) (disk.Usage, error) {
	return disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}, nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
