package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// Deps are the Agent's injected collaborators (INV-01). None of them may be nil
// except Recorder, whose nil is a working no-op (telemetry is never load-bearing).
type Deps struct {
	Clock        clock.Clock
	ControlPlane storagev1connect.ControlPlaneServiceClient
	Device       Device
	Volumes      VolumeSource
	Recorder     *obs.Recorder
}

// Loop is the Agent's reconciliation loop: heartbeat, read the desired state, serve
// it, report. It owns no data path of its own — a VolumeManager does, and the loop
// hands it the desired state and reports what it observes afterwards. That split is
// ADR-0021: spin's runner must be able to take the manager without taking this loop.
type Loop struct {
	cfg  Config
	clk  clock.Clock
	cp   storagev1connect.ControlPlaneServiceClient
	dev  Device
	vols VolumeSource
	rec  *obs.Recorder

	// reconcile is the volume source when it can also be *told* what to serve — a
	// VolumeManager. It is discovered from Deps.Volumes rather than configured
	// separately, because the thing that reports what is being served and the thing
	// that decides what is being served must be the same object or they will disagree.
	// A plain VolumeSource (agent.VolumeSet, and every test that drives one) leaves it
	// nil, and the loop then records the desired state without acting on it.
	reconcile VolumeReconciler

	mu       sync.Mutex
	lease    *lease.Manager
	leaseTTL time.Duration
	desired  []*storagev1.DesiredVolume
	state    storagev1.HostState
	fenced   []string
	failures int
	// keys is the key material this Agent has been handed, by volume id. It is a
	// cache with one eviction rule and no expiry: an entry lives exactly as long as
	// its volume stays in the desired state (see readDesiredState). Key material
	// does not change while the volume is this host's, and when it stops being this
	// host's the entry must go — not because it would be stale, but because holding
	// it means holding the means to open a volume the fleet has taken away.
	keys map[string]VolumeKeys
}

// New validates the configuration and the wiring and returns a Loop.
func New(cfg Config, deps Deps) (*Loop, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch {
	case deps.Clock == nil:
		return nil, errors.New("agent: a clock must be injected (INV-01)")
	case deps.ControlPlane == nil:
		return nil, errors.New("agent: a Control Plane client must be injected")
	case deps.Device == nil:
		return nil, errors.New("agent: a device must be injected")
	case deps.Volumes == nil:
		return nil, errors.New("agent: a volume source must be injected")
	}
	l := &Loop{
		cfg:  cfg,
		clk:  deps.Clock,
		cp:   deps.ControlPlane,
		dev:  deps.Device,
		vols: deps.Volumes,
		rec:  deps.Recorder,
		keys: map[string]VolumeKeys{},
	}
	if r, ok := deps.Volumes.(VolumeReconciler); ok {
		l.reconcile = r
	}
	return l, nil
}

// Run reconciles until ctx is done, returning ctx's error. The first cycle runs
// immediately; afterwards the delay is one heartbeat interval, or the retry backoff
// while cycles keep failing.
func (l *Loop) Run(ctx context.Context) error {
	var delay time.Duration
	for {
		if delay > 0 {
			if err := l.clk.Sleep(ctx, delay); err != nil {
				return err
			}
		}
		err := l.Reconcile(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		delay = l.nextDelay(err)
		if err != nil {
			// The backoff above keeps the Agent trying, which is right; saying nothing
			// is not. An Agent that can never succeed — wrong Control Plane URL, a host
			// id the database refuses, expired credentials — otherwise behaves exactly
			// like a healthy one from the outside: it logs a line at startup and then
			// heartbeats into nothing forever. Every increment on top of this loop is
			// debugged through it.
			//
			// One line per failed cycle, not per retry, and it carries the delay: "it
			// failed" without "and I retry in 1s" reads as fatal to whoever is watching.
			slog.Warn("reconciliation cycle failed",
				"error", err,
				"retry_in", delay,
				"host_id", l.cfg.HostID)
		}
	}
}

// nextDelay records the outcome of a cycle and returns how long to wait before the
// next one: the plain interval after a success, an exponential backoff capped at the
// interval while failures continue. The cap matters — the Control Plane must hear
// from a host at least as often as the fleet's own timers assume, even a sick one.
func (l *Loop) nextDelay(err error) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil {
		l.failures = 0
		return l.cfg.HeartbeatInterval
	}
	l.failures++
	delay := l.cfg.RetryBackoff
	for range l.failures - 1 {
		delay *= 2
		if delay >= l.cfg.HeartbeatInterval {
			return l.cfg.HeartbeatInterval
		}
	}
	return delay
}

// Reconcile runs one cycle: heartbeat, read the desired state, report what this
// host observed. It stops at the first failure — a Control Plane that did not
// answer the heartbeat has nothing useful to say to the rest of the cycle — and the
// lease is left to run down, which is what fences this host if the condition lasts.
func (l *Loop) Reconcile(ctx context.Context) error {
	usage, err := l.dev.Usage(ctx)
	if err != nil {
		return fmt.Errorf("agent: reading the device: %w", err)
	}
	vols, err := l.vols.Volumes(ctx)
	if err != nil {
		return fmt.Errorf("agent: reading the served volumes: %w", err)
	}

	if err := l.heartbeat(ctx, usage, vols); err != nil {
		return err
	}
	if err := l.readDesiredState(ctx); err != nil {
		return err
	}

	// Re-read before reporting. The set above is what the cycle *started* with, and
	// readDesiredState has since started and stopped runtimes: reporting the old set
	// would put every volume one cycle behind, and a volume started and stopped within
	// a single cycle would never be reported at all. The heartbeat still uses the
	// earlier picture, and must — it renews the lease, and nothing may come between
	// that and the instant it was anchored to (§12.2).
	served, err := l.vols.Volumes(ctx)
	if err != nil {
		return fmt.Errorf("agent: reading the served volumes: %w", err)
	}
	if err := l.report(ctx, served); err != nil {
		return err
	}
	return l.fence(ctx)
}

// fence stops serving whatever the Control Plane refused a report for. Until this
// existed the refusal was recorded in l.fenced and read by nothing, so a host that had
// lost a volume kept serving it — DEV-0012.
//
// It runs after the report and not inside it because tearing a runtime down closes a
// WAL, and l.mu must not be held across that.
func (l *Loop) fence(ctx context.Context) error {
	l.mu.Lock()
	fenced := append([]string(nil), l.fenced...)
	reconcile := l.reconcile
	l.mu.Unlock()

	if reconcile == nil || len(fenced) == 0 {
		return nil
	}
	if err := reconcile.Fence(ctx, fenced); err != nil {
		return fmt.Errorf("agent: fencing refused volumes: %w", err)
	}
	return nil
}

func (l *Loop) heartbeat(ctx context.Context, usage disk.Usage, vols []VolumeStatus) error {
	var backlog int64
	for _, v := range vols {
		backlog += v.RemoteGapBytes
	}

	// The lease is anchored to the instant the request leaves, never to the answer's
	// (§12.2): the Control Plane stamps last_renewal at or after this instant, so the
	// Agent's window can only ever be a subset of the one it is fenced against.
	gen := l.generation()
	sentAt := l.clk.Now()

	resp, err := l.cp.Heartbeat(ctx, connect.NewRequest(&storagev1.HeartbeatRequest{
		HostId:           l.cfg.HostID,
		AgentVersion:     l.cfg.AgentVersion,
		MaxFormatVersion: l.cfg.MaxFormatVersion,
		Device: &storagev1.DeviceStatus{
			TotalBytes:         usage.TotalBytes,
			UsedBytes:          usage.UsedBytes,
			RemoteBacklogBytes: backlog,
		},
	}))
	if err != nil {
		l.rec.Count(ctx, "lease_renewal_failures_total", 1, obs.String("host", l.cfg.HostID))
		return fmt.Errorf("agent: heartbeat: %w", err)
	}

	l.applyLease(gen, sentAt, time.Duration(resp.Msg.GetLeaseTtlSeconds())*time.Second)
	l.setHostState(resp.Msg.GetState())
	l.rec.Gauge(ctx, "lease_remaining_seconds", l.leaseRemaining().Seconds(), obs.String("host", l.cfg.HostID))
	return nil
}

func (l *Loop) readDesiredState(ctx context.Context) error {
	resp, err := l.cp.GetDesiredState(ctx, connect.NewRequest(&storagev1.GetDesiredStateRequest{
		HostId: l.cfg.HostID,
	}))
	if err != nil {
		return fmt.Errorf("agent: reading the desired state: %w", err)
	}
	desired := resp.Msg.GetVolumes()

	l.mu.Lock()
	l.desired = desired
	l.forgetKeysOutsideLocked(desired)
	reconcile := l.reconcile
	l.mu.Unlock()

	// Handing the desired state to whatever serves volumes is where this loop stops
	// being a reporter. It is a separate call and not part of the assignment above
	// because the lock must not be held across starting a runtime: Apply opens a WAL
	// and binds a socket, and a heartbeat blocked behind that is a lease not renewed.
	if reconcile == nil {
		return nil
	}
	if err := reconcile.Apply(ctx, desired); err != nil {
		// Returned, not swallowed: a volume that could not be started is the whole
		// reason this Agent exists, and the cycle's backoff is what retries it.
		return fmt.Errorf("agent: applying the desired state: %w", err)
	}
	return nil
}

// forgetKeysOutsideLocked drops the key material of every volume the Control Plane
// no longer lists for this host. A volume leaves the desired state because it was
// promoted away, detached, or fenced — in each case this host has stopped being its
// writer, and there is no reason for it to keep what opens it. Callers hold l.mu.
func (l *Loop) forgetKeysOutsideLocked(desired []*storagev1.DesiredVolume) {
	if len(l.keys) == 0 {
		return
	}
	live := make(map[string]bool, len(desired))
	for _, v := range desired {
		live[v.GetVolumeId()] = true
	}
	for id := range l.keys {
		if !live[id] {
			delete(l.keys, id)
		}
	}
}

// VolumeKeys returns the key material for one volume, fetching it the first time and
// holding it afterwards (§15.1, ADR-0018). It is what the data path will call before
// opening a volume: every payload it writes is sealed with this DEK.
//
// Fetched once, not per cycle. The material does not change while the volume is this
// host's, and the desired state — which is re-read every few seconds, for every
// volume — is deliberately not where it travels. The entry is dropped when the
// volume leaves that desired state, which is the only invalidation this cache has
// and the only one it needs.
func (l *Loop) VolumeKeys(ctx context.Context, volumeID string) (VolumeKeys, error) {
	if volumeID == "" {
		return VolumeKeys{}, errors.New("agent: a volume id is required to ask for key material")
	}
	if keys, ok := l.cachedKeys(volumeID); ok {
		return keys, nil
	}

	resp, err := l.cp.GetVolumeKeys(ctx, connect.NewRequest(&storagev1.GetVolumeKeysRequest{
		HostId:   l.cfg.HostID,
		VolumeId: volumeID,
	}))
	if err != nil {
		// A refusal here is the fencing story arriving through a different door:
		// this host is not the volume's writer. It is returned rather than swallowed
		// — an Agent that cannot get the keys must not open the volume.
		return VolumeKeys{}, fmt.Errorf("agent: reading the keys of volume %q: %w", volumeID, err)
	}

	keys := VolumeKeys{
		VolumeID:   volumeID,
		DEKWrapped: resp.Msg.GetDekWrapped(),
		KEKID:      resp.Msg.GetKekId(),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys[volumeID] = keys
	return keys, nil
}

func (l *Loop) cachedKeys(volumeID string) (VolumeKeys, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys, ok := l.keys[volumeID]
	return keys, ok
}

func (l *Loop) report(ctx context.Context, vols []VolumeStatus) error {
	reports := make([]*storagev1.VolumeReport, 0, len(vols))
	for _, v := range vols {
		reports = append(reports, &storagev1.VolumeReport{
			VolumeId:          v.VolumeID,
			Epoch:             v.Epoch,
			LocalSequence:     v.LocalSequence,
			DurableSequence:   v.DurableSequence,
			PublishedSequence: v.PublishedSequence,
			RemoteGapBytes:    v.RemoteGapBytes,
		})
	}
	resp, err := l.cp.ReportVolumeState(ctx, connect.NewRequest(&storagev1.ReportVolumeStateRequest{
		HostId:  l.cfg.HostID,
		Volumes: reports,
	}))
	if err != nil {
		return fmt.Errorf("agent: reporting volume state: %w", err)
	}

	// A refusal is information: this host is no longer the writer for that volume.
	// Recording it is all this increment can do — the transition to SELF_FENCED and
	// the end of guest ACKs belong to the data path (§16, §12.2).
	var fenced []string
	for _, r := range resp.Msg.GetResults() {
		switch r.GetOutcome() {
		case storagev1.ReportOutcome_REPORT_OUTCOME_STALE_EPOCH,
			storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY,
			storagev1.ReportOutcome_REPORT_OUTCOME_UNKNOWN_VOLUME:
			fenced = append(fenced, r.GetVolumeId())
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fenced = fenced
	return nil
}

// applyLease arms (or re-arms) the host lease from a heartbeat that was *requested*
// at sentAt under generation gen. A TTL of zero is the Control Plane refusing to
// renew — a DEAD host — and must drop the lease rather than leave the last one
// standing: a successful round trip is not a lease.
func (l *Loop) applyLease(gen uint64, sentAt clock.Instant, ttl time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ttl <= 0 {
		l.lease = nil
		l.leaseTTL = 0
		return
	}
	if l.lease == nil || l.leaseTTL != ttl {
		// The Control Plane's TTL wins, including when it shortens: the Agent's
		// window must stay inside the window it is fenced against.
		l.lease = lease.NewManager(l.clk, ttl)
		l.leaseTTL = ttl
		l.lease.GrantAt(gen, sentAt)
		return
	}
	l.lease.RenewAt(gen, sentAt)
}

func (l *Loop) generation() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lease == nil {
		return 0
	}
	return l.lease.Generation()
}

func (l *Loop) setHostState(s storagev1.HostState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state = s
}

// LeaseValid reports whether the host lease is still valid on the monotonic clock
// (§12.2). It is what the data path will consult before ACKing a durable write.
func (l *Loop) LeaseValid() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lease != nil && l.lease.Valid()
}

func (l *Loop) leaseRemaining() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lease == nil {
		return 0
	}
	return l.lease.Remaining()
}

// Desired returns the volumes the Control Plane last said this host should serve.
func (l *Loop) Desired() []*storagev1.DesiredVolume {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]*storagev1.DesiredVolume(nil), l.desired...)
}

// HostState returns the fleet state the Control Plane last reported for this host.
func (l *Loop) HostState() storagev1.HostState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Fenced returns the volumes whose last report was refused — this host is not their
// writer any more, whether because the epoch moved on, the primary changed, or the
// Control Plane has never heard of them.
func (l *Loop) Fenced() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.fenced...)
}
