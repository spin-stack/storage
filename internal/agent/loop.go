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
	"github.com/spin-stack/storage/internal/wal"
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

// Sustain keeps this host visible to the Control Plane while the Agent is shutting down
// and cannot yet let go — a publish that keeps failing holds the data directory, and this
// is what keeps the fleet able to see the difference between a host that is stuck and a
// host that is gone. It returns when ctx
// is done, which is when the teardown has finished one way or another.
//
// It is a *reduced* cycle, and each omission is deliberate:
//
//   - it heartbeats, because a lease that lapses is what makes a host look dead;
//   - it reports the volumes still being held, because a held volume is still this host's
//     — the epoch has not moved and the Control Plane still names this host its primary,
//     so the report is accepted and the operator can see the sequence that has not
//     reached the object store. A volume that has published is out of the served set by
//     then and simply stops being reported, which is the honest end state;
//   - it does **not** read the desired state, because applying it would start runtimes
//     the teardown has just stopped, and this loop would fight the shutdown it is
//     supposed to narrate;
//   - it does **not** fence on a refused report. There is nothing left to stop — every
//     runtime is already quiesced — and the one decision a refusal could inform, whether
//     to publish, is made by the manifest's compare-and-set, which is authoritative and
//     cannot be raced by a slower answer over HTTP.
func (l *Loop) Sustain(ctx context.Context) {
	for {
		if err := l.clk.Sleep(ctx, l.cfg.HeartbeatInterval); err != nil {
			return
		}
		usage, err := l.dev.Usage(ctx)
		if err != nil {
			slog.Warn("reading the device while shutting down", "error", err, "host_id", l.cfg.HostID)
			continue
		}
		vols, err := l.vols.Volumes(ctx)
		if err != nil {
			slog.Warn("reading the volumes still held while shutting down", "error", err, "host_id", l.cfg.HostID)
			continue
		}
		if err := l.heartbeat(ctx, usage, vols); err != nil {
			// Warned and retried, never returned: the Control Plane being unreachable is
			// not a reason to stop saying we are here, and this loop's exit condition is
			// the teardown finishing rather than anything about the fleet.
			slog.Warn("heartbeat while shutting down", "error", err, "host_id", l.cfg.HostID)
			continue
		}
		if err := l.report(ctx, vols); err != nil {
			slog.Warn("reporting the volumes still held", "error", err, "host_id", l.cfg.HostID)
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

// Reconcile runs one cycle: heartbeat, read the desired state, serve it, report what
// this host observed. It stops at the first failure of the three calls to the Control
// Plane — one that did not answer the heartbeat has nothing useful to say to the rest of
// the cycle — and the lease is left to run down, which is what fences this host if the
// condition lasts.
//
// **A failure to serve the desired state is the one exception, and it is the point.**
// Applying it is local work, and the volumes it could not start are precisely the ones
// this host has something to say about: they have no runtime, so the report is the only
// thing that can tell the fleet they exist and are not being served. Returning at that
// failure skipped the report that carries it — an Agent refusing a volume printed a WARN
// every second and the catalog showed the volume healthy for ever, because the only cycle
// with news never reached the sentence.
//
// It was found by starting the two real binaries: a volume-agent without -kek-file
// against a Control Plane holding an encrypted volume. Nothing in the suite could see it.
// Every test of a refusal builds the VolumeManager and asks it directly, which is the one
// caller that does not go through this function, and the loop's own tests drive
// agent.VolumeSet, whose Apply cannot fail.
//
// The error is still returned, at the end, unchanged: the backoff and the per-cycle WARN
// are what retry it, and a fix that swallowed it to reach the report would trade one
// silence for another.
func (l *Loop) Reconcile(ctx context.Context) error {
	usage, err := l.dev.Usage(ctx)
	if err != nil {
		return fmt.Errorf("agent: reading the device: %w", err)
	}
	vols, err := l.vols.Volumes(ctx)
	if err != nil {
		return fmt.Errorf("agent: reading the served volumes: %w", err)
	}

	// Before the heartbeat, and that placement is the whole point: the heartbeat is the
	// call that fails during a partition, and this function returns at its first failure.
	// Checked here, the lease left behind by the previous cycle is read on every cycle,
	// failing or not.
	if err := l.giveUpOnExpiredLease(ctx, vols); err != nil {
		return err
	}

	if err := l.heartbeat(ctx, usage, vols); err != nil {
		return err
	}
	desired, err := l.readDesiredState(ctx)
	if err != nil {
		// Nothing was read, so there is nothing new to serve and nothing new to say.
		return err
	}
	// Carried rather than returned: see the note above. What could not be started is
	// recorded on the manager as a refusal, and the report below is the only thing that
	// carries it off this host.
	applyErr := l.applyDesiredState(ctx, desired)

	// Re-read before reporting. The set above is what the cycle *started* with, and
	// applyDesiredState has since started and stopped runtimes: reporting the old set
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
	if err := l.fence(ctx); err != nil {
		return err
	}
	return applyErr
}

// giveUpOnExpiredLease stops serving every volume on this host once its lease has
// lapsed on the Agent's own monotonic clock.
//
// It is the only thing in the process that acts on a lease running out, and without it a
// partitioned Agent served forever. Kill the network in front of one and it keeps its
// socket bound, keeps answering the guest, and keeps ACKing its flushes — while the fleet
// declares the host dead after one TTL and an operator moves the volume to another host.
// Two guests then write one volume, both are told their fsyncs are durable, and one of
// them is wrong for the whole partition. The Control Plane cannot prevent it by
// construction: the Agent pulls (ADR-0018), so a host that cannot hear the Control Plane
// cannot be told anything, and the only actor left is this one.
//
// # What "expired" means here
//
// The lease TTL is liveness, not a durability grant (§14.8). This is not the lease-gated
// ACK that ADR-0026 withdrew and it is not being rebuilt: no FLUSH consults the lease, and
// an ACK still means exactly what it meant — captured, fdatasync'd, durable_sequence
// advanced. What is being said is narrower and it is about ownership, not durability: a
// host that has not been able to confirm for a whole TTL that it is still a volume's
// writer must stop acting as one. The timing works because the Control Plane's own
// deadline is that same TTL measured from the renewal it recorded, which it stamped at or
// after the instant this Agent anchored to (§12.2, applyLease) — so this host gives the
// device up no later than the moment the fleet becomes entitled to hand the volume to
// someone else, never after.
//
// # What the guest sees
//
// Its disk disappears. Dropping the socket kills QEMU's connection, the guest's I/O
// stalls, and nothing is coming back to serve it. That is chosen, not incidental. The
// alternative — keep serving reads, refuse writes — hands a guest bytes that another host
// may already have overwritten, with no way for the guest to tell; and the alternative to
// both is the behaviour being fixed, where the guest is told a write is durable by a host
// that may no longer own the volume. A stalled guest is a visible failure. The other two
// are silent ones.
//
// # It gives up rather than pauses
//
// Fence is reused, so a volume dropped here comes back only at a *higher* epoch — only
// when the Control Plane grants it to this host again. A brief partition therefore costs
// the volume on this host until the fleet says something about it, which is the side to be
// wrong on: the alternative is a host that resumes serving on a stale desired state read
// seconds before a promotion it never heard about.
func (l *Loop) giveUpOnExpiredLease(ctx context.Context, vols []VolumeStatus) error {
	l.mu.Lock()
	lm, reconcile, ttl := l.lease, l.reconcile, l.leaseTTL
	l.mu.Unlock()

	// The trigger is "something is being served under a claim this host cannot confirm",
	// not "the lease is invalid": a nil lease is also every Agent's first cycle, before
	// any heartbeat has been answered, and a host serving nothing has nothing to give up.
	// Testing the volumes rather than the lease covers both ways a claim ends — the TTL
	// lapsing, and the Control Plane answering with a zero TTL, which is it saying this
	// host is DEAD (applyLease).
	if len(vols) == 0 || reconcile == nil || (lm != nil && lm.Valid()) {
		return nil
	}

	volumeIDs := make([]string, 0, len(vols))
	for _, v := range vols {
		volumeIDs = append(volumeIDs, v.VolumeID)
	}
	// Error, not Warn: this is a guest losing its disk, and it is the one line that
	// explains why a VM that was running is now stuck on I/O.
	slog.Error("this host's lease has expired; it can no longer confirm that it owns these volumes and is giving them up",
		"host_id", l.cfg.HostID, "volumes", volumeIDs, "lease_ttl", ttl)

	// Revoked before the volumes go, so that from this instant the host makes no claim
	// at all: Revoke bumps the generation, and a heartbeat answer that was already in
	// flight when this ran cannot re-arm the lease behind the teardown (§12.2). Only a
	// fresh grant can, which is what applyLease does when a renewal is refused.
	if lm != nil {
		lm.Revoke()
	}
	// Reported, not only logged. This is the one teardown nothing outside the process
	// knows about: the Control Plane still names this host the volume's primary at this
	// epoch, so nothing has moved and nothing will until somebody looks — and until this
	// reached the report, "looking" meant reading this Agent's log. The report is refused
	// on the same host-and-epoch predicate the watermarks are, so if the fleet *has*
	// moved on, this says nothing rather than something wrong.
	if err := reconcile.Fence(ctx, volumeIDs,
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST,
		fmt.Sprintf("agent: this host's lease (%s) expired and it gave the volume up; it will not serve it again until the control plane grants a higher epoch", ttl),
	); err != nil {
		return fmt.Errorf("agent: giving up the volumes of an expired lease: %w", err)
	}
	return nil
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
	// No refusal reported for these. The Control Plane is where the refusal came from —
	// it refused the report a moment ago — so telling it back is telling it what it told
	// us, and the report would be refused again on the same predicate. Silence here is
	// what keeps a *reported* refusal meaning "something the fleet does not already
	// know".
	if err := reconcile.Fence(ctx, fenced, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""); err != nil {
		return fmt.Errorf("agent: fencing refused volumes: %w", err)
	}
	return nil
}

func (l *Loop) heartbeat(ctx context.Context, usage disk.Usage, vols []VolumeStatus) error {
	// The heartbeat used to carry the host's remote backlog — the bytes no verified
	// object covered yet. It went with the uploader (ADR-0026 increment 4.5): with the
	// ACK local and the volume published at stop there is no continuous distance to S3
	// to measure. What a host would lose if it died mid-session is bounded by the session
	// and, deliberately, nothing measures it — see STATUS.md.
	var backlog int64

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

// readDesiredState asks what this host should be serving and records the answer. It is
// split from applying it because the two failures are not the same kind: not learning the
// desired state ends the cycle, while not being able to serve it is news the cycle has to
// go on and deliver.
func (l *Loop) readDesiredState(ctx context.Context) ([]*storagev1.DesiredVolume, error) {
	resp, err := l.cp.GetDesiredState(ctx, connect.NewRequest(&storagev1.GetDesiredStateRequest{
		HostId: l.cfg.HostID,
	}))
	if err != nil {
		return nil, fmt.Errorf("agent: reading the desired state: %w", err)
	}
	desired := resp.Msg.GetVolumes()

	l.mu.Lock()
	l.desired = desired
	l.forgetKeysOutsideLocked(desired)
	l.mu.Unlock()
	return desired, nil
}

// applyDesiredState hands the desired state to whatever serves volumes. This is where
// the loop stops being a reporter.
//
// It is a call of its own and not part of readDesiredState's locked section, because
// l.mu must not be held across starting a runtime: Apply opens a WAL and binds a socket,
// and a heartbeat blocked behind that is a lease not renewed.
func (l *Loop) applyDesiredState(ctx context.Context, desired []*storagev1.DesiredVolume) error {
	l.mu.Lock()
	reconcile := l.reconcile
	l.mu.Unlock()
	if reconcile == nil {
		return nil
	}
	if err := reconcile.Apply(ctx, desired); err != nil {
		// Returned, not swallowed: a volume that could not be started is the whole
		// reason this Agent exists, and the cycle's backoff is what retries it. Its
		// caller carries it past the report rather than returning at it — Reconcile says
		// why.
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

	// Refused here rather than carried: KeyID 0 means "plaintext record" on the WAL
	// path (§14.1), so a 0 from the Control Plane is not a usable version — it is a
	// volume whose key material this host cannot honestly use. Caching it would turn
	// one bad answer into a permanently unopenable volume, since VolumeKeys never
	// re-asks once it has an entry.
	if id := resp.Msg.GetDekKeyId(); id == 0 {
		return VolumeKeys{}, fmt.Errorf("agent: volume %q was handed a DEK with no version (§15.1): %w",
			volumeID, wal.ErrUnversionedKey)
	}
	keys := VolumeKeys{
		VolumeID:   volumeID,
		DEKWrapped: resp.Msg.GetDekWrapped(),
		KEKID:      resp.Msg.GetKekId(),
		DEKKeyID:   resp.Msg.GetDekKeyId(),
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
			SnapshotId:        v.SnapshotID,
			SnapshotSequence:  v.SnapshotSequence,
			SnapshotError:     v.SnapshotError,
			// The one field here that is not a measurement: this host saying it is not
			// serving the volume, and why. Unset is it saying it is, so a healthy cycle
			// clears whatever the catalog holds without anything having to notice.
			Refusal:       v.Refusal,
			RefusalDetail: v.RefusalDetail,
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
		// The new manager's own generation, not the one read before the request was
		// sent. gen identifies an incarnation of a manager that no longer exists, and a
		// manager built one line ago holds no lease for a stale answer to extend — there
		// is nothing here for the generation guard to protect. Passing gen would refuse
		// every grant after the first Revoke, which is a host that never serves again.
		l.lease.GrantAt(l.lease.Generation(), sentAt)
		return
	}
	if l.lease.RenewAt(gen, sentAt) {
		return
	}
	// A refused renewal is not a lost heartbeat — the Control Plane just answered, with a
	// TTL, which is it counting this host alive. RenewAt refuses a lease that has lapsed
	// or been revoked, and neither can be renewed by construction: only a grant arms a
	// lease. Without this line the first expiry would be permanent, because
	// giveUpOnExpiredLease revokes — LeaseValid would stay false for the life of the
	// process and every volume granted afterwards would be given up on the next cycle.
	//
	// Re-arming is safe here and would not be earlier in this function: by the time a
	// renewal is refused, the volumes that were held under the dead lease have already
	// been given up (giveUpOnExpiredLease runs at the top of the cycle, before the
	// heartbeat), so this grants a claim over nothing until the desired state says
	// otherwise. gen is passed unchanged, so an answer that a Revoke overtook *after it
	// was sent* is still refused, and sentAt still anchors the window to the request
	// rather than to the reply (§12.2).
	l.lease.GrantAt(gen, sentAt)
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
