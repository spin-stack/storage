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
	"github.com/spin-stack/storage/internal/crypto"
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
	// Witness is the second signal a lapsed lease is checked against. Nil is allowed and
	// is what an Agent with no object store configured has: with no second path, nothing
	// can confirm supersession, and the loop keeps serving rather than stopping guests on
	// silence. See giveUpWhatIsNoLongerOurs.
	Witness Witness
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
	// witness is the object-store side of the two-signal rule; see Deps.Witness.
	witness Witness

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
	// term is the Control Plane term that last answered a heartbeat; see noteTerm.
	term int64
	// keys is key material by volume id, cached with one eviction rule and no expiry: an
	// entry lives exactly as long as its volume stays in the desired state. When the volume
	// stops being this host's the entry must go — not because it would be stale, but because
	// it is the means to open a volume the fleet has taken away.
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
		cfg:     cfg,
		clk:     deps.Clock,
		cp:      deps.ControlPlane,
		dev:     deps.Device,
		vols:    deps.Volumes,
		rec:     deps.Recorder,
		witness: deps.Witness,
		keys:    map[string]VolumeKeys{},
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
			// An Agent that can never succeed — wrong Control Plane URL, a host id the
			// database refuses, expired credentials — otherwise looks exactly like a healthy
			// one from the outside. One line per failed cycle, not per retry, and it carries
			// the delay: "it failed" without "and I retry in 1s" reads as fatal.
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

// Reconcile runs one cycle: heartbeat, read the desired state, serve it, report what
// this host observed. It stops at the first failure of the three Control Plane calls — one
// that did not answer the heartbeat has nothing useful to say to the rest of the cycle —
// and the lease is left to run down, which is what fences this host if it lasts.
//
// A failure to *serve* the desired state is the exception, and it is the point: the
// volumes that could not start have no runtime, so the report is the only thing that can
// tell the fleet they exist and are not being served. Returning at that failure skipped
// the report carrying it, and the catalog showed a refused volume healthy for ever. The
// error is still returned at the end — the backoff and the per-cycle WARN are what retry
// it, and swallowing it to reach the report would trade one silence for another.
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
	if err := l.giveUpWhatIsNoLongerOurs(ctx, vols); err != nil {
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

// Witness answers whether a volume has been granted to somebody else, over a path that
// does not run through the Control Plane. Defined here because this is its only consumer.
//
// Only an epoch *higher* than the one this host holds is an answer; the same epoch, no
// record, and an unreachable store are deliberately not distinguished — none of them is
// evidence that somebody else is writing.
type Witness interface {
	GrantedEpoch(ctx context.Context, volumeID string) (int64, error)
}

// giveUpWhatIsNoLongerOurs stops serving the volumes this host can confirm belong to
// somebody else. A lease that merely lapsed is not that confirmation: v5 stopped every
// volume at the lapse because the Agent *was* the data path, but under v6 nothing a
// superseded host writes can enter the published history (the compare-and-set on HEAD
// plus the epoch), so stopping on silence cost a tenant's VM for a partition nobody had
// acted on.
//
// Three things confirm supersession: the Control Plane refusing this host's report, the
// compare-and-set on HEAD losing, and this function's signal — `volumes/<id>/epoch`,
// written at the grant and read over the object store, a path that is still there when
// the Control Plane is not. It is the shape of vSphere HA requiring the datastore
// heartbeat to agree. A zero-TTL answer is the Control Plane speaking and is handled in
// applyLease, not here.
//
// A confirmed volume's guest is stopped rather than degraded: a volume this host does not
// own cannot be served read-only either, because the bytes underneath it may already have
// been overwritten, and a guest cannot be told that.
//
// Unresolved: a host that can reach neither the Control Plane nor the object store cannot
// tell isolation from supersession and keeps serving. Nothing then stops the successor's
// guest from starting — this system has no equivalent of vSphere's datastore lock — but
// stopping here would trade a certain loss for a possible one in the more common case.
func (l *Loop) giveUpWhatIsNoLongerOurs(ctx context.Context, vols []VolumeStatus) error {
	l.mu.Lock()
	lm, reconcile, ttl, wit := l.lease, l.reconcile, l.leaseTTL, l.witness
	l.mu.Unlock()

	// The trigger is "something is being served under a claim this host cannot confirm",
	// not "the lease is invalid": a nil lease is also every Agent's first cycle, before
	// any heartbeat has been answered, and a host serving nothing has nothing to give up.
	if len(vols) == 0 || reconcile == nil || (lm != nil && lm.Valid()) {
		return nil
	}

	// A nil lease past the first cycle is the Control Plane having answered with a zero
	// TTL, which is it saying this host is DEAD. That is the Control Plane speaking, so
	// it needs no second signal — asking the bucket to confirm what the owner of the
	// decision just said would only add a way to disagree with it.
	confirmed, unconfirmed := vols, []string(nil)
	if lm != nil {
		confirmed, unconfirmed = Superseded(ctx, wit, vols)
	}

	if len(unconfirmed) > 0 {
		// Warn, not Error: a degraded host, not a lost volume. It is the only line that says
		// this host has stopped being able to confirm its claim and is serving anyway.
		slog.Warn("this host's lease has expired and nothing confirms the volumes were granted elsewhere; it is still serving them",
			"host_id", l.cfg.HostID, "volumes", unconfirmed, "lease_ttl", ttl)
	}
	if len(confirmed) == 0 {
		return nil
	}

	volumeIDs := make([]string, 0, len(confirmed))
	for _, v := range confirmed {
		volumeIDs = append(volumeIDs, v.VolumeID)
	}
	// Error, not Warn: this is a guest losing its disk, and it is the one line that
	// explains why a VM that was running is now stuck on I/O.
	slog.Error("these volumes have been granted to another host; this one is giving them up and stopping their guests",
		"host_id", l.cfg.HostID, "volumes", volumeIDs, "lease_ttl", ttl)

	// Revoked before the volumes go: Revoke bumps the generation, so a heartbeat answer
	// already in flight cannot re-arm the lease behind the teardown (§12.2). Deliberately
	// not done on the unconfirmed path — a host still serving must be able to re-arm on the
	// next answered heartbeat, or an isolated host would need a promotion at a higher epoch
	// to recover from a partition it was right to sit through.
	if lm != nil {
		lm.Revoke()
	}
	// Reported, not only logged. This is the one teardown nothing outside the process
	// knows about: the Control Plane still names this host the volume's primary at this
	// epoch, so nothing has moved and nothing will until somebody looks. The report is
	// refused on the same host-and-epoch predicate every report is, so if the fleet *has*
	// moved on, this says nothing rather than something wrong.
	if err := reconcile.Fence(ctx, volumeIDs,
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST,
		fmt.Sprintf("agent: this host's lease (%s) expired and the object store records a higher epoch for these volumes, so another host has been granted them; it will not serve them again until the control plane grants a higher epoch here", ttl),
	); err != nil {
		return fmt.Errorf("agent: giving up the volumes of an expired lease: %w", err)
	}
	return nil
}

// Superseded splits the volumes into the ones the object store confirms were granted
// elsewhere and the ones it does not. Every failure to answer — no witness, an unreachable
// store, no epoch recorded, an epoch that has not moved — lands in the second group: none
// of them is evidence that somebody else is writing, and reading them as supersession
// would stop guests on silence.
//
// Exported because the DST scenario drives it directly; a scenario that re-stated the rule
// in its own words would agree with itself while production disagreed.
func Superseded(ctx context.Context, wit Witness, vols []VolumeStatus) (confirmed []VolumeStatus, unconfirmed []string) {
	for _, v := range vols {
		if wit == nil {
			unconfirmed = append(unconfirmed, v.VolumeID)
			continue
		}
		granted, err := wit.GrantedEpoch(ctx, v.VolumeID)
		switch {
		case err != nil:
			// Logged at Debug and not Warn: the cycle already warns once for the whole
			// set, and this runs every cycle for as long as the partition lasts.
			slog.Debug("could not read the granted epoch while the lease was expired",
				"volume_id", v.VolumeID, "error", err)
			unconfirmed = append(unconfirmed, v.VolumeID)
		case granted > v.Epoch:
			confirmed = append(confirmed, v)
		default:
			unconfirmed = append(unconfirmed, v.VolumeID)
		}
	}
	return confirmed, unconfirmed
}

// fence stops serving whatever the Control Plane refused a report for. Until this
// existed the refusal was recorded in l.fenced and read by nothing, so a host that had
// lost a volume kept serving it — DEV-0012.
//
// It runs after the report and not inside it because tearing a runtime down closes the
// volume's local storage, and l.mu must not be held across that.
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
			TotalBytes: usage.TotalBytes,
			UsedBytes:  usage.UsedBytes,
		},
	}))
	if err != nil {
		l.rec.Count(ctx, "lease_renewal_failures_total", 1, obs.String("host", l.cfg.HostID))
		return fmt.Errorf("agent: heartbeat: %w", err)
	}

	l.applyLease(gen, sentAt, time.Duration(resp.Msg.GetLeaseTtlSeconds())*time.Second)
	l.setHostState(resp.Msg.GetState())
	l.noteTerm(resp.Msg.GetTerm())
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

// applyDesiredState hands the desired state to whatever serves volumes. It is not part of
// readDesiredState's locked section because l.mu must not be held across starting a
// runtime: Apply opens a volume's local storage and binds a socket, and a heartbeat blocked
// behind that is a lease not renewed.
func (l *Loop) applyDesiredState(ctx context.Context, desired []*storagev1.DesiredVolume) error {
	l.mu.Lock()
	reconcile := l.reconcile
	l.mu.Unlock()
	if reconcile == nil {
		return nil
	}
	if err := reconcile.Apply(ctx, desired); err != nil {
		// Returned, not swallowed; Reconcile says why it is then carried past the report.
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
// holding it afterwards (§15.1, ADR-0018): every payload the data path writes is sealed
// with this DEK. The material does not change while the volume is this host's, and the
// entry is dropped when the volume leaves the desired state — the only invalidation this
// cache has and the only one it needs.
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

	// KeyID 0 is the reserved marker for "these bytes are cleartext", so a 0 here is not a
	// usable version. Caching it would turn one bad answer into a permanently unopenable
	// volume, since VolumeKeys never re-asks once it has an entry.
	if id := resp.Msg.GetDekKeyId(); id == 0 {
		return VolumeKeys{}, fmt.Errorf("agent: volume %q was handed a DEK with no version (§15.1): %w",
			volumeID, crypto.ErrUnversionedKey)
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
			VolumeId:                  v.VolumeID,
			Epoch:                     v.Epoch,
			SnapshotId:                v.SnapshotID,
			SnapshotCommitId:          v.SnapshotCommitID,
			PublishedCommitId:         v.PublishedCommitID,
			PublishStalled:            v.PublishStalled,
			LastSuccessfulCommitAgeMs: v.LastCommitAge.Milliseconds(),
			UnpublishedLocalBytes:     v.UnpublishedLocalBytes,
			SnapshotError:             v.SnapshotError,
			// The one field here that is not a measurement: this host saying it is not
			// serving the volume, and why. Unset is it saying it is, so a healthy cycle
			// clears whatever the catalog holds without anything having to notice.
			Refusal:       v.Refusal,
			RefusalDetail: v.RefusalDetail,
		})
		// The same four numbers, exported from the same v that is going on the wire, so a
		// host cannot report one and scrape another.
		//
		// Recorded before the call and not after it: a host that cannot reach the Control
		// Plane is the host whose commit age is growing, and the scrape is the only
		// channel it has left. They are measurements of this host, true whether or not
		// anybody was told.
		vol := obs.String("volume", v.VolumeID)
		l.rec.Gauge(ctx, "last_successful_commit_age_seconds", v.LastCommitAge.Seconds(), vol)
		l.rec.Gauge(ctx, "unpublished_local_bytes", float64(v.UnpublishedLocalBytes), vol)
		l.rec.Gauge(ctx, "local_chain_depth", float64(v.ChainDepth), vol)
		l.rec.Gauge(ctx, "local_disk_bytes", float64(v.LocalDiskBytes), vol)
	}

	resp, err := l.cp.ReportVolumeState(ctx, connect.NewRequest(&storagev1.ReportVolumeStateRequest{
		HostId:  l.cfg.HostID,
		Volumes: reports,
	}))
	if err != nil {
		return fmt.Errorf("agent: reporting volume state: %w", err)
	}

	// A refusal is information: this host is no longer the writer for that volume.
	// Recorded here and acted on by fence(), which Reconcile calls once this returns and
	// which tears those runtimes down. The two are separate because closing a volume's
	// local storage must not happen under l.mu (§12.2).
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

// noteTerm logs the Control Plane's term when it changes, which is the only thing this
// host does with it: a term is the Control Plane's own fencing and no decision here is
// taken against it. A failover is otherwise invisible from a host's journal, and it is
// exactly what an operator correlates against everything that went strange at that minute.
//
// On change and not per heartbeat: at one line every few seconds it is noise, and the fact
// is a transition rather than a level. The first answer of a process is a change from
// nothing and says so, because "which leader has been answering me since I started" is the
// same question.
func (l *Loop) noteTerm(term int64) {
	l.mu.Lock()
	was, first := l.term, l.term == 0
	l.term = term
	l.mu.Unlock()
	if term == was {
		return
	}
	if first {
		slog.Info("the Control Plane answering this host", "host_id", l.cfg.HostID, "term", term)
		return
	}
	slog.Warn("the Control Plane answering this host changed term; a leader election happened",
		"host_id", l.cfg.HostID, "was", was, "now", term)
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
	// A refused renewal is not a lost heartbeat: the Control Plane just answered, with a
	// TTL. RenewAt refuses a lapsed or revoked lease and only a grant arms one, so without
	// this line the first expiry would be permanent — giveUpWhatIsNoLongerOurs revokes —
	// and every volume granted afterwards would be given up on the next cycle.
	//
	// Safe here and not earlier: the volumes held under the dead lease were already given
	// up at the top of the cycle. gen is passed unchanged, so an answer a Revoke overtook
	// after it was sent is still refused, and sentAt anchors the window to the request
	// (§12.2).
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

// LeaseValid reports whether this host's lease has not yet lapsed on the Agent's own
// monotonic clock (§12.2). Nothing on the data path reads it — giveUpWhatIsNoLongerOurs is
// what acts on a lease running out — and this is the observation point the loop's tests
// and the e2e guest lane assert against.
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
