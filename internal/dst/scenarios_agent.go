package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// The Agent's seam, rather than the WAL's. Everything else in this harness drives
// wal.Log, controlplane and recovery directly, which is where the durability
// invariants live — but "this host stopped serving a volume it lost" is not a
// property of any of them. It belongs to agent.VolumeManager, and until this file
// existed nothing simulated proved it: DEV-0012 was closed with unit tests alone,
// which CLAUDE.md counts as a stop signal for fencing code.

func agentScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "fenced-volume-stops-serving", Run: scenarioFencedVolumeStopsServing},
		{Name: "truncated-volume-survives-a-restart", Run: scenarioTruncatedVolumeSurvivesARestart},
		{Name: "lapsed-lease-stops-publishing", Run: scenarioLapsedLeaseStopsPublishing},
		{Name: "crashed-flush-does-not-collide-on-restart", Run: scenarioCrashedFlushDoesNotCollideOnRestart},
		{Name: "agent-encrypts-what-leaves-the-host", Run: scenarioAgentEncryptsWhatLeavesTheHost},
		{Name: "encrypted-volume-survives-a-restart", Run: scenarioEncryptedVolumeSurvivesARestart},
		{Name: "local-volume-drains-without-claiming", Run: scenarioLocalVolumeDrainsWithoutClaimingWithoutALease},
		{Name: "a-clone-reads-through-its-parent", Run: scenarioACloneReadsThroughItsParent},
		{Name: "a-promoted-host-reads-the-previous-epoch", Run: scenarioAPromotedHostReadsThePreviousEpoch},
	}
}

func agentCheckers() []Checker {
	return []Checker{NewFencedVolumeChecker(), NewDurableRangeChecker(), NewCheckpointLeaseChecker()}
}

// FencedVolumeChecker enforces the Agent's half of INV-10 (§16, §12.3): once the
// Control Plane has refused a volume's report, this host must not answer another
// request for it. The fleet's guarantee is one effective writer; a host that keeps
// serving reads out of a WAL it no longer owns hands a guest bytes that another
// writer has already moved past, and the guest has no way to tell.
type FencedVolumeChecker struct{ violation error }

// NewFencedVolumeChecker returns a fresh checker.
func NewFencedVolumeChecker() *FencedVolumeChecker { return &FencedVolumeChecker{} }

func (c *FencedVolumeChecker) Name() string { return "fenced-volume-not-served" }

func (c *FencedVolumeChecker) Observe(e Event) {
	if e.Kind == EventVolumeServe && e.ServedAfterFence && c.violation == nil {
		c.violation = fmt.Errorf("volume %s was served after being fenced at step %d (violates §16/INV-10)",
			e.Key, e.Step)
	}
}

func (c *FencedVolumeChecker) Check() error { return c.violation }

// simListener is the vhost socket a simulated Agent binds. There is no front-end in
// this world, so Accept blocks until Close — which is the whole contract Server.Serve
// relies on to be cancellable, and the only part of the socket a simulation can
// legitimately model (INV-01: no real sockets here).
// Close is idempotent through a sync.Once rather than a select-on-closed, which is not
// a guard at all: two callers can both find the channel open and both close it. Both
// callers exist — vhost.Server.Serve closes the listener from a context.AfterFunc while
// the manager's teardown closes it directly — so this paniced under -race the first time
// a scenario ran two managers in one simulation.
type simListener struct {
	closed chan struct{}
	once   sync.Once
}

func newSimListener() *simListener { return &simListener{closed: make(chan struct{})} }

func (l *simListener) Accept() (vhost.Conn, error) {
	<-l.closed
	return nil, errors.New("listener closed")
}

func (l *simListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

// simMapper and simEventFD stand where the kernel objects go. vhost.NewServer refuses
// a Config without them and no session ever reaches them, so they fail rather than
// pretend: a simulation that quietly mapped memory would be modelling something that
// does not exist here.
type simMapper struct{}

func (simMapper) Map(*os.File, uint64, uint64) ([]byte, error) {
	return nil, errors.New("no front-end in the simulation")
}
func (simMapper) Unmap([]byte) error { return nil }

func simEventFD(*os.File) (vhost.EventFD, error) {
	return nil, errors.New("no front-end in the simulation")
}

// scenarioFencedVolumeStopsServing drives the real agent.VolumeManager through the
// three moments that decide whether fencing means anything:
//
//  1. a volume the Control Plane lists is served, and a guest's WRITE lands in its WAL;
//  2. the Control Plane refuses its report — this host is not the writer any more — and
//     the runtime must be gone: no device, no socket, nothing to answer with;
//  3. the desired state has *not* caught up, and repeating it must not bring the volume
//     back. Only a higher epoch does, because a higher epoch is the fleet granting the
//     volume to this host again.
//
// Step 3 is the one worth simulating. The Control Plane refuses the *report* while
// GetDesiredState may keep listing the volume for seconds afterwards, so a manager with
// no memory of the fencing restarts it on the very next cycle — serving a volume it was
// just told it lost, with nothing in the trace to say so.
func scenarioFencedVolumeStopsServing(s *Sim) error {
	return fencedVolumeStopsServing(s, honestFencing)
}

const (
	honestFencing  = false
	fencingIgnored = true
)

func fencedVolumeStopsServing(s *Sim, ignoreFencing bool) error {
	ctx := context.Background()

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
	})
	if err != nil {
		return fmt.Errorf("building the volume manager: %w", err)
	}
	defer func() { _ = m.Close() }()

	// A deterministic id: same seed, same volume, same trace (INV-02).
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	desired := func(epoch int64) []*storagev1.DesiredVolume {
		return []*storagev1.DesiredVolume{{
			VolumeId:   volumeID,
			SizeBytes:  1 << 20,
			BlockSize:  512,
			Epoch:      epoch,
			Durability: storagev1.Durability_DURABILITY_REMOTE,
			State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		}}
	}

	// (1) The volume is served and takes a guest's write.
	if err := m.Apply(ctx, desired(1)); err != nil {
		return fmt.Errorf("applying the desired state: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the volume the Control Plane listed is not being served")
	}
	if _, err := dev.WriteAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("a guest write before fencing: %w", err)
	}
	s.Notef("volume %s served at epoch 1; one guest write landed", volumeID)

	// (2) The Control Plane refuses the report. This host is not the writer.
	if !ignoreFencing {
		if err := m.Fence(ctx, []string{volumeID}); err != nil {
			return fmt.Errorf("fencing: %w", err)
		}
	}

	// Whether anything still answers for this volume is exactly the invariant. It is
	// emitted rather than only returned, so the checker sees it on every seed and a
	// future scenario that serves a fenced volume some other way trips the same wire.
	_, stillServed := m.Device(volumeID)
	s.Emit(Event{Kind: EventVolumeServe, Key: volumeID, ServedAfterFence: stillServed})
	if stillServed {
		return fmt.Errorf("volume %s still has a device after being fenced (§16/INV-10)", volumeID)
	}
	if vols, err := m.Volumes(ctx); err != nil {
		return err
	} else if len(vols) != 0 {
		return fmt.Errorf("a fenced volume is still reported as served: %+v", vols)
	}

	// (3) The desired state has not caught up. Repeating it must change nothing.
	for range 3 {
		if err := m.Apply(ctx, desired(1)); err != nil {
			return fmt.Errorf("re-applying the stale desired state: %w", err)
		}
	}
	_, restarted := m.Device(volumeID)
	s.Emit(Event{Kind: EventVolumeServe, Key: volumeID, ServedAfterFence: restarted})
	if restarted {
		return fmt.Errorf("volume %s came back at the epoch it was fenced out of (§16/INV-10)", volumeID)
	}
	s.Notef("the stale desired state did not resurrect the fenced volume")

	// A higher epoch is the fleet granting it again, and it is the only thing that does.
	if err := m.Apply(ctx, desired(2)); err != nil {
		return fmt.Errorf("applying the re-granted volume: %w", err)
	}
	vols, err := m.Volumes(ctx)
	if err != nil {
		return err
	}
	if len(vols) != 1 || vols[0].Epoch != 2 {
		return fmt.Errorf("a volume re-granted at epoch 2 is not being served: %+v", vols)
	}
	s.Notef("epoch 2 re-granted the volume; it is served again from a fresh WAL root")
	return nil
}

// hidingStore answers as if the volume's WAL objects were not there. It is not a
// hypothetical fault: an object store that lists nothing under a prefix is what a
// mis-typed bucket, a lost listing or a wrong-epoch key looks like from here — and
// recovery cannot tell any of those from a volume that never wrote anything.
type hidingStore struct{ objectstore.Store }

func (hidingStore) List(context.Context, string) ([]objectstore.ObjectInfo, error) {
	return nil, nil
}

// DurableRangeChecker enforces, from the guest's side, the promise INV-08 and INV-13
// make from the store's: a range this volume ACKed as durable must never come back as
// something other than what the guest wrote. Every other checker here watches
// watermarks and objects; this one watches the bytes a guest would actually receive,
// which is the only place the difference between "recovered" and "recovered correctly"
// is visible.
//
// It watches two shapes of the same wrong answer, because a rebuilt base can be wrong
// in two directions and only one of them was ever modelled: zeros, which is a base that
// is *missing*, and foreign bytes, which is a base that was *built wrongly*. The second
// is why this comment grew — an Agent replaying its own sealed objects with no key
// folded ciphertext into the view at exactly the plaintext's length, and every
// watermark, every object and every zero-check agreed the volume was fine.
type DurableRangeChecker struct{ violation error }

// NewDurableRangeChecker returns a fresh checker.
func NewDurableRangeChecker() *DurableRangeChecker { return &DurableRangeChecker{} }

func (c *DurableRangeChecker) Name() string { return "durable-range-survives-restart" }

func (c *DurableRangeChecker) Observe(e Event) {
	if e.Kind != EventDurableRead || c.violation != nil {
		return
	}
	switch {
	case e.ZerosAfterRestart:
		c.violation = fmt.Errorf("volume %s read zeros at step %d for a range it ACKed as durable (violates §5.8/INV-08)",
			e.Key, e.Step)
	case e.ForeignBytesAfterRestart:
		c.violation = fmt.Errorf("volume %s was served bytes it never wrote at step %d for a range it ACKed as durable (violates §5.8/INV-08)",
			e.Key, e.Step)
	}
}

func (c *DurableRangeChecker) Check() error { return c.violation }

// scenarioTruncatedVolumeSurvivesARestart is BUILD-INVENTORY increment 5 as a guest
// experiences it: write, flush, publish, truncate away the local segments, restart the
// Agent, read the range back.
//
// Two details are what make it a test rather than a formality. The segments must be
// small enough to seal, because reclaim unlinks only sealed ones — a truncation that
// unlinks nothing proves nothing. And the read must go through the *Agent*, not a Log
// the scenario built: what regressed before was the Agent's decision to create rather
// than resume, which no test of wal could have seen.
func scenarioTruncatedVolumeSurvivesARestart(s *Sim) error {
	return truncatedVolumeSurvivesARestart(s, honestStore)
}

const (
	honestStore          = false
	storeHidesTheObjects = true
)

func truncatedVolumeSurvivesARestart(s *Sim, hideObjects bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	vol := [16]byte(u)
	payload := bytes.Repeat([]byte{0xAB}, 4096)
	const segBytes = 8192
	limits := wal.Limits{SegmentBytes: segBytes}

	// Phase 1: a previous run of this Agent. Written directly through wal because the
	// truncation a durability scheduler will do (increment 3) does not exist yet.
	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	l := wal.NewLog(s.Disk, "/var/lib/spin/wal", s.Clock, vol, 1, limits)
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 3), lm)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, payload, 0); err != nil {
			return fmt.Errorf("seeding write %d: %w", i, err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("seeding flush: %w", err)
	}
	durable := l.Watermarks().Durable

	dir := wal.SegmentDir("/var/lib/spin/wal", vol, 1)
	before, err := s.Disk.List(dir)
	if err != nil {
		return err
	}
	if err := l.AdvancePublished(durable); err != nil {
		return err
	}
	if err := l.TruncateLocal(durable); err != nil {
		return err
	}
	after, err := s.Disk.List(dir)
	if err != nil {
		return err
	}
	if len(after) >= len(before) {
		return fmt.Errorf("truncation unlinked nothing (%d segments, then %d): this scenario proves nothing",
			len(before), len(after))
	}
	if err := l.Close(); err != nil {
		return err
	}
	s.Notef("volume %s: %d segments reclaimed, durable=%d in the store", volumeID, len(before)-len(after), durable)

	// Phase 2: the Agent starts and finds that on disk.
	var store objectstore.Store = s.Store
	if hideObjects {
		store = hidingStore{Store: s.Store}
		s.Emit(Event{Kind: EventFault, Msg: "the object store lists nothing under the volume's prefix"})
	}
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Limits: limits,
		HostID: ids.NewAt(simEpoch*1000, s.Rand).String(),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   store,
		Lease:   func() bool { return true },
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("the restarted Agent could not start the volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the restarted Agent is not serving the volume")
	}

	got := make([]byte, len(payload))
	_, readErr := dev.ReadAt(got, 0)

	// Zeros are the violation; an error is not. Refusing to answer is the designed
	// behaviour when the base cannot be rebuilt — it is the *silent* wrong answer that
	// this checker exists for, because a guest cannot tell those zeros from a range it
	// never wrote.
	zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
	s.Emit(Event{Kind: EventDurableRead, Key: volumeID, ZerosAfterRestart: zeros})
	if zeros {
		return fmt.Errorf("volume %s read zeros for a range it ACKed as durable", volumeID)
	}
	if readErr != nil {
		if hideObjects {
			s.Notef("the read was refused rather than answered: %v", readErr)
			return nil
		}
		return fmt.Errorf("read after restart: %w", readErr)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("read %x after restart, want %x", got[:8], payload[:8])
	}
	s.Notef("the restarted Agent read back what truncation had reclaimed locally")

	// And now the *scheduler* does what phase 1 did by hand: publish a checkpoint and
	// reclaim what it covers. Phase 1 stays hand-driven on purpose — it is the on-disk
	// state a restart has to survive, and it must exist before the Agent starts — but
	// deciding to truncate is the scheduler's job, and until this ran nothing simulated
	// had ever watched it decide.
	//
	// It must come after the read: the read is what waits for the base, and a checkpoint
	// taken while the base is pending sees durable = 0 against a store that proves more,
	// which ADR-0023 would read as a second writer. wal.BasePending declines it; this
	// ordering is what a real Agent does anyway, since the guest reads first.
	newBytes := bytes.Repeat([]byte{0xCD}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(newBytes, int64(i)*4096); err != nil {
			return fmt.Errorf("post-restart write %d: %w", i, err)
		}
	}
	if err := m.Checkpoint(ctx, volumeID); err != nil {
		return fmt.Errorf("the scheduler could not checkpoint after the restart: %w", err)
	}
	w := servedStatus(m, volumeID)
	s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: uint64(w.PublishedSequence), Published: uint64(w.PublishedSequence)})
	s.Notef("the scheduler published a checkpoint at sequence %d and reclaimed behind it", w.PublishedSequence)
	return nil
}

// CheckpointLeaseChecker enforces the *other* half of §12.6. The sentence names two
// things a SELF_FENCED Agent stops doing — "deja de ACKear durabilidad, deja de publicar
// checkpoints/manifests" — and only the first has ever had a checker
// (DurableAckLeaseChecker, INV-06, on the FLUSH path). This watches the second: no
// checkpoint object appears while the host's lease is invalid.
//
// It matters because the two gates protect different things. A durable ACK under a
// lapsed lease tells a guest its data is safe when another writer may already own the
// volume. A *checkpoint* under a lapsed lease is worse in one specific way: publishing
// advances `published`, and advancing `published` is what authorises throwing away the
// last local copy of the WAL (INV-13). A fenced host that publishes is a fenced host
// deleting data the writer that replaced it may still need.
type CheckpointLeaseChecker struct{ violation error }

// NewCheckpointLeaseChecker returns a fresh checker.
func NewCheckpointLeaseChecker() *CheckpointLeaseChecker { return &CheckpointLeaseChecker{} }

func (c *CheckpointLeaseChecker) Name() string { return "checkpoint-requires-lease" }

func (c *CheckpointLeaseChecker) Observe(e Event) {
	if e.Kind == EventCheckpoint && e.PublishedWithoutLease && c.violation == nil {
		c.violation = fmt.Errorf("volume %s published a checkpoint at step %d with an invalid lease (violates §12.6)",
			e.Key, e.Step)
	}
}

func (c *CheckpointLeaseChecker) Check() error { return c.violation }

// scenarioLapsedLeaseStopsPublishing drives the durability scheduler across the moment
// the host's lease expires.
//
// The volume is healthy in every other respect: the epoch object names this host, so
// §12.4's ownership check inside checkpoint.Create *passes*, and the object store is
// reachable and answers honestly. Nothing has taken the volume away — being fenced by a
// lapsed lease is precisely the case where nobody has, yet. The single `if` at the top of
// checkpointOnce is all that stands between this Agent and a published checkpoint, which
// is what makes it worth a checker.
func scenarioLapsedLeaseStopsPublishing(s *Sim) error {
	return lapsedLeaseStopsPublishing(s, leaseAskedEveryTime)
}

const (
	// leaseAskedEveryTime is the honest wiring: the lease is resolved per call, which is
	// what agent.applyLease does and why volume.go warns against capturing a manager.
	leaseAskedEveryTime = false
	// leaseAnsweredFromASnapshot is the bug: the lease question asked once at start-up
	// and never asked again. Not a hypothetical — it is the shortcut every other Agent
	// scenario here takes, harmlessly, because their leases never lapse.
	leaseAnsweredFromASnapshot = true
)

func lapsedLeaseStopsPublishing(s *Sim, cacheTheLease bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	hostID := ids.NewAt(simEpoch*1000, s.Rand).String()

	// §12.4: the epoch is granted to *this* host, so VerifyPublisher will let the
	// checkpoint through. Without this the volume would have no epoch object and Create
	// would allow the publish for a different reason (§22.5) — proving less.
	es := epoch.NewStore(s.Store)
	etag, err := es.Init(ctx, volumeID, 0)
	if err != nil {
		return fmt.Errorf("seeding the epoch object: %w", err)
	}
	if _, err := es.Grant(ctx, volumeID, etag, 1, hostID); err != nil {
		return fmt.Errorf("granting epoch 1 to this host: %w", err)
	}

	const leaseTTL = 10 * time.Second // §10 `lease_ttl: 10s`
	lm := lease.NewManager(s.Clock, leaseTTL)
	lm.Grant()

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", HostID: hostID,
		Limits: wal.Limits{SegmentBytes: 8192},
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		Lease: func() bool {
			if cacheTheLease {
				return true
			}
			return lm.Valid()
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("starting the volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the volume is not being served")
	}

	// Under a valid lease: write, flush, and checkpoint. This half is the control — if
	// the scheduler could not publish here, the silence after the lapse would prove
	// nothing about the lease.
	payload := bytes.Repeat([]byte{0x5A}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(payload, int64(i)*4096); err != nil {
			return fmt.Errorf("guest write %d: %w", i, err)
		}
	}
	if err := dev.Flush(ctx); err != nil {
		return fmt.Errorf("the FLUSH under a valid lease: %w", err)
	}
	// A read before the checkpoint, because the read is what waits for the base and a
	// checkpoint taken while it is pending is declined (wal.BasePending). That used to
	// apply only to a *resumed* volume; since a promoted destination turned out to need
	// a base too, every volume with an object store waits for one — and a real Agent
	// satisfies this ordering by itself, because the guest reads.
	if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("the read that waits for the base: %w", err)
	}
	s.Emit(Event{Kind: EventDurableAck, Durable: 4, LeaseValid: lm.Valid()})
	if err := m.Checkpoint(ctx, volumeID); err != nil {
		return fmt.Errorf("the checkpoint under a valid lease was refused: %w", err)
	}
	underLease, err := countCheckpoints(ctx, s, volumeID)
	if err != nil {
		return err
	}
	if underLease == 0 {
		return errors.New("no checkpoint was published under a valid lease: the lapse below would prove nothing")
	}
	s.Notef("volume %s: %d checkpoint(s) published while the lease was valid", volumeID, underLease)

	// The lease lapses. Nothing renews it — a partitioned Agent, a Control Plane that
	// cannot reach Postgres (§23, "PostgreSQL caído"). The host is SELF_FENCED on its own monotonic clock,
	// with no message from anyone.
	s.Tick(leaseTTL + time.Second)
	if lm.Valid() {
		return errors.New("the lease did not lapse: the scenario advanced past its TTL")
	}
	s.Emit(Event{Kind: EventFault, Msg: "the host lease lapsed with no renewal (SELF_FENCED, §12.6)"})

	// More guest writes, and a FLUSH that must fail. A scheduler that declined because
	// there was nothing new to publish would look identical to one that declined because
	// of the lease, so there has to be something new — and this is how a real partitioned
	// Agent produces it. §14.4 orders the steps: the objects are uploaded (step 4) and
	// *then* the lease is checked (step 5), so a lapsed lease leaves the WAL objects in
	// S3 and refuses the ACK. The store can now prove a longer durable prefix than this
	// volume ever ACKed, which is exactly the material a checkpoint publishes.
	newBytes := bytes.Repeat([]byte{0xA5}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(newBytes, int64(4+i)*4096); err != nil {
			return fmt.Errorf("guest write after the lapse %d: %w", i, err)
		}
	}
	if ferr := dev.Flush(ctx); ferr == nil {
		// The ACK escaped. That is INV-06, not the property this scenario is named for —
		// and a cached lease answer breaks both gates, because one function feeds both.
		// It is emitted rather than returned so DurableAckLeaseChecker sees it too, and
		// the scenario carries on to the gate it exists to watch.
		s.Emit(Event{Kind: EventDurableAck, Durable: 8, LeaseValid: lm.Valid()})
		s.Notef("the FLUSH was ACKed with a lapsed lease (§12.2/INV-06)")
	} else {
		s.Notef("the lapsed lease refused the durable ACK, and the objects are in the store anyway: %v", ferr)
	}
	err = m.Checkpoint(ctx, volumeID)

	// The verdict comes from the store, not from the error. A gate that returned the
	// right error and published anyway would satisfy an assertion on err; only counting
	// objects can tell the difference.
	after, cerr := countCheckpoints(ctx, s, volumeID)
	if cerr != nil {
		return cerr
	}
	published := after > underLease
	s.Emit(Event{
		Kind: EventCheckpoint, Key: volumeID,
		LeaseValid:            lm.Valid(),
		PublishedWithoutLease: published && !lm.Valid(),
		Msg:                   fmt.Sprintf("objects=%d->%d refusal=%v", underLease, after, err),
	})
	if published {
		return fmt.Errorf("volume %s published a checkpoint with a lapsed lease (%d objects, was %d) (§12.6)",
			volumeID, after, underLease)
	}
	if err == nil {
		return errors.New("the checkpoint reported success while publishing nothing: a refusal must be visible to its caller")
	}
	s.Notef("the lapsed lease refused the checkpoint: %v", err)
	return nil
}

// countCheckpoints reports how many checkpoint objects exist for a volume. It reads the
// store rather than the log's watermark on purpose: `published` is what the Agent
// *believes*, and the question here is what it actually put in the bucket.
func countCheckpoints(ctx context.Context, s *Sim, volumeID string) (int, error) {
	objs, err := s.Store.List(ctx, "checkpoints/"+volumeID+"/")
	if err != nil {
		return 0, fmt.Errorf("listing the volume's checkpoints: %w", err)
	}
	return len(objs), nil
}

// scenarioCrashedFlushDoesNotCollideOnRestart pins ADR-0024 — a restarted Agent
// re-attaches at the *same* epoch — against the one state that makes it interesting:
// **objects in S3 for sequences the guest was never told were durable.**
//
// §14.4 produces that state on purpose. Step 4 uploads; step 5 checks the lease. So a
// writer whose lease lapses mid-FLUSH (or a process killed between the two) leaves the
// bucket holding a *longer* contiguous prefix than anything it ever ACKed. If a restart
// resumed from the last ACK — the number a dead process held, and the intuitive one — it
// would re-issue those sequences with whatever the guest writes next. Same key, different
// content hash: INV-21 hard-fails the PUT, and the volume stops being able to flush at
// all.
//
// The assertion is that the second flush *succeeds*, which is only true if the resumed
// writer numbered above the whole bucket. It is a behavioural check of ADR-0024's
// properties 2 and 4 together, and it is the reason the ADR names them: relaxing either
// turns this scenario red rather than leaving the decision quietly invalid.
func scenarioCrashedFlushDoesNotCollideOnRestart(s *Sim) error {
	// Both listings, because the difference between them is the point. The first attempt
	// at this scenario ran only the honest one and asserted "the writer resumed above
	// the bucket" — which was true, and for the wrong reason. Running it against a
	// listing that comes back short was supposed to break it. It did not, and that is
	// what identified which mechanism is actually load-bearing (see below).
	if err := crashedFlushDoesNotCollideOnRestart(s, honestListing); err != nil {
		return err
	}
	return crashedFlushDoesNotCollideOnRestart(s, shortListing)
}

const (
	honestListing = false
	// shortListing is a listing that comes back one object short — a truncated page, an
	// eventually-consistent index, or the same resumed point that trusting a remembered
	// watermark would produce. The no-collision property must survive it.
	shortListing = true
)

// shortListingStore drops the newest object from every listing.
type shortListingStore struct{ objectstore.Store }

func (s shortListingStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	objs, err := s.Store.List(ctx, prefix)
	if err != nil || len(objs) == 0 {
		return objs, err
	}
	return objs[:len(objs)-1], nil
}

func crashedFlushDoesNotCollideOnRestart(s *Sim, shortenListings bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	vol := [16]byte(u)
	limits := wal.Limits{SegmentBytes: 8192}
	const leaseTTL = 10 * time.Second

	// Phase 1: the incarnation that dies. Four records ACKed, four more uploaded and
	// never ACKed because the lease went away between step 4 and step 5.
	lm := lease.NewManager(s.Clock, leaseTTL)
	lm.Grant()
	l := wal.NewLog(s.Disk, "/var/lib/spin/wal", s.Clock, vol, 1, limits)
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 3), lm)

	acked := bytes.Repeat([]byte{0x11}, 4096)
	for i := range 4 {
		if _, err := l.Write(uint64(i)*4096, acked, 0); err != nil {
			return fmt.Errorf("acked write %d: %w", i, err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("the flush that is ACKed: %w", err)
	}
	ackedDurable := l.Watermarks().Durable

	s.Tick(leaseTTL + time.Second)
	unacked := bytes.Repeat([]byte{0x22}, 4096)
	for i := range 4 {
		if _, err := l.Write(uint64(4+i)*4096, unacked, 0); err != nil {
			return fmt.Errorf("unacked write %d: %w", i, err)
		}
	}
	if err := l.Flush(ctx); err == nil {
		return errors.New("a FLUSH was ACKed with a lapsed lease (§12.2/INV-06)")
	}
	// The crash. Not Close(): a killed process does not get to run cleanup, and the
	// point of the scenario is what the *next* process finds on disk and in S3.
	inBucket, err := recovery.DurablePoint(ctx, s.Store, vol, 1)
	if err != nil {
		return fmt.Errorf("what the bucket can prove after the crash: %w", err)
	}
	if inBucket <= ackedDurable {
		return fmt.Errorf("the bucket proves %d and the guest was told %d: this scenario needs the uploads to have outrun the ACK (§14.4 steps 4 and 5)",
			inBucket, ackedDurable)
	}
	s.Notef("crash: the guest was told %d was durable, the bucket holds %d", ackedDurable, inBucket)

	// Phase 2: the same host comes back, at the same epoch, with a fresh lease. No
	// Control Plane involvement — GetDesiredState still lists epoch 1 (ADR-0024).
	lm2 := lease.NewManager(s.Clock, leaseTTL)
	lm2.Grant()
	var store objectstore.Store = s.Store
	if shortenListings {
		store = shortListingStore{Store: s.Store}
		s.Emit(Event{Kind: EventFault, Msg: "the object listing comes back one object short"})
	}
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Limits: limits,
		HostID: ids.NewAt(simEpoch*1000, s.Rand).String(),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   store,
		Lease:   func() bool { return lm2.Valid() },
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("the restarted Agent could not re-attach at epoch 1: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the restarted Agent is not serving the volume")
	}
	// The read is what waits for the base, and the base is what carries the resumed
	// durable point (InstallBase). Nothing below is meaningful before it lands.
	if _, err := dev.ReadAt(make([]byte, 4096), 0); err != nil {
		return fmt.Errorf("the first read after the restart: %w", err)
	}

	// **This is the mechanism.** The resumed writer numbers above everything the bucket
	// holds, and it does so whether or not the listing was honest — because the number
	// comes from the *local segments*, not from the base. `Resume` sets `local` to the
	// last record on disk, `InstallBase` only ever raises watermarks, and INV-13 forbids
	// truncating above `published`, which never exceeds what the bucket can prove. So
	// everything the dead incarnation uploaded is still on this disk, with its sequence
	// numbers, and the new incarnation cannot re-use them.
	resumed := servedStatus(m, volumeID)
	if uint64(resumed.LocalSequence) < inBucket {
		return fmt.Errorf("the resumed writer numbers from %d while the bucket already holds %d: it would re-issue sequences that exist (ADR-0024, INV-13)",
			resumed.LocalSequence, inBucket)
	}

	// The guest writes something *different* over the range whose sequences the dead
	// incarnation had already uploaded. This is the collision, if there is one.
	divergent := bytes.Repeat([]byte{0x33}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(divergent, int64(4+i)*4096); err != nil {
			return fmt.Errorf("post-restart write %d: %w", i, err)
		}
	}
	if err := dev.Flush(ctx); err != nil {
		// ErrDivergentObject here is the failure ADR-0024 exists to rule out: it means
		// the resumed writer re-used sequences the bucket already had under a different
		// content hash (INV-21, §14.5).
		return fmt.Errorf("the first FLUSH after re-attaching at the same epoch: %w", err)
	}
	w := servedStatus(m, volumeID)
	if uint64(w.DurableSequence) <= inBucket {
		return fmt.Errorf("after the restart durable is %d, not past the %d the bucket already held (ADR-0024, INV-08)",
			w.DurableSequence, inBucket)
	}
	s.Emit(Event{Kind: EventWatermark, Durable: uint64(w.DurableSequence), Published: uint64(w.PublishedSequence), Local: uint64(w.LocalSequence)})
	s.Notef("re-attached at epoch 1 (listing short=%t) and flushed past the crash: durable %d -> %d",
		shortenListings, inBucket, w.DurableSequence)
	return nil
}

// scenarioAgentEncryptsWhatLeavesTheHost is INV-15 (§5.10) at the seam that decides
// it. `scenarioEncryptedWALNoPlaintextLeak` already proves the *WAL* encrypts when it
// is given an Encryption — but until BUILD-INVENTORY increment 6 nothing ever gave it
// one: every volume the Agent served built its log with `enc == nil`, and the checker
// that watches for cleartext had never seen an Agent.
//
// So this drives the real VolumeManager with a real crypto.DevKMS, writes a pattern a
// guest would recognise, flushes it into the object store, and reads every object back
// looking for that pattern. What it watches is not the flag — it is the bytes in the
// bucket.
//
// It also covers the half that has no other test: an Agent whose KMS cannot unwrap a
// volume's DEK must serve *nothing*. Falling back to plaintext would put this guest's
// data in the bucket under a name that says it is encrypted, and §15.3's
// crypto-shredding guarantee does not survive that — the objects stay readable after
// the DEK is destroyed.
func scenarioAgentEncryptsWhatLeavesTheHost(s *Sim) error {
	return agentEncryptsWhatLeavesTheHost(s, hostHoldsItsKEK)
}

const (
	hostHoldsItsKEK = false
	// noKEKOnTheHost is an Agent started without -kek-file. It is a *supported* mode —
	// dev and the QEMU lane run in it — and running a real volume in it is still an
	// INV-15 violation, which is the point: the misconfiguration is the bug, it is
	// reachable by leaving one flag off, and nothing but this checker notices.
	noKEKOnTheHost = true
)

func agentEncryptsWhatLeavesTheHost(s *Sim, withoutKEK bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()

	// A KEK drawn from the seeded PRNG, so the same seed gives the same key and the
	// same ciphertext (INV-02).
	var kek [crypto.DEKSize]byte
	if _, err := io.ReadFull(s.Rand, kek[:]); err != nil {
		return err
	}
	kms := crypto.NewDevKMS(kek, "kek-dst")
	dek, err := crypto.GenerateDEK(s.Rand, 7)
	if err != nil {
		return err
	}
	wrapped, err := kms.WrapDEK(s.Rand, dek)
	if err != nil {
		return err
	}

	keys := func(honest bool) agent.KeysFunc {
		return func(context.Context, string) (agent.VolumeKeys, error) {
			k := agent.VolumeKeys{VolumeID: volumeID, DEKWrapped: wrapped, KEKID: "kek-dst", DEKKeyID: dek.KeyID}
			if !honest {
				// The version the Control Plane hands over, rewritten. It is bound as
				// GCM additional authenticated data, so the unwrap fails — this is a
				// key this host cannot open, reached without touching the Agent.
				k.DEKKeyID = dek.KeyID + 1
			}
			return k, nil
		}
	}

	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	// A data directory per manager. The second one below is a *different* Agent, and
	// since DEV-0014 a manager claims its directory exclusively (§10: one Agent per
	// host) — two sharing one would be the corruption that lock exists to prevent,
	// not a convenience.
	newManager := func(honest bool, dataDir string) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Limits: wal.Limits{SegmentBytes: 8192},
			HostID: ids.NewAt(simEpoch*1000, s.Rand).String(),
		}, agent.VolumeManagerDeps{
			Clock:   s.Clock,
			Disk:    s.Disk,
			Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
			Mapper:  simMapper{},
			EventFD: simEventFD,
			Store:   s.Store,
			Lease:   func() bool { return lm.Valid() },
			KMS:     kmsOrNil(kms, withoutKEK),
			Keys:    keys(honest),
		})
	}
	desired := []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}

	m, err := newManager(true, "/var/lib/spin")
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	if err := m.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting an encrypted volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the encrypted volume is not being served")
	}

	// A pattern no encryption would leave intact and no header would produce by
	// accident: 4 KiB of one byte, which is exactly what a guest filesystem writing
	// zeroed or filled blocks looks like.
	pattern := bytes.Repeat([]byte{0xE7}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(pattern, int64(i)*4096); err != nil {
			return fmt.Errorf("guest write %d: %w", i, err)
		}
	}
	if err := dev.Flush(ctx); err != nil {
		return fmt.Errorf("the FLUSH that puts it in the bucket: %w", err)
	}

	// Everything this volume put in the store, examined for the guest's bytes. The
	// event carries what was found, so the INV-15 checker sees it on every seed rather
	// than only when this scenario's own assertion happens to run.
	objs, err := s.Store.List(ctx, "wal/"+volumeID+"/")
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return errors.New("no WAL objects were uploaded: this scenario would prove nothing")
	}
	leaked := false
	for _, o := range objs {
		body, err := s.Store.Get(ctx, o.Key)
		if err != nil {
			return err
		}
		if bytes.Contains(body, pattern) {
			leaked = true
			s.Emit(Event{Kind: EventLeavesHost, ClearLeak: true,
				Msg: fmt.Sprintf("object %s carries the guest's plaintext", o.Key)})
			break
		}
	}
	if !leaked {
		s.Emit(Event{Kind: EventLeavesHost, ClearLeak: false,
			Msg: fmt.Sprintf("%d WAL objects, none carrying the guest's pattern", len(objs))})
	}
	if leaked {
		return fmt.Errorf("volume %s wrote the guest's plaintext into the object store (§5.10/INV-15)", volumeID)
	}

	// And it is not encrypted-to-noise: the same log replays through the same key.
	got := make([]byte, len(pattern))
	if _, err := dev.ReadAt(got, 0); err != nil {
		return fmt.Errorf("reading back through the encrypted log: %w", err)
	}
	if !bytes.Equal(got, pattern) {
		return errors.New("the encrypted volume did not read back what the guest wrote")
	}
	s.Notef("volume %s: %d objects in the bucket, none in the clear, and the guest reads its own bytes",
		volumeID, len(objs))

	if withoutKEK {
		// The second half asserts a refusal that only a KMS can produce. An Agent
		// without one has already said everything it has to say.
		return nil
	}

	// The other half: a key this host cannot unwrap serves nothing. A different volume
	// id, because the first one's runtime is still up.
	badID := ids.NewAt(simEpoch*1000, s.Rand).String()
	m2, err := newManager(false, "/var/lib/spin-second")
	if err != nil {
		return err
	}
	defer func() { _ = m2.Close() }()
	badDesired := []*storagev1.DesiredVolume{{
		VolumeId: badID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}
	if err := m2.Apply(ctx, badDesired); err == nil {
		return errors.New("a volume whose DEK could not be unwrapped was started anyway (§15)")
	}
	if _, served := m2.Device(badID); served {
		return errors.New("a volume whose DEK could not be unwrapped is being served (§15)")
	}
	s.Notef("a volume whose DEK will not unwrap is not served at all, rather than served in the clear")
	return nil
}

// kmsOrNil drops the KMS, which is all it takes to serve a volume in the clear.
func kmsOrNil(k crypto.KMS, drop bool) crypto.KMS {
	if drop {
		return nil
	}
	return k
}

// scenarioACloneReadsThroughItsParent is DEV-0007's clone half, at the seam that
// decides it.
//
// §20 says a clone is pure metadata: "a new active child at epoch 1 that reuses the
// parent snapshot's already-durable objects, with no data copy". Nothing made that true.
// The clone's Agent started an empty WAL under the *clone's* volume id and recovered
// against that id, which finds nothing — every object the parent wrote is under the
// parent's — so the base installed empty and the clone read **zeros for everything its
// parent ever wrote**. A volume advertised as a copy, delivered blank.
//
// The checker is the one that already watches for exactly this: `DurableRangeChecker`
// reads the bytes a guest would receive, which is the only place "cloned" and "cloned
// correctly" differ.
func scenarioACloneReadsThroughItsParent(s *Sim) error {
	return aCloneReadsThroughItsParent(s, chainLinkCarried)
}

const (
	chainLinkCarried = false
	// chainLinkDropped is the defect this closes: the Control Plane knows the clone's
	// parent and the desired state does not carry it. Reachable by one missing field,
	// and the Agent cannot look it up — ADR-0021 keeps it from knowing what a Control
	// Plane is.
	chainLinkDropped = true
)

func aCloneReadsThroughItsParent(s *Sim, dropLink bool) error {
	ctx := context.Background()
	parentID := ids.NewAt(simEpoch*1000, s.Rand).String()
	cloneID := ids.NewAt(simEpoch*1000, s.Rand).String()
	pu, err := ids.Parse(parentID)
	if err != nil {
		return err
	}
	parentVol := [16]byte(pu)

	// The parent writes, flushes, and is snapshotted. Everything the clone will read
	// lives under the parent's id from here on.
	payload := bytes.Repeat([]byte{0x77}, 4096)
	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	parent := wal.NewLog(s.Disk, "/var/lib/parent/wal", s.Clock, parentVol, 1, wal.Limits{})
	parent.EnableRemote(wal.NewBatcher(s.Clock, parentVol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 3), leaseAlways{lm})
	if _, err := parent.Write(0, payload, 0); err != nil {
		return fmt.Errorf("the parent's write: %w", err)
	}
	if err := parent.Flush(ctx); err != nil {
		return fmt.Errorf("the parent's flush: %w", err)
	}
	snapID := ids.NewAt(simEpoch*1000, s.Rand).String()
	man, _, err := snapshot.NewSnapshotter(s.Store, s.Clock).Create(ctx, parent, parentVol, 1, snapID, "")
	if err != nil {
		return fmt.Errorf("snapshotting the parent: %w", err)
	}
	if err := parent.Close(); err != nil {
		return err
	}
	s.Notef("parent %s snapshotted at sequence %d", parentID, man.TargetSequence)

	// The clone: its own volume id, its own empty WAL, and a desired state that names
	// what it descends from.
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/clone", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		Lease:   func() bool { return lm.Valid() },
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	desired := &storagev1.DesiredVolume{
		VolumeId: cloneID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability:       storagev1.Durability_DURABILITY_REMOTE,
		State:            storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		ParentSnapshotId: snapID,
		ParentVolumeId:   parentID,
	}
	if dropLink {
		desired.ParentSnapshotId, desired.ParentVolumeId = "", ""
		s.Emit(Event{Kind: EventFault, Msg: "the desired state does not carry the clone's chain link"})
	}
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{desired}); err != nil {
		return fmt.Errorf("starting the clone: %w", err)
	}
	dev, ok := m.Device(cloneID)
	if !ok {
		return errors.New("the clone is not being served")
	}

	got := make([]byte, len(payload))
	_, readErr := dev.ReadAt(got, 0)

	// Zeros are the violation and an error is not: refusing to answer is the designed
	// behaviour when the chain cannot be followed. It is the *silent* wrong answer this
	// watches for, because a guest cannot tell those zeros from a range nobody wrote.
	zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
	s.Emit(Event{Kind: EventDurableRead, Key: cloneID, ZerosAfterRestart: zeros})
	if zeros {
		return fmt.Errorf("clone %s read zeros for a range its parent wrote (§20)", cloneID)
	}
	if readErr != nil {
		if dropLink {
			s.Notef("the read was refused rather than answered: %v", readErr)
			return nil
		}
		return fmt.Errorf("the clone's read: %w", readErr)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("clone read %x, the parent wrote %x", got[:8], payload[:8])
	}
	s.Notef("clone %s read its parent's bytes through the snapshot, with no data copy", cloneID)
	return nil
}

// scenarioAPromotedHostReadsThePreviousEpoch is INV-09 where a guest can see it: every
// write the fenced writer ACKed as durable must be readable on the host that replaced it.
//
// The invariant was never in doubt in the object store — `recovery.DurablePrefix` finds
// the data, and the drain proves it does. What nothing checked is whether the **Agent on
// the destination ever asks**. It did not: a promoted volume has no local segments and no
// parent snapshot, so the Agent skipped the base fetch entirely and served **zeros for
// its predecessor's whole volume**, with no error anywhere. INV-09 held in S3 and the
// guest still got nothing.
//
// The promotion here is the real sequence, not a shortcut: the previous epoch's writer
// flushes, and the new epoch gets a recovery point (§12.5) — the immutable boundary that
// says what it adopted. Without that object the epoch chain is broken and the Agent
// refuses, which is a different (and correct) behaviour that would have hidden this.
func scenarioAPromotedHostReadsThePreviousEpoch(s *Sim) error {
	return aPromotedHostReadsThePreviousEpoch(s, storeIsReachable)
}

const (
	storeIsReachable = false
	// destinationCannotList is how a promoted host is made to see nothing: a listing
	// that returns no objects for the volume. It is the same fault the truncated-restart
	// arm uses, and it belongs here too — the destination has *only* the object store,
	// so a store that answers empty is the whole of its world.
	destinationCannotList = true
)

func aPromotedHostReadsThePreviousEpoch(s *Sim, hideObjects bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	vol := [16]byte(u)
	payload := bytes.Repeat([]byte{0x42}, 4096)

	// Epoch 1, on the host that is about to be fenced.
	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	l := wal.NewLog(s.Disk, "/var/lib/source/wal", s.Clock, vol, 1, wal.Limits{})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 3), leaseAlways{lm})
	if _, err := l.Write(0, payload, 0); err != nil {
		return fmt.Errorf("the fenced writer's write: %w", err)
	}
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("the fenced writer's flush: %w", err)
	}
	acked := l.Watermarks().Durable
	if err := l.Close(); err != nil {
		return err
	}

	// The promotion's own durable step: epoch 2 records what it adopted from epoch 1
	// (§12.5). This object is the boundary the chain is read across.
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, acked); err != nil {
		return fmt.Errorf("writing the recovery point: %w", err)
	}
	s.Notef("epoch 1 ACKed %d; epoch 2's recovery point adopts it", acked)

	// The destination: another host's data directory, no local WAL, epoch 2.
	var store objectstore.Store = s.Store
	if hideObjects {
		store = hidingStore{Store: s.Store}
		s.Emit(Event{Kind: EventFault, Msg: "the destination's object store lists nothing"})
	}
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/dest", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   store,
		Lease:   func() bool { return lm.Valid() },
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 2,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("the destination could not start the promoted volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the promoted volume is not being served on the destination")
	}

	got := make([]byte, len(payload))
	_, readErr := dev.ReadAt(got, 0)

	zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
	s.Emit(Event{Kind: EventDurableRead, Key: volumeID, ZerosAfterRestart: zeros})
	if zeros {
		return fmt.Errorf("the promoted host read zeros for a range epoch 1 ACKed as durable (§12.3/INV-09)")
	}
	if readErr != nil {
		if hideObjects {
			s.Notef("the read was refused rather than answered: %v", readErr)
			return nil
		}
		return fmt.Errorf("the promoted host's read: %w", readErr)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("the promoted host read %x, epoch 1 wrote %x", got[:8], payload[:8])
	}
	s.Notef("the promoted host serves epoch 1's bytes from the object store alone")
	return nil
}

// leaseAlways adapts a lease.Manager into the wal.LeaseChecker the Log wants.
type leaseAlways struct{ m *lease.Manager }

func (l leaseAlways) Valid() bool { return l.m.Valid() }

// servedStatus reads one volume's reported status out of the manager.
func servedStatus(m *agent.VolumeManager, volumeID string) agent.VolumeStatus {
	vols, err := m.Volumes(context.Background())
	if err != nil {
		return agent.VolumeStatus{}
	}
	for _, v := range vols {
		if v.VolumeID == volumeID {
			return v
		}
	}
	return agent.VolumeStatus{}
}

// scenarioEncryptedVolumeSurvivesARestart is the cross nothing in this harness made:
// encryption *and* a restart. Both halves were modelled separately and each was green —
// agent-encrypts-what-leaves-the-host never restarts, and
// truncated-volume-survives-a-restart runs with keyID 0 and no KMS in its deps — so the
// path where they meet had no coverage at all, and that is exactly where the defect was:
// internal/agent handed recovery.RecoverOver a literal nil Encryption for a volume whose
// DEK it had just unwrapped, and the guest was served its own data still sealed.
//
// It is the "test the seams, not only the parts" rule with a name: two well-covered
// components, and the bug living in the argument one passes the other.
func scenarioEncryptedVolumeSurvivesARestart(s *Sim) error {
	return encryptedVolumeSurvivesARestart(s, restartHoldingTheKEK)
}

const (
	restartHoldingTheKEK = false
	restartWithoutTheKEK = true
)

func encryptedVolumeSurvivesARestart(s *Sim, dropKEKOnRestart bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	vol := [16]byte(u)

	// Key material from the seeded PRNG, so the same seed produces the same ciphertext
	// (INV-02) and this scenario is reproducible byte for byte.
	var kek [crypto.DEKSize]byte
	if _, err := io.ReadFull(s.Rand, kek[:]); err != nil {
		return err
	}
	kms := crypto.NewDevKMS(kek, "kek-dst")
	dek, err := crypto.GenerateDEK(s.Rand, 7)
	if err != nil {
		return err
	}
	wrapped, err := kms.WrapDEK(s.Rand, dek)
	if err != nil {
		return err
	}
	enc, err := wal.NewEncryption(dek, vol)
	if err != nil {
		return err
	}

	// A pattern no encryption leaves intact and no header produces by accident.
	pattern := bytes.Repeat([]byte{0xE7}, 4096)
	const segBytes = 8192
	limits := wal.Limits{SegmentBytes: segBytes}

	// Phase 1 — a previous run of this Agent, hand-driven through wal for the same
	// reason the truncated-restart scenario is: what a restart has to survive is an
	// on-disk state that must already exist before the Agent starts.
	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	l := wal.NewLog(s.Disk, "/var/lib/spin/wal", s.Clock, vol, 1, limits)
	l.EnableEncryption(enc)
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, dek.KeyID, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 3), lm)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, pattern, 0); err != nil {
			return fmt.Errorf("seeding write %d: %w", i, err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("seeding flush: %w", err)
	}
	durable := l.Watermarks().Durable

	// Truncate, so the read view after the restart *must* come from the object store.
	// Without this the local segments answer the read and the scenario proves nothing
	// about the recovered base — which is the only thing it exists to prove.
	dir := wal.SegmentDir("/var/lib/spin/wal", vol, 1)
	before, err := s.Disk.List(dir)
	if err != nil {
		return err
	}
	if err := l.AdvancePublished(durable); err != nil {
		return err
	}
	if err := l.TruncateLocal(durable); err != nil {
		return err
	}
	after, err := s.Disk.List(dir)
	if err != nil {
		return err
	}
	if len(after) >= len(before) {
		return fmt.Errorf("truncation unlinked nothing (%d segments, then %d): this scenario proves nothing",
			len(before), len(after))
	}
	if err := l.Close(); err != nil {
		return err
	}

	// The objects really are sealed, checked here rather than assumed. If this ever
	// stopped holding, the restart below would read back the pattern for the wrong
	// reason and the scenario would pass while proving nothing.
	objs, err := s.Store.List(ctx, "wal/"+volumeID+"/")
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return errors.New("no WAL objects were uploaded: this scenario would prove nothing")
	}
	for _, o := range objs {
		body, err := s.Store.Get(ctx, o.Key)
		if err != nil {
			return err
		}
		if bytes.Contains(body, pattern) {
			return fmt.Errorf("object %s carries the guest's plaintext (§5.10/INV-15)", o.Key)
		}
	}
	s.Notef("volume %s: %d segments reclaimed, %d sealed objects hold durable=%d",
		volumeID, len(before)-len(after), len(objs), durable)

	// Phase 2 — the Agent restarts and must rebuild that view from the sealed objects.
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Limits: limits,
		HostID: ids.NewAt(simEpoch*1000, s.Rand).String(),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		Lease:   func() bool { return lm.Valid() },
		KMS:     kmsOrNil(kms, dropKEKOnRestart),
		Keys: func(context.Context, string) (agent.VolumeKeys, error) {
			return agent.VolumeKeys{
				VolumeID: volumeID, DEKWrapped: wrapped, KEKID: "kek-dst", DEKKeyID: dek.KeyID,
			}, nil
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("the restarted Agent could not start the volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the restarted Agent is not serving the volume")
	}

	got := make([]byte, len(pattern))
	_, readErr := dev.ReadAt(got, 0)

	// Three outcomes, and only one of them is correct. Zeros mean the base was missing;
	// foreign bytes mean it was built wrongly — the ciphertext case, which is why this
	// scenario exists. An error is not a violation: refusing to answer is the designed
	// behaviour when the base cannot be rebuilt, and it is the *silent* wrong answer
	// that a guest has no way to detect.
	answered := readErr == nil
	zeros := answered && bytes.Equal(got, make([]byte, len(got)))
	foreign := answered && !zeros && !bytes.Equal(got, pattern)
	s.Emit(Event{Kind: EventDurableRead, Key: volumeID,
		ZerosAfterRestart: zeros, ForeignBytesAfterRestart: foreign})
	if zeros {
		return fmt.Errorf("volume %s read zeros for a range it ACKed as durable", volumeID)
	}
	if foreign {
		return fmt.Errorf("volume %s was served %x… for a range where it wrote %x… (§5.8/INV-08)",
			volumeID, got[:8], pattern[:8])
	}
	if readErr != nil {
		if dropKEKOnRestart {
			// The whole point of the planted arm: one missing flag must cost the volume
			// its reads, not cost the guest its data.
			s.Notef("restarted without the KEK: the read was refused rather than answered: %v", readErr)
			return nil
		}
		return fmt.Errorf("read after restart: %w", readErr)
	}
	if dropKEKOnRestart {
		return fmt.Errorf("volume %s answered a read after restarting with no KEK at all", volumeID)
	}
	s.Notef("the restarted Agent decrypted its own sealed objects and served the guest its own bytes")
	return nil
}

// scenarioLocalVolumeDrainsWithoutClaimingWithoutALease is §14.8 rule 3 under the INV-06
// checker that already governs every other way durable_sequence moves.
//
// The drain is a *second* path to advancing durable — the first is §14.4 step 6 — and a
// second path is a second place for the lease rule to be forgotten. That is not
// hypothetical here: the reading of §14.8 that this increment rejected ("the lease does
// not gate durability in local mode") is a coherent sentence, and taking it would move a
// watermark a promoted successor reads while this host is fenced.
//
// So the arm asserts the split the increment decided: the objects reach the store with no
// lease at all, because a fenced host holds the only copy and refusing to upload would
// turn fencing into data loss; and durable stands still until the lease is back.
func scenarioLocalVolumeDrainsWithoutClaimingWithoutALease(s *Sim) error {
	return localVolumeDrains(s, leaseResolvedPerCall)
}

const (
	leaseResolvedPerCall  = false
	leaseReadOnceAtStart  = true
	localDrainVolumeBytes = 1 << 20
)

func localVolumeDrains(s *Sim, staleLease bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()

	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	leaseValid := true

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Limits: wal.Limits{SegmentBytes: 8192},
		HostID: ids.NewAt(simEpoch*1000, s.Rand).String(),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		// The planted bug's lever, and it is a shortcut a real wiring can take: a Lease
		// that answers from a snapshot taken at construction instead of resolving the
		// current one per call. Every other Agent scenario takes it harmlessly, because
		// their leases never lapse. Here it makes the fenced drain claim durability.
		Lease: func() bool {
			if staleLease {
				return true
			}
			return leaseValid && lm.Valid()
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: localDrainVolumeBytes, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_LOCAL,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("starting a local-durability volume: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the local-durability volume is not being served")
	}

	// A guest write and its FLUSH. In local mode this ACKs on fdatasync and puts nothing
	// in the store — that is the mode, not a failure.
	pattern := bytes.Repeat([]byte{0x5A}, 4096)
	for i := range 4 {
		if _, err := dev.WriteAt(pattern, int64(i)*4096); err != nil {
			return fmt.Errorf("guest write %d: %w", i, err)
		}
	}
	if err := dev.Flush(ctx); err != nil {
		return fmt.Errorf("the local-mode FLUSH: %w", err)
	}
	objs, err := s.Store.List(ctx, "wal/"+volumeID+"/")
	if err != nil {
		return err
	}
	if len(objs) != 0 {
		return fmt.Errorf("a local-mode FLUSH put %d object(s) in the store; §14.8 says it ACKs on fdatasync alone", len(objs))
	}

	// Now fence the host and drain. The objects must land; the claim must not.
	leaseValid = false
	if err := m.Drain(ctx, volumeID); err != nil {
		return fmt.Errorf("the fenced drain: %w", err)
	}
	objs, err = s.Store.List(ctx, "wal/"+volumeID+"/")
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return errors.New("the fenced host uploaded nothing: its records exist nowhere else, so this turns fencing into data loss")
	}
	// Emitted only if the drain actually claimed something. An event saying "durable
	// ACK" when nothing was ACKed would be a false entry in the trace, and LeaseValid
	// carries the *ground truth* rather than what the (possibly stale) closure answered
	// — the checker's job is to compare the claim against reality, so reality is what it
	// has to be given.
	if fencedDurable := m.WatermarksOf(volumeID).Durable; fencedDurable > 0 {
		s.Emit(Event{Kind: EventDurableAck, Key: volumeID, Durable: fencedDurable, LeaseValid: leaseValid && lm.Valid()})
	}

	// And with the lease back, the claim it already earned.
	leaseValid = true
	if err := m.Drain(ctx, volumeID); err != nil {
		return fmt.Errorf("the drain after the lease returned: %w", err)
	}
	after := m.WatermarksOf(volumeID).Durable
	if after > 0 {
		s.Emit(Event{Kind: EventDurableAck, Key: volumeID, Durable: after, LeaseValid: leaseValid && lm.Valid()})
	}
	if after == 0 {
		return errors.New("durable never advanced once the lease was valid: the volume is stuck below its own objects")
	}
	s.Notef("volume %s: %d object(s) drained while fenced, durable advanced to %d only once the lease was back",
		volumeID, len(objs), after)
	return nil
}
