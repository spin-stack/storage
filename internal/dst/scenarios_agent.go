package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/objectstore"
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
	}
}

func agentCheckers() []Checker {
	return []Checker{NewFencedVolumeChecker(), NewDurableRangeChecker()}
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
type simListener struct{ closed chan struct{} }

func newSimListener() *simListener { return &simListener{closed: make(chan struct{})} }

func (l *simListener) Accept() (vhost.Conn, error) {
	<-l.closed
	return nil, errors.New("listener closed")
}

func (l *simListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
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
			VolumeId:  volumeID,
			SizeBytes: 1 << 20,
			BlockSize: 512,
			Epoch:     epoch,
			State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
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
// zeros. Every other checker here watches watermarks and objects; this one watches the
// bytes a guest would actually receive, which is the only place the difference between
// "recovered" and "recovered correctly" is visible.
type DurableRangeChecker struct{ violation error }

// NewDurableRangeChecker returns a fresh checker.
func NewDurableRangeChecker() *DurableRangeChecker { return &DurableRangeChecker{} }

func (c *DurableRangeChecker) Name() string { return "durable-range-survives-restart" }

func (c *DurableRangeChecker) Observe(e Event) {
	if e.Kind == EventDurableRead && e.ZerosAfterRestart && c.violation == nil {
		c.violation = fmt.Errorf("volume %s read zeros at step %d for a range it ACKed as durable (violates §5.8/INV-08)",
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
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
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
