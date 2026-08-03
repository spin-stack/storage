package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
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
		{Name: "a-stopped-volume-comes-back-from-its-image", Run: scenarioAStoppedVolumeComesBackFromItsImage},
		{Name: "a-clone-reads-through-its-parent", Run: scenarioACloneReadsThroughItsParent},
		{Name: "a-snapshot-of-a-live-volume-is-frozen", Run: scenarioASnapshotOfALiveVolumeIsFrozen},
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

// hidingStore answers as if the volume's objects were not there. It is not a
// hypothetical fault: a store that answers nothing under a prefix is what a mis-typed
// bucket, a lost listing or a wrong-epoch key looks like from here — and the boot path
// cannot tell any of those from a volume that never wrote anything.
//
// **It hides Head and Get, not only List.** It used to hide only the listing, which was
// enough while the boot path was a replay that began with one. The image path never
// lists: it reads a manifest by key. A fault that leaves the manifest readable does not
// hide anything, and the arm that planted it passed while proving nothing — which is why
// this now covers every way a reader can find an object.
type hidingStore struct{ objectstore.Store }

func (hidingStore) List(context.Context, string) ([]objectstore.ObjectInfo, error) {
	return nil, nil
}

func (h hidingStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	if strings.HasPrefix(key, "image/") {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return h.Store.Head(ctx, key)
}

func (h hidingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "image/") {
		return nil, objectstore.ErrNotFound
	}
	return h.Store.Get(ctx, key)
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

	// The parent writes and publishes a snapshot. Everything the clone will read lives
	// under the parent's id from here on, in chunks the snapshot's manifest names — the
	// clone copies nothing, which is §20's whole claim.
	payload := bytes.Repeat([]byte{0x77}, 4096)
	snapID := ids.NewAt(simEpoch*1000, s.Rand).String()
	parentDone := cow.NewIntervalMap()
	parentDone.Overwrite(0, payload)
	if _, err := image.PublishSnapshot(ctx, s.Store, s.Rand, nil, parentVol, parentDone, 1, snapID); err != nil {
		return fmt.Errorf("publishing the parent's snapshot: %w", err)
	}
	s.Notef("parent %s published snapshot %s", parentID, snapID)

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

// scenarioASnapshotOfALiveVolumeIsFrozen is §19 as a guest experiences it, and the only
// place "snapshot" means anything.
//
// The source VM is not stopped and not paused: it writes pattern A, a snapshot is taken,
// it writes pattern B over the same offset, and a clone of that snapshot must read **A**.
// If it reads B the copy was never frozen — it is whatever the volume happened to hold
// when the upload finished, which is a snapshot of no moment in particular.
//
// §19 is what makes this cost nothing: freezing is a pointer swap (cow.NewIntervalMapOver
// layers a new map over the old and never writes through to it), so the "pause" §2 budgets
// at ~0 really is one lock acquisition. The scenario drives it through the *Agent*, not
// through wal.Freeze, because the sequence-capture and the upload are on opposite sides of
// the seam this project keeps breaking.
func scenarioASnapshotOfALiveVolumeIsFrozen(s *Sim) error {
	return aSnapshotOfALiveVolumeIsFrozen(s, snapshotFrozen)
}

const (
	snapshotFrozen = false
	// snapshotTakenLate models the implementation that does not freeze: the copy is taken
	// from the live view, so it carries every write that landed between the snapshot and
	// the upload. Reached here by taking the snapshot after the later writes, which is
	// byte-for-byte what an unfrozen implementation publishes.
	snapshotTakenLate = true
)

func aSnapshotOfALiveVolumeIsFrozen(s *Sim, late bool) error {
	ctx := context.Background()
	sourceID := ids.NewAt(simEpoch*1000, s.Rand).String()
	cloneID := ids.NewAt(simEpoch*1000, s.Rand).String()
	snapID := ids.NewAt(simEpoch*1000, s.Rand).String()
	before := bytes.Repeat([]byte{0xA1}, 4096)
	after := bytes.Repeat([]byte{0xB2}, 4096)

	start := func(dataDir string) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Limits:         wal.Limits{SegmentBytes: 8192},
			HostID:         ids.NewAt(simEpoch*1000, s.Rand).String(),
			CheckpointPoll: 24 * time.Hour,
		}, agent.VolumeManagerDeps{
			Clock:   s.Clock,
			Disk:    s.Disk,
			Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
			Mapper:  simMapper{},
			EventFD: simEventFD,
			Store:   s.Store,
			Rand:    s.Rand,
		})
	}

	source, err := start("/var/lib/spin")
	if err != nil {
		return err
	}
	// The source VM stays up for the whole scenario, including while the clone reads. A
	// snapshot that only works once its parent has stopped is the stop-and-upload path
	// with extra steps.
	defer func() { _ = source.Close() }()
	if err := source.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: sourceID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		return fmt.Errorf("starting the source volume: %w", err)
	}
	dev, ok := source.Device(sourceID)
	if !ok {
		return errors.New("the source volume is not being served")
	}
	// The read is what waits for the base; driving the volume before it lands makes the
	// trace depend on a goroutine's timing (INV-02).
	if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("waiting for the read view: %w", err)
	}
	if _, err := dev.WriteAt(before, 0); err != nil {
		return fmt.Errorf("the write before the snapshot: %w", err)
	}
	if !late {
		if _, err := source.Snapshot(ctx, sourceID, snapID); err != nil {
			return fmt.Errorf("snapshotting the live volume: %w", err)
		}
	}
	// The guest carries on. This is the write the snapshot must not contain.
	if _, err := dev.WriteAt(after, 0); err != nil {
		return fmt.Errorf("the write after the snapshot: %w", err)
	}
	if late {
		s.Emit(Event{Kind: EventFault, Msg: "the snapshot is taken from the live view, after the later writes"})
		if _, err := source.Snapshot(ctx, sourceID, snapID); err != nil {
			return fmt.Errorf("snapshotting the live volume: %w", err)
		}
	}
	s.Notef("volume %s snapshotted as %s while still writing", sourceID, snapID)

	// The clone, on its own data directory so only the snapshot can answer.
	clone, err := start("/var/lib/spin-clone")
	if err != nil {
		return err
	}
	defer func() { _ = clone.Close() }()
	if err := clone.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: cloneID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		Durability:       storagev1.Durability_DURABILITY_REMOTE,
		State:            storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		ParentSnapshotId: snapID,
		ParentVolumeId:   sourceID,
	}}); err != nil {
		return fmt.Errorf("starting the clone: %w", err)
	}
	cdev, ok := clone.Device(cloneID)
	if !ok {
		return errors.New("the clone is not being served")
	}

	got := make([]byte, len(before))
	if _, err := cdev.ReadAt(got, 0); err != nil {
		return fmt.Errorf("the clone's read: %w", err)
	}
	// "Bytes it never wrote" is exactly right for the failure: the clone descends from a
	// point where the offset held A, and it is served B — a write made by another volume
	// after the moment this one claims to copy.
	foreign := bytes.Equal(got, after)
	s.Emit(Event{Kind: EventDurableRead, Key: cloneID,
		ZerosAfterRestart:        bytes.Equal(got, make([]byte, len(got))),
		ForeignBytesAfterRestart: foreign})
	if foreign {
		return fmt.Errorf("clone %s read the write that followed snapshot %s: the copy was not frozen (§19)", cloneID, snapID)
	}
	if !bytes.Equal(got, before) {
		return fmt.Errorf("clone read %x, the snapshot held %x", got[:8], before[:8])
	}
	s.Notef("clone %s read the snapshot's bytes, not the %d the source wrote afterwards", cloneID, len(after))
	return nil
}

// scenarioAStoppedVolumeComesBackFromItsImage is ADR-0026's contract as a guest
// experiences it: write, stop, start again, read the bytes back — and with the volume
// encrypted, because that is the seam where the same property last broke (DEV-0019).
//
// It replaced three scenarios that proved guest-visible properties through machinery
// ADR-0026 withdrew: `truncated-volume-survives-a-restart` and
// `encrypted-volume-survives-a-restart` restarted through a replay of WAL objects, and
// `a-promoted-host-reads-the-previous-epoch` proved a promotion V1 does not perform. The
// first two properties did not change and are here; the third went with its mechanism.
//
// Three things make it a test rather than a formality. The read goes through the *Agent*,
// because what has regressed before is the Agent's decision about what to load — no test
// of `wal` or of `image` could have seen it. The second manager gets its own data
// directory, so a local WAL left behind cannot be what answers. And the volume is
// encrypted, so a boot that forgot the key would fold ciphertext into the view — which is
// exactly what shipped once.
func scenarioAStoppedVolumeComesBackFromItsImage(s *Sim) error {
	return aStoppedVolumeComesBack(s, honestStore)
}

const (
	honestStore          = false
	storeHidesTheImage   = true
	stoppedVolumeSizeCap = 1 << 20
)

func aStoppedVolumeComesBack(s *Sim, hideImage bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	pattern := bytes.Repeat([]byte{0xAB}, 4096)

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

	desired := []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}

	start := func(dataDir string, store objectstore.Store) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Limits:         wal.Limits{SegmentBytes: 8192},
			HostID:         ids.NewAt(simEpoch*1000, s.Rand).String(),
			CheckpointPoll: 24 * time.Hour,
		}, agent.VolumeManagerDeps{
			Clock:   s.Clock,
			Disk:    s.Disk,
			Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
			Mapper:  simMapper{},
			EventFD: simEventFD,
			Store:   store,
			KMS:     kms,
			Rand:    s.Rand,
			Keys: func(context.Context, string) (agent.VolumeKeys, error) {
				return agent.VolumeKeys{
					VolumeID: volumeID, DEKWrapped: wrapped, KEKID: "kek-dst", DEKKeyID: dek.KeyID,
				}, nil
			},
		})
	}

	// The session that writes.
	first, err := start("/var/lib/spin", s.Store)
	if err != nil {
		return err
	}
	if err := first.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting the volume: %w", err)
	}
	dev, ok := first.Device(volumeID)
	if !ok {
		return errors.New("the volume is not being served")
	}
	// The read is what waits for the base; driving the volume before it lands makes the
	// trace depend on a goroutine's timing (INV-02).
	if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("waiting for the read view: %w", err)
	}
	for i := range 4 {
		if _, err := dev.WriteAt(pattern, int64(i)*4096); err != nil {
			return fmt.Errorf("guest write %d: %w", i, err)
		}
	}
	// Stopping is what publishes (ADR-0026). Nothing before this leaves the host.
	if err := first.Close(); err != nil {
		return fmt.Errorf("stopping the volume: %w", err)
	}

	objs, err := s.Store.List(ctx, "image/")
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return errors.New("stopping the volume published nothing: the session's writes exist only on a host that has released them")
	}
	// §5.10/INV-15: what left the host is sealed.
	for _, o := range objs {
		body, err := s.Store.Get(ctx, o.Key)
		if err != nil {
			return err
		}
		if bytes.Contains(body, pattern) {
			s.Emit(Event{Kind: EventLeavesHost, ClearLeak: true,
				Msg: fmt.Sprintf("image object %s carries the guest's plaintext", o.Key)})
			return fmt.Errorf("image object %s carries the guest's plaintext (§5.10/INV-15)", o.Key)
		}
	}
	s.Emit(Event{Kind: EventLeavesHost, ClearLeak: false,
		Msg: fmt.Sprintf("%d image objects, none carrying the guest's pattern", len(objs))})

	// The session that reads, on a different data directory so only the image can answer.
	var store objectstore.Store = s.Store
	if hideImage {
		store = hidingStore{Store: s.Store}
		s.Emit(Event{Kind: EventFault, Msg: "the object store answers nothing under the volume's image prefix"})
	}
	second, err := start("/var/lib/spin-second", store)
	if err != nil {
		return err
	}
	defer func() { _ = second.Close() }()
	if err := second.Apply(ctx, desired); err != nil {
		return fmt.Errorf("restarting the volume: %w", err)
	}
	dev2, ok := second.Device(volumeID)
	if !ok {
		return errors.New("the restarted volume is not being served")
	}

	got := make([]byte, len(pattern))
	_, readErr := dev2.ReadAt(got, 0)

	// Zeros are a missing image; foreign bytes are one loaded wrongly — ciphertext folded
	// into the view is the shape that shipped. An error is neither: refusing to answer is
	// the designed behaviour, and it is the *silent* wrong answer this checker exists for.
	zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
	foreign := readErr == nil && !zeros && !bytes.Equal(got, pattern)
	s.Emit(Event{Kind: EventDurableRead, Key: volumeID,
		ZerosAfterRestart: zeros, ForeignBytesAfterRestart: foreign})
	if zeros {
		return fmt.Errorf("volume %s read zeros for a range it wrote before stopping", volumeID)
	}
	if foreign {
		return fmt.Errorf("volume %s was served %x… where it wrote %x…", volumeID, got[:8], pattern[:8])
	}
	if readErr != nil {
		if hideImage {
			s.Notef("the read was refused rather than answered: %v", readErr)
			return nil
		}
		return fmt.Errorf("read after restart: %w", readErr)
	}
	s.Notef("the restarted volume decrypted its own image and read back what it wrote")
	return nil
}
