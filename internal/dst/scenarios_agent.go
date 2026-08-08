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

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
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
		{Name: "a-clone-of-a-clone-reads-its-grandparents-bytes", Run: scenarioACloneOfACloneReadsItsGrandparentsBytes},
		{Name: "a-snapshot-of-a-live-volume-is-frozen", Run: scenarioASnapshotOfALiveVolumeIsFrozen},
		{Name: "two-hosts-cannot-both-publish-an-image", Run: scenarioTwoHostsCannotBothPublishAnImage},
		{Name: "a-rebuilt-catalog-can-serve-its-volumes", Run: scenarioARebuiltCatalogCanServeItsVolumes},
		{Name: "a-volume-stopped-mid-fetch-still-publishes", Run: scenarioAVolumeStoppedMidFetchStillPublishes},
		{Name: "device-budget-holds-across-volumes", Run: scenarioDeviceBudgetHoldsAcrossVolumes},
	}
}

// scenarioBudget is the device budget the agent scenarios hand their managers: a
// literal, not a measurement, because these scenarios are about fencing, publishing
// and cloning and each one wants a share it can reason about rather than whatever an
// eighth of a simulated device happens to be.
//
// 128 KiB per volume, so Budget.Limits derives 16 KiB segments — small enough that a
// scenario writing a few 4 KiB records crosses a segment boundary (which is where the
// interesting crash cases are) and far more than any of them writes, so nothing here
// meets backpressure by accident. The scenario that *is* about the bound —
// device-budget-holds-across-volumes — takes the opposite route and derives its budget
// from a measured device through agent.NewBudget, because there the production
// derivation is the subject.
func scenarioBudget(volumes int) agent.Budget {
	const share = 128 << 10
	return agent.Budget{
		DeviceBytes:  int64(volumes) * share * 2,
		ReserveBytes: int64(volumes) * share / 2,
		GuestBytes:   int64(volumes) * share,
		MaxVolumes:   volumes,
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
		Budget:    scenarioBudget(1),
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
	defer func() { _ = m.Close(context.Background()) }()

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
	if hidden(key) {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return h.Store.Head(ctx, key)
}

func (h hidingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if hidden(key) {
		return nil, objectstore.ErrNotFound
	}
	return h.Store.Get(ctx, key)
}

// hidden is every prefix a volume's durable state lives under: its manifests, and — since
// the chunk store moved to the lineage — the chunks, which are no longer inside image/.
// A fault that hid the manifests and left the chunks readable would still be a volume that
// cannot be found, but it would not be the fault this claims to inject.
func hidden(key string) bool {
	return strings.HasPrefix(key, "image/") || strings.HasPrefix(key, "chunks/")
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
	// The parent descends from nothing, so it is its own lineage root and its chunks are
	// under its own id (image.ChunksPrefix). The clone will resolve the same root by
	// walking, which is the only reason it can find these bytes at all.
	if _, err := image.PublishSnapshot(ctx, s.Store, s.Rand, nil, image.OwnLineage(parentVol), parentDone, nil, 1, snapID); err != nil {
		return fmt.Errorf("publishing the parent's snapshot: %w", err)
	}
	// The descriptor a provisioner writes. The clone's Agent reads it to learn whether the
	// parent descends from anything itself, and refuses to attach when it is not there
	// rather than assuming the lineage ends (agent.parentChain).
	if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
		VolumeID: parentID, SizeBytes: 1 << 20, BlockSize: 512, CurrentEpoch: 1,
	}); err != nil {
		return fmt.Errorf("writing the parent's descriptor: %w", err)
	}
	s.Notef("parent %s published snapshot %s", parentID, snapID)

	// The clone: its own volume id, its own empty WAL, and a desired state that names
	// what it descends from.
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/clone", SocketDir: "/run/spin",
		Budget: scenarioBudget(1),
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
	defer func() { _ = m.Close(context.Background()) }()

	desired := &storagev1.DesiredVolume{
		VolumeId: cloneID, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
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

// scenarioACloneOfACloneReadsItsGrandparentsBytes is the chain walk, in the harness, at
// the depth nothing in this repository had ever built: `chain_depth > 1`.
//
// `parentView` resolved exactly one link until 2026-08-08, and a depth-2 clone read its
// grandparent's bytes only because publishing flattened — every snapshot manifest held
// everything its volume could read. The two ancestors here publish **deltas**, each
// manifest naming only the ranges that volume itself wrote, which is what the publisher
// produces since step 3 of CHUNK-ADDRESSING-SPEC and what makes the walk the only thing
// that can answer the grandparent's offset.
//
// The assertion is the bytes the guest reads back at two offsets, and the checker is the
// one that already watches for this failure: DurableRangeChecker sees zeros. Reading zeros
// is the violation and an error is not — a refusal is the designed answer when the lineage
// cannot be followed, and zeros are the answer a guest cannot tell from a range nobody
// wrote (DEV-0007).
func scenarioACloneOfACloneReadsItsGrandparentsBytes(s *Sim) error {
	ctx := context.Background()
	const grandparentOffset, parentOffset = 0, 8192
	grandBytes := bytes.Repeat([]byte{0x77}, 4096)
	parentBytes := bytes.Repeat([]byte{0x88}, 4096)

	// publish writes one ancestor as a session of it leaves it: a snapshot naming only
	// this volume's own range, and a descriptor carrying its link upward — the only place
	// the bucket states a lineage.
	//
	// The root is passed in rather than derived because it is what the whole chain's
	// chunks are keyed by: a zero root means "this volume is the top", and every volume
	// below it seals into the same prefix under the same DEK (image.Ident). Publishing
	// each ancestor under its *own* id would scatter the lineage's bytes across three
	// prefixes, and the Agent's walk — which resolves one root — would find none of them.
	publish := func(payload []byte, offset uint64, root [16]byte, parent descriptor.Descriptor) (string, string, [16]byte, error) {
		volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
		u, err := ids.Parse(volumeID)
		if err != nil {
			return "", "", root, err
		}
		id := image.Ident{Volume: [16]byte(u), Lineage: root}
		if root == ([16]byte{}) {
			id = image.OwnLineage([16]byte(u))
		}
		own := cow.NewIntervalMap()
		own.Overwrite(offset, payload)
		snapID := ids.NewAt(simEpoch*1000, s.Rand).String()
		if _, err := image.PublishSnapshot(ctx, s.Store, s.Rand, nil, id, own, nil, 1, snapID); err != nil {
			return "", "", id.Lineage, fmt.Errorf("publishing the snapshot of %s: %w", volumeID, err)
		}
		if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
			VolumeID: volumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, CurrentEpoch: 1,
			ParentSnapshotID: parent.ParentSnapshotID, ParentVolumeID: parent.ParentVolumeID,
		}); err != nil {
			return "", "", id.Lineage, fmt.Errorf("writing the descriptor of %s: %w", volumeID, err)
		}
		return volumeID, snapID, id.Lineage, nil
	}

	grandID, grandSnap, root, err := publish(grandBytes, grandparentOffset, [16]byte{}, descriptor.Descriptor{})
	if err != nil {
		return err
	}
	parentID, parentSnap, _, err := publish(parentBytes, parentOffset, root, descriptor.Descriptor{
		ParentSnapshotID: grandSnap, ParentVolumeID: grandID,
	})
	if err != nil {
		return err
	}
	s.Notef("snapshot %s of %s descends from snapshot %s of %s, and neither manifest holds the other's ranges",
		parentSnap, parentID, grandSnap, grandID)

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/clone-of-clone", SocketDir: "/run/spin",
		Budget: scenarioBudget(1),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		Rand:    s.Rand,
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close(context.Background()) }()

	cloneID := ids.NewAt(simEpoch*1000, s.Rand).String()
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: cloneID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		State:            storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		ParentSnapshotId: parentSnap,
		ParentVolumeId:   parentID,
	}}); err != nil {
		return fmt.Errorf("starting the clone of a clone: %w", err)
	}
	dev, ok := m.Device(cloneID)
	if !ok {
		return errors.New("the clone of a clone is not being served")
	}

	for _, want := range []struct {
		offset int64
		bytes  []byte
		whose  string
	}{
		{grandparentOffset, grandBytes, "its grandparent"},
		{parentOffset, parentBytes, "its parent"},
	} {
		got := make([]byte, len(want.bytes))
		_, readErr := dev.ReadAt(got, want.offset)
		zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
		s.Emit(Event{Kind: EventDurableRead, Key: cloneID, ZerosAfterRestart: zeros})
		if zeros {
			return fmt.Errorf("clone %s read zeros at %d for a range %s wrote (§20)", cloneID, want.offset, want.whose)
		}
		if readErr != nil {
			return fmt.Errorf("the clone's read at %d: %w", want.offset, readErr)
		}
		if !bytes.Equal(got, want.bytes) {
			return fmt.Errorf("clone read %x at %d, %s wrote %x", got[:8], want.offset, want.whose, want.bytes[:8])
		}
	}
	s.Notef("clone %s read through two links, with neither ancestor's manifest holding the other's bytes", cloneID)
	return nil
}

// scenarioARebuiltCatalogCanServeItsVolumes is INV-20 stated as the thing an operator
// would actually need on the worst day: the database is gone, the bucket is intact, and
// the question is whether a guest can be handed its volume back.
//
// It is not "the rows came back". A rebuild that recreated a volume with the wrong
// wrapped DEK, or the right key and the wrong version, produces rows that look perfect
// and a volume nothing can open — and the failure would surface at the guest's first
// read, a long way from here. So the assertion goes the whole way: a *new* Agent, on a
// data directory that has never seen this volume, using only key material the rebuilt
// catalog supplies, must serve the bytes the original guest wrote.
//
// The volume is encrypted for exactly that reason. Unencrypted, every field the rebuild
// could get wrong is unused.
func scenarioARebuiltCatalogCanServeItsVolumes(s *Sim) error {
	ctx := context.Background()
	pattern := bytes.Repeat([]byte{0x5A}, 4096)

	var kek [crypto.DEKSize]byte
	if _, err := io.ReadFull(s.Rand, kek[:]); err != nil {
		return err
	}
	kms := crypto.NewDevKMS(kek, "kek-dst")

	// The fleet as it was: a leader, a host, one provisioned volume. Provisioning is what
	// writes the descriptor, so the bucket is populated by production code rather than by
	// the scenario.
	md := metasim.New(s.Clock.Wall)
	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		return err
	}
	host := ids.NewAt(simEpoch*1000, s.Rand).String()
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		return err
	}
	// Provisioned by hand rather than through controlplane.Provisioner, for one reason:
	// the Provisioner allocates its volume id with ids.New(), which reads the wall clock,
	// so a scenario built on it produces a different trace every run (INV-02). Everything
	// else here is the production path — the same DEK wrap, the same descriptor writer,
	// the same catalog write — because those are what the rebuild reads back.
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	dek, err := crypto.GenerateDEK(s.Rand, 3)
	if err != nil {
		return err
	}
	wrapped, err := kms.WrapDEK(s.Rand, dek)
	if err != nil {
		return err
	}
	vol := metadata.Volume{
		VolumeID: volumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, State: lifecycle.VolumeActive,
		PrimaryHostID: host, CurrentEpoch: 1,
		DEKWrapped: wrapped, KEKID: "kek-dst", DEKKeyID: dek.KeyID,
	}
	if err := md.CreateVolume(ctx, term, vol, nil); err != nil {
		return err
	}
	if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
		VolumeID: vol.VolumeID, SizeBytes: vol.SizeBytes, BlockSize: vol.BlockSize, CurrentEpoch: vol.CurrentEpoch,
		KEKID: vol.KEKID, DEKWrapped: vol.DEKWrapped, DEKKeyID: vol.DEKKeyID,
	}); err != nil {
		return err
	}

	// A session: the guest writes, the Agent stops, the image is published.
	keysFrom := func(cat metadata.Store) agent.KeysFunc {
		return func(ctx context.Context, volumeID string) (agent.VolumeKeys, error) {
			v, err := cat.GetVolume(ctx, volumeID)
			if err != nil {
				return agent.VolumeKeys{}, err
			}
			return agent.VolumeKeys{
				VolumeID: v.VolumeID, DEKWrapped: v.DEKWrapped, KEKID: v.KEKID, DEKKeyID: v.DEKKeyID,
			}, nil
		}
	}
	start := func(dataDir string, keys agent.KeysFunc) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Budget: scenarioBudget(2),
		}, agent.VolumeManagerDeps{
			Clock:   s.Clock,
			Disk:    s.Disk,
			Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
			Mapper:  simMapper{},
			EventFD: simEventFD,
			Store:   s.Store,
			KMS:     kms,
			Rand:    s.Rand,
			Keys:    keys,
		})
	}
	desired := []*storagev1.DesiredVolume{{
		VolumeId: vol.VolumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}

	first, err := start("/var/lib/spin", keysFrom(md))
	if err != nil {
		return err
	}
	if err := first.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting the volume: %w", err)
	}
	dev, ok := first.Device(vol.VolumeID)
	if !ok {
		return errors.New("the volume is not being served")
	}
	if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("waiting for the read view: %w", err)
	}
	if _, err := dev.WriteAt(pattern, 0); err != nil {
		return fmt.Errorf("the guest write: %w", err)
	}
	if err := first.Close(ctx); err != nil {
		return fmt.Errorf("stopping the volume: %w", err)
	}

	// The catastrophe: the catalog is gone. Not emptied of one table — a database that
	// has never heard of this fleet, which is what a restore from nothing looks like.
	s.Emit(Event{Kind: EventFault, Msg: "the control-plane database is gone"})
	rebuilt := metasim.New(s.Clock.Wall)
	newTerm, err := rebuilt.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		return err
	}
	sum, err := controlplane.RebuildMetadata(ctx, rebuilt, s.Store, newTerm)
	if err != nil {
		return fmt.Errorf("rebuilding the catalog: %w", err)
	}
	if sum.Volumes != 1 {
		return fmt.Errorf("the rebuild recorded %d volumes, want 1", sum.Volumes)
	}
	s.Notef("catalog rebuilt from the bucket: %d volume(s)", sum.Volumes)

	// And the answer that matters: a new Agent, a data directory that has never seen this
	// volume, and key material that comes only from the rebuilt rows.
	second, err := start("/var/lib/spin-restored", keysFrom(rebuilt))
	if err != nil {
		return err
	}
	defer func() { _ = second.Close(context.Background()) }()
	if err := second.Apply(ctx, desired); err != nil {
		return fmt.Errorf("serving the rebuilt volume: %w", err)
	}
	dev2, ok := second.Device(vol.VolumeID)
	if !ok {
		return errors.New("the rebuilt volume is not being served")
	}
	got := make([]byte, len(pattern))
	_, readErr := dev2.ReadAt(got, 0)
	zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
	foreign := readErr == nil && !zeros && !bytes.Equal(got, pattern)
	s.Emit(Event{Kind: EventDurableRead, Key: vol.VolumeID,
		ZerosAfterRestart: zeros, ForeignBytesAfterRestart: foreign})
	switch {
	case readErr != nil:
		return fmt.Errorf("the rebuilt volume's read: %w", readErr)
	case zeros:
		return fmt.Errorf("volume %s read zeros after its catalog was rebuilt (§22.5/INV-20)", vol.VolumeID)
	case foreign:
		return fmt.Errorf("volume %s was served bytes it never wrote after its catalog was rebuilt", vol.VolumeID)
	}
	s.Notef("volume %s served its own bytes from a catalog rebuilt out of the bucket", vol.VolumeID)
	return nil
}

// scenarioTwoHostsCannotBothPublishAnImage is INV-10 in the only form it still takes.
//
// ADR-0026 removed the lease-gated ACK, the epochs racing for one prefix and the
// promotion protocol; what survives is one sentence: **two incarnations of a volume must
// not both publish an image.** They would not conflict — the second simply overwrites the
// first, and everything the first host's guest wrote disappears with no error anywhere,
// which is the worst shape a durability bug can take.
//
// The guard is a compare-and-set on the manifest's ETag, and this drives it through two
// real VolumeManagers on separate data directories against one object store: both serve
// the same volume id, both write, both stop. Exactly one image may exist afterwards, and
// the loser must be refused rather than merged.
//
// The fault the planted arm injects is not a code change but a *backend*: an object store
// that ignores preconditions. That is why `task backend:conformance` is blocking per
// backend (§6.1) — every claim on this page rests on If-Match meaning what it says, and a
// store that quietly accepts a stale ETag turns this invariant off with nothing failing.
func scenarioTwoHostsCannotBothPublishAnImage(s *Sim) error {
	return twoHostsCannotBothPublish(s, honestPreconditions)
}

const (
	honestPreconditions = false
	// preconditionsIgnored is the backend that accepts every conditional write — the
	// hazard §6.1's conformance suite exists to keep out of production.
	preconditionsIgnored = true
)

func twoHostsCannotBothPublish(s *Sim, ignorePreconditions bool) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	desired := []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}
	start := func(dataDir string) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Budget: scenarioBudget(2),
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

	// Both hosts start before either stops: each reads the same absent manifest, so each
	// holds the same empty ETag. That is the race — not two hosts at different times, but
	// two that believe the same thing about the object store.
	first, err := start("/var/lib/spin-a")
	if err != nil {
		return err
	}
	second, err := start("/var/lib/spin-b")
	if err != nil {
		return err
	}
	for i, m := range []*agent.VolumeManager{first, second} {
		if err := m.Apply(ctx, desired); err != nil {
			return fmt.Errorf("starting incarnation %d: %w", i, err)
		}
		dev, ok := m.Device(volumeID)
		if !ok {
			return fmt.Errorf("incarnation %d is not serving the volume", i)
		}
		// The read waits for the base; driving before it lands makes the trace depend on
		// a goroutine's timing (INV-02).
		if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
			return fmt.Errorf("incarnation %d waiting for its read view: %w", i, err)
		}
		// Different bytes, so "one image survived" is a statement about *which* one.
		if _, err := dev.WriteAt(bytes.Repeat([]byte{byte(0xC0 + i)}, 4096), 0); err != nil {
			return fmt.Errorf("incarnation %d writing: %w", i, err)
		}
	}

	if ignorePreconditions {
		s.Store.InjectIgnorePreconditions()
		s.Emit(Event{Kind: EventFault, Msg: "the object store accepts every conditional write"})
	}

	// Stopping is what publishes (ADR-0026).
	if err := first.Close(ctx); err != nil {
		return fmt.Errorf("stopping the first incarnation: %w", err)
	}
	firstETag, err := etagOf(ctx, s, volumeID)
	if err != nil {
		return err
	}
	// The second incarnation loses the compare-and-set, and since C5 that refusal is
	// *returned* rather than logged: ErrSuperseded is the one publish failure the Agent
	// neither retries nor holds its data directory for, because retrying would replace a
	// newer image with an older one. Under the injected fault the store accepts the write
	// regardless, so there is no error to see and the ETag below is what catches it.
	switch err := second.Close(ctx); {
	case ignorePreconditions && err != nil:
		return fmt.Errorf("stopping the second incarnation: %w", err)
	case !ignorePreconditions && !errors.Is(err, image.ErrSuperseded):
		return fmt.Errorf("the second incarnation's publish should have been refused as superseded; Close returned %v", err)
	}
	secondETag, err := etagOf(ctx, s, volumeID)
	if err != nil {
		return err
	}

	// The observable is the object, not the error. A publish that returns the right
	// refusal and moves the manifest anyway would satisfy any assertion on err, so what
	// says whether the second host won is whether the manifest moved under it.
	overwritten := firstETag != secondETag
	s.Emit(Event{Kind: EventStalePublsh, StalePublishOK: overwritten})
	if overwritten {
		return fmt.Errorf("volume %s: the second incarnation overwrote the first's image (%s -> %s), losing everything its guest wrote (§12.4/INV-10)",
			volumeID, firstETag, secondETag)
	}
	s.Notef("volume %s: one image survived two incarnations", volumeID)
	return nil
}

// etagOf reads the volume manifest's ETag, which is the identity of the image the object
// store currently holds.
func etagOf(ctx context.Context, s *Sim, volumeID string) (string, error) {
	u, err := ids.Parse(volumeID)
	if err != nil {
		return "", err
	}
	info, err := s.Store.Head(ctx, image.ManifestKey([16]byte(u)))
	if err != nil {
		return "", fmt.Errorf("reading the manifest of %s: %w", volumeID, err)
	}
	return info.ETag, nil
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
			Budget: scenarioBudget(2),
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
	defer func() { _ = source.Close(context.Background()) }()
	if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
		VolumeID: sourceID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, CurrentEpoch: 1,
	}); err != nil {
		return fmt.Errorf("writing the source volume's descriptor: %w", err)
	}
	if err := source.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: sourceID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
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
	defer func() { _ = clone.Close(context.Background()) }()
	if err := clone.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: cloneID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
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
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}

	start := func(dataDir string, store objectstore.Store) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Budget: scenarioBudget(2),
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
	if err := first.Close(ctx); err != nil {
		return fmt.Errorf("stopping the volume: %w", err)
	}

	objs, err := s.Store.List(ctx, "image/")
	if err != nil {
		return err
	}
	// The chunks are under chunks/<lineage>/, not under the volume's image prefix, and
	// they are the objects that carry guest bytes at all: listing image/ alone would
	// check manifests for a leak and never look at a sealed chunk.
	chunks, err := s.Store.List(ctx, "chunks/")
	if err != nil {
		return err
	}
	if len(objs) == 0 || len(chunks) == 0 {
		return errors.New("stopping the volume published nothing: the session's writes exist only on a host that has released them")
	}
	objs = append(objs, chunks...)
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
	defer func() { _ = second.Close(context.Background()) }()
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

// gatedStore holds one Get — the volume's manifest — until the scenario lets it go, and
// refuses it outright if the context it was handed has been cancelled meanwhile.
//
// It is the fault a `sim.ObjectStore` injector cannot express. Every injector there
// returns an *answer* (throttled, stale, not found), which is a fetch that has already
// finished; this scenario needs one that is genuinely **in flight** across another
// event, because the defect it proves is about what happens to a read while the teardown
// runs. Adding a hold to sim.ObjectStore was rejected for two reasons: its methods
// deliberately ignore the context (`_ context.Context`), so it could not model the
// cancellation half at all, and blocking inside the shared store would stall every
// unrelated key.
//
// The cancellation check is after the wait rather than inside the select, and that is
// what keeps the scenario deterministic (INV-02). A select with both `ctx.Done()` and
// `release` ready picks at random, so a bug that cancels this read would be caught only
// on some runs; re-reading ctx.Err() after either wakes it makes cancellation win
// whenever it happened at all, which is the outcome the scenario asserts on.
type gatedStore struct {
	objectstore.Store
	key     string
	arrived chan struct{}
	once    sync.Once
	release chan struct{}
}

func newGatedStore(store objectstore.Store, key string) *gatedStore {
	return &gatedStore{
		Store: store, key: key,
		arrived: make(chan struct{}), release: make(chan struct{}),
	}
}

func (g *gatedStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key != g.key {
		return g.Store.Get(ctx, key)
	}
	g.once.Do(func() { close(g.arrived) })
	select {
	case <-ctx.Done():
	case <-g.release:
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("the read of %s was cancelled while it was in flight: %w", key, err)
	}
	return g.Store.Get(ctx, key)
}

// scenarioAVolumeStoppedMidFetchStillPublishes is SHUTDOWN-PUBLISH-SPEC §5, from the
// only side that can tell: what a later guest reads back.
//
// The Agent's base fetch used to run under the serve context, and `stop()` cancels that
// context and *then* waits for the fetch's result — so a volume stopped while its base
// was still loading cancelled its own read. `fetchBase` recorded a failed view,
// `publish()` correctly refused to write an image missing everything the volume held
// before this session, and the entire session was dropped with nothing but a log line.
// Every ingredient is ordinary: a volume attached shortly before a restart, an image
// large enough to take a moment, or a store having a slow minute.
//
// The three sessions are the shape of the proof. The first writes and stops, so there is
// a base worth waiting for. The second writes and is stopped **with its manifest read
// blocked in the store**, which is the moment the defect lives in. The third is a fresh
// Agent on a data directory that has never seen this volume, so only the object store
// can answer it — and it must answer with *both* patterns: the one the base carried and
// the one the interrupted session wrote. A published image missing either is the loss.
//
// Ordering, and it is what makes this deterministic rather than a race the scenario wins
// most of the time: the release of the blocked read is triggered by the *listener
// closing*, which happens only after the teardown has cancelled the serve context. Under
// the defect the read is therefore already cancelled by the time it is released, and
// under the fix it is not — no sleep, no timeout, and the same trace on every run.
func scenarioAVolumeStoppedMidFetchStillPublishes(s *Sim) error {
	ctx := context.Background()
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	fromTheBase := bytes.Repeat([]byte{0xC3}, 4096)
	fromTheInterruptedSession := bytes.Repeat([]byte{0x5E}, 4096)
	const interruptedOffset = 4096

	desired := []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: stoppedVolumeSizeCap, BlockSize: 512, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}
	start := func(dataDir string, store objectstore.Store, listen agent.ListenFunc) (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin",
			Budget: scenarioBudget(2),
		}, agent.VolumeManagerDeps{
			Clock:   s.Clock,
			Disk:    s.Disk,
			Listen:  listen,
			Mapper:  simMapper{},
			EventFD: simEventFD,
			Store:   store,
			Rand:    s.Rand,
		})
	}
	anyListener := func(string) (vhost.Listener, error) { return newSimListener(), nil }

	// Session one: something for the base to hold.
	first, err := start("/var/lib/spin-midfetch-1", s.Store, anyListener)
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
	if _, err := dev.WriteAt(fromTheBase, 0); err != nil {
		return fmt.Errorf("the first session's write: %w", err)
	}
	if err := first.Close(ctx); err != nil {
		return fmt.Errorf("stopping the first session: %w", err)
	}
	s.Notef("session one published an image for volume %s", volumeID)

	// Session two, on its own data directory, with the manifest read held in the store.
	gate := newGatedStore(s.Store, image.ManifestKey([16]byte(u)))
	listeners := make(chan *simListener, 1)
	second, err := start("/var/lib/spin-midfetch-2", gate, func(string) (vhost.Listener, error) {
		ln := newSimListener()
		select {
		case listeners <- ln:
		default: // only the first one is the volume's; supervise re-listens after a session ends
		}
		return ln, nil
	})
	if err != nil {
		return err
	}
	if err := second.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting the second session: %w", err)
	}
	dev2, ok := second.Device(volumeID)
	if !ok {
		return errors.New("the second session is not being served")
	}
	// A write, deliberately without a read first: writes do not wait for the base, which
	// is what makes this session worth saving while its base is still in flight. Reading
	// here would park until the gate opened and there would be no defect left to catch.
	if _, err := dev2.WriteAt(fromTheInterruptedSession, interruptedOffset); err != nil {
		return fmt.Errorf("the interrupted session's write: %w", err)
	}
	<-gate.arrived
	s.Emit(Event{Kind: EventFault, Msg: "the volume is stopped with its base read still in flight"})

	stopped := make(chan error, 1)
	go func() { stopped <- second.Close(ctx) }()
	ln := <-listeners
	<-ln.closed // the teardown has begun, and it has cancelled the serve context
	close(gate.release)
	if err := <-stopped; err != nil {
		return fmt.Errorf("stopping the second session: %w", err)
	}

	// Session three: a fresh Agent on a directory that has never seen this volume, so the
	// object store is the only thing that can answer it.
	third, err := start("/var/lib/spin-midfetch-3", s.Store, anyListener)
	if err != nil {
		return err
	}
	defer func() { _ = third.Close(context.Background()) }()
	if err := third.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting the third session: %w", err)
	}
	dev3, ok := third.Device(volumeID)
	if !ok {
		return errors.New("the third session is not being served")
	}

	for _, want := range []struct {
		what   string
		offset int64
		bytes  []byte
	}{
		{"the base the interrupted session was still loading", 0, fromTheBase},
		{"what the interrupted session wrote", interruptedOffset, fromTheInterruptedSession},
	} {
		got := make([]byte, len(want.bytes))
		_, readErr := dev3.ReadAt(got, want.offset)
		zeros := readErr == nil && bytes.Equal(got, make([]byte, len(got)))
		foreign := readErr == nil && !zeros && !bytes.Equal(got, want.bytes)
		s.Emit(Event{Kind: EventDurableRead, Key: volumeID,
			ZerosAfterRestart: zeros, ForeignBytesAfterRestart: foreign})
		switch {
		case readErr != nil:
			return fmt.Errorf("reading %s at %d: %w", want.what, want.offset, readErr)
		case zeros:
			return fmt.Errorf("volume %s read zeros at %d: the image published by the interrupted session is missing %s",
				volumeID, want.offset, want.what)
		case foreign:
			return fmt.Errorf("volume %s read %x… at %d, where %s is %x…",
				volumeID, got[:8], want.offset, want.what, want.bytes[:8])
		}
	}
	s.Notef("the volume stopped mid-fetch published a complete image: both the base and the session's own write came back")
	return nil
}

// scenarioDeviceBudgetHoldsAcrossVolumes is ADR-0013 §1's property, and the one thing
// a per-volume limit cannot give: **the sum of what N volumes hold on one device stays
// inside the device's budget**, and a WRITE past it is refused with backpressure rather
// than met by the device's own ENOSPC.
//
// The difference is what a guest can do about it. ErrBackpressure fails one request
// with an error the guest understands and the volume survives; ENOSPC arrives as a
// partial append, in the middle of a WRITE, and after it every WRITE on *every* volume
// on that device fails with an I/O error nothing can act on — one greedy volume takes
// the host down with it. Under ADR-0026 that is the ordinary case rather than a corner:
// a session's whole WAL stays local until the volume stops, so nothing reclaims a byte
// while these four volumes are running.
//
// It is deliberately the production derivation end to end: a measured device
// (Disk.Usage, the simulated statfs), agent.NewBudget dividing it, agent.VolumeManager
// handing each Log its share, and the guest's own WriteAt as the thing that is refused.
// A scenario that set wal.Limits itself would prove the WAL enforces a number somebody
// gave it — which eight unit tests already do — and would say nothing about whether a
// real Agent ever gives it one. It did not: `grep -rn "Limits" cmd/` returned nothing.
//
// The assertions are on the device, not on the Agent: what the simulated statfs
// reports after every volume is in backpressure. A counter the Agent keeps could agree
// with itself while the disk filled underneath it.
func scenarioDeviceBudgetHoldsAcrossVolumes(s *Sim) error {
	ctx := context.Background()

	// Small enough to fill in a few hundred writes, and sized so the four shares are
	// comfortably inside it: the point is that the *sum* is bounded, and a device that
	// only just fits its own budget could not tell a budget that holds from one that
	// is saved by rounding.
	const device = 4 << 20
	s.Disk.SetDeviceBudget(device)

	usage, err := s.Disk.Usage()
	if err != nil {
		return fmt.Errorf("measuring the simulated device: %w", err)
	}
	budget, err := agent.NewBudget(usage, 4)
	if err != nil {
		return fmt.Errorf("dividing a %d-byte device: %w", device, err)
	}
	s.Notef("device %d bytes: %d for guests, %d reserved, %d per volume",
		budget.DeviceBytes, budget.GuestBytes, budget.ReserveBytes, budget.Share())

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Budget: budget,
	}, agent.VolumeManagerDeps{
		Clock:  s.Clock,
		Disk:   s.Disk,
		Listen: func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper: simMapper{}, EventFD: simEventFD,
		// No object store, and that is the honest shape of the case: with one, these
		// volumes would publish and the interesting question would become how much the
		// upload reclaims — which is nothing (ADR-0026 reclaims at stop, not during a
		// session). What is under test is the device while four guests are writing.
	})
	if err != nil {
		return err
	}
	defer func() { _ = m.Close(context.Background()) }()

	desired := make([]*storagev1.DesiredVolume, 0, budget.MaxVolumes)
	for i := range budget.MaxVolumes {
		desired = append(desired, &storagev1.DesiredVolume{
			VolumeId:  ids.NewAt(simEpoch*1000+int64(i), s.Rand).String(),
			SizeBytes: device, BlockSize: 512, Epoch: 1,
			State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		})
	}
	if err := m.Apply(ctx, desired); err != nil {
		return fmt.Errorf("starting %d volumes: %w", len(desired), err)
	}

	// Every volume writes until the host stops it. The cap is what makes the scenario
	// terminate if the bound is missing entirely — without it a lost bound is an
	// endless loop rather than a failure anybody can read.
	block := bytes.Repeat([]byte{0xD1}, 4096)
	maxWrites := int(device/int64(len(block))) + 1
	for _, d := range desired {
		dev, ok := m.Device(d.GetVolumeId())
		if !ok {
			return fmt.Errorf("volume %s is not being served", d.GetVolumeId())
		}
		held := 0
		for i := range maxWrites {
			_, werr := dev.WriteAt(block, int64(i)*int64(len(block)))
			if werr == nil {
				held += len(block)
				continue
			}
			if errors.Is(werr, sim.ErrNoSpace) {
				return fmt.Errorf("volume %s met the device's ENOSPC after %d bytes: the budget did not bound it, the device did",
					d.GetVolumeId(), held)
			}
			if !errors.Is(werr, wal.ErrBackpressure) {
				return fmt.Errorf("volume %s: WRITE %d failed with %v, want backpressure", d.GetVolumeId(), i, werr)
			}
			// EventDisk and not a new EventKind: the trace's vocabulary is shared by
			// every lane, and one more kind for one scenario is a registry entry
			// nobody else reads.
			s.Emit(Event{Kind: EventDisk, Key: d.GetVolumeId(),
				Msg: fmt.Sprintf("backpressure at %d bytes held, share %d", held, budget.Share())})
			break
		}
		if held == 0 {
			return fmt.Errorf("volume %s took no write at all; the arm proves nothing", d.GetVolumeId())
		}
		if held >= maxWrites*len(block) {
			return fmt.Errorf("volume %s wrote the whole device without being refused: it has no bound", d.GetVolumeId())
		}
	}

	// The property. Four volumes, each stopped at its own share, and the device they
	// share still inside what the Agent said it would use.
	after, err := s.Disk.Usage()
	if err != nil {
		return fmt.Errorf("measuring the device after the fill: %w", err)
	}
	if after.UsedBytes > budget.GuestBytes {
		return fmt.Errorf("the volumes hold %d bytes together, past the %d-byte guest budget of a %d-byte device",
			after.UsedBytes, budget.GuestBytes, budget.DeviceBytes)
	}
	// And the reserve is still there. Nothing withdraws from it — the publish at stop
	// writes no local byte — so what it has to be is untouched: a device that is full
	// for guests is not a full device, which is what leaves the stop's fdatasync and
	// the filesystem's own metadata somewhere to go (agent.ReserveRatio).
	if after.AvailBytes < budget.ReserveBytes {
		return fmt.Errorf("the guests left %d bytes free on the device, inside the %d-byte reserve",
			after.AvailBytes, budget.ReserveBytes)
	}
	s.Notef("four volumes in backpressure hold %d bytes of a %d-byte budget; %d bytes free, reserve %d",
		after.UsedBytes, budget.GuestBytes, after.AvailBytes, budget.ReserveBytes)
	return nil
}
