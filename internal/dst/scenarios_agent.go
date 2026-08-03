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
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/recovery"
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
		{Name: "crashed-flush-does-not-collide-on-restart", Run: scenarioCrashedFlushDoesNotCollideOnRestart},
		{Name: "agent-encrypts-what-leaves-the-host", Run: scenarioAgentEncryptsWhatLeavesTheHost},
		{Name: "a-stopped-volume-comes-back-from-its-image", Run: scenarioAStoppedVolumeComesBackFromItsImage},
		{Name: "a-clone-reads-through-its-parent", Run: scenarioACloneReadsThroughItsParent},
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
			// Seeded, so the same seed produces the same chunk nonces and the same
			// ciphertext (INV-02, §15).
			Rand: s.Rand,
			Keys: keys(honest),
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

	// The parent writes and publishes its image. Everything the clone will read lives
	// under the parent's id from here on.
	//
	// Under ADR-0026 that publish *is* the snapshot: a parent's state in the object store
	// is its image, and a clone reads it with the same operation a boot uses. It used to
	// be a checkpoint plus the WAL objects after it, assembled into a snapshot manifest.
	payload := bytes.Repeat([]byte{0x77}, 4096)
	lm := lease.NewManager(s.Clock, time.Minute)
	lm.Grant()
	parentDone := cow.NewIntervalMap()
	parentDone.Overwrite(0, payload)
	if _, err := image.Publish(ctx, s.Store, s.Rand, nil, parentVol, parentDone, 1, ""); err != nil {
		return fmt.Errorf("publishing the parent's image: %w", err)
	}
	snapID := ids.NewAt(simEpoch*1000, s.Rand).String()
	s.Notef("parent %s published its image", parentID)

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
			Lease:   func() bool { return true },
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
