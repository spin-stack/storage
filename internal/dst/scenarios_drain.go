package dst

// Drain and placement scenarios (§28.1) live here rather than in scenarios.go so an
// increment working on the drain never contends with one working on recovery or on
// the harness itself for the same list.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func drainScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "drain-crash-at-every-boundary", Run: scenarioDrainCrashAtEveryBoundary},
		{Name: "drain-source-cannot-ack-afterwards", Run: scenarioDrainSourceCannotAck},
	}
}

func drainCheckers() []Checker { return nil }

const (
	drainLeaseTTL = 10 * time.Second
	drainMaxSkew  = 2 * time.Second
	drainVolBytes = int64(1) << 30
)

// crashHere is the panic value that aborts a drain pass the way a process crash
// does. A returned error would let the drain run its own tidy-up — releasing what it
// reserved, recording what it did — which is exactly the code a crash does not get
// to run, and exactly the code whose absence the resumed pass must survive.
type crashHere struct{ at string }

// faultMD wraps the metadata store so a boundary can be crashed at. Each hook is
// called before its write with done=false and after it landed with done=true, so a
// scenario can put the crash on either side of a durable effect.
type faultMD struct {
	metadata.Store
	onUpdate func(op metadata.Operation, done bool)
	onLease  func(hostID string)
}

func (s *faultMD) UpdateOperation(ctx context.Context, term int64, op metadata.Operation, bound *metadata.CapacityBound) error {
	if s.onUpdate != nil {
		s.onUpdate(op, false)
	}
	err := s.Store.UpdateOperation(ctx, term, op, bound)
	if err == nil && s.onUpdate != nil {
		s.onUpdate(op, true)
	}
	return err
}

func (s *faultMD) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	if s.onLease != nil {
		s.onLease(hostID)
	}
	return s.Store.GetHostLease(ctx, hostID)
}

// faultStore wraps the object store so the epoch-boundary PUT can be crashed at,
// before or after the object exists.
type faultStore struct {
	objectstore.Store
	onPut func(key string, done bool)
}

func (s *faultStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if s.onPut != nil {
		s.onPut(key, false)
	}
	res, err := s.Store.Put(ctx, key, data, opts)
	if err == nil && s.onPut != nil {
		s.onPut(key, true)
	}
	return res, err
}

// drainWorld is one volume on one source host, durable in S3, plus a destination
// with room for it — the smallest world in which every claim below is decidable.
type drainWorld struct {
	md      *faultMD
	store   *faultStore
	epochs  *epoch.Store
	drainer *controlplane.Drainer
	term    int64
	src     string
	dst     string
	opID    string
	vol     [16]byte
	volID   string
	log     *wal.Log
	acked   uint64
	dstHeld int64
}

// newDrainWorld builds that world with ids derived from tag, so several worlds can
// share the simulation's object store without colliding. fence is the source
// Agent's lease checker: the scenarios that are not about fencing hold a valid one,
// and the one that is passes a real lease.Manager on the monotonic clock.
func newDrainWorld(s *Sim, tag byte, fence wal.LeaseChecker) (*drainWorld, error) {
	ctx := context.Background()
	w := &drainWorld{
		src:   fmt.Sprintf("00000000-0000-7000-8000-0000000%03de1", tag),
		dst:   fmt.Sprintf("00000000-0000-7000-8000-0000000%03de2", tag),
		opID:  fmt.Sprintf("00000000-0000-7000-8000-0000000%03de3", tag),
		store: &faultStore{Store: s.Store},
	}
	w.vol[6], w.vol[8] = 0x70, 0x80
	w.vol[14], w.vol[15] = tag, 0xb0
	w.volID = format.UUIDString(w.vol)

	base := metasim.New(s.Clock.Wall)
	w.md = &faultMD{Store: base}
	term, err := base.AcquireLeadership(ctx, "cp")
	if err != nil {
		return nil, err
	}
	w.term = term
	for _, h := range []string{w.src, w.dst} {
		if err := base.UpsertHost(ctx, term, metadata.Host{
			HostID: h, State: lifecycle.HostActive, NVMeTotalBytes: 10 * drainVolBytes,
		}); err != nil {
			return nil, err
		}
	}
	if err := base.RenewHostLease(ctx, term, w.src, int(drainLeaseTTL/time.Second)); err != nil {
		return nil, err
	}
	if err := base.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: w.volID, SizeBytes: drainVolBytes, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, PrimaryHostID: w.src, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		return nil, err
	}
	// The destination already holds a volume of its own, so "the moved volume was
	// charged exactly once" is visible as a number rather than as a zero: a second
	// charge shows up as 3 GiB, a missing one as 1 GiB.
	var idle [16]byte
	idle[6], idle[8] = 0x70, 0x80
	idle[14], idle[15] = tag, 0xb1
	if err := base.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: format.UUIDString(idle), SizeBytes: drainVolBytes, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, PrimaryHostID: w.dst, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		return nil, err
	}
	w.dstHeld = drainVolBytes

	w.epochs = epoch.NewStore(s.Store)
	if _, err := w.epochs.Init(ctx, w.volID, 1); err != nil {
		return nil, err
	}

	f, err := s.Disk.Create("wal/" + w.volID + ".wal")
	if err != nil {
		return nil, err
	}
	w.log = wal.NewLog(f, s.Clock, w.vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	w.log.EnableRemote(wal.NewBatcher(s.Clock, w.vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5), fence)
	if _, err := w.log.Write(0, []byte("on-source"), 0); err != nil {
		return nil, err
	}
	if err := w.log.Flush(ctx); err != nil {
		return nil, fmt.Errorf("the source must be able to ACK while its lease is valid: %w", err)
	}
	w.acked = w.log.Watermarks().Durable

	w.drainer = controlplane.NewDrainer(w.md,
		controlplane.NewPromoter(w.md, w.epochs, s.Clock, drainLeaseTTL, drainMaxSkew),
		materialize.New(s.Store, nil, nil), w.store,
		placement.Policy{MaxOversubscription: 2.0})
	return w, nil
}

// pass runs one Drain, turning a crash into (crashed=true, nil).
func (w *drainWorld) pass() (crashed bool, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if _, ok := r.(crashHere); ok {
			crashed = true
			return
		}
		panic(r)
	}()
	_, err = w.drainer.Drain(context.Background(), w.term, w.src, w.opID)
	return crashed, err
}

// disarm removes every fault, so the resumed pass runs clean.
func (w *drainWorld) disarm() {
	w.md.onUpdate, w.md.onLease = nil, nil
	w.store.onPut = nil
}

// drainBoundary is one point inside move() at which the pass is killed. `arm` sets
// the fault on a world that has not run yet.
type drainBoundary struct {
	name string
	arm  func(w *drainWorld)
}

// drainBoundaries covers every durable step of a move, on the far side of the write:
// the effect landed and the process died before anything recorded or undid it.
func drainBoundaries() []drainBoundary {
	crash := func(at string) { panic(crashHere{at: at}) }
	return []drainBoundary{
		{"after reserving the destination", func(w *drainWorld) {
			// The reservation is the progress entry that names the destination
			// (ADR-0017), so this is the far side of that write.
			w.md.onUpdate = func(op metadata.Operation, done bool) {
				if done && strings.Contains(string(op.CurrentState), w.dst) {
					crash("reserve")
				}
			}
		}},
		{"after the bulk materialize", func(w *drainWorld) {
			// The lease read sits between the bulk pass and the promotion.
			w.md.onLease = func(hostID string) {
				if hostID == w.src {
					crash("bulk-materialize")
				}
			}
		}},
		{"after the promotion, before it was recorded", func(w *drainWorld) {
			w.md.onUpdate = func(op metadata.Operation, done bool) {
				if !done && strings.Contains(string(op.CurrentState), `"PROMOTED"`) {
					crash("promote")
				}
			}
		}},
		{"after the final materialize, before the boundary", func(w *drainWorld) {
			w.store.onPut = func(key string, done bool) {
				if !done && strings.HasSuffix(key, "recovery-point.json") {
					crash("final-materialize")
				}
			}
		}},
		{"after the epoch boundary", func(w *drainWorld) {
			w.store.onPut = func(key string, done bool) {
				if done && strings.HasSuffix(key, "recovery-point.json") {
					crash("boundary")
				}
			}
		}},
		{"after the volume became the destination's", func(w *drainWorld) {
			// Where the source's release used to be. There is no release any more —
			// the source stops being charged because the volume stopped being its
			// primary — so the boundary is the write that records the promotion.
			w.md.onUpdate = func(op metadata.Operation, done bool) {
				if done && strings.Contains(string(op.CurrentState), `"PROMOTED"`) {
					crash("release")
				}
			}
		}},
		{"after the progress record", func(w *drainWorld) {
			w.md.onUpdate = func(op metadata.Operation, done bool) {
				if done && strings.Contains(string(op.CurrentState), `"DONE"`) {
					crash("progress")
				}
			}
		}},
	}
}

// scenarioDrainCrashAtEveryBoundary is the §25.1 crash-boundary matrix for the drain
// (REBASELINE step 2). Every exactly-once claim the drain rests on is only as good
// as the boundary it was last interrupted at, so the pass is killed at each durable
// step of move() in turn, resumed under the same operation id, and then asked for
// all four claims at once:
//
//   - the accounting moved by exactly one volume size: the source is charged for
//     nothing and the destination for its own volume plus the moved one — never
//     twice, never not at all;
//   - exactly one epoch was granted, in PostgreSQL and in the S3 epoch object;
//   - exactly one epoch-boundary object exists, with the PrevEpoch and RecoveredUpTo
//     the move actually established — the object is create-only, so a second,
//     different one can never be corrected;
//   - the operation reaches SUCCEEDED rather than failing forever.
//
// The order of the boundaries comes from the seed, so a failure names both the
// boundary and the seed that reproduces it.
func scenarioDrainCrashAtEveryBoundary(s *Sim) error {
	ctx := context.Background()
	boundaries := drainBoundaries()
	for i, idx := range s.Rand.Perm(len(boundaries)) {
		b := boundaries[idx]
		w, err := newDrainWorld(s, byte(i+1), alwaysValidLease{})
		if err != nil {
			return err
		}
		// Wait the fence out: this scenario is about the crash boundaries, not about
		// FENCING_WAIT (scenarioDrainMovesVolumesFenced owns that).
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)

		b.arm(w)
		crashed, err := w.pass()
		if err != nil {
			return fmt.Errorf("%s: the armed pass failed instead of crashing: %w", b.name, err)
		}
		if !crashed {
			return fmt.Errorf("%s: the boundary was never reached", b.name)
		}
		s.Emit(Event{Kind: EventFault, Msg: "drain crashed " + b.name})

		w.disarm()
		if _, err := w.pass(); err != nil {
			return fmt.Errorf("%s: resume: %w", b.name, err)
		}
		if err := w.assertMovedExactlyOnce(ctx); err != nil {
			return fmt.Errorf("%s: %w", b.name, err)
		}
		// INV-09 for the resumed move: what the destination recovered covers what the
		// source ACKed as durable.
		rp, err := recovery.ReadRecoveryPoint(ctx, s.Store, w.vol, 2)
		if err != nil {
			return fmt.Errorf("%s: %w", b.name, err)
		}
		s.Emit(Event{Kind: EventFailover, AckedDurable: w.acked, Recovered: rp.RecoveredUpTo})
	}
	s.Notef("drain resumed from %d crash boundaries: one release, one epoch, one boundary each", len(boundaries))
	return nil
}

// assertMovedExactlyOnce checks the four claims above against the world's final state.
func (w *drainWorld) assertMovedExactlyOnce(ctx context.Context) error {
	src, err := w.md.GetHost(ctx, w.src)
	if err != nil {
		return err
	}
	if src.NVMeCommittedBytes != 0 {
		return fmt.Errorf("the evacuated source is still charged %d bytes", src.NVMeCommittedBytes)
	}
	dst, err := w.md.GetHost(ctx, w.dst)
	if err != nil {
		return err
	}
	if want := w.dstHeld + drainVolBytes; dst.NVMeCommittedBytes != want {
		return fmt.Errorf("destination committed %d bytes, want %d (its own volume plus exactly one move)",
			dst.NVMeCommittedBytes, want)
	}

	v, err := w.md.GetVolume(ctx, w.volID)
	if err != nil {
		return err
	}
	if v.CurrentEpoch != 2 || v.PrimaryHostID != w.dst {
		return fmt.Errorf("volume is on %s at epoch %d, want %s at epoch 2", v.PrimaryHostID, v.CurrentEpoch, w.dst)
	}
	stored, _, err := w.epochs.Current(ctx, w.volID)
	if err != nil {
		return err
	}
	if stored != 2 {
		return fmt.Errorf("the epoch object is at %d, want 2 — more than one epoch was granted", stored)
	}

	infos, err := w.store.List(ctx, fmt.Sprintf("wal/%s/", w.volID))
	if err != nil {
		return err
	}
	points := 0
	for _, info := range infos {
		if strings.HasSuffix(info.Key, "recovery-point.json") {
			points++
		}
	}
	if points != 1 {
		return fmt.Errorf("%d epoch-boundary objects exist, want exactly 1", points)
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, w.store, w.vol, 2)
	if err != nil {
		return err
	}
	if rp.PrevEpoch != 1 || rp.RecoveredUpTo != w.acked {
		return fmt.Errorf("epoch boundary %+v, want prev=1 up_to=%d", rp, w.acked)
	}

	op, err := w.md.GetOperation(ctx, w.opID)
	if err != nil {
		return err
	}
	if op.Phase != lifecycle.OpSucceeded {
		return fmt.Errorf("operation phase %q, want SUCCEEDED (error: %s)", op.Phase, op.Error)
	}
	return nil
}

// scenarioDrainSourceCannotAck asks the question the drain scenario never asked: once
// the volume has moved, can the old writer still ACK a FLUSH? A drain that promotes
// while the source's lease is alive loses every write the source ACKs afterwards —
// the source keeps extending epoch N past the recovery point the destination
// materialized, and recovery discards all of it.
//
// The source here is a real Agent: a lease.Manager on the monotonic clock gating a
// real wal.Log. The Control Plane's FENCING_WAIT is what makes that lease invalid at
// the moment of promotion, so the late FLUSH must self-fence (INV-06) and the
// recovered prefix must still cover everything ever ACKed (INV-09).
//
// The checkers, not just the assertions, catch this: replacing the manager with a
// lease that never expires — a production behaviour flip, not a literal event — makes
// the late FLUSH ACK, and DurableAckLeaseChecker fails on "durable ACK of seq 2 with
// an invalid lease" because the emitted LeaseValid is the Agent's real monotonic
// answer. With the assertions below removed as well, NoLostAckedWriteChecker sees the
// same thing from the other side: an ACKed sequence outside the recorded boundary.
// It runs against two shapes of source. The first has already gone quiet, which is
// the shape every earlier drain scenario assumed. The second is a *healthy* host:
// its lease is live at the instant the evacuation starts, which is what evacuating a
// working host actually looks like, and it is where the order of the fencing step
// decides whether any of this holds. The Control Plane has to withdraw its own
// record of the source as a writer before it waits — otherwise the wait is measured
// against an instant that keeps moving — and withdrawing it must not advance the
// promotion by a single tick, because the Agent's copy of the lease is counted down
// on a monotonic clock that never hears about the row.
func scenarioDrainSourceCannotAck(s *Sim) error {
	for _, tc := range []struct {
		name    string
		tag     byte
		healthy bool
	}{
		{"a source that has already gone quiet", 0x0f, false},
		{"a healthy source, fenced by the drain itself", 0x1f, true},
	} {
		if err := drainSourceCannotAck(s, tc.tag, tc.healthy); err != nil {
			return fmt.Errorf("%s: %w", tc.name, err)
		}
	}
	return nil
}

func drainSourceCannotAck(s *Sim, tag byte, healthy bool) error {
	ctx := context.Background()
	// The source Agent's own lease, granted now and never renewed again.
	agent := lease.NewManager(s.Clock, drainLeaseTTL)
	agent.Grant()
	w, err := newDrainWorld(s, tag, agent)
	if err != nil {
		return err
	}
	s.Emit(Event{Kind: EventDurableAck, Durable: w.acked, LeaseValid: agent.Valid()})

	if healthy {
		// The evacuation starts while the source still holds a live lease. The pass
		// must refuse to promote, and must leave the Control Plane's record of that
		// lease gone, so nothing on this side re-arms it behind the fence.
		if _, err := w.pass(); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			return fmt.Errorf("a live source lease must hold the promotion back, got %v", err)
		}
		if _, err := w.md.GetHostLease(ctx, w.src); !errors.Is(err, metadata.ErrNotFound) {
			return fmt.Errorf("the drain waited on the source's lease but never revoked it: %v", err)
		}
		// Revoking is not a shortcut through the wait. One second short of the TTL the
		// Agent's own copy of the lease is still valid — it never heard about the row
		// — and the promotion must still be refused.
		s.Tick(drainLeaseTTL - time.Second)
		if !agent.Valid() {
			return fmt.Errorf("the Agent's lease expired early; this step no longer tests anything")
		}
		if _, err := w.pass(); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			return fmt.Errorf("revoking the lease shortened the fencing wait, got %v", err)
		}
		s.Emit(Event{Kind: EventFault, Msg: "drain revoked the lease of a healthy source"})

		// Past lease_ttl + max_clock_skew, and only there, the healthy host moves.
		s.Tick(drainMaxSkew + 2*time.Second)
		if agent.Valid() {
			return fmt.Errorf("the promotion is about to run against an Agent whose lease is still valid")
		}
		if _, err := w.pass(); err != nil {
			return fmt.Errorf("drain of a healthy source: %w", err)
		}
	} else {
		// The Control Plane waits the fence out and evacuates the host.
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)
		if _, err := w.pass(); err != nil {
			return fmt.Errorf("drain: %w", err)
		}
	}
	if v, err := w.md.GetVolume(ctx, w.volID); err != nil || v.PrimaryHostID != w.dst {
		return fmt.Errorf("the volume did not move: %+v err=%v", v, err)
	}

	// The old writer, which never noticed, keeps writing into the old epoch.
	if _, err := w.log.Write(4096, []byte("after-the-drain"), 0); err != nil {
		return err
	}
	flushErr := w.log.Flush(ctx)
	if !errors.Is(flushErr, wal.ErrSelfFenced) {
		// It ACKed. Record that as the ACK it would be — the object is already in S3
		// either way — and let the INV-06/INV-09 checkers judge it rather than hiding
		// the outcome behind an early return.
		s.Emit(Event{Kind: EventDurableAck, Durable: w.log.Watermarks().Durable, LeaseValid: agent.Valid()})
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("late flush: %v", flushErr)})

	// INV-09, recomputed after the late attempt: the boundary the drain recorded
	// bounds everything the old writer could ever have ACKed.
	rp, err := recovery.ReadRecoveryPoint(ctx, s.Store, w.vol, 2)
	if err != nil {
		return err
	}
	s.Emit(Event{Kind: EventFailover, AckedDurable: w.log.Watermarks().Durable, Recovered: rp.RecoveredUpTo})

	if !errors.Is(flushErr, wal.ErrSelfFenced) {
		return fmt.Errorf("the drained source ACKed a FLUSH after the promotion (%v)", flushErr)
	}
	if w.log.Watermarks().Durable != w.acked {
		return fmt.Errorf("the fenced source advanced its durable point from %d to %d",
			w.acked, w.log.Watermarks().Durable)
	}
	if err := w.log.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("a self-fenced log must stay fenced, got %v", err)
	}
	s.Notef("drained source self-fenced: no ACK after the promotion, boundary bounds its last one")
	return nil
}
