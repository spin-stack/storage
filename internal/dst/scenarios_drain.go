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
		{Name: "stale-lease-read-does-not-shorten-the-fence", Run: scenarioStaleLeaseReadDoesNotShortenTheFence},
		{Name: "drain-revocation-window-is-bounded", Run: scenarioDrainRevocationWindowIsBounded},
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
	// staleLease, when non-zero, is the last_renewal every lease read reports: a
	// replica lagging far enough that any deadline derived from the timestamp
	// elapsed long ago (ADR-0015).
	staleLease time.Time
	// wholeDrainWindow is the design ADR-0016 rejected, planted: renewals refused for
	// as long as the host is DRAINING, rather than for the length of one promotion.
	// It fixes the same bug and turns every drain of a healthy host into an
	// availability event for the volumes nobody is moving.
	wholeDrainWindow bool
	// ancientFence is the design ADR-0015 replaced, planted: the fence-start instant
	// is read out of the row as an hour ago, so the promoter believes it has been
	// waiting all that time instead of measuring since it looked.
	ancientFence bool
}

func (s *faultMD) RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error {
	if s.wholeDrainWindow {
		h, err := s.GetHost(ctx, hostID)
		if err != nil {
			return err
		}
		if h.State == lifecycle.HostDraining {
			return fmt.Errorf("%w: planted: %s is draining", metadata.ErrRenewalsBlocked, hostID)
		}
	}
	return s.Store.RenewHostLease(ctx, term, hostID, ttlSeconds)
}

func (s *faultMD) GetVolume(ctx context.Context, volumeID string) (metadata.Volume, error) {
	v, err := s.Store.GetVolume(ctx, volumeID)
	if err != nil || !s.ancientFence {
		return v, err
	}
	now, nerr := s.Now(ctx)
	if nerr != nil {
		return v, nerr
	}
	v.FencingStartedAt = now.Add(-time.Hour)
	return v, nil
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
	l, err := s.Store.GetHostLease(ctx, hostID)
	if err == nil && !s.staleLease.IsZero() {
		l.LastRenewal = s.staleLease
	}
	return l, err
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
	if err := base.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
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
	if err := base.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
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

	w.log = wal.NewLog(s.Disk, "wal", s.Clock, w.vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
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

// settle runs Drain passes until the operation stops asking for time. A fencing wait
// is not a failure, and after ADR-0015 there is one per volume: each volume's dwell
// begins when its own promotion does, so the pass that fences volume k+1 is the pass
// that promotes volume k. A crash stops the loop at once — that is what it is for.
func (w *drainWorld) settle(s *Sim) (crashed bool, err error) {
	for range 8 {
		crashed, err = w.pass()
		if crashed || !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			return crashed, err
		}
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)
	}
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
		// The fence is waited out by settle rather than by one tick: this scenario is
		// about the crash boundaries, not about FENCING_WAIT
		// (scenarioDrainMovesVolumesFenced owns that), and after ADR-0015 the pass
		// that opens a volume's fence is never the pass that promotes it.
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)

		b.arm(w)
		crashed, err := w.settle(s)
		if err != nil {
			return fmt.Errorf("%s: the armed pass failed instead of crashing: %w", b.name, err)
		}
		if !crashed {
			return fmt.Errorf("%s: the boundary was never reached", b.name)
		}
		s.Emit(Event{Kind: EventFault, Msg: "drain crashed " + b.name})

		w.disarm()
		if _, err := w.settle(s); err != nil {
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
		if _, err := w.settle(s); err != nil {
			return fmt.Errorf("drain of a healthy source: %w", err)
		}
	} else {
		// The Control Plane waits the fence out and evacuates the host.
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)
		if _, err := w.settle(s); err != nil {
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

// scenarioStaleLeaseReadDoesNotShortenTheFence is ADR-0015 in the harness. Every read
// of the source's lease is served by a replica lagging by more than
// lease_ttl + max_clock_skew, so the timestamp the row carries says the wait was over
// before the promotion was even contemplated. Both clocks agree — the Control Plane's
// and the store's — so the §12.1 offset check sees nothing wrong. It is the *data*
// that is old, and a wait derived from it is no wait at all.
//
// The source here is a real Agent counting its own lease down on the monotonic clock,
// exactly as §12.2 requires, and it never hears about any of this. So the claim is
// decidable: while that Agent's lease is valid the epoch must not be granted, however
// old the row looks.
//
// PromotionWaitChecker is driven from the same two facts (the Agent's own answer and
// whether the grant landed), so reverting the dwell to a comparison against
// last_renewal makes the checker fail, not just the assertions.
func scenarioStaleLeaseReadDoesNotShortenTheFence(s *Sim) error {
	return staleLeaseFence(false)(s)
}

// staleLeaseFence is the scenario, with the ADR-0015 design optionally reverted.
// plantTimestampDwell makes the promoter read how long it has been fencing out of the
// same lagging rows, which is what the wait used to be — and the planted-bug proof in
// planted_bug_drain_test.go requires it to be caught.
func staleLeaseFence(plantTimestampDwell bool) Scenario {
	return func(s *Sim) error {
		return staleLeaseFenceRun(s, plantTimestampDwell)
	}
}

func staleLeaseFenceRun(s *Sim, plantTimestampDwell bool) error {
	ctx := context.Background()
	agent := lease.NewManager(s.Clock, drainLeaseTTL)
	agent.Grant()
	w, err := newDrainWorld(s, 0x2f, agent)
	if err != nil {
		return err
	}
	// The replica is an hour behind: last_renewal predates the simulation.
	w.md.staleLease = s.Clock.Wall().Add(-time.Hour)
	w.md.ancientFence = plantTimestampDwell

	// First pass. Whatever the row says, this Control Plane has observed nothing yet.
	_, err = w.pass()
	s.Emit(Event{
		Kind: EventPromotion, EarlyGrant: err == nil && agent.Valid(),
		Msg: "first look at an hour-stale lease row",
	})
	if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		return fmt.Errorf("an hour-stale lease read let the drain promote at once: %v", err)
	}
	// The observation is durable, recorded with the §7 state (ADR-0015).
	v, err := w.md.GetVolume(ctx, w.volID)
	if err != nil {
		return err
	}
	if v.State != lifecycle.VolumeFencingWait {
		return fmt.Errorf("volume state is %q, want FENCING_WAIT", v.State)
	}
	if v.FencingStartedAt.IsZero() {
		return fmt.Errorf("the promoter waited without recording when it started")
	}

	// Half a TTL in, the Agent's lease is provably still valid: a grant here is
	// exactly the loss INV-11 exists to prevent, and the row says the wait is over.
	s.Tick(drainLeaseTTL / 2)
	if !agent.Valid() {
		return fmt.Errorf("the Agent's lease expired early; this step no longer tests anything")
	}
	_, err = w.pass()
	s.Emit(Event{
		Kind: EventPromotion, EarlyGrant: err == nil && agent.Valid(),
		Msg: "half a TTL in, with the writer's own lease still valid",
	})
	if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		return fmt.Errorf("the dwell was cut short by the stale row while the writer was alive: %v", err)
	}

	// One second short of the dwell, still refused.
	s.Tick(drainLeaseTTL/2 + drainMaxSkew - time.Second)
	_, err = w.pass()
	s.Emit(Event{
		Kind: EventPromotion, EarlyGrant: err == nil && agent.Valid(),
		Msg: "one second short of the dwell",
	})
	if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		return fmt.Errorf("the dwell was cut short by the stale row: %v", err)
	}

	// Past it, and only there.
	s.Tick(2 * time.Second)
	if agent.Valid() {
		return fmt.Errorf("the promotion is about to run against an Agent whose lease is still valid")
	}
	if _, err := w.settle(s); err != nil {
		return fmt.Errorf("the dwell elapsed and the drain still refused: %w", err)
	}
	v, err = w.md.GetVolume(ctx, w.volID)
	if err != nil {
		return err
	}
	if v.PrimaryHostID != w.dst || v.CurrentEpoch != 2 {
		return fmt.Errorf("the volume did not move: %+v", v)
	}
	s.Emit(Event{Kind: EventPromotion, EarlyGrant: agent.Valid(), Msg: "granted after a full monotonic dwell"})
	s.Notef("an hour-stale lease read cost the fence nothing: the dwell ran on the promoter's own clock")
	return nil
}

// scenarioDrainRevocationWindowIsBounded is ADR-0016 stage 1. The drain has to stop
// the source re-arming the lease it just revoked, or a healthy heartbeating host can
// never be evacuated: every renewal moves the instant the fencing wait is measured
// from, and the promotion never starts.
//
// The whole question is *how long* that refusal lasts. The fix wave 2 rejected —
// refusing renewals for any DRAINING host — stops the durable ACKs of every volume
// the host still holds, including the ones nobody is moving, for as long as the drain
// runs. Stage 1 keeps the mechanism and bounds it to one promotion: at most one
// lease_ttl + max_clock_skew per volume moved.
//
// So the scenario measures it. The drain is deliberately slow — the reconciler comes
// back long after the dwell has elapsed, which is what a busy Control Plane looks like
// — and the source has to be able to renew again in the gap. plantWholeDrain reverts
// the decision, and the assertion below catches it.
func scenarioDrainRevocationWindowIsBounded(s *Sim) error {
	return revocationWindow(false)(s)
}

func revocationWindow(plantWholeDrain bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		agent := lease.NewManager(s.Clock, drainLeaseTTL)
		agent.Grant()
		w, err := newDrainWorld(s, 0x3f, agent)
		if err != nil {
			return err
		}
		w.md.wholeDrainWindow = plantWholeDrain
		renew := func() error { return w.md.RenewHostLease(ctx, w.term, w.src, int(drainLeaseTTL/time.Second)) }

		// Before anything is drained the source renews normally.
		if err := renew(); err != nil {
			return fmt.Errorf("a healthy source could not renew before the drain started: %w", err)
		}

		// The first pass fences the volume: the window opens, and with it the refusal
		// that makes the evacuation possible at all.
		if _, err := w.pass(); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			return fmt.Errorf("the first pass must open the fence, got %v", err)
		}
		if err := renew(); !errors.Is(err, metadata.ErrRenewalsBlocked) {
			return fmt.Errorf("the source re-armed the lease the drain revoked: %v", err)
		}
		s.Emit(Event{Kind: EventFault, Msg: "the source's renewals are refused while its volume is promoted"})

		// Now the reconciler is slow. The window is bounded by the promotion it was
		// opened for, so once that much time has passed with nobody driving the drain,
		// the host is serving normally again — its other volumes can ACK a FLUSH.
		// This is the assertion the rejected design fails: a window that lasts as long
		// as the host is DRAINING is still shut here.
		s.Tick(drainLeaseTTL + drainMaxSkew + time.Second)
		if err := renew(); err != nil {
			return fmt.Errorf("the revocation window outlived the promotion it was opened for: %w", err)
		}
		s.Emit(Event{Kind: EventNote, Msg: "the window closed on its own; the source is serving again"})

		// And the drain still converges once the reconciler is running at its normal
		// cadence — inside the window it arms, where a real one polls in seconds. The
		// source heartbeats before every pass, which is what a healthy host does, and
		// must not be able to wedge its own evacuation.
		blocked := 0
		for range 12 {
			switch rerr := renew(); {
			case errors.Is(rerr, metadata.ErrRenewalsBlocked):
				blocked++
			case rerr != nil:
				return fmt.Errorf("unexpected renewal error: %w", rerr)
			}
			_, err = w.pass()
			if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
				break
			}
			s.Tick((drainLeaseTTL + drainMaxSkew) / 2)
		}
		if blocked == 0 {
			return fmt.Errorf("no heartbeat was ever refused, so nothing stopped the source re-arming its lease")
		}
		if err != nil {
			return fmt.Errorf("a heartbeating source wedged its own evacuation: %w", err)
		}
		v, err := w.md.GetVolume(ctx, w.volID)
		if err != nil {
			return err
		}
		if v.PrimaryHostID != w.dst {
			return fmt.Errorf("the volume did not move: %+v", v)
		}

		// The evacuation is over: nothing keeps the source from renewing.
		if err := renew(); err != nil {
			return fmt.Errorf("the drain finished with the window still open: %w", err)
		}
		s.Notef("the revocation window lasted one promotion, not the length of the drain")
		return nil
	}
}
