package dst

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// A checker that cannot fail is decoration — but a checker proven against a
// hand-written Emit is barely better. `s.Emit(Event{StalePublishOK: true})` proves the
// struct field is read; it says nothing about whether any real sequence of events
// could ever set it, which is the only question a regression cares about.
//
// So a planted bug here breaks *production behaviour* and the checker has to see it
// through a scenario driving real code: a bucket without versioning, a backend without
// conditional writes, a backend serving a stale read, a listing that never catches up,
// a listing that goes backwards, a clock that goes backwards, a lease row read from a
// replica, a lease checker that keeps saying yes, a volume created without encryption,
// a background consumer wired without a scheduler. Every one is an operational
// reality, and each is injected into the simulated I/O or into how the code under test
// is built — never into the code itself.
//
// Every checker now has one. TestPlantedBugCoverageIsNotSilentlyWeakened pins the
// count, so a checker quietly downgraded to a hand-written Emit fails there rather than
// disappearing into the absence of a test.
//
// Two of them are planted at a seam rather than at an I/O fault: INV-03's ordering
// rules and INV-17's arbitration are comparisons over numbers held in memory, and no
// disk, clock or object-store fault changes their answer. Those plants substitute one
// named policy (wal.OrderPolicy) or omit one constructor argument (the scheduler a
// background consumer is handed), which is how both invariants are actually lost —
// never by editing the code under test.

// Planted-bug outcomes. A behavioural planted bug also trips the scenario's own
// assertions; these name what went wrong for a reader of a failing run.
var (
	errNotYetExpired           = errors.New("planted: the lease should already have expired")
	errLeaseResurrected        = errors.New("planted: a monotonic regression revalidated an expired lease")
	errStaleWriterPublished    = errors.New("planted: a fenced writer verified its own epoch")
	errPromotedOverLiveLease   = errors.New("planted: an epoch was granted while the fenced writer's lease was still valid")
	errTruncatedAbovePublished = errors.New("planted: local WAL was reclaimed above the verified published point")
	errPublishedAheadOfDurable = errors.New("planted: a record no object store holds was reclaimed as published")
)

// plantedBug runs sc and requires checker to reject it, naming itself and printing the
// reproducing seed (without which a DST failure is not actionable).
//
// The scenario's own error is deliberately discarded: a checker that only fires when
// the scenario already caught the problem is not a checker. What is under test is
// whether the event stream carries the violation.
func plantedBug(t *testing.T, seed int64, checker Checker, wantName string, sc Scenario) {
	t.Helper()
	res := Run(seed, func(s *Sim) error { _ = sc(s); return nil }, checker)
	if res.Err == nil {
		t.Fatalf("%s did not catch the planted violation", wantName)
	}
	if !strings.Contains(res.Err.Error(), wantName) {
		t.Fatalf("failure must name the checker %q, got: %v", wantName, res.Err)
	}
	if !strings.Contains(res.Err.Error(), "seed=") {
		t.Fatalf("failure must report the reproducing seed, got: %v", res.Err)
	}
}

// requirePasses is the control every planted bug needs: without the injected fault the
// same scenario and the same checker must be green, or the "bug" proved nothing.
func requirePasses(t *testing.T, seed int64, checker Checker, sc Scenario) {
	t.Helper()
	if res := Run(seed, sc, checker); res.Err != nil {
		t.Fatalf("the unplanted scenario must pass: %v\n--- trace ---\n%s", res.Err, res.TraceString())
	}
}

// ---------------------------------------------------------------------------
// Behavioural planted bugs: real code, real fault, checker sees the consequence.
// ---------------------------------------------------------------------------

// INV-16: a published snapshot never changes. Planted by a backend that accepts a
// conditional write unconditionally — a real §6.1 conformance failure, and the one
// that silently makes every create-only publication in the system overwritable.
func TestPlantedBugSnapshotMutated(t *testing.T) {
	requirePasses(t, 17, NewImmutableSnapshotChecker(), scenarioSnapshotPauseFreeImmutable)
	plantedBug(t, 17, NewImmutableSnapshotChecker(), "immutable-snapshots", func(s *Sim) error {
		s.Store.InjectIgnorePreconditions()
		return scenarioSnapshotPauseFreeImmutable(s)
	})
}

// INV-09: the promoted writer recovers everything the fenced writer ACKed. Planted by
// a listing that never catches up: the durable prefix is computed from a LIST, and a
// backend that answers a short listing with no error hands recovery a prefix of zero
// while the objects sit right there.
func TestPlantedBugLostAckedWrite(t *testing.T) {
	requirePasses(t, 16, NewNoLostAckedWriteChecker(), scenarioFencedWriterNoLostAck)
	plantedBug(t, 16, NewNoLostAckedWriteChecker(), "no-lost-acked-write", func(s *Sim) error {
		s.Store.SetEventualList(true)
		return scenarioFencedWriterNoLostAck(s)
	})
}

// INV-10: a fenced writer never publishes. Planted by a backend that serves a stale
// read of the epoch object. Read-after-write on that one small object is the whole of
// §12.4's belt-and-suspenders fence: without it the superseded writer asks whether it
// is still current, is told yes, and publishes into a namespace another writer owns.
func TestPlantedBugStaleWriterPublished(t *testing.T) {
	const vid = "00000000-0000-7000-8000-0000000000a7"
	sc := func(staleAfterPromotion bool) Scenario {
		return func(s *Sim) error {
			ctx := t.Context()
			epochs := epoch.NewStore(s.Store)
			if _, err := epochs.Init(ctx, vid, 1); err != nil {
				return err
			}
			_, w1ETag, err := epochs.Current(ctx, vid)
			if err != nil {
				return err
			}
			// The CP promotes: the epoch object advances to 2 and W1 is fenced.
			if _, err := epochs.CompareAndAdvance(ctx, vid, w1ETag, 2); err != nil {
				return err
			}
			if staleAfterPromotion {
				s.Store.InjectStaleRead(epoch.Key(vid))
				s.Emit(Event{Kind: EventFault, Msg: "epoch object serves the pre-promotion version"})
			}
			// W1 returns and asks whether it may still publish (§18 "old writer
			// returns"): both answers must be no.
			verr := epochs.Verify(ctx, vid, 1)
			_, caserr := epochs.CompareAndAdvance(ctx, vid, w1ETag, 3)
			s.Emit(Event{Kind: EventStalePublsh, StalePublishOK: verr == nil || caserr == nil})
			if verr == nil || caserr == nil {
				return errStaleWriterPublished
			}
			return nil
		}
	}
	requirePasses(t, 15, NewSingleWriterChecker(), sc(false))
	plantedBug(t, 15, NewSingleWriterChecker(), "effective-single-writer", sc(true))
}

// INV-06: no FLUSH is ACKed while the lease is invalid. Planted by the lease checker
// the WAL consults answering "valid" for ever while the real lease manager has long
// expired — a stuck renewal, a heartbeat thread that died holding its last answer. The
// event carries the *real* manager's verdict, so the checker sees the ACK escape.
func TestPlantedBugDurableAckWithoutLease(t *testing.T) {
	requirePasses(t, 13, NewDurableAckLeaseChecker(), scenarioLeaseFencesDurableAck)
	plantedBug(t, 13, NewDurableAckLeaseChecker(), "durable-ack-requires-lease", func(s *Sim) error {
		return leaseFencesDurableAck(s, lyingLeaseChecker)
	})
}

// INV-15: nothing leaves the host in clear. Planted by creating the volume without
// encryption — no bug in the crypto, just a Log that was never handed a DEK, which is
// exactly how a plaintext volume reaches production.
func TestPlantedBugPlaintextLeavesHost(t *testing.T) {
	requirePasses(t, 12, NewNoPlaintextLeavesHostChecker(), scenarioEncryptedWALNoPlaintextLeak)
	plantedBug(t, 12, NewNoPlaintextLeavesHostChecker(), "no-plaintext-leaves-host", func(s *Sim) error {
		return walPlaintextScenario(s, plaintextWAL)
	})
}

// The monotonic clock is the basis of lease safety (§12.1). Planted by the clock
// source itself going backwards — a live migration, a broken CLOCK_MONOTONIC — and the
// scenario shows what that buys: a lease manager that had correctly expired reports
// itself valid again, un-fencing a writer the CP has already replaced.
func TestPlantedBugMonotonicClock(t *testing.T) {
	sc := func(regress time.Duration) Scenario {
		return func(s *Sim) error {
			lm := lease.NewManager(s.Clock, 10*time.Second)
			lm.Grant()
			s.Tick(11 * time.Second)
			if lm.Valid() {
				return errNotYetExpired
			}
			if regress > 0 {
				s.Clock.InjectMonotonicRegression(regress)
				s.Emit(Event{Kind: EventFault, Msg: "monotonic clock stepped backwards"})
			}
			s.Tick(time.Second)
			if lm.Valid() {
				return errLeaseResurrected
			}
			return nil
		}
	}
	requirePasses(t, 777, NewMonotonicClockChecker(), sc(0))
	plantedBug(t, 777, NewMonotonicClockChecker(), "monotonic-clock", sc(9*time.Second))
}

// publishAnything is the one ordering rule this plant removes: published may move
// anywhere, durable and truncation stay exactly as production has them (wal.StrictOrder
// is zero-size, so embedding it keeps the other two rules verbatim rather than
// reimplementing them here).
//
// One rule, not three. A Log with no rules at all would prove nothing about which
// check the checker depends on, and a checker that only fires when everything is off
// is not a regression test for anything.
type publishAnything struct{ wal.StrictOrder }

func (publishAnything) AllowPublished(uint64, wal.Watermarks) error { return nil }

// publishedAheadOfDurable is INV-03 where it costs something. A log holds three
// records and has ACKed two of them: local=3, durable=2, published=0. Advancing
// published to 3 claims that a verified checkpoint covers a record the object store
// has never seen — and INV-13, still strict, then *correctly* allows the local copy of
// that record to be reclaimed, because published is exactly what INV-13 trusts. The
// record exists nowhere afterwards.
//
// The move is the real one a checkpointer makes (Log.AdvancePublished, §21.1 step 2)
// and the trio is the log's own. relax substitutes the ordering policy behind that one
// move and leaves the other two rules strict, so what the checker sees is one rule
// missing rather than a Log with no rules at all.
func publishedAheadOfDurable(relax bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[15] = 0xd2

		l := wal.NewLog(s.Disk, "wal", s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
		l.EnableRemote(
			wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
			wal.NewUploader(s.Store, 5),
			alwaysValidLease{},
		)
		for i, payload := range []string{"acked", "acked-too", "never-uploaded"} {
			if _, err := l.Write(uint64(i)*8, []byte(payload), 0); err != nil {
				return err
			}
			if i == 1 {
				if err := l.Flush(ctx); err != nil {
					return fmt.Errorf("flush: %w", err)
				}
			}
		}
		// Record 3 has to sit in a segment that is not the newest, because the newest
		// is the one reclamation never takes. Sealing here is what an ordinary
		// checkpoint would have done a moment later; the fourth record opens the next
		// segment and is no more durable than the third.
		if err := l.Seal(); err != nil {
			return fmt.Errorf("seal: %w", err)
		}
		if _, err := l.Write(24, []byte("also-never-uploaded"), 0); err != nil {
			return err
		}
		emitWatermarks(s, l)
		w := l.Watermarks()
		if w.Local != 4 || w.Durable != 2 {
			return fmt.Errorf("setup: local=%d durable=%d, want 4 and 2", w.Local, w.Durable)
		}

		if relax {
			l.SetOrderPolicy(publishAnything{})
			s.Emit(Event{Kind: EventFault, Msg: "the published watermark stopped being checked against durable"})
		}
		// What a checkpoint's second step does, for a checkpoint that covers a record
		// S3 does not hold.
		switch err := l.AdvancePublished(w.Local); {
		case relax && err != nil:
			return fmt.Errorf("the relaxed policy still refused the move: %w", err)
		case !relax && !errors.Is(err, wal.ErrWatermarkOrder):
			return fmt.Errorf("publishing above durable: want ErrWatermarkOrder, got %v", err)
		}
		emitWatermarks(s, l)
		if !relax {
			return nil
		}

		// INV-13 is untouched and does its job against a number that is now a lie: the
		// only copy of record 3 is reclaimed.
		if err := l.TruncateLocal(l.Watermarks().Published); err != nil {
			return fmt.Errorf("truncate to the published point: %w", err)
		}
		left, err := wal.ReplaySegments(s.Disk, "wal", vol, 1)
		if err != nil {
			return err
		}
		for _, r := range left {
			if r.Sequence == 3 {
				return errors.New("the plant did not reach the disk: the record no object store " +
					"holds is still in the local WAL")
			}
		}
		return errPublishedAheadOfDurable
	}
}

// INV-03: published <= durable <= local at every observation.
//
// FAILS: the ordering rules are three comparisons over numbers held in memory and the
// Log applies them itself, so the trio it reports is ordered by construction whatever
// the disk and the object store do. Driving the real move only produces the error the
// Log is supposed to return.
func TestPlantedBugPublishedAheadOfDurable(t *testing.T) {
	requirePasses(t, 20, NewWatermarkOrderChecker(), publishedAheadOfDurable(false))
	plantedBug(t, 20, NewWatermarkOrderChecker(), "watermark-order", publishedAheadOfDurable(true))
}

// Ids for the fencing-wait plant, kept apart from the mandatory scenario's so the two
// never share an epoch object.
const (
	fencedVol   = "00000000-0000-7000-8000-0000000000c0"
	fencedHost1 = "00000000-0000-7000-8000-0000000000c1"
	fencedHost2 = "00000000-0000-7000-8000-0000000000c2"
)

// The §12 parameters this plant runs with.
const (
	fencingLeaseTTL = 10 * time.Second
	fencingSkew     = 2 * time.Second
)

// replicaLeaseMD answers GetHostLease from a PostgreSQL read replica that is `lag`
// behind the primary: the row it hands back carries the renewal *before* the one the
// Agent has already made. Everything else goes to the real store.
//
// This is a deployment, not a bug: reads are routed to a replica to keep them off the
// primary, and replication lag is measured in seconds on a good day. The promoter has
// no defence against it — with no instant from the caller, the lease row is the whole
// authority for FENCING_WAIT, and the deadline it computes from a stale renewal has
// already passed while the writer's own monotonic countdown has not.
type replicaLeaseMD struct {
	metadata.Store
	lag time.Duration
}

func (m *replicaLeaseMD) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	l, err := m.Store.GetHostLease(ctx, hostID)
	if err != nil {
		return l, err
	}
	l.LastRenewal = l.LastRenewal.Add(-m.lag)
	return l, nil
}

// timestampDwellMD is the design ADR-0015 replaced, planted: a promoter that decides
// how long it has been fencing by reading a timestamp somebody else wrote, instead of
// measuring elapsed time since it looked. The store reports a fence that began an hour
// ago, so the dwell is over before it starts — which is exactly what a lagging replica,
// a restored backup or a mis-set database clock would produce.
type timestampDwellMD struct {
	metadata.Store
}

func (m *timestampDwellMD) GetVolume(ctx context.Context, volumeID string) (metadata.Volume, error) {
	v, err := m.Store.GetVolume(ctx, volumeID)
	if err != nil {
		return v, err
	}
	now, nerr := m.Now(ctx)
	if nerr != nil {
		return v, nerr
	}
	v.FencingStartedAt = now.Add(-time.Hour)
	return v, nil
}

// promoFault is what the promotion scenario runs against.
type promoFault struct {
	// jump steps the *wall clock* forward before the attempt — an NTP correction, a VM
	// restored from a snapshot, a bad RTC. It is not a violation and must not be read
	// as one: the promoter and the Agent's lease sit on the same clock, so a jump that
	// carries the CP past the deadline carries the writer past the end of its lease
	// too. It is kept as a control against a checker that would cry wolf.
	jump time.Duration
	// replicaLag is how far behind the lease row the CP reads is. Since ADR-0015 it
	// is a control rather than a plant: the dwell is measured from the promoter's own
	// observation, so however old the row is the wait is still served in full.
	replicaLag time.Duration
	// timestampDwell reverts the dwell to a timestamp comparison (see
	// timestampDwellMD). This is the plant.
	timestampDwell bool
}

// earlyPromotion drives a real controlplane.Promoter against a volume whose primary
// still holds a valid lease on its own monotonic clock, and reports whether the epoch
// was granted anyway. Both halves of that question are answered by production code:
// the grant is Promote's return, and the liveness of the writer being fenced is a real
// lease.Manager counting down the same TTL the Control Plane recorded.
func earlyPromotion(f promoFault) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		var md metadata.Store = metasim.New(s.Clock.Wall)
		if f.replicaLag > 0 {
			md = &replicaLeaseMD{Store: md, lag: f.replicaLag}
		}
		if f.timestampDwell {
			md = &timestampDwellMD{Store: md}
		}
		epochs := epoch.NewStore(s.Store)
		p := controlplane.NewPromoter(md, epochs, s.Clock, fencingLeaseTTL, fencingSkew)

		term, err := md.AcquireLeadership(ctx, "cp")
		if err != nil {
			return err
		}
		for _, h := range []string{fencedHost1, fencedHost2} {
			if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
				return err
			}
		}
		if err := md.CreateVolume(ctx, term, metadata.Volume{
			VolumeID: fencedVol, State: lifecycle.VolumeActive,
			PrimaryHostID: fencedHost1, DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
		}, nil); err != nil {
			return err
		}
		if _, err := epochs.Init(ctx, fencedVol, 0); err != nil {
			return err
		}

		// W1 takes the lease the Control Plane records. The row and the Agent's own
		// monotonic countdown start at the same instant, which is the only condition
		// under which the CP's deadline says anything about the writer (§12.1).
		if err := md.RenewHostLease(ctx, term, fencedHost1, int(fencingLeaseTTL/time.Second)); err != nil {
			return err
		}
		w1 := lease.NewManager(s.Clock, fencingLeaseTTL)
		w1.Grant()
		renewedAt := s.Clock.Wall()

		// Half a TTL in: the reconciler has seen missed heartbeats and carries no
		// instant of its own, so the lease row is the whole authority (§12.3).
		s.Tick(fencingLeaseTTL / 2)
		if f.jump > 0 {
			s.Clock.Advance(f.jump)
			s.Emit(Event{Kind: EventFault, Msg: "the Control Plane's wall clock stepped forward"})
		}
		if f.replicaLag > 0 {
			s.Emit(Event{Kind: EventFault, Msg: "the lease row is read from a replica that is behind"})
		}
		newEpoch, err := p.Promote(ctx, term, fencedVol, time.Time{}, fencedHost2)
		granted := err == nil
		s.Emit(Event{
			Kind: EventPromotion, EarlyGrant: granted && w1.Valid(),
			Msg: fmt.Sprintf("epoch=%d granted=%t w1_lease_valid=%t deadline=%s",
				newEpoch, granted, w1.Valid(), p.FencingDeadline(renewedAt).UTC()),
		})
		switch {
		case granted && w1.Valid():
			return errPromotedOverLiveLease
		case !granted && !errors.Is(err, controlplane.ErrFencingWaitNotElapsed):
			return fmt.Errorf("promotion before the wait: want ErrFencingWaitNotElapsed, got %w", err)
		}

		// Past the deadline the same promotion must succeed: a checker that fires on
		// every promotion would say nothing about the early ones. The wait is the
		// promoter's dwell (ADR-0015), which is a skew longer than the lease TTL.
		s.Tick(p.FencingDwell() + time.Second)
		newEpoch, err = p.Promote(ctx, term, fencedVol, time.Time{}, fencedHost2)
		if err != nil {
			return fmt.Errorf("promotion after the wait: %w", err)
		}
		s.Emit(Event{
			Kind: EventPromotion, EarlyGrant: w1.Valid(),
			Msg: fmt.Sprintf("epoch=%d after the wait w1_lease_valid=%t", newEpoch, w1.Valid()),
		})
		if newEpoch != 1 {
			return fmt.Errorf("new epoch = %d, want 1", newEpoch)
		}
		return nil
	}
}

// INV-11: no epoch is granted before FENCING_WAIT elapses. Planted by reverting the
// fencing wait to what ADR-0015 replaced — a comparison against a timestamp in a row
// rather than elapsed time since the promoter itself looked. The row says the fence
// began an hour ago, the writer's own monotonic lease says otherwise, and the epoch is
// granted over a writer that can still ACK a FLUSH (§12.2, §12.3).
//
// The three controls matter as much as the plant: an honest run must be green, so must
// a wall clock that steps forward (not an early grant, and a checker that counted it as
// one would cry wolf), and so must a lease read from a lagging replica — the case this
// checker used to be planted with, and the one ADR-0015 exists to close.
func TestPlantedBugEarlyPromotion(t *testing.T) {
	requirePasses(t, 14, NewPromotionWaitChecker(), earlyPromotion(promoFault{}))
	requirePasses(t, 14, NewPromotionWaitChecker(), earlyPromotion(promoFault{jump: fencingLeaseTTL}))
	// The lagging replica used to be the plant. ADR-0015 closed it — the dwell is
	// elapsed time since the promoter observed the fence, not a deadline derived from
	// the row — so it is a *control* now: it must pass, and if it ever stops passing
	// the fix has been undone.
	requirePasses(t, 14, NewPromotionWaitChecker(), earlyPromotion(promoFault{replicaLag: 2 * fencingLeaseTTL}))
	// The plant is the design that was replaced, under the deployment that made it
	// wrong: how long the fence has been running read out of a row instead of
	// measured, with the rows served by the lagging replica. Both halves are needed
	// — which is the point of the control above: with the dwell in place the lag on
	// its own buys nothing.
	plantedBug(t, 14, NewPromotionWaitChecker(), "promotion-fencing-wait",
		earlyPromotion(promoFault{replicaLag: 2 * fencingLeaseTTL, timestampDwell: true}))
}

// truncateAfterListingRegresses is INV-13 end to end: a checkpoint publishes a durable
// point, local WAL is reclaimed up to it, and then the volume is checkpointed again.
//
// Both numbers in the event come from the log itself — what it reclaimed and what it
// has verified as published — so the only way to make them disagree is to make real
// code move one of them. checkpoint.Create recomputes the published point from what
// the object store can prove *now* (a LIST) and hands it to Log.AdvancePublished, which
// guards the ordering against durable but not against its own past: a listing that
// went backwards would walk the published point back under WAL that no longer exists.
//
// regress asks the store for one listing served by an index replica that is behind the
// data — an eventually consistent LIST offers no monotonic-read guarantee, so the
// second of two listings can be the older one. The checkpointer then publishes a lower
// point, walks published back under WAL that has already been reclaimed, and reports
// no error at all: the volume is missing records and every number the system prints
// about it is consistent.
func truncateAfterListingRegresses(regress bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[15] = 0xd1

		l := wal.NewLog(s.Disk, "wal", s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
		l.EnableRemote(
			wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
			wal.NewUploader(s.Store, 5),
			alwaysValidLease{},
		)
		// Two objects, so a listing can lose the newer one and still answer.
		for i, payload := range []string{"a", "b", "c"} {
			if _, err := l.Write(uint64(i)*8, []byte(payload), 0); err != nil {
				return err
			}
			if i == 1 {
				if err := l.Flush(ctx); err != nil {
					return fmt.Errorf("flush %d: %w", i, err)
				}
			}
		}
		if err := l.Flush(ctx); err != nil {
			return err
		}

		cp := checkpoint.NewCheckpointer(s.Store)
		if _, err := cp.Create(ctx, l, vol, 1); err != nil {
			return fmt.Errorf("first checkpoint: %w", err)
		}
		published := l.Watermarks().Published
		if published != l.Watermarks().Durable {
			return fmt.Errorf("the checkpoint published %d of %d durable", published, l.Watermarks().Durable)
		}
		// §21.1: published, verified, and only then is the local copy disposable.
		if err := l.TruncateLocal(published); err != nil {
			return fmt.Errorf("truncate to the published point: %w", err)
		}
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: published})

		if regress {
			// The stale listing is still what makes a *republication* claim less than
			// the last one did; what it can no longer do by itself is move published
			// down, because StrictOrder now refuses a value below its own past. So the
			// plant relaxes exactly that rule and leaves the other two strict: this is
			// the invariant under test, and the fault has to reach it.
			l.SetOrderPolicy(publishedMayRegress{})
			s.Store.InjectStaleListing(s.Rand, 1)
			s.Emit(Event{Kind: EventFault, Msg: "the listing that proves the durable point goes backwards"})
		}
		// The next checkpoint recomputes the published point from the backend.
		if _, err := cp.Create(ctx, l, vol, 1); err != nil {
			return fmt.Errorf("second checkpoint: %w", err)
		}
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: l.Watermarks().Published})
		if l.TruncatedUpTo() > l.Watermarks().Published {
			return errTruncatedAbovePublished
		}
		return nil
	}
}

// publishedMayRegress is StrictOrder with the monotonic floor on the published point
// removed and nothing else touched — the shape wal.Log actually had until the floor
// was added, which this proof is what found.
type publishedMayRegress struct{ wal.StrictOrder }

func (publishedMayRegress) AllowPublished(seq uint64, w wal.Watermarks) error {
	if seq > w.Durable {
		return wal.ErrWatermarkOrder
	}
	return nil
}

// INV-13: local WAL is never truncated above the verified published point. Planted by
// one listing served from behind the data — enough for checkpoint.Create to republish
// at a lower sequence — over a Log whose published point is allowed to move backwards.
// Both halves are needed now: the listing is the real-world fault, and the relaxed
// rule is the defect it used to exploit, kept here as the thing under test.
func TestPlantedBugTruncateAbovePublished(t *testing.T) {
	requirePasses(t, 18, NewTruncateBelowPublishedChecker(), truncateAfterListingRegresses(false))
	plantedBug(t, 18, NewTruncateBelowPublishedChecker(), "no-truncate-above-published", truncateAfterListingRegresses(true))
}

// INV-17: background I/O yields. Planted where the invariant is actually lost — not in
// the arbitration, which is a counter no injected fault can reach, but in a background
// consumer that was never handed the scheduler. A real cross-host materialization then
// fetches object after object while a foreground op is in flight, exactly as it would
// on a host serving a guest, and the checker sees the grant against a real in-flight
// count.
//
// The control is the same scenario wired to the same scheduler: it yields with
// ErrThrottled and the checker stays quiet.
func TestPlantedBugBackgroundDidNotYield(t *testing.T) {
	requirePasses(t, 19, NewBackgroundYieldsChecker(), scenarioCrossHostMaterialization)
	plantedBug(t, 19, NewBackgroundYieldsChecker(), "background-yields", func(s *Sim) error {
		s.Emit(Event{Kind: EventFault, Msg: "the materializer was built without the data path's scheduler"})
		return crossHostMaterialization(s, unscheduledMaterializer)
	})
}

// ---------------------------------------------------------------------------

// proofKind records how strong a checker's planted-bug proof is.
type proofKind int

const (
	// proofBehavioural: a fault injected into the simulated I/O makes real production
	// code violate the invariant, and the checker catches it through a real scenario.
	proofBehavioural proofKind = iota
	// proofLiteral: only the event is planted. The checker is proven to read the
	// field; nothing proves a real run could ever set it. No checker is here now; the
	// kind stays so a new checker can be added honestly before its proof exists.
	proofLiteral
)

// plantedProofs maps every checker to the strength of its proof.
var plantedProofs = map[string]proofKind{
	"immutable-snapshots":         proofBehavioural,
	"no-lost-acked-write":         proofBehavioural,
	"effective-single-writer":     proofBehavioural,
	"durable-ack-requires-lease":  proofBehavioural,
	"no-plaintext-leaves-host":    proofBehavioural,
	"monotonic-clock":             proofBehavioural,
	"promotion-fencing-wait":      proofBehavioural,
	"no-truncate-above-published": proofBehavioural,
	"background-yields":           proofBehavioural,
	"watermark-order":             proofBehavioural,
	// Contributed by scenarios_recovery.go; proofs in planted_bug_recovery_test.go.
	"boundary-monotonic":      proofBehavioural,
	"durable-point-monotonic": proofBehavioural,
	// Contributed by scenarios_agent.go; proof in planted_bug_agent_test.go.
	"fenced-volume-not-served":       proofBehavioural,
	"durable-range-survives-restart": proofBehavioural,
	"checkpoint-requires-lease":      proofBehavioural,
}

// TestEveryCheckerHasAPlantedBugProof fails when a checker is added without one, so
// this file cannot fall behind DefaultCheckers again.
func TestEveryCheckerHasAPlantedBugProof(t *testing.T) {
	for _, c := range DefaultCheckers() {
		if _, ok := plantedProofs[c.Name()]; !ok {
			t.Fatalf("checker %q has no planted-bug proof: add one in this file (a CLAUDE.md stop signal)", c.Name())
		}
	}
	if len(plantedProofs) != len(DefaultCheckers()) {
		t.Fatalf("plantedProofs has %d entries for %d checkers — it drifted", len(plantedProofs), len(DefaultCheckers()))
	}
}

// TestPlantedBugCoverageIsNotSilentlyWeakened pins the number of behavioural proofs.
// Converting a literal proof to a behavioural one is progress and raises this number;
// a checker quietly downgraded to a hand-written Emit is not, and fails here.
//
// It went 16 -> 15 on 2026-08-02, which is the one shape of decrease this test is not
// meant to stop: `no-permanent-delete` was removed *with its subject*. ADR-0026 deleted
// internal/gc, nothing issues a delete any more, and a checker that cannot fire proves
// nothing. INV-14 is `pending` rather than dropped — the number goes back up with the
// sweeper. A decrease for any other reason is the weakening this test exists to catch,
// and the comment is the difference between the two.
func TestPlantedBugCoverageIsNotSilentlyWeakened(t *testing.T) {
	const wantBehavioural = 15
	got := 0
	var literal []string
	for name, kind := range plantedProofs {
		switch kind {
		case proofBehavioural:
			got++
		case proofLiteral:
			literal = append(literal, name)
		}
	}
	if got != wantBehavioural {
		sort.Strings(literal)
		t.Fatalf("%d checkers have a behavioural planted-bug proof, expected %d "+
			"(literal: %v): raise the constant when converting one, never lower it",
			got, wantBehavioural, literal)
	}
}
