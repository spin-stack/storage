package dst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Harness-level scenarios: fault injection driven by the seed, resource exhaustion,
// and anything whose subject is the simulation itself rather than one subsystem.
// See scenarios_drain.go for why the list is split by area.

func harnessScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "disk-fills-under-sustained-write-with-s3-down", Run: scenarioDiskFillsWithS3Down},
		{Name: "lagging-list-never-lowers-the-boundary", Run: scenarioLaggingListNeverLowersTheBoundary},
		{Name: "seeded-faults-across-failover", Run: scenarioSeededFaultsAcrossFailover},
		{Name: "restored-control-plane", Run: scenarioRestoredControlPlane},
	}
}

func harnessCheckers() []Checker { return nil }

// enospcRecord is the payload size the out-of-space scenario writes; the encoded
// record is format.RecordHeaderSize (104) larger.
const enospcRecord = 200

// scenarioDiskFillsWithS3Down is the §5.7 backlog bound under the operational case it
// exists for: S3 has been unreachable long enough that no object closed the remote
// gap and no checkpoint authorised a local truncation, so the WAL device runs out of
// space. Two arms:
//
//  1. With MaxRemoteGapBytes configured below the device, the WAL must refuse WRITEs
//     with ErrBackpressure *before* the device fills. Backpressure is an error the
//     guest understands; ENOSPC arriving as a partial append is not.
//  2. With that bound disabled — a `local` volume, or an operator who never set it —
//     the device does fill. The first ENOSPC is a partial append, and what matters is
//     what it leaves behind: no phantom sequence, every later WRITE failing the same
//     way rather than silently succeeding, and a WAL that still replays to exactly the
//     records the guest was told were accepted.
//
// Known hole (reported, not asserted here): after arm 2 the log is not `Fenced()` and
// exposes no "out of space" state at all — `wal` has no ENOSPC policy, so a caller
// cannot distinguish a full device from a transient I/O error. Asserting a degraded
// state would need an API `internal/wal` does not have; this scenario pins down
// everything that *is* observable so the missing piece is the only gap left.
func scenarioDiskFillsWithS3Down(s *Sim) error {
	if err := enospcBackpressureFirst(s); err != nil {
		return err
	}
	return enospcTailReplaysClean(s)
}

// enospcBackpressureFirst is arm 1: the remote-gap bound fires before the device does.
func enospcBackpressureFirst(s *Sim) error {
	const (
		device   = "wal/gap-bound.wal"
		capacity = 1 << 13 // 8 KiB of device
		gapBound = 1500    // ~4 records: reached long before the device is
	)
	f, err := s.Disk.Create(device)
	if err != nil {
		return err
	}
	s.Disk.InjectENOSPC(device, capacity)

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf1
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxRemoteGapBytes: gapBound})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 2), alwaysValidLease{})

	// S3 is unreachable for the whole arm: nothing can close the gap.
	s.Store.InjectThrottle(1 << 20)
	s.Emit(Event{Kind: EventFault, Msg: "object store unreachable; remote gap cannot close"})

	accepted := 0
	for i := range 100 {
		_, err := l.Write(uint64(i)*4096, make([]byte, enospcRecord), 0)
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, wal.ErrBackpressure):
			s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("backpressure after %d records", accepted)})
			// The bound did its job: the device still has room, so no WRITE ever saw
			// a raw ENOSPC.
			size, serr := f.Size()
			if serr != nil {
				return serr
			}
			if size >= capacity {
				return fmt.Errorf("backpressure arrived only once the device was full (%d/%d bytes)", size, capacity)
			}
			// A FLUSH cannot rescue it while S3 is down: durable must not move.
			if ferr := l.Flush(context.Background()); ferr == nil {
				return errors.New("a FLUSH ACKed while the object store was unreachable (INV-07)")
			}
			emitWatermarks(s, l)
			if w := l.Watermarks(); w.Durable != 0 {
				return fmt.Errorf("durable advanced to %d with nothing in S3", w.Durable)
			}
			return nil
		case errors.Is(err, sim.ErrNoSpace):
			return fmt.Errorf("the device filled at record %d before the WAL applied backpressure (§5.7)", i)
		default:
			return fmt.Errorf("write %d: %w", i, err)
		}
	}
	return errors.New("the remote-gap bound never fired: 100 records were accepted with S3 down")
}

// enospcTailReplaysClean is arm 2: with no gap bound the device fills, and the WAL
// must survive it — no phantom record, sticky failure, clean replay, and correct
// numbering once space is reclaimed.
func enospcTailReplaysClean(s *Sim) error {
	const (
		device   = "wal/no-bound.wal"
		capacity = 1000 // three 304-byte records fit, the fourth does not
	)
	f, err := s.Disk.Create(device)
	if err != nil {
		return err
	}
	s.Disk.InjectENOSPC(device, capacity)

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf2
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})

	accepted := 0
	var full error
	for i := range 20 {
		_, err := l.Write(uint64(i)*4096, make([]byte, enospcRecord), 0)
		if err == nil {
			accepted++
			continue
		}
		full = err
		break
	}
	if full == nil {
		return errors.New("a 1000-byte device never returned ENOSPC")
	}
	if !errors.Is(full, sim.ErrNoSpace) {
		return fmt.Errorf("the first out-of-space WRITE reported %v, not ENOSPC", full)
	}
	if accepted == 0 {
		return errors.New("no record fit on the device at all; the arm proves nothing")
	}
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("device full after %d records", accepted)})

	// The rejected WRITE left no phantom sequence behind.
	if got := l.Watermarks().Local; got != uint64(accepted) {
		return fmt.Errorf("local watermark = %d after a rejected WRITE, want %d", got, accepted)
	}

	// Sticky: the device does not recover on its own, so every later WRITE fails the
	// same way. A WRITE that silently succeeded here would be the guest being told
	// its data is safe on a device that cannot hold it.
	for i := range 3 {
		if _, err := l.Write(uint64(1<<20)+uint64(i), make([]byte, enospcRecord), 0); !errors.Is(err, sim.ErrNoSpace) {
			return fmt.Errorf("WRITE %d on a full device returned %v, want ENOSPC", i, err)
		}
	}
	if l.Fenced() {
		return errors.New("the log self-fenced on ENOSPC; only a lease failure may do that (§16)")
	}

	// The WAL replays to exactly the accepted records: the partial append was rolled
	// back, so replay neither resurrects a rejected record nor stops at the tear.
	if err := l.Sync(); err != nil {
		return err
	}
	size, err := f.Size()
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	recs, err := wal.Replay(buf)
	if err != nil {
		return fmt.Errorf("replay of a WAL whose tail was cut by ENOSPC: %w", err)
	}
	if len(recs) != accepted {
		return fmt.Errorf("replay returned %d records, want the %d accepted ones", len(recs), accepted)
	}
	for i, r := range recs {
		if r.Sequence != uint64(i+1) {
			return fmt.Errorf("replayed sequence %d at position %d: the ENOSPC tail broke contiguity", r.Sequence, i)
		}
	}
	emitWatermarks(s, l)

	// Space comes back (a checkpoint authorised a truncation, or an operator grew the
	// device). Numbering continues from the accepted prefix — the rejected WRITE's
	// sequence is not re-used against different content.
	s.Disk.ClearENOSPC(device)
	seq, err := l.Write(1<<21, make([]byte, enospcRecord), 0)
	if err != nil {
		return fmt.Errorf("WRITE after space was reclaimed: %w", err)
	}
	if seq != uint64(accepted+1) {
		return fmt.Errorf("the WRITE after ENOSPC took sequence %d, want %d", seq, accepted+1)
	}
	s.Notef("device filled after %d records: backpressure first, no phantom record, clean replay", accepted)
	return nil
}

// scenarioLaggingListNeverLowersTheBoundary drives the one object-store behaviour the
// design tolerates but recovery must never trust: an eventually consistent LIST
// (§6.1). GET and HEAD are read-after-write in every accepted backend; LIST is not,
// and it answers a short listing with no error at all. The durable point is computed
// from a LIST (INV-08) and the epoch boundary is immutable (§12.5), so a listing that
// has not caught up is one PUT away from recording a floor below what the previous
// writer ACKed — every FLUSH under it lost for good.
//
// The lag is seed-driven: sometimes the listing catches up on its own before the
// boundary is written, sometimes not until Settle. Both must be safe, and safe means
// the same thing either way — the boundary is either refused or honest, never low.
func scenarioLaggingListNeverLowersTheBoundary(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf3

	// A lag measured in store operations: with the smallest lags the listing catches
	// up mid-scenario, with the largest it never does before Settle.
	lag := 1 + s.Rand.Intn(24)
	s.Store.SetListLag(lag)
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("LIST lags %d operations behind", lag)})

	f, err := s.Disk.Create("wal/lagging.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5), alwaysValidLease{})

	for i := range 3 {
		if _, err := l.Write(uint64(i)*4096, []byte("acked"), 0); err != nil {
			return err
		}
		if err := l.Flush(ctx); err != nil {
			return fmt.Errorf("flush %d: %w", i, err)
		}
	}
	// The summary is a strongly consistent record of what the writer ACKed; it is the
	// only thing that can contradict a short listing.
	if err := l.WriteSummary(ctx); err != nil {
		return err
	}
	acked := l.Watermarks().Durable
	if acked != 3 {
		return fmt.Errorf("setup: ACKed durable = %d, want 3", acked)
	}

	// What the lagging listing can prove. It may be anything from 0 to the truth, but
	// never more — a LIST that over-reports is a different fault entirely.
	stale, err := recovery.DurablePrefix(ctx, s.Store, vol, 1)
	if err != nil {
		return fmt.Errorf("durable prefix under a lagging LIST: %w", err)
	}
	if stale > acked {
		return fmt.Errorf("a lagging LIST reported %d, above the ACKed %d", stale, acked)
	}
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("lagging LIST proves %d of %d ACKed", stale, acked)})

	if stale < acked {
		// The cross-check must notice rather than smooth it over: the summary claims
		// more than the listing can produce (§22.1).
		var over *recovery.SummaryOverclaim
		if _, err := recovery.DurablePoint(ctx, s.Store, vol, 1); !errors.As(err, &over) {
			return fmt.Errorf("a listing short of the summary must be reported as an overclaim, got %v", err)
		}
		if over.Claimed != acked || over.Contiguous != stale {
			return fmt.Errorf("overclaim reported %+v, want claimed=%d contiguous=%d", over, acked, stale)
		}
		// And the boundary write must refuse the short number outright.
		err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, stale)
		if !errors.Is(err, recovery.ErrBoundaryRegression) {
			return fmt.Errorf("a boundary of %d was accepted while the writer ACKed %d: want ErrBoundaryRegression, got %v", stale, acked, err)
		}
		if _, err := recovery.ReadRecoveryPoint(ctx, s.Store, vol, 2); !errors.Is(err, objectstore.ErrNotFound) {
			return fmt.Errorf("a refused boundary must leave no object behind, got %v", err)
		}
	}

	// The listing catches up; the honest boundary is writable and covers every ACK.
	s.Store.Settle()
	settled, err := recovery.DurablePrefix(ctx, s.Store, vol, 1)
	if err != nil {
		return err
	}
	if settled != acked {
		return fmt.Errorf("after the listing caught up DurablePrefix = %d, want %d", settled, acked)
	}
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, settled); err != nil {
		return fmt.Errorf("the honest boundary must be writable: %w", err)
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, s.Store, vol, 2)
	if err != nil {
		return err
	}
	s.Emit(Event{Kind: EventFailover, AckedDurable: acked, Recovered: rp.RecoveredUpTo})
	if rp.RecoveredUpTo < acked {
		return fmt.Errorf("the epoch boundary settled at %d, below the ACKed %d (INV-09)", rp.RecoveredUpTo, acked)
	}
	s.Notef("LIST lagging %d ops: proved %d of %d, boundary refused until honest", lag, stale, acked)
	return nil
}

// faultPlan is the set of faults one run of the failover scenario meets, drawn from
// the seed. Every field is read from the PRNG in a fixed order, so a seed names one
// plan and only one (INV-02).
type faultPlan struct {
	records        int  // how many records W1 ACKs before it is fenced
	unflushedTail  bool // one more record is written and the host dies before its FLUSH
	throttleEpoch  int  // refusals the epoch object serves before the promotion lands
	promoteRetries int  // times the promotion is re-driven (a crash between its writes)
	loseBoundary   bool // the boundary PUT persists but its response is lost
	throttleBound  int  // refusals the boundary key serves
}

func drawFaultPlan(s *Sim) faultPlan {
	return faultPlan{
		records:        2 + s.Rand.Intn(4),
		unflushedTail:  s.Rand.Intn(2) == 0,
		throttleEpoch:  s.Rand.Intn(3),
		promoteRetries: 1 + s.Rand.Intn(3),
		loseBoundary:   s.Rand.Intn(2) == 0,
		throttleBound:  s.Rand.Intn(3),
	}
}

func (p faultPlan) String() string {
	return fmt.Sprintf("records=%d unflushed_tail=%t throttle_epoch=%d promote_retries=%d lose_boundary=%t throttle_boundary=%d",
		p.records, p.unflushedTail, p.throttleEpoch, p.promoteRetries, p.loseBoundary, p.throttleBound)
}

// v7-shaped ids for the seeded-fault failover scenario.
const (
	seededHost1 = "00000000-0000-7000-8000-0000000000f4"
	seededHost2 = "00000000-0000-7000-8000-0000000000f5"
)

// scenarioSeededFaultsAcrossFailover is the fault-injection matrix §25.1 advertises
// and the gate did not have. Until now the seed decided a payload size, a crash side
// of one fdatasync and a clock drift, and nothing else: every fencing, promotion and
// recovery-point run explored exactly one interleaving of exactly one fault set, which
// is why trivially reproducible bugs on those paths survived a six-seed gate.
//
// Here the seed picks the whole plan — how much W1 ACKs, whether a record dies
// unflushed with the host, how often the epoch object and the boundary key refuse,
// how many times the promotion is re-driven, and whether the boundary PUT's response
// is lost — and the post-conditions are asserted after the resume, not instead of it:
//
//   - the promotion grants exactly one epoch however many times it runs;
//   - the fenced writer can neither ACK, verify its epoch, nor advance it;
//   - the boundary that ends up in S3 covers everything W1 ACKed, whatever sequence
//     of refusals and lost responses it took to write it (INV-09/INV-12/INV-21).
func scenarioSeededFaultsAcrossFailover(s *Sim) error {
	ctx := context.Background()
	plan := drawFaultPlan(s)
	s.Emit(Event{Kind: EventFault, Msg: "fault plan: " + plan.String()})

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf6
	vid := format.UUIDString(vol)

	md := metasim.New(s.Clock.Wall)
	epochs := epoch.NewStore(s.Store)
	term, _ := md.AcquireLeadership(ctx, "cp")
	for _, h := range []string{seededHost1, seededHost2} {
		if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			return err
		}
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: vid, CurrentEpoch: 1, State: lifecycle.VolumeActive,
		PrimaryHostID: seededHost1, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		return err
	}
	if _, err := epochs.Init(ctx, vid, 1); err != nil {
		return err
	}
	if err := md.RenewHostLease(ctx, term, seededHost1, 10); err != nil {
		return err
	}
	renewedAt := s.Clock.Wall()

	// W1 at epoch 1, fenced by a real lease manager on the monotonic clock.
	const leaseTTL = 10 * time.Second
	lm := lease.NewManager(s.Clock, leaseTTL)
	lm.Grant()
	f, err := s.Disk.Create("wal/seeded.wal")
	if err != nil {
		return err
	}
	w1 := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	w1.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5), lm)

	for i := range plan.records {
		if _, err := w1.Write(uint64(i)*4096, []byte("acked"), 0); err != nil {
			return fmt.Errorf("w1 write %d: %w", i, err)
		}
		if err := w1.Flush(ctx); err != nil {
			return fmt.Errorf("w1 flush %d: %w", i, err)
		}
	}
	acked := w1.Watermarks().Durable
	s.Emit(Event{Kind: EventDurableAck, Durable: acked, LeaseValid: lm.Valid()})
	if acked != uint64(plan.records) {
		return fmt.Errorf("w1 ACKed %d of %d records", acked, plan.records)
	}
	if err := w1.WriteSummary(ctx); err != nil {
		return err
	}

	// The host may die with one record appended and never FLUSHed: the crash discards
	// the local bytes the fdatasync never covered, while the batch still holds the
	// record, so W1's last (self-fenced) FLUSH lands it in S3 without ever ACKing it.
	// That late object is a harmless superset (§12.5) — the boundary may include it,
	// and must never fall below what W1 did ACK.
	if plan.unflushedTail {
		if _, err := w1.Write(uint64(plan.records)*4096, []byte("never-acked"), 0); err != nil {
			return err
		}
		s.Disk.Crash()
		s.Emit(Event{Kind: EventFault, Msg: "host died with an unflushed record"})
	}

	// The lease expires. W1 must self-fence rather than ACK anything else, whether or
	// not its objects still reach S3.
	s.Tick(leaseTTL + time.Second)
	if err := w1.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("w1 flush past its lease: want ErrSelfFenced, got %v", err)
	}
	if got := w1.Watermarks().Durable; got != acked {
		return fmt.Errorf("w1 durable moved to %d past its last ACK with an invalid lease (INV-06)", got)
	}

	// The CP promotes W2 after FENCING_WAIT, through a refusing epoch object and
	// however many re-drives the plan calls for. Every one of them must land on the
	// same epoch: a second grant would fence the writer the first one installed.
	s.Tick(3 * time.Second)
	p := controlplane.NewPromoter(md, epochs, s.Clock, leaseTTL, 2*time.Second)
	deadline := p.FencingDeadline(renewedAt)
	if plan.throttleEpoch > 0 {
		s.Store.InjectThrottleKey(epoch.Key(vid), plan.throttleEpoch)
	}
	granted, redrives := uint64(0), 0
	budget := plan.promoteRetries + plan.throttleEpoch + 4
	for attempt := 0; attempt < budget && redrives < plan.promoteRetries; attempt++ {
		newEpoch, err := p.Promote(ctx, term, vid, renewedAt, seededHost2)
		switch {
		case errors.Is(err, sim.ErrThrottled):
			s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("promotion attempt %d refused by the epoch object", attempt)})
			continue
		case err != nil:
			return fmt.Errorf("promote attempt %d: %w", attempt, err)
		}
		redrives++
		s.Emit(Event{Kind: EventPromotion, EarlyGrant: s.Clock.Wall().Before(deadline), Msg: fmt.Sprintf("epoch=%d attempt=%d", newEpoch, attempt)})
		if granted != 0 && newEpoch != granted {
			return fmt.Errorf("re-driving the promotion granted %d after %d: two writers now share one namespace (INV-10)", newEpoch, granted)
		}
		granted = newEpoch
	}
	if redrives != plan.promoteRetries {
		return fmt.Errorf("the promotion completed %d of the %d re-drives the plan calls for", redrives, plan.promoteRetries)
	}
	if granted != 2 {
		return fmt.Errorf("promotion granted epoch %d, want 2", granted)
	}
	stored, staleETag, err := epochs.Current(ctx, vid)
	if err != nil {
		return err
	}
	if stored != 2 {
		return fmt.Errorf("the epoch object settled at %d after %d promotion attempts, want 2", stored, plan.promoteRetries)
	}
	if v, _ := md.GetVolume(ctx, vid); v.CurrentEpoch != 2 {
		return fmt.Errorf("PostgreSQL settled at epoch %d, want 2", v.CurrentEpoch)
	}

	// INV-10: W1 is fenced. It cannot verify epoch 1 and cannot advance the object.
	verr := epochs.Verify(ctx, vid, 1)
	_, caserr := epochs.CompareAndAdvance(ctx, vid, staleETag, 1)
	s.Emit(Event{Kind: EventStalePublsh, StalePublishOK: verr == nil || caserr == nil})
	if verr == nil || caserr == nil {
		return fmt.Errorf("a fenced writer could still publish: verify=%v cas=%v", verr, caserr)
	}

	// W2 recovers epoch 1's durable prefix from S3 and records the boundary, through
	// whatever the plan throws at the boundary key.
	recovered, err := recovery.DurablePrefix(ctx, s.Store, vol, 1)
	if err != nil {
		return fmt.Errorf("recover epoch 1: %w", err)
	}
	if recovered < acked {
		return fmt.Errorf("recovered %d < ACKed %d before the boundary was even written (INV-09)", recovered, acked)
	}

	boundaryKey := fmt.Sprintf("wal/%s/%d/recovery-point.json", vid, 2)
	if plan.loseBoundary {
		s.Store.InjectLostResponse(boundaryKey)
	}
	if plan.throttleBound > 0 {
		s.Store.InjectThrottleKey(boundaryKey, plan.throttleBound)
	}
	if err := writeBoundaryThroughFaults(s, ctx, vol, recovered); err != nil {
		return err
	}

	rp, err := recovery.ReadRecoveryPoint(ctx, s.Store, vol, 2)
	if err != nil {
		return fmt.Errorf("the boundary must exist after the retries (key %q): %w", boundaryKey, err)
	}
	s.Emit(Event{Kind: EventFailover, AckedDurable: acked, Recovered: rp.RecoveredUpTo})
	if rp.PrevEpoch != 1 {
		return fmt.Errorf("boundary prev_epoch = %d, want 1", rp.PrevEpoch)
	}
	if rp.RecoveredUpTo < acked {
		return fmt.Errorf("the boundary recorded %d, below the %d W1 ACKed (INV-09/INV-12)", rp.RecoveredUpTo, acked)
	}
	s.Notef("seeded failover survived {%s}: one epoch granted, boundary %d >= acked %d", plan, rp.RecoveredUpTo, acked)
	return nil
}

// writeBoundaryThroughFaults drives the create-only boundary PUT until it is
// established, reconciling the two answers a lost response leaves behind: the retry
// either succeeds or reports the key already there (§14.5, INV-21). A boundary that
// is never written is a volume nobody can recover, so exhausting the budget fails.
func writeBoundaryThroughFaults(s *Sim, ctx context.Context, vol [16]byte, recovered uint64) error {
	var last error
	for attempt := range 8 {
		err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, recovered)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, objectstore.ErrPreconditionFailed):
			// The lost response had persisted it. That is success, and the read-back
			// above is what proves the persisted value is the one we meant.
			s.Emit(Event{Kind: EventObject, Key: "recovery-point", Msg: "lost response reconciled: already present"})
			return nil
		case errors.Is(err, sim.ErrThrottled), errors.Is(err, sim.ErrLostResponse):
			s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("boundary PUT attempt %d refused: %v", attempt, err)})
			last = err
		default:
			return fmt.Errorf("boundary PUT: %w", err)
		}
	}
	return fmt.Errorf("the epoch boundary was never established: %w", last)
}

// rewoundLeadership is a metadata store whose leadership row was restored from a
// backup: elections resume from an earlier term, so the same term is handed out
// twice. Nothing inside the database can tell — the term is derived from the row.
type rewoundLeadership struct {
	metadata.Store
	replay []int64
	n      int
}

func (s *rewoundLeadership) AcquireLeadership(ctx context.Context, holderID string) (int64, error) {
	if s.n < len(s.replay) {
		t := s.replay[s.n]
		s.n++
		return t, nil
	}
	return s.Store.AcquireLeadership(ctx, holderID)
}

// scenarioRestoredControlPlane is ADR-0011 under the incident it was written for: the
// Control Plane database is restored to a point before the running leader's term, so
// the next election offers a term that is still in use. Every §7 mutation is guarded
// by that number, so two processes would pass every guard at once — not a zombie and a
// leader, but two leaders, each reading the other's writes as its own resumed work.
//
// The scenario drives the real elector against the real simulated object store and
// asserts both halves: that the rewound database really does offer the live term back
// (otherwise the rest proves nothing), and that the elector refuses to return it,
// climbing past every term the bucket has ever witnessed.
func scenarioRestoredControlPlane(s *Sim) error {
	ctx := context.Background()
	md := metasim.New(s.Clock.Wall)
	elector := controlplane.NewElector(md, s.Store)

	first, err := elector.Acquire(ctx, "cp-a")
	if err != nil {
		return err
	}
	host := "00000000-0000-7000-8000-0000000000a1"
	if err := md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-a at term %d", first)})

	// The restore. The row is back before cp-a's term while cp-a is still running.
	restored := &rewoundLeadership{Store: md, replay: []int64{first}}
	s.Emit(Event{Kind: EventFault, Msg: "control-plane database restored to an earlier term"})

	// The fault is real: read the database alone and it offers the live term back.
	offered, err := restored.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		return err
	}
	if offered != first {
		return fmt.Errorf("the rewind did not reproduce: election offered %d, not the live %d", offered, first)
	}

	// The elector is what stops it: the claim for that term is already in the bucket.
	second, err := controlplane.NewElector(&rewoundLeadership{Store: md, replay: []int64{first}}, s.Store).
		Acquire(ctx, "cp-b")
	if err != nil {
		return err
	}
	if second <= first {
		return fmt.Errorf("a restored database re-issued term %d while cp-a still holds %d (violates §7)", second, first)
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-b climbed to term %d", second)})

	// And the §7 guard is doing its job again: cp-a is now the zombie.
	err = md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	})
	if !errors.Is(err, metadata.ErrStaleTerm) {
		return fmt.Errorf("the superseded leader's mutation returned %v, want ErrStaleTerm", err)
	}
	return nil
}
