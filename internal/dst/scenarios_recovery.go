package dst

// Recovery, materialization and epoch-boundary scenarios (§22) — see
// scenarios_drain.go for why the list is split by area.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Two event kinds this area contributes. Both carry their subject in Key — the real
// object key or prefix, so the trace reads as the bucket does — and their number in
// Recovered.
const (
	// EventBoundary is one observation of an epoch-boundary object (§12.5). Key is the
	// recovery-point key, so the volume and the epoch travel with it; Recovered is
	// recovered_up_to.
	EventBoundary EventKind = "boundary"
	// EventDurablePoint is one observation of a volume/epoch's durable point (INV-08).
	// Key is the epoch's WAL prefix; Recovered is the point observed.
	EventDurablePoint EventKind = "durable-point"
)

func recoveryScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "superseded-epoch-has-a-ceiling", Run: scenarioSupersededEpochCeiling},
		{Name: "boundary-chain-is-monotonic", Run: scenarioBoundaryChainMonotonic},
		{Name: "divergent-objects-are-refused", Run: scenarioDivergentObjectsRefused},
		{Name: "publishers-must-hold-the-epoch", Run: scenarioPublishersMustHoldTheEpoch},
	}
}

func recoveryCheckers() []Checker {
	return []Checker{
		NewBoundaryMonotonicChecker(),
		NewDurablePointMonotonicChecker(),
	}
}

// epochFromKey pulls the epoch out of `wal/<vol>/<epoch>/...`. The volume is
// everything before it, so the pair identifies the subject of an observation.
func epochFromKey(key string) (volume string, epoch uint64, ok bool) {
	parts := strings.Split(key, "/")
	if len(parts) < 3 || parts[0] != "wal" {
		return "", 0, false
	}
	e, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return parts[1], e, true
}

// BoundaryMonotonicChecker enforces §12.5 / INV-12 across a whole run: for one
// volume, the recovered_up_to of successive epochs never goes down, and an epoch's
// boundary never changes value once observed.
//
// This is the one number in the system nothing can walk back. The recovery-point
// object is create-only, and the next epoch's floor is derived from it, so a boundary
// recorded below an earlier one does not lose data slowly — every ACKed write beneath
// it is unreachable from the moment of the write, with no in-band repair. A checker
// that sees the whole run catches the orderings a single write-time guard cannot:
// boundaries written out of order, or by two promoters that never saw each other.
type BoundaryMonotonicChecker struct {
	seen      map[string]map[uint64]uint64 // volume -> epoch -> recovered_up_to
	violation error
}

// NewBoundaryMonotonicChecker returns a fresh checker.
func NewBoundaryMonotonicChecker() *BoundaryMonotonicChecker {
	return &BoundaryMonotonicChecker{seen: map[string]map[uint64]uint64{}}
}

func (c *BoundaryMonotonicChecker) Name() string { return "boundary-monotonic" }

func (c *BoundaryMonotonicChecker) Observe(e Event) {
	if e.Kind != EventBoundary || c.violation != nil {
		return
	}
	vol, epoch, ok := epochFromKey(e.Key)
	if !ok {
		c.violation = fmt.Errorf("boundary observation at step %d has an unparseable key %q", e.Step, e.Key)
		return
	}
	byEpoch := c.seen[vol]
	if byEpoch == nil {
		byEpoch = map[uint64]uint64{}
		c.seen[vol] = byEpoch
	}
	if prev, dup := byEpoch[epoch]; dup && prev != e.Recovered {
		c.violation = fmt.Errorf("the create-only boundary of epoch %d of %s changed from %d to %d at step %d (violates §12.5/INV-12)",
			epoch, vol, prev, e.Recovered, e.Step)
		return
	}
	byEpoch[epoch] = e.Recovered
}

func (c *BoundaryMonotonicChecker) Check() error {
	if c.violation != nil {
		return c.violation
	}
	for vol, byEpoch := range c.seen {
		epochs := make([]uint64, 0, len(byEpoch))
		for e := range byEpoch {
			epochs = append(epochs, e)
		}
		sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
		for i := 1; i < len(epochs); i++ {
			lo, hi := byEpoch[epochs[i-1]], byEpoch[epochs[i]]
			if hi < lo {
				return fmt.Errorf("volume %s: epoch %d records recovered_up_to %d, below epoch %d's %d — "+
					"the object is create-only, so everything between them is under a floor nobody can raise (violates §12.5/INV-12)",
					vol, epochs[i], hi, epochs[i-1], lo)
			}
		}
	}
	return nil
}

// DurablePointMonotonicChecker enforces INV-08 against a lagging listing: the durable
// point observed for a volume/epoch never drops below a value already observed for it.
//
// A LIST that has not caught up — an S3-compatible backend behind, or a listing racing
// an in-flight upload — makes recovery.DurablePrefix answer a smaller number with no
// error at all. That number is what a promotion writes into the next epoch's
// create-only recovery point, so a stale listing is enough to bury ACKed FLUSHes
// permanently. Nothing else in the system would notice: the answer is not an error,
// it is just smaller.
type DurablePointMonotonicChecker struct {
	high      map[string]uint64
	violation error
}

// NewDurablePointMonotonicChecker returns a fresh checker.
func NewDurablePointMonotonicChecker() *DurablePointMonotonicChecker {
	return &DurablePointMonotonicChecker{high: map[string]uint64{}}
}

func (c *DurablePointMonotonicChecker) Name() string { return "durable-point-monotonic" }

func (c *DurablePointMonotonicChecker) Observe(e Event) {
	if e.Kind != EventDurablePoint || c.violation != nil {
		return
	}
	if prev, ok := c.high[e.Key]; ok && e.Recovered < prev {
		c.violation = fmt.Errorf("the durable point of %s fell from %d to %d at step %d — "+
			"a promotion writes this number into a create-only boundary (violates §5.8/INV-08)",
			e.Key, prev, e.Recovered, e.Step)
		return
	}
	c.high[e.Key] = e.Recovered
}

func (c *DurablePointMonotonicChecker) Check() error { return c.violation }

// The three hosts of scenarioPublishersMustHoldTheEpoch: the one the object store
// granted epoch 1 to, the one a promoter's half-finished work left named in
// PostgreSQL at that same epoch, and the destination epoch 2 is granted to.
const (
	hostHoldsEpoch1 = "00000000-0000-7000-8000-0000000000d1"
	hostNamedInPG   = "00000000-0000-7000-8000-0000000000d2"
	hostHoldsEpoch2 = "00000000-0000-7000-8000-0000000000d3"
)

// grantEpochTo advances the volume's epoch object to ep, naming its holder (§12.3
// step 4b).
func grantEpochTo(ctx context.Context, es *epoch.Store, volumeID string, ep uint64, holder string) error {
	_, etag, err := es.Current(ctx, volumeID)
	if err != nil {
		return err
	}
	_, err = es.Grant(ctx, volumeID, etag, ep, holder)
	return err
}

// scenarioPublishersMustHoldTheEpoch is the split a number-only fence cannot see, run
// end to end: PostgreSQL and the epoch object name two different hosts at the *same*
// epoch, because the two records of a promotion are written by two steps of §12.3 and
// a promoter can die between them.
//
// Both hosts are right about the number. One of them was granted the epoch. The
// scenario asserts the three places that difference has to be decided:
//
//   - a checkpoint (a publication into checkpoints/<vol>/<epoch>/ that also authorises
//     truncating the local WAL, INV-13) is refused for the host that was not granted
//     the epoch, and its published watermark does not move;
//   - the epoch boundary (create-only, immutable, §12.5) is refused for it too, and
//     nothing is left in the bucket — the epoch's real holder must not inherit a floor
//     it did not choose;
//   - materialization is *not* refused. ADR-0008's drain has the destination rebuild
//     the durable prefix of the epoch the source still holds, before the fence, so a
//     holder check on the read path would make evacuating a dead host impossible.
func scenarioPublishersMustHoldTheEpoch(s *Sim) error {
	ctx := context.Background()
	vol := recVol()
	vid := format.UUIDString(vol)
	es := epoch.NewStore(s.Store)

	if _, err := es.Init(ctx, vid, 0); err != nil {
		return err
	}
	if err := grantEpochTo(ctx, es, vid, 1, hostHoldsEpoch1); err != nil {
		return fmt.Errorf("granting epoch 1: %w", err)
	}

	l, err := recLog(s, vol, 1, 0)
	if err != nil {
		return err
	}
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("acked"), 0); err != nil {
			return err
		}
		if err := l.Flush(ctx); err != nil {
			return fmt.Errorf("flush %d: %w", i, err)
		}
	}
	acked := l.Watermarks().Durable

	// The host PostgreSQL names checkpoints the very same log, at the very same epoch.
	// Only the publisher's identity differs from the call below it, and only that may
	// decide the outcome.
	cps := checkpoint.NewCheckpointer(s.Store)
	if _, err := cps.HeldBy(hostNamedInPG).Create(ctx, l, vol, 1); !errors.Is(err, epoch.ErrNotHolder) {
		return fmt.Errorf("a host the epoch was never granted to published a checkpoint into it: %v", err)
	}
	if p := l.Watermarks().Published; p != 0 {
		return fmt.Errorf("a refused checkpoint advanced published to %d, authorising truncation of "+
			"local WAL this host does not own (INV-13)", p)
	}
	if err := l.TruncateLocal(acked); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		return fmt.Errorf("truncation must stay refused after a refused checkpoint, got %v", err)
	}

	// The holder publishes the identical checkpoint, and only now is the local copy
	// disposable.
	if _, err := cps.HeldBy(hostHoldsEpoch1).Create(ctx, l, vol, 1); err != nil {
		return fmt.Errorf("the epoch's holder could not publish its own checkpoint: %w", err)
	}
	if p := l.Watermarks().Published; p != acked {
		return fmt.Errorf("published = %d after the holder's checkpoint, want %d", p, acked)
	}
	if err := l.TruncateLocal(acked); err != nil {
		return fmt.Errorf("truncation to the published point: %w", err)
	}

	// ADR-0008 step 1: the destination rebuilds epoch 1 while hostHoldsEpoch1 still
	// holds it. This must succeed — it is the whole reason a drain works against a
	// host that is dead or refusing to cooperate.
	_, prog, err := materialize.New(s.Store, nil, nil).FromEpoch(ctx, vol, 1)
	if err != nil {
		return fmt.Errorf("the destination cannot rebuild an epoch another host holds (ADR-0008): %w", err)
	}
	if prog.UpTo != acked {
		return fmt.Errorf("the bulk pass covered %d, the epoch is durable through %d", prog.UpTo, acked)
	}

	// ADR-0008 step 2: the fence. Epoch 2 goes to the destination.
	if err := grantEpochTo(ctx, es, vid, 2, hostHoldsEpoch2); err != nil {
		return fmt.Errorf("granting epoch 2: %w", err)
	}

	// The boundary is create-only and immutable, so the wrong author is not a mistake
	// anyone can repair: it must be refused, and it must leave nothing behind.
	rp := recovery.RecoveryPoint{PrevEpoch: 1, RecoveredUpTo: acked}
	if err := rp.WriteAs(ctx, s.Store, vol, 2, hostNamedInPG); !errors.Is(err, epoch.ErrNotHolder) {
		return fmt.Errorf("a host epoch 2 was never granted to recorded its immutable boundary: %v", err)
	}
	if _, err := recovery.ReadRecoveryPoint(ctx, s.Store, vol, 2); !errors.Is(err, objectstore.ErrNotFound) {
		return errors.New("the refused boundary object was written anyway — it can never be rewritten")
	}
	if err := rp.WriteAs(ctx, s.Store, vol, 2, hostHoldsEpoch2); err != nil {
		return fmt.Errorf("the holder of epoch 2 could not record its own boundary: %w", err)
	}
	if err := observeBoundary(s, vol, 2); err != nil {
		return err
	}

	s.Notef("two hosts at epoch 1, one grant: the holder published and truncated, the other was "+
		"refused at the checkpoint and at the boundary, and the drain still read %d", prog.UpTo)
	return nil
}

// recVol is the v7-shaped volume id this area's scenarios use.
func recVol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	v[15] = 0x51
	return v
}

// observeDurablePoint records one observation of a volume/epoch's durable point, so
// the monotonicity checker sees the whole sequence of them.
func observeDurablePoint(s *Sim, vol [16]byte, epoch uint64) (uint64, error) {
	got, err := recovery.DurablePrefix(context.Background(), s.Store, vol, epoch)
	if err != nil {
		return 0, err
	}
	s.Emit(Event{
		Kind:      EventDurablePoint,
		Key:       fmt.Sprintf("wal/%s/%d/", format.UUIDString(vol), epoch),
		Recovered: got,
	})
	return got, nil
}

// observeBoundary reads an epoch-boundary object and records it.
func observeBoundary(s *Sim, vol [16]byte, epoch uint64) error {
	rp, err := recovery.ReadRecoveryPoint(context.Background(), s.Store, vol, epoch)
	if err != nil {
		return err
	}
	s.Emit(Event{
		Kind:      EventBoundary,
		Key:       fmt.Sprintf("wal/%s/%d/recovery-point.json", format.UUIDString(vol), epoch),
		Recovered: rp.RecoveredUpTo,
		Msg:       fmt.Sprintf("prev_epoch=%d", rp.PrevEpoch),
	})
	return nil
}

// recLog opens a remote-enabled log for one epoch, continuing the sequence space
// after `startSeq` (§12.5: a volume's sequences are continuous across epochs).
func recLog(s *Sim, vol [16]byte, epoch, startSeq uint64) (*wal.Log, error) {
	return recLogAt(s, "wal", vol, epoch, startSeq)
}

// recLogAt is recLog with the WAL root named. A scenario that stages two writers over
// one (volume, epoch) — which is the whole point of the split-brain and late-PUT
// cases — gives each its own root: they share the bucket, which is where the conflict
// lives, and not a local directory, which two hosts never would.
func recLogAt(s *Sim, root string, vol [16]byte, epoch, startSeq uint64) (*wal.Log, error) {
	l := wal.NewLogAfter(s.Disk, root, s.Clock, vol, epoch, startSeq, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(s.Clock, vol, epoch, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5),
		alwaysValidLease{},
	)
	return l, nil
}

// scenarioSupersededEpochCeiling is the ordering that used to produce two divergent
// volumes out of one bucket: W1's PUT is still in flight when W2 promotes, so it lands
// *after* the create-only recovery point of epoch N+1 is written.
//
// Everything above that boundary was written by a writer that was already fenced and
// was never adopted. Recovery and materialization must both stop there — and must stop
// at the same place, because the drain compares the number it recomputes against the
// immutable object and fails permanently when they differ.
func scenarioSupersededEpochCeiling(s *Sim) error {
	ctx := context.Background()
	vol := recVol()

	w1, err := recLog(s, vol, 1, 0)
	if err != nil {
		return err
	}
	if _, err := w1.Write(0, []byte("acked-by-w1"), 0); err != nil {
		return err
	}
	if err := w1.Flush(ctx); err != nil {
		return fmt.Errorf("w1 flush: %w", err)
	}
	acked := w1.Watermarks().Durable

	// W2 promotes: it recovers what S3 proves and fixes the frontier of epoch 2.
	recovered, err := observeDurablePoint(s, vol, 1)
	if err != nil {
		return err
	}
	if recovered != acked {
		return fmt.Errorf("recovered %d but W1 ACKed %d (INV-09)", recovered, acked)
	}
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, recovered); err != nil {
		return err
	}
	if err := observeBoundary(s, vol, 2); err != nil {
		return err
	}

	// W1's in-flight PUT completes now. The object is valid; its writer is not.
	late, err := recLogAt(s, "wal-late", vol, 1, acked)
	if err != nil {
		return err
	}
	if _, err := late.Write(4096, []byte("never-adopted"), 0); err != nil {
		return err
	}
	if err := late.Flush(ctx); err != nil {
		return fmt.Errorf("late flush: %w", err)
	}

	after, err := observeDurablePoint(s, vol, 1)
	if err != nil {
		return err
	}
	if after != recovered {
		return fmt.Errorf("a fenced writer's late PUT raised epoch 1's durable point from %d to %d, "+
			"above the immutable boundary of epoch 2", recovered, after)
	}

	// The drain's source has to land on exactly the number the boundary records —
	// finishMovedVolume recomputes it and treats a disagreement as permanent.
	_, prog, err := materialize.New(s.Store, nil, nil).FromEpoch(ctx, vol, 1)
	if err != nil {
		return fmt.Errorf("materialize epoch 1: %w", err)
	}
	if prog.UpTo != recovered {
		return fmt.Errorf("materialization covered %d, the boundary records %d — the same bucket "+
			"produced two different volumes", prog.UpTo, recovered)
	}
	s.Notef("epoch 1 stayed at %d after a fenced late PUT; materialization agrees", recovered)
	return nil
}

// scenarioBoundaryChainMonotonic walks a volume through three epochs and feeds every
// boundary to the monotonicity checker, then attempts the write that would break it.
func scenarioBoundaryChainMonotonic(s *Sim) error {
	ctx := context.Background()
	vol := recVol()

	var seq uint64
	for epoch := uint64(1); epoch <= 3; epoch++ {
		if epoch > 1 {
			if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, epoch, epoch-1, seq); err != nil {
				return fmt.Errorf("boundary of epoch %d: %w", epoch, err)
			}
			if err := observeBoundary(s, vol, epoch); err != nil {
				return err
			}
		}
		l, err := recLog(s, vol, epoch, seq)
		if err != nil {
			return err
		}
		if _, err := l.Write(epoch*4096, []byte("payload"), 0); err != nil {
			return err
		}
		if err := l.Flush(ctx); err != nil {
			return fmt.Errorf("epoch %d flush: %w", epoch, err)
		}
		seq = l.Watermarks().Durable
		if _, err := observeDurablePoint(s, vol, epoch); err != nil {
			return err
		}
	}

	// A fourth epoch recording less than the third is refused at the write, which is
	// the only place it can be refused: the object cannot be rewritten afterwards.
	err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 4, 3, 1)
	if !errors.Is(err, recovery.ErrBoundaryRegression) {
		return fmt.Errorf("a boundary below its predecessor was not refused: %v", err)
	}
	if _, err := recovery.ReadRecoveryPoint(ctx, s.Store, vol, 4); err == nil {
		return errors.New("the regressing boundary object was written anyway")
	}
	s.Notef("three chained boundaries, non-decreasing; a fourth below them was refused")
	return nil
}

// scenarioDivergentObjectsRefused is INV-21 (§14.5) at the only layer that can still
// see it. The uploader refuses to claim a sequence span another object already
// claims — but that guard is a LIST, and a LIST can be behind (§22.1, §24). Under a
// lagging listing two writers sharing one epoch each see an empty span and each
// object lands: they claim the same sequences with different bytes, on two different
// keys (the key embeds the payload digest), so both create-only PUTs succeed and both
// objects pass validation.
//
// From there the volume's content is decided by whichever SHA prefix sorts first.
// Recovery is the last layer that can still refuse, and it must — this is exactly the
// split-brain INV-21 exists to name.
func scenarioDivergentObjectsRefused(s *Sim) error {
	ctx := context.Background()
	vol := recVol()

	// The honest control first, in its own epoch: the guard must not make an ordinary
	// epoch unrecoverable.
	honest, err := recLog(s, vol, 1, 0)
	if err != nil {
		return err
	}
	if _, err := honest.Write(0, []byte("one-writer"), 0); err != nil {
		return err
	}
	if err := honest.Flush(ctx); err != nil {
		return err
	}
	boundary, err := observeDurablePoint(s, vol, 1)
	if err != nil {
		return err
	}
	// A well-formed promotion into epoch 2, so what follows is about the objects and
	// not about a broken chain.
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, boundary); err != nil {
		return err
	}
	if err := observeBoundary(s, vol, 2); err != nil {
		return err
	}

	// The listing goes behind, blinding the uploader's span guard. Two writers now
	// share epoch 2 — the state fencing exists to prevent — and pick the same
	// sequence.
	s.Store.SetEventualList(true)
	for i, payload := range []string{"written-by-w1", "written-by-w2"} {
		// Two hosts, so two WAL roots: they share the volume and the epoch, which is
		// exactly the split this scenario stages, but not a directory.
		w := wal.NewLogAfter(s.Disk, fmt.Sprintf("wal-split-%d", i), s.Clock, vol, 2, boundary, wal.Limits{MaxUnflushedBytes: 1 << 20})
		w.EnableRemote(
			wal.NewBatcher(s.Clock, vol, 2, 0, wal.DefaultBatchConfig()),
			wal.NewUploader(s.Store, 5),
			alwaysValidLease{},
		)
		if _, err := w.Write(0, []byte(payload), 0); err != nil {
			return err
		}
		if err := w.Flush(ctx); err != nil {
			return fmt.Errorf("split writer %d flush: %w", i, err)
		}
	}
	s.Store.Settle() // the listing catches up; both objects are now visible

	objs, err := s.Store.List(ctx, fmt.Sprintf("wal/%s/2/", format.UUIDString(vol)))
	if err != nil {
		return err
	}
	var wals int
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".wal") {
			wals++
		}
	}
	if wals != 2 {
		return fmt.Errorf("setup: expected two divergent objects under epoch 2, found %d — "+
			"the lagging listing did not let both through", wals)
	}

	if _, err := recovery.DurablePrefix(ctx, s.Store, vol, 2); !errors.Is(err, recovery.ErrAmbiguousSequence) {
		return fmt.Errorf("two objects carrying different records for sequence 1 were accepted: %v", err)
	}
	if _, _, err := recovery.Recover(ctx, s.Store, nil, vol, 2); !errors.Is(err, recovery.ErrAmbiguousSequence) {
		return fmt.Errorf("recovery picked a side instead of refusing the ambiguous epoch: %v", err)
	}
	// Epoch 1 is untouched by its neighbour's ambiguity: the guard must not spread.
	if _, err := observeDurablePoint(s, vol, 1); err != nil {
		return fmt.Errorf("the honest epoch stopped being recoverable: %w", err)
	}
	s.Notef("an epoch claimed by two writers is refused, not resolved by sort order")
	return nil
}
