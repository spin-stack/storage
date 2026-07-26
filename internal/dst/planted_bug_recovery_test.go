package dst

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Planted-bug proofs for the checkers scenarios_recovery.go contributes. A checker
// that has never been seen to reject anything is decoration (PLAN.md §3), so each
// case below plants the exact violation its invariant forbids and requires the
// checker to fail, name itself, and print the reproducing seed.

const plantedVol = "00000000-0000-7000-8000-000000000051"

// Planted-bug outcomes for this area.
var (
	errBoundaryRegressed    = errors.New("planted: a boundary was written below its predecessor")
	errDurablePointFellBack = errors.New("planted: the durable point fell below a value already observed")
)

// §12.5 / INV-12: the recovered_up_to of successive epochs never goes down. The
// recovery-point object is create-only and the next epoch's floor is derived from it,
// so a boundary below an earlier one buries every ACKed write between them for good.
func TestPlantedBugBoundaryRegressed(t *testing.T) {
	plantedBug(t, 21, NewBoundaryMonotonicChecker(), "boundary-monotonic", func(s *Sim) error {
		s.Emit(Event{Kind: EventBoundary, Key: "wal/" + plantedVol + "/2/recovery-point.json", Recovered: 40})
		s.Emit(Event{Kind: EventBoundary, Key: "wal/" + plantedVol + "/3/recovery-point.json", Recovered: 12})
		return nil
	})
}

// The same checker's other half: a create-only object that answers two different
// values is either two writers behind one key or a backend that lost a version.
func TestPlantedBugBoundaryRewritten(t *testing.T) {
	plantedBug(t, 22, NewBoundaryMonotonicChecker(), "boundary-monotonic", func(s *Sim) error {
		s.Emit(Event{Kind: EventBoundary, Key: "wal/" + plantedVol + "/2/recovery-point.json", Recovered: 40})
		s.Emit(Event{Kind: EventBoundary, Key: "wal/" + plantedVol + "/2/recovery-point.json", Recovered: 41})
		return nil
	})
}

// boundaryChainUnderAStaleRead writes the boundary of a second epoch and then attempts
// the boundary of a third, below it. recovery.WriteRecoveryPoint refuses that — and it
// establishes what "below" means by *reading* the previous epoch's boundary: one GET of
// a small create-only object, the same shape of read §12.4's epoch fence depends on.
//
// blind serves that GET from a replica that does not have the object yet. The floor
// collapses to zero, the regressing boundary is written, and it is create-only: from
// then on the volume's epoch chain says every sequence between 12 and 40 was never
// recovered, and no in-band action can walk it back. The checker has to see it in the
// values the objects themselves report.
func boundaryChainUnderAStaleRead(blind bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		vol := recVol()

		if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, 40); err != nil {
			return fmt.Errorf("boundary of epoch 2: %w", err)
		}
		if err := observeBoundary(s, vol, 2); err != nil {
			return err
		}

		if blind {
			s.Store.InjectStaleRead(fmt.Sprintf("wal/%s/2/recovery-point.json", format.UUIDString(vol)))
			s.Emit(Event{Kind: EventFault, Msg: "the epoch-2 boundary object reads as absent"})
		}
		// Epoch 3 recovered far less than epoch 2 recorded — a shortened listing, a
		// GC-trimmed prefix, a promoter that started from the wrong floor.
		err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 3, 2, 12)
		if !blind {
			if !errors.Is(err, recovery.ErrBoundaryRegression) {
				return fmt.Errorf("a boundary below its predecessor was not refused: %v", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("the blinded guard still refused the write: %w", err)
		}
		if err := observeBoundary(s, vol, 3); err != nil {
			return err
		}
		return errBoundaryRegressed
	}
}

// §12.5 / INV-12 behaviourally: the write-time guard is one GET away from blind, and
// the object it would have refused cannot be rewritten afterwards.
func TestPlantedBugBoundaryRegressedBehindAStaleRead(t *testing.T) {
	requirePasses(t, 24, NewBoundaryMonotonicChecker(), boundaryChainUnderAStaleRead(false))
	plantedBug(t, 24, NewBoundaryMonotonicChecker(), "boundary-monotonic", boundaryChainUnderAStaleRead(true))
}

// durablePointUnderARegressingList observes one epoch's durable point twice with
// nothing written in between. recovery.DurablePrefix derives it from a LIST, so the
// second answer is the backend's, not the harness's: if the listing serves less than
// it did a moment ago the number falls, with no error anywhere, and that number is
// what a promotion writes into a create-only boundary.
//
// regress serves the second listing from an index replica that is behind the data. It
// is the only fault in this file that needs no crash, no partition and no second
// writer: one backend answering an ordinary LIST from a stale shard is enough to move
// the number recovery would carve into stone.
func durablePointUnderARegressingList(regress bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		vol := recVol()

		l, err := recLog(s, vol, 1, 0)
		if err != nil {
			return err
		}
		for i, payload := range []string{"first", "second"} {
			if _, err := l.Write(uint64(i)*4096, []byte(payload), 0); err != nil {
				return err
			}
			if err := l.Flush(ctx); err != nil {
				return fmt.Errorf("flush %d: %w", i, err)
			}
		}
		acked := l.Watermarks().Durable
		proven, err := observeDurablePoint(s, vol, 1)
		if err != nil {
			return err
		}
		if proven != acked {
			return fmt.Errorf("S3 proves %d of the %d ACKed", proven, acked)
		}

		if regress {
			s.Store.InjectStaleListing(s.Rand, 1)
			s.Emit(Event{Kind: EventFault, Msg: "the listing serves less than it already has"})
		}
		again, err := observeDurablePoint(s, vol, 1)
		if err != nil {
			return err
		}
		if again < proven {
			return errDurablePointFellBack
		}
		return nil
	}
}

// §5.8 / INV-08: a durable point that goes backwards is what a lagging LIST produces,
// with no error at all — and it is the number a promotion writes into a create-only
// boundary.
//
// Planted by one listing served from behind the data: recovery.DurablePrefix walks the
// same objects and answers a smaller number, with no error, from a bucket that has lost
// nothing.
func TestPlantedBugDurablePointRegressed(t *testing.T) {
	requirePasses(t, 23, NewDurablePointMonotonicChecker(), durablePointUnderARegressingList(false))
	plantedBug(t, 23, NewDurablePointMonotonicChecker(), "durable-point-monotonic", durablePointUnderARegressingList(true))
}
