package dst

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// Planted-bug proofs for the checkers scenarios_recovery.go contributes. A checker
// that has never been seen to reject anything is decoration (PLAN.md §3), so each
// case below plants the exact violation its invariant forbids and requires the
// checker to fail, name itself, and print the reproducing seed.

const plantedVol = "00000000-0000-7000-8000-000000000051"

// Planted-bug outcomes for this area.
var errDurablePointFellBack = errors.New("planted: the durable point fell below a value already observed")

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

// durablePointUnderARegressingList observes one epoch's durable point twice with
// nothing written in between. recovery.DurablePrefix derives it from a LIST, so the
// second answer is the backend's, not the harness's: if the listing serves less than
// it did a moment ago the number falls, with no error anywhere, and that number is
// what a promotion writes into a create-only boundary.
//
// SetEventualList is the closest fault the store has today and it is the wrong
// direction: it withholds keys that were never listed and lets them catch up, so a
// point already observed can never fall.
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
			s.Store.SetEventualList(true)
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
// FAILS: sim.ObjectStore cannot un-list a key it has already served, so the second
// observation equals the first and there is no regression to catch.
func TestPlantedBugDurablePointRegressed(t *testing.T) {
	requirePasses(t, 23, NewDurablePointMonotonicChecker(), durablePointUnderARegressingList(false))
	plantedBug(t, 23, NewDurablePointMonotonicChecker(), "durable-point-monotonic", durablePointUnderARegressingList(true))
}
