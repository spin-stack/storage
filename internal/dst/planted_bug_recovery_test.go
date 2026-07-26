package dst

import "testing"

// Planted-bug proofs for the checkers scenarios_recovery.go contributes. A checker
// that has never been seen to reject anything is decoration (PLAN.md §3), so each
// case below plants the exact violation its invariant forbids and requires the
// checker to fail, name itself, and print the reproducing seed.

const plantedVol = "00000000-0000-7000-8000-000000000051"

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

// §5.8 / INV-08: a durable point that goes backwards is what a lagging LIST produces,
// with no error at all — and it is the number a promotion writes into a create-only
// boundary.
func TestPlantedBugDurablePointRegressed(t *testing.T) {
	plantedBug(t, 23, NewDurablePointMonotonicChecker(), "durable-point-monotonic", func(s *Sim) error {
		s.Emit(Event{Kind: EventDurablePoint, Key: "wal/" + plantedVol + "/1/", Recovered: 30})
		s.Emit(Event{Kind: EventDurablePoint, Key: "wal/" + plantedVol + "/1/", Recovered: 29})
		return nil
	})
}
