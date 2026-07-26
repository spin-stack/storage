package dst

import "testing"

// Planted-bug proofs for the checkers scenarios_drain.go contributes. A checker that
// has never been seen to reject anything is decoration (PLAN.md §3), so each case
// below plants the exact violation its invariant forbids — as a change to how
// production code behaves, never as a fabricated event — and requires the checker to
// fail, name itself, and print the reproducing seed.

// INV-11 / ADR-0015: the fencing wait is elapsed time since the promoter observed the
// fence, not a comparison against a timestamp somebody else wrote.
//
// The control is the scenario as it ships: every lease read is served by a replica an
// hour behind, and the fence is still served in full. The plant reverts the decision —
// how long the fence has been running is read out of the same lagging rows — and the
// epoch is then granted while the source Agent's own monotonic lease is still valid,
// which is exactly the write loss INV-11 exists to prevent.
//
// Nothing here injects an event: the grant is Promote's return value and the liveness
// of the writer being fenced is a real lease.Manager counting down on the simulated
// monotonic clock, so what the checker sees is what production would have done.
func TestPlantedBugStaleLeaseReadShortensTheFence(t *testing.T) {
	requirePasses(t, 23, NewPromotionWaitChecker(), staleLeaseFence(false))
	plantedBug(t, 23, NewPromotionWaitChecker(), "promotion-fencing-wait", staleLeaseFence(true))
}
