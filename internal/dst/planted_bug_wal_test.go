package dst

import "testing"

// Planted-bug proof for the local-WAL segmentation scenario. A scenario that has never
// been seen to reject anything is decoration (PLAN.md §3), so this plants the exact
// violation the seal/publish/unlink ordering exists to prevent — as a change to how
// production code behaves, never as a fabricated event.
//
// INV-13: local WAL is never reclaimed above the verified published point. The control
// is the scenario as it ships: reclamation stops at the published point, so the segment
// holding it survives every crash boundary and the records above it are still there
// afterwards. The plant substitutes the one ordering rule that says so (wal.OrderPolicy,
// AllowTruncate) and asks production's own TruncateLocal to reclaim the whole log; the
// segments it then unlinks hold records no verified checkpoint covers, and the crash
// that follows leaves a WAL whose oldest record is above what was ever published.
//
// The checker sees the truncation event the scenario emits from the *post-crash*
// directory — the first sequence that actually survived, not the number the log
// intended — so what it rejects is the state on disk, not a claim about it.
func TestPlantedBugReclaimPastThePublishedPoint(t *testing.T) {
	requirePasses(t, 31, NewTruncateBelowPublishedChecker(), walSegmentCrashBoundaries(false))
	plantedBug(t, 31, NewTruncateBelowPublishedChecker(), "no-truncate-above-published",
		walSegmentCrashBoundaries(true))
}
