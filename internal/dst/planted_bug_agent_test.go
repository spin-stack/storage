package dst

import "testing"

// INV-10's Agent half (§16): once the Control Plane has refused a volume's report, this
// host answers nothing else for it.
//
// Planted by the Agent simply not acting on the refusal — no fault in the disk, the
// store or the clock. That is not a hypothetical: it is DEV-0012 exactly as it stood
// until 2026-07-29, when `Loop.report` computed the fenced list into a field and the
// data path never read it. The same category as the plaintext-WAL proof: not a broken
// algorithm, a wiring omission, which is how this class of bug actually reaches
// production.
//
// The event the checker reads is not hand-written either — `ServedAfterFence` is
// whatever the real VolumeManager answers when asked for the volume's device, so the
// proof is that production state, not the test's opinion of it.
func TestPlantedBugFencedVolumeStillServed(t *testing.T) {
	requirePasses(t, 21, NewFencedVolumeChecker(), scenarioFencedVolumeStopsServing)
	plantedBug(t, 21, NewFencedVolumeChecker(), "fenced-volume-not-served", func(s *Sim) error {
		return fencedVolumeStopsServing(s, fencingIgnored)
	})
}

// INV-08 from the guest's side (§5.8): a range ACKed as durable never comes back as
// zeros. Every other checker watches watermarks and objects; this one watches the bytes
// a guest would receive, which is the only place "recovered" and "recovered correctly"
// differ.
//
// Planted by an object store that lists nothing under the volume's prefix — a mis-typed
// bucket, a lost listing, a wrong-epoch key. Recovery cannot tell any of those from a
// volume that never wrote anything, so it rebuilds an *empty* base, installs it without
// complaint, and the read comes back as zeros. That is exactly the behaviour this
// increment removed, reached through a fault in the simulated store rather than by
// disabling the fix.
func TestPlantedBugDurableRangeReadsZeros(t *testing.T) {
	requirePasses(t, 23, NewDurableRangeChecker(), scenarioTruncatedVolumeSurvivesARestart)
	plantedBug(t, 23, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		return truncatedVolumeSurvivesARestart(s, storeHidesTheObjects)
	})
}
