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
