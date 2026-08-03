package dst

import (
	"testing"
)

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
	requirePasses(t, 23, NewDurableRangeChecker(), scenarioAStoppedVolumeComesBackFromItsImage)
	plantedBug(t, 23, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		return aStoppedVolumeComesBack(s, storeHidesTheImage)
	})
}

// INV-15 (§5.10) at the Agent's seam rather than the WAL's. The existing proof
// (TestPlantedBugPlaintextLeavesHost) drives wal.Log directly, so it can only ever show
// that the *WAL* encrypts when handed a key. Until BUILD-INVENTORY increment 6 nothing
// handed it one: every volume the Agent served built its log with enc == nil, and the
// bytes in the bucket were the guest's own.
//
// Planted by leaving -kek-file off — one flag, a supported mode, and still a violation
// for any real volume. That is the honest shape of this bug: it is not an algorithm that
// breaks, it is a host configured without a key, and the only thing that can notice is
// something reading the objects.
func TestPlantedBugAgentServesAVolumeInTheClear(t *testing.T) {
	requirePasses(t, 25, NewNoPlaintextLeavesHostChecker(), scenarioAgentEncryptsWhatLeavesTheHost)
	plantedBug(t, 25, NewNoPlaintextLeavesHostChecker(), "no-plaintext-leaves-host", func(s *Sim) error {
		return agentEncryptsWhatLeavesTheHost(s, noKEKOnTheHost)
	})
}

// §20's promise — a clone "reuses the parent snapshot's already-durable objects, with no
// data copy" — was not true of anything. The clone's Agent recovered against the clone's
// own volume id, which finds nothing, so the base installed empty and the clone read
// zeros for everything its parent ever wrote (DEV-0007).
//
// Planted by dropping the chain link from the desired state, which is the defect exactly:
// the Control Plane knows what the clone descends from and the Agent is not told. One
// missing field, and the Agent cannot compensate — ADR-0021 keeps it from knowing what a
// Control Plane is.
//
// The checker is the one that already exists for this shape: it reads the bytes a guest
// would receive, which is the only place "cloned" and "cloned correctly" differ.
func TestPlantedBugACloneReadsZeros(t *testing.T) {
	requirePasses(t, 26, NewDurableRangeChecker(), scenarioACloneReadsThroughItsParent)
	plantedBug(t, 26, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		return aCloneReadsThroughItsParent(s, chainLinkDropped)
	})
}
