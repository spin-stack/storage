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

// §12.6's second obligation: a SELF_FENCED Agent "deja de publicar checkpoints/manifests".
// The first — no durable ACK — has had a checker since the beginning; this one never did,
// even though the gate is one `if` at the top of checkpointOnce.
//
// Planted by the wiring, not by a fault: a Lease function that answers from a snapshot
// taken at start-up instead of resolving the lease per call. That is the shortcut every
// other Agent scenario here takes — harmlessly, because their leases never lapse — and it
// is the same category as DEV-0012 and the plaintext-WAL proof: not a broken algorithm, a
// question that stopped being asked.
//
// Everything else stays honest. The epoch object names this host, so §12.4's ownership
// check inside checkpoint.Create passes and the publish genuinely succeeds under the bug;
// the verdict is read from the object store's contents rather than from the error the
// call returned, because a gate that returns the right error and publishes anyway would
// satisfy any assertion on err.
func TestPlantedBugCheckpointPublishedWithoutLease(t *testing.T) {
	requirePasses(t, 24, NewCheckpointLeaseChecker(), scenarioLapsedLeaseStopsPublishing)
	plantedBug(t, 24, NewCheckpointLeaseChecker(), "checkpoint-requires-lease", func(s *Sim) error {
		return lapsedLeaseStopsPublishing(s, leaseAnsweredFromASnapshot)
	})

	// One cached answer, both of §12.6's obligations gone: the same wiring that let the
	// checkpoint out also let a FLUSH be ACKed as durable after the lease had expired.
	// Asserted rather than left as a remark, because it is the reason the new checker is
	// a second gate and not a duplicate of the old one — INV-06 sees the ACK, this sees
	// the publish, and a host can lose the right to do the second while doing neither.
	plantedBug(t, 24, NewDurableAckLeaseChecker(), "durable-ack-requires-lease", func(s *Sim) error {
		return lapsedLeaseStopsPublishing(s, leaseAnsweredFromASnapshot)
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
