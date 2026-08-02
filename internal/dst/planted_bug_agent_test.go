package dst

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
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

// INV-09 where a guest can see it (§12.3): every write the fenced writer ACKed as durable
// must be readable on the host that replaced it.
//
// The invariant was never in doubt in the object store — recovery.DurablePrefix finds the
// data and the drain proves it. What nothing checked is whether the **Agent on the
// destination ever asks**. It did not: a promoted volume has no local segments and no
// parent snapshot, so the base fetch was skipped entirely and the destination served
// zeros for its predecessor's whole volume, with no error anywhere. INV-09 held in S3 and
// the guest still got nothing.
//
// Planted with a store that lists nothing for the volume — the destination has *only* the
// object store, so a listing that comes back empty is the whole of its world, and it is
// the same fault the truncated-restart arm uses. The read must then be refused, not
// answered with zeros: a guest cannot tell those from a range nobody wrote.
func TestPlantedBugAPromotedHostReadsZeros(t *testing.T) {
	requirePasses(t, 27, NewDurableRangeChecker(), scenarioAPromotedHostReadsThePreviousEpoch)
	plantedBug(t, 27, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		return aPromotedHostReadsThePreviousEpoch(s, destinationCannotList)
	})
}

// §5.8/INV-08 from the guest's side, in the shape that is not zeros: a range the volume
// ACKed as durable comes back as bytes the guest never wrote.
//
// This one is planted differently from every other bug in this file, and the difference
// is the finding. The others inject a fault — an unversioned bucket, a store that lists
// nothing, a lease read from a stale snapshot — because the code under them will do the
// wrong thing when the world misbehaves. Here the production fix
// (recovery.ErrSealedWithoutKey) makes the violation unreachable *through the world*:
// there is no configuration, no fault and no missing flag that gets ciphertext into a
// read view any more, because a sealed record with no key is refused rather than folded
// in. Restarting the Agent with no KEK at all — the one lever that used to produce it —
// now costs the volume its reads, which is asserted by the scenario itself.
//
// So the bug is planted by replaying the volume's own sealed objects exactly the way
// internal/agent did before this increment: view.Overwrite with the undecrypted payload.
// Not a hypothetical either — it is a transcription of the shipped call, which passed a
// literal nil Encryption to recovery.RecoverOver for a volume whose DEK it had unwrapped
// four lines earlier. What it proves is the thing that has to be true: had the checker
// existed, it would have caught it. Its silence is the whole hazard — GCM leaves the
// length intact, the plaintext CRC is never re-checked on this path, and every watermark
// and every zero-check agreed the volume was fine.
func TestPlantedBugRestartServesCiphertext(t *testing.T) {
	requirePasses(t, 29, NewDurableRangeChecker(), scenarioEncryptedVolumeSurvivesARestart)

	// The same seed and the same volume, read back the pre-fix way.
	ctx := t.Context()
	plantedBug(t, 29, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		if err := scenarioEncryptedVolumeSurvivesARestart(s); err != nil {
			return err
		}
		objs, err := s.Store.List(ctx, "wal/")
		if err != nil || len(objs) == 0 {
			return err
		}
		view := cow.NewIntervalMap()
		var volumeID string
		for _, o := range objs {
			body, err := s.Store.Get(ctx, o.Key)
			if err != nil {
				return err
			}
			recs, err := wal.Replay(body[format.ObjectHeaderSize:])
			if err != nil {
				return err
			}
			for _, rec := range recs {
				if rec.Type != format.RecordWrite {
					continue
				}
				volumeID = format.UUIDString(rec.VolumeID)
				// The shipped line, verbatim: no key, so the ciphertext is the payload.
				view.Overwrite(rec.Offset, rec.Payload)
			}
		}
		pattern := bytes.Repeat([]byte{0xE7}, 4096)
		got := make([]byte, len(pattern))
		view.Read(0, got)
		zeros := bytes.Equal(got, make([]byte, len(got)))
		s.Emit(Event{Kind: EventDurableRead, Key: volumeID,
			ZerosAfterRestart: zeros, ForeignBytesAfterRestart: !zeros && !bytes.Equal(got, pattern)})
		return nil
	})
}

// One missing flag must cost the volume its reads, not cost the guest its data.
//
// An Agent restarted without -kek-file is a supported mode (dev, and the QEMU lane run
// in it) reached by leaving one flag off a command line, so it is not a fault to be
// injected — it is a Tuesday. Before this increment it was the cheapest route to the
// silent defect: no KMS means no Encryption, and the volume's own sealed objects were
// replayed straight into the read view. Now the volume fails closed, and this is what
// asserts that the failure is a refusal rather than an answer.
func TestRestartWithoutTheKEKRefusesRatherThanAnswering(t *testing.T) {
	res := Run(29, func(s *Sim) error {
		return encryptedVolumeSurvivesARestart(s, restartWithoutTheKEK)
	}, NewDurableRangeChecker())
	if res.Err != nil {
		t.Fatalf("restarting without the KEK must refuse the read, not violate: %v\n--- trace ---\n%s",
			res.Err, res.TraceString())
	}
}

// INV-06 (§12.2) reaching the second path that can advance durable_sequence.
//
// §14.4 step 6 was the only one until §14.8's asynchronous drain landed, and the drain is
// where the rule is easiest to lose: the mode's whole point is that the FLUSH ACK does
// *not* wait for the lease, and reading that as "durability does not need the lease in
// local mode" is a coherent sentence rather than a slip. It is the reading this increment
// rejected, and this is what stops it coming back.
//
// Planted by the wiring, not by a fault, and by the same shortcut CheckpointLeaseChecker
// uses: a Lease function that answers from a snapshot taken at construction instead of
// resolving the current one per call. Every other Agent scenario takes that shortcut
// harmlessly because their leases never lapse; here it makes a fenced host claim
// durability for records it uploaded after losing the volume.
func TestPlantedBugLocalDrainClaimsWithoutALease(t *testing.T) {
	requirePasses(t, 31, NewDurableAckLeaseChecker(), scenarioLocalVolumeDrainsWithoutClaimingWithoutALease)
	plantedBug(t, 31, NewDurableAckLeaseChecker(), "durable-ack-requires-lease", func(s *Sim) error {
		return localVolumeDrains(s, leaseReadOnceAtStart)
	})
}
