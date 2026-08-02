# ADR-0023 — the object store is a fencing witness the data path may act on

- **Status:** Accepted — 2026-08-01
- **Date:** 2026-08-01
- **Deciders:** human owner (approved the increment plan), implementer agent (proposed)
- **Relates to:** §12.3–12.6 (fencing), §21.1 (objectization order), INV-10 (effective
  single writer), ADR-0016 (fencing granularity), DEV-0012

## Context

The design says how a writer learns it has been fenced, twice:

- **From its own lease** (§12.2): the lease lapses on the monotonic clock, the Agent
  enters `SELF_FENCED`, stops ACKing durability and stops publishing checkpoints and
  manifests.
- **From the Control Plane** (§12.3–12.4): a report is refused with `STALE_EPOCH`,
  `NOT_PRIMARY` or `UNKNOWN_VOLUME`, and the data path tears the runtime down
  (`VolumeManager.Fence`, which resolved DEV-0012's Agent half).

The durability scheduler (BUILD-INVENTORY increment 3) introduces a third way, and the
design does not describe it. `checkpoint.Create` can fail with:

- **`ErrDurablePointMismatch`** — the object store proves a durable prefix *longer* than
  anything this log ever ACKed. No sequence of events involving only this writer produces
  that; somebody else uploaded into this epoch.
- **`ErrCheckpointConflict`** — a checkpoint already exists at this sequence and is
  **different** from the one this host computed. A checkpoint at a sequence is immutable,
  so this is not our own retry: it is two writers claiming one epoch.

Both are the object store reporting a second writer, and it is reporting it to a host
whose lease may still be perfectly valid and whose last heartbeat may have been accepted.
Fencing has not reached this host by either sanctioned route, and may not for a whole
heartbeat interval.

Doing nothing is not available. The scheduler would retry on its next tick, against a
volume it has already lost, for as long as the process lives.

## Decision

**A writer that learns from the object store that another writer is in its epoch treats
that as fencing, immediately, and by the same path the Control Plane's refusal takes.**

Concretely, on `ErrDurablePointMismatch` or `ErrCheckpointConflict`:

1. The volume's runtime is torn down — log, socket and device — exactly as
   `VolumeManager.Fence` does. Reads stop with writes, for the reason they stop there: a
   read served from a WAL another host has moved past is a stale answer a guest cannot
   detect.
2. **The fenced epoch is recorded**, so the next `Apply` does not restart the volume at
   the epoch it just lost. Only a higher epoch — the Control Plane granting it again —
   brings it back.

Point 2 is the part that needed an ADR rather than a comment: it means **the data path can
fence a volume the Control Plane still lists as this host's**. That is an authority the
design grants nowhere.

## Why

The object store is a *better* witness of a second writer than either sanctioned route,
for this specific fact.

A lease says "I have not been told I lost the volume", which is a statement about this
host's connectivity, not about who is writing. A heartbeat that has not yet failed says
the same. The object store, by contrast, is holding **an object that this host did not
write, in this host's epoch**. It is not an inference; it is the evidence itself, and
§5.8 already makes S3 the recovery authority precisely because it is the one participant
that cannot be partitioned away from the truth.

Refusing to act on it would mean a writer that *knows* it has been replaced continuing to
serve reads and take writes until a heartbeat tells it what it already knows. INV-10 is
about the effect, not the channel: at most one writer per volume. Nothing in it says the
news must arrive by a particular door.

The asymmetry with the Control Plane is real and is accepted deliberately: this is
**fail-closed in one direction only**. A host may stop serving a volume the Control Plane
still assigns to it; a host may never *start* serving one the Control Plane has not
assigned. The first costs availability for one volume on one host and the Control Plane
corrects it on the next cycle by granting a new epoch. The second would be two writers,
which is the thing the whole design exists to prevent.

## Consequences

- **A false positive costs one volume's availability on one host.** The recovery is the
  ordinary one — the Control Plane grants a higher epoch, `Apply` starts a fresh runtime
  under the new WAL root — and it requires no operator action.
- **The scheduler cannot loop on a lost volume.** It stops at the first proof.
- **Two fencing paths now write `fencedEpoch`**, and they must stay one implementation:
  the report-refusal path and this one both go through `VolumeManager.Fence`. A second
  copy of the teardown is how the two would drift.
- **It is observable.** The teardown logs which witness fenced it — lease, Control Plane,
  or object store — because "the volume stopped serving" with no attribution is the kind
  of event that costs an hour at 3 a.m.
- **It needs its own DST arm**, planted the same way the other fencing arms are: a store
  that proves a longer prefix than the log ACKed, and the volume must stop serving.

## Alternatives rejected

- **Retry and wait for the Control Plane.** The writer keeps serving a volume it knows it
  has lost, for up to a heartbeat interval, and the scheduler burns object-store requests
  on a volume that will never accept them. The only argument for it is that the Control
  Plane is the sole fencing authority — which is a statement about *granting*, not about
  a writer declining to continue.
- **Tear down but do not record the epoch.** `Apply` restarts the volume on the very next
  cycle, at the same epoch, and the scheduler discovers the same conflict again. That is
  the loop with extra steps, plus a socket that flaps.
- **Self-fence the `wal.Log` only** (stop ACKing durability, keep serving reads). That is
  §12.2's answer for a *lapsed lease*, where nobody is known to have taken the volume.
  Here somebody demonstrably has, and their writes are already in the store — which is
  exactly the case where a stale read is wrong rather than merely old.
