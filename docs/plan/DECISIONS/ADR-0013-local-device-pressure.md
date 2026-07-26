# ADR-0013 — Local device pressure: a device budget, a reserve, and who is allowed to react

- **Status:** Proposed (needs human review — durability/fencing zone, and it changes
  the on-disk WAL layout)
- **Date:** 2026-07-26
- **Deciders:** human (to decide), implementer agent (proposes)
- **Implements/Extends:** §5.7 (backpressure rather than silently filling NVMe), §14.8
  (ACK contracts), §21.1 (checkpoint then truncate), §28.1–28.2 (cordon/drain,
  capacity accounting), INV-04, INV-13.
- **Related:** DEV-0007 (no Agent exists yet, so nothing reports device usage),
  ADR-0009 (typed lifecycles), ADR-0012 (the GC is about the bucket).

## Context

"What happens when a node runs out of disk" has an answer that is easy to get wrong,
because the obvious tool — the GC — is the wrong one. The GC sweeps the **bucket**. It
frees no local byte, ever.

What frees local NVMe is one chain:

```
upload → verified object → checkpoint.Create publishes → AdvancePublished → TruncateLocal
```

INV-13 ties it together: local WAL is never truncated above the verified published
point, because below it the records exist in a checkpoint and above it they exist on
this host alone. So **local disk pressure is not a disk problem, it is an object-store
availability problem**. With S3 unreachable nothing is verified, `published` is frozen,
and by design there is nothing to reclaim — the correct behaviour, and also the one
that fills the device.

What exists today:

- **Admission.** `placement.Policy.Admits` plus the §28.2 oversubscription bound, which
  since wave 3 is a predicate of the write that adds the bytes rather than a rule the
  reader evaluates. Only an ACTIVE host takes new volumes.
- **Per-volume backpressure.** `Limits.MaxRemoteGapBytes` bounds the bytes no verified
  object covers; crossing it fails the WRITE with `ErrBackpressure`, which is an error a
  guest understands, rather than an ENOSPC nobody modelled.
- **A device state.** `Log.Degraded() == OUT_OF_SPACE`, sticky, with the
  `wal_out_of_space` gauge, orthogonal to `Fenced()`.
- **Fleet-level levers.** Cordon (stop taking new volumes) and drain (evacuate),
  both term-guarded.

### What is actually missing

1. **`TruncateLocal` almost never reclaims anything.** It records `truncatedUpTo` and
   calls `file.Truncate(0)` only when `upTo >= local` — i.e. only when the checkpoint
   reached the end of the log. On a volume under continuous write that never happens, so
   the WAL is a file that grows and is emptied only when the volume goes idle. Every
   other defence here is downstream of this one.
2. **Backpressure is per volume, not per device.** N volumes × `MaxRemoteGapBytes` can
   exceed the device comfortably, and nothing sums them.
3. **Admission counts *committed* bytes, not *used* ones.** `nvme_used_bytes` exists in
   the schema and in the heartbeat shape, and nothing writes it (DEV-0007). The
   oversubscription bound therefore protects against over-provisioning, not against
   filling: the thing that grows is the WAL backlog, which no reservation covers.
4. **No reserve.** Nothing keeps headroom so that the writes which get a host *out* of
   this state — a checkpoint, a recovery point — can still happen once the device is
   full. ENOSPC arrives for the recovery path at the same instant as for the data path.
5. **No feedback loop.** Nothing cordons on device pressure, nothing triggers a drain,
   and nothing decides which volume to evacuate first.
6. **ENOSPC at fsync is not modelled.** A filesystem with delayed allocation reports it
   at `fdatasync`, not at `write`; the simulated disk charges allocation at append time.
   Recorded in `wal/degraded.go` as a known limitation.

## Decision (proposed)

### 1. The device budget replaces the per-volume limit

The Agent owns a **device budget** and hands each `Log` a share; `MaxRemoteGapBytes`
is derived from that share rather than configured per volume. Admitting a volume to a
host requires budget, not just capacity: today a host can be inside its §28.2 bound and
still have no room for the backlog of the volumes it already holds.

### 2. A reserve that the data path may never touch

A fixed fraction of the device (proposal: 5%, floor 1 GiB) is never allocatable to
volume backlog. Its only users are the writes that reclaim space: the checkpoint that
advances `published`, the recovery point a promotion needs, the summary object. A
device that is "full" for guest writes must still be able to run the sequence that
un-fills it.

### 3. Thresholds with distinct actions, not one cliff

| Device usage | Action | Who |
|---|---|---|
| ≥ 70% | report; Control Plane **cordons** the host (term-guarded, exists) | CP, on the Agent's report |
| ≥ 85% | aggressive backpressure; refuse new attaches on this host | Agent, locally |
| ≥ 95% | only the reclaim path may write (the reserve) | Agent, locally |
| ENOSPC | `Degraded() = OUT_OF_SPACE`, gauge, no self-fence | Agent, locally |

### 4. Segment the WAL so truncation reclaims

The WAL becomes a sequence of segment files; `TruncateLocal` unlinks the segments
entirely below the published point instead of truncating one growing file. This is the
change that makes reclamation work on an active volume, and it is an **on-disk format
change**: it needs its own increment, a human review of the format, and a DST scenario
covering a crash between the unlink and the metadata update.

### 5. Who is allowed to react

The Agent has **local, defensive authority only**: backpressure, refusing attaches,
marking itself degraded. Moving volumes stays exclusively the Control Plane's, driven
by the Agent's reports. The Agent sees the device in real time but not the fleet; the CP
sees the fleet but arrives late. If both may evacuate, two actors drain one host — the
class of bug wave 2 and wave 3 spent most of their effort closing.

Automatic drain on pressure picks by **remote gap**, not by size: the volume whose
backlog is not closing is the one that will keep growing, and the largest volume may be
perfectly healthy.

## Alternatives considered

- **Let it fill and rely on ENOSPC.** What we have now. The first ENOSPC arrives as a
  partial append (handled, INV-05), and after that every WRITE fails with an I/O error
  the guest cannot act on; the reclaim path fails at the same moment. Rejected.
- **A global quota per volume, enforced at attach.** Simple and static, and wrong in the
  case that matters: the backlog is driven by S3 availability, not by the volume's size.
- **Reclaim by deleting the oldest WAL regardless of `published`.** Directly violates
  INV-13; the whole point is that those records exist nowhere else.
- **Evict (detach) the largest volume under pressure.** Attractive and dangerous: it
  turns a local, recoverable condition into an availability event for a guest that may
  be doing nothing wrong.

## The tests that would enforce it

- A DST scenario `device-budget-holds-across-volumes`: N volumes on one simulated device
  with S3 down, asserting the sum of their backlogs never exceeds the budget and that
  every WRITE past it gets `ErrBackpressure` rather than `sim.ErrNoSpace`.
- A DST scenario `reserve-lets-the-host-recover`: fill to the reserve, restore S3, and
  assert the checkpoint + truncate sequence completes and the device drops below the
  threshold — the property the reserve exists for.
- A unit test that `TruncateLocal` below `local` actually reclaims bytes (fails today).
- A metadata contract case that a host reporting usage past the cordon threshold is
  refused new placements, and a drain test that picks by remote gap.

## Consequences

- `wal.Limits` stops being per-volume configuration and becomes a share of a device
  budget the Agent computes. Existing callers keep working (the field stays), but the
  Agent becomes the only correct place to set it.
- The WAL's on-disk layout changes (segments). Recovery, `Resume` and the replay
  property tests all touch it; it is the largest single piece of work proposed here and
  should be its own increment.
- Cordon becomes something the fleet does automatically, so the reason a host is
  cordoned has to be visible — otherwise an operator sees a cordoned host and no cause.
- None of this is reachable until the Agent exists (DEV-0007). What *is* reachable
  today, and should not wait: the `TruncateLocal` reclamation bug and the device-level
  sum of backlogs.
