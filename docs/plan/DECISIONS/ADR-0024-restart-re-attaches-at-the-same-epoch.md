# ADR-0024 — a restarted writer re-attaches at the same epoch

> **Amended by ADR-0026 (2026-08-03): the decision stands, its justification is new.** The
> four mechanisms this ADR rested on — `recovery.DurablePoint`, `InstallBase`,
> `VerifyAgreement` and INV-21's divergent-PUT failure — are three-quarters deleted. A
> restarted Agent does not reconstruct a durable point from a prefix of WAL objects any
> more; it reads one manifest (`image.Load`) and replays its own local WAL over it.
>
> Same-epoch re-attach is still right, and now for a simpler reason: **there is nothing a
> second incarnation can silently overwrite until it stops.** Two live Agents on one data
> directory are refused by the flock (DEV-0014's fix), and two on *different* hosts are
> caught at publish time by the manifest CAS (ADR-0023 as amended, INV-10). Nothing
> mid-session leaves the host, so the window the old argument had to close no longer
> exists. `InstallBase` is the one mechanism below that is unchanged and still
> load-bearing.

- **Status:** Accepted — 2026-08-01
- **Date:** 2026-08-01
- **Deciders:** human owner (approved the increment plan), implementer agent (proposed)
- **Relates to:** §12.3 (promotion), §12.4 (the epoch object), §12.5 (late PUTs from the
  old writer), §14.5 (deterministic keys), §22.1 (recovery point), INV-08, INV-10,
  INV-21

## Context

The Agent process dies — `kill -9`, an OOM, a host reboot — and starts again. The Control
Plane knows nothing about it: the heartbeat has not necessarily expired, and
`GetDesiredState` keeps listing the volume **at epoch N**. The new process finds a WAL
directory under `<data-dir>/wal/<vol>/<N>/` and has to decide what to do with it.

The design does not cover this. §12.3 is about promoting a volume to a **different** host
after `FENCING_WAIT`, and every mention of incrementing the epoch is inside that protocol
— step 4 of a sequence that begins with an expired heartbeat and a failover decision.
Nothing says what a writer does when it comes back as itself.

The 2026-07-26 audit split on it, which is why this is written down:

- **Two verifiers:** same-epoch is safe here. `wal.Resume` refuses a foreign volume/epoch
  and re-queues the tail; one host, one writer.
- **One verifier:** a restarted writer reusing the epoch writes records
  *indistinguishable* from the dead incarnation's, and `BumpVolumeEpoch` exists but is
  reachable from no binary — so the safe option was never even wired.

## Decision

**A restarted Agent re-attaches at the same epoch.** No bump, no Control Plane round trip,
no `FENCING_WAIT`. `Apply` resumes the existing WAL root exactly as it does today.

## Why that is safe — the four properties it rests on

This is not "one host, one writer, therefore fine". It is four specific mechanisms, and
the decision is only as good as they are:

1. **The uploads a crash can leave behind are a prefix, never a gap.** `durableStep`
   uploads `batcher.Pending()` in order under `flushMu`, and stops at the first error. A
   process killed mid-flush therefore leaves S3 holding a contiguous run — possibly
   *longer* than anything ACKed to the guest (§14.4 uploads at step 4 and only checks the
   lease at step 5), never a run with a hole in it.

2. **The restarted writer numbers above everything in the bucket — and the local WAL, not
   S3, is what guarantees it.** This is the property the DST scenario was written to pin,
   and writing it corrected the first draft of this ADR, which credited the wrong
   mechanism.

   The draft said S3 decides: `recovery.DurablePoint` finds the longest contiguous run of
   validated records, `fetchBase` hands it to `InstallBase`, and the writer resumes there.
   That is true and it is not sufficient — a listing that comes back short (a truncated
   page, an eventually-consistent index) would then resume *below* objects that exist.
   Running the scenario against exactly that fault did not break it, which is what
   exposed the real mechanism:

   `Resume` sets `local` from the last record **on disk**, `InstallBase` only ever
   *raises* watermarks, and INV-13 forbids truncating above `published` — which never
   exceeds what the bucket can prove. So every sequence the dead incarnation managed to
   upload is still in a local segment on this host, and the new incarnation numbers above
   it no matter what the listing says. S3 raises the floor; the local WAL is what stops
   it from ever being lowered.

   The two together are what make it safe, and either alone is not. The scenario
   therefore runs both listings, and a plausible regression — `InstallBase` *setting*
   rather than raising — fails the short-listing arm while the honest arm still passes.

3. **Any residual overlap is checked, not assumed.** `recovery.VerifyAgreement` compares
   overlapping runs record by record and `ContiguousEnd` walks sequences rather than
   object boundaries — precisely so a restarted writer's re-batch reads as redundancy.
   Two objects carrying one sequence with *different* records is `ErrAmbiguousSequence`, a
   hard failure, not a silent pick.

4. **A divergent write cannot even be stored.** INV-21/§14.5: the object key is
   deterministic over `(vol, epoch, first_seq, last_seq, content_hash)` and the PUT is
   `If-None-Match: *`. Same range with a different hash hard-fails at upload
   (`ErrDivergentObject`), which is the case the dissenting verifier was worried about,
   caught one layer below where they were looking for it.

## Why the alternative is worse, not merely unnecessary

Bumping the epoch on every restart means a restart cannot complete without the Control
Plane: a CAS on the epoch object, a `BumpVolumeEpoch` under a valid CP term, and a fresh
lease grant. §23's "PostgreSQL caído" wants Agents to survive a Control Plane outage;
this would make the one moment an Agent is most fragile — coming back up — depend on the
one component the design assumes can be down. It also drags in `FENCING_WAIT`, which exists to bound a
writer that may still be alive on *another* host, and buys nothing when the previous
incarnation is a dead PID on this one.

It also costs a new epoch directory in S3 and locally per restart, for a writer that has
not changed identity.

## What this decision does *not* cover, and must not be read as covering

**Two live incarnations on one host.** Everything above assumes the previous process is
gone. If it is not — an operator starts a second `volume-agent` against the same
`--data-dir`, or a supervisor restarts one that never actually died — same-epoch
re-attach puts two processes on **one segment directory**, both appending through
`disk.Open` (read + append), both numbering from the same resumed point. That is local
corruption, and none of the four properties above touch it: they are all about what is in
S3.

Two things make it worse than it looks:

- **Nothing detects it.** `hostio.Listen` unlinks a stale socket before binding, which is
  right for the crash case and means the second incarnation **silently steals the socket**
  from the first rather than failing with `EADDRINUSE`.
- **Bumping the epoch would not have fixed it either.** It would separate the two WALs
  into different directories and let §12.4's holder check fence the older one's
  publications — but only if the second incarnation went through the Control Plane, which
  is exactly what a stale supervisor restart does not do.

This is a **mutual-exclusion problem on the data directory**, not an epoch policy, and it
wants an exclusive lock taken at start-up — the one thing that fails closed regardless of
how the second process got there. It is not a consequence of this decision, it predates
it, and choosing the other option would not have removed it.

**Closed 2026-08-02 (DEV-0014).** The Agent takes an exclusive `flock` on `agent.lock`
under its data directory and names the directory in the refusal, because the operator's
next action is to find the process holding it. `git log` records the
decisions; `TestASecondAgentRefusesTheSameDataDir` in `integration/e2e` proves it against
two real processes.

## Consequences

- `Apply` keeps resuming at the listed epoch; no code changes with this ADR.
- `BumpVolumeEpoch` stays reachable only through `controlplane.Promoter`, which is where
  §12.3 puts it. That it is wired into no binary is increment 8's problem, not this one's.
- The four properties above become things a change must not break. In particular, making
  `durableStep` upload out of order, relaxing `VerifyAgreement`, or letting `InstallBase`
  set watermarks rather than raise them would silently invalidate this decision.
  `scenarioCrashedFlushDoesNotCollideOnRestart` pins the last of those; the others are
  pinned by `WatermarkOrderChecker` and `TruncateBelowPublishedChecker`.
- If the fleet ever runs two Agents per host (per-device sharding, a blue/green upgrade),
  this ADR must be revisited *before* that lands, not after.
