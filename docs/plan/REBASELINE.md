# REBASELINE — honest state of the work (2026-07-25)

A human review found that `STATUS.md` and the `PHASE-*.md` documents claimed more
than the repository does. This file is the correction and the order of work that
follows from it. It supersedes the optimistic reading of "DONE ✓" in every earlier
document; `PLAN.md` and `STATUS.md` now use the maturity model defined below.

## What is actually true

**What exists:** a set of well-tested library models — WAL format and replay, CoW
interval map, crypto, batching/upload, lease and epoch primitives, a Control-Plane
metadata store on real Postgres, placement, materialization, drain orchestration —
plus a deterministic simulation harness with 21 mandatory scenarios and invariant
checkers, and (as of this week) a pinned QEMU build and a certified object-store
backend with a single S3 wrapper.

**What does not exist:** any binary. There is no `cmd/`, no `api/`, no Agent, no
vhost-user data path, no guest layout, no deployment. Nothing in this repository
currently serves a block device to a VM. The libraries are exercised by tests and by
the simulator, never by an integrated system.

That distinction is what the old single "DONE ✓" flag hid, and it is why the count
was wrong (nine roadmap phases had increments merged, not ten).

## Maturity model (replaces the single done flag)

| State | Meaning |
|---|---|
| **model** | The logic exists as a library with unit/property/DST coverage. No integrated caller, no real I/O path. |
| **integrated** | Wired into a running component (Agent/CP binary) through the real interfaces, exercised end to end in the integration lane. |
| **production-verified** | Exercised against real hardware/backends under fault injection, with the §26 telemetry actually recorded and the runbook timings measured. |

Every phase in `PLAN.md` and `STATUS.md` now carries one of these three, not "done".
Nothing in this repository is `production-verified` today.

## Status of the gaps (updated 2026-07-25)

Seven of the eight are closed in code, each with its failing tests committed first:
**DEV-0003** (object integrity + prefix floor), **DEV-0004** (fail-closed lease +
resumable staged promotion), **DEV-0005** (term guards + a structural test that
enumerates mutating queries), **DEV-0006** (reversible delete markers everywhere and a GC that marks with a grace period), **DEV-0008** (drain driven by the recorded plan,
capacity released exactly once), **DEV-0009** (rebuild reconstructs the snapshot
catalog and reports what S3 cannot speak for), **DEV-0010** (metrics recorded, not
just declared).

**DEV-0007 remains open**, and it is the one that cannot be closed by fixing a
function: snapshot lifecycle, objectization segments, chain links, and persisting a
materialized volume all need the spine — `api/`, `cmd/volume-agent`,
`cmd/control-plane`, Phases 02/03 — before they mean anything. That work is steps 5–6
below and is where the *model → integrated* transition actually happens.

## Correctness gaps that must be fixed before more features

These are recorded as deviations (`DEVIATIONS.md`, DEV-0003…DEV-0010) and are ordered
by how badly they undermine a claimed invariant.

1. **DEV-0003 — recovery accepts an unvalidated object as durable.**
   `recovery.listObjects` parses a WAL object header and trusts it: it never checks
   `PayloadSHA256`, `PayloadLength`, `RecordCount`, or that the object belongs to the
   volume/epoch being recovered. `DurablePrefix` then derives the durable point from
   header `LastSequence` values *without reading a single record*, so a truncated or
   corrupt object still contributes its claimed last sequence. The prefix also starts
   at the lowest object present rather than requiring sequence 1 (or the previous
   epoch's recovery point) as its floor. **This undermines INV-08 and INV-09 — the
   two invariants the whole durability argument rests on.**
2. **DEV-0004 — fencing is fail-open.** `Log.Flush` only self-fences when a lease
   checker was installed (`l.lease != nil`); a `remote`-durability log constructed
   without one ACKs FLUSH with no lease at all. INV-06 is claimed as structural but is
   opt-in. Promotion is three independently-failing steps (PG epoch bump → S3 CAS →
   lease grant) with no idempotent resume, so a crash between them can skip an epoch
   or leave ownership split.
3. **DEV-0005 — not every CP mutation is term-guarded.** `RecordOperation` and
   `UpdateOperationPhase` carry no leader-term predicate, contradicting §7 and the
   claim in `CLAUDE.md` that every CP write rejects a zombie term.
4. **DEV-0006 — the object store can permanently delete.** Both the sim and the
   filesystem store implement `Delete` as an irreversible removal, and the
   filesystem store's conditional PUT is check-then-write rather than an atomic CAS.
   The interface comment claims permanent deletion is not exposed (INV-14) — it is.
   The GC's `Collect` only *computes* candidate keys: no marking, no grace period, no
   versioned delete.
5. **DEV-0007 — "done" phases are partial models.** Snapshot sealing is synchronous
   rather than a background lifecycle, and `local` mode does not upload asynchronously.
   Clone records no parent/read-chain link. Objectization publishes no segment objects
   and no manifest→epoch→PG sequence; a checkpoint digests only a sequence number plus
   key strings. Cross-host materialization returns an in-memory `IntervalMap` that
   drain then discards — nothing is persisted on the destination host.
6. **DEV-0008 — drain is not idempotent across every crash boundary.** After a
   successful promotion, a failure while writing the recovery point or releasing the
   source's capacity leaves a volume that is no longer listed under the source, so a
   resumed drain skips it: no recovery point, leaked capacity.
7. **DEV-0009 — rebuild-metadata is not a rebuild.** It reconstructs volume rows only;
   descriptors carry no snapshot lineage, hosts, or operations. INV-20 is "active
   (basic)" at best and must not be read as the §22.5 guarantee.
8. **DEV-0010 — observability is scaffolding.** The metric catalog is registered and
   never recorded: no production code calls a counter, histogram, or gauge, and only
   an in-memory test provider exists. Phase documents that say metrics are flowing are
   wrong.

## Order of work (features are paused until 1–4 are done)

1. **Integrity + fail-closed durability** (DEV-0003, DEV-0004 first half). Validate
   every WAL object against its header before it may contribute to the durable point;
   make the durable prefix require an explicit floor; make a `remote`-mode log without
   a lease checker refuse to ACK, structurally (constructor-enforced, not a nil check
   at ACK time). New DST scenarios: truncated/corrupt object at the prefix edge,
   header lying about `LastSequence`, log built without a lease.
2. **Staged, idempotent promotion and drain** (DEV-0004 second half, DEV-0008). Make
   both explicit state machines over `lifecycle.VolumeState` / `OperationPhase`, with
   every step resumable and re-entrant at each crash boundary. DST: crash injected at
   each boundary, resume, assert exactly-once effects.
3. **Term-guard the remaining mutations** (DEV-0005) and add a structural test that
   enumerates every mutating query and fails if one lacks the predicate.
4. **Object-store semantics** (DEV-0006). Reversible delete in the interface and both
   implementations (versioned delete markers), atomic conditional PUT in the
   filesystem store, and GC that marks with a grace period. The S3 wrapper and its
   conformance suite already exist and stay the reference for what "reversible" means.
5. **The spine** (the gap behind DEV-0007): `api/` + `cmd/volume-agent` +
   `cmd/control-plane` + `cmd/volctl`, then Phases 02/03 (guest layout, vhost-user)
   against the QEMU build that now exists. Only then can any phase move from *model*
   to *integrated*.
6. **Reopen 09/10/11 as integration work** once the spine exists: snapshot lifecycle,
   objectization with real segments, GC marking, materialization that persists on the
   destination, resize end to end.
7. **Telemetry** (DEV-0010): record the §26.2 metrics from the code paths that own
   them, with an exporter, before claiming any RTO/RPO number.

## Process corrections

- Commits must keep the documented sequence visible: failing tests first, then the
  implementation, then the doc update — not one commit containing all three. Recent
  phase commits did all three at once, which is why the documentation could drift
  from the code without anything failing.
- A phase moves state only when its exit gate is demonstrated by something that runs
  in CI, not by prose in a phase document.
- `RISKS.md` statuses are updated with the phase states, not left pointing at phases
  that were marked done.
