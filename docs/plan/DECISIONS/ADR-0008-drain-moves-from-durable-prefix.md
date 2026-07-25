# ADR-0008 — Drain moves a volume from the durable prefix, not from a snapshot

- **Status:** Accepted (Phase 11 / Increment 11.3) — **pending human review of the
  fencing diff**
- **Date:** 2026-07-25
- **Deciders:** implementer agent, with the human review of the Phase 11 spec
- **Implements/Extends:** §28.1 (cordon/drain), §20 (cross-host), §22.1/§22.3
  (recovery authority, cold materialization), §12.3–12.5 (fencing). Recorded in
  DEVIATIONS.md as DEV-0002.

## Context

§28.1 describes drain as "mueve volúmenes fuera (**snapshot + restore cross-host** en el
MVP)". A snapshot is created by the *source Agent* — it captures a sequence in the live
`wal.Log` (§19) — so a snapshot-based drain requires the host being evacuated to be alive
and cooperating, and requires the CP↔Agent RPC surface, which does not exist yet
(Phases 02/03 are the Agent-side work and are still planning-only).

Meanwhile everything a destination actually needs is already in the object store: §5.8
makes S3 the recovery authority and §22.1 defines the durable point as the longest
contiguous WAL prefix of the volume's epoch.

## Decision

`controlplane.Drainer` moves a volume from **the durable prefix of its current epoch**
(`materialize.FromEpoch`), not from a freshly taken snapshot:

1. **Bulk pass** — the destination materializes the epoch's durable prefix from S3 while
   the source still serves. This is where the cold RTO is spent (§22.3) and it overlaps
   the fencing wait.
2. **Fence** — `Promoter.Promote` waits out `last_renewal + lease_ttl + max_clock_skew`,
   bumps the epoch in PG (term-guarded), CASes the epoch object, and grants the lease to
   the destination (§12.3–12.4).
3. **Final pass** — with the old writer fenced, the old epoch's prefix can no longer
   grow, so a second `FromEpoch` yields the authoritative state and its end is the
   epoch boundary written to `recovery-point.json` (§12.5).

## Consequences

- **Strictly more available than the doc's phrasing:** the drain works when the source
  host is dead, partitioned, or simply not cooperating — the case that matters most for
  maintenance and decommissioning. A snapshot-based drain cannot run at all there.
- **Same durability guarantee:** the moved state is exactly what S3 says is durable, and
  the fence precedes any write by the destination, so INV-09 (no lost ACKed write) and
  INV-10 (effective single writer) hold — proven by the mandatory DST scenario
  `drain-moves-volumes-fenced`.
- **A planned drain still needs the source to stop renewing its lease**, since promotion
  refuses until FENCING_WAIT elapses past the last renewal. The CP marks the host
  `DRAINING` first; the Agent-side release is Phase 02/03 work. Until then the drain
  simply reports `ErrFencingWaitNotElapsed` and the reconciler retries — it never
  bypasses the wait.
- **The snapshot path is not lost:** `controlplane.CloneCrossHost` materializes from a
  published snapshot manifest (§20), so the doc's "snapshot + restore" flow exists for
  clones; drain just does not depend on it.
- If the Agent RPC lands and a quiesced snapshot is preferred for planned drains, it can
  be added as a first step of `move` without changing the fencing order.
