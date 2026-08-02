# ADR-0016 — Revoke inside a bounded window now; fence per volume once the Agent exists

- **Status:** Accepted 2026-07-26 (two-stage: the window now, per-volume fencing with
  the Agent)
- **Date:** 2026-07-26
- **Deciders:** human (decided), implementer agent (proposed the options)
- **Implements/Extends:** §12.2 (lease gates the durable ACK), §12.6 (per-host leases),
  §28.1 (drain), INV-06, INV-09.
- **Closes:** TEST-GAPS "a heartbeat will re-arm the lease a drain revokes".

## Context

The drain revokes the source's lease so a healthy host can be evacuated (wave 3). Once
an Agent heartbeat exists, the same host renews it again seconds later —
`RenewHostLease` refuses only a DEAD host — and the drain waits forever.

The obvious fix, refusing renewals for a DRAINING host, is the one wave 2 deliberately
rejected: **the lease is per host and the evacuation is per volume**. Stopping renewals
stops the ACKs of every volume the host still holds, including the ones nobody is
moving. That turns an orderly drain into an availability event for volumes that were
fine.

The mismatch is the actual defect. Everything else is a way of living with it.

## Decision

### Stage 1 — a bounded revocation window (now)

The drain revokes and refuses renewals **only for the duration of one volume's
promotion**, not for the whole drain. The host's other volumes lose the ability to ACK a
FLUSH for at most one `lease_ttl + max_clock_skew` per volume moved, and the window is
opened and closed by the same pass that owns the move.

This is explicitly a compromise, and it goes in the runbook in those words: **draining a
healthy host briefly interrupts durable ACKs for its other volumes.** A guest sees a
FLUSH take longer, not a write fail — the WAL keeps accepting writes; it is the durable
ACK that waits.

### Stage 2 — the fence follows the volume

> **Its stated blocker is gone, and it has not been scheduled.** This was written as
> waiting on the Agent (DEV-0007), and DEV-0007's chain closed on 2026-08-02: the Agent
> exists, serves volumes, and holds an epoch per volume. `internal/controlplane/drain.go`
> carries the marker for where stage 2 lands.
>
> Nothing here is decided by that. Stage 2 changes **what `wal.Log` consults before ACKing
> a FLUSH** (see the consequences below), which is the durability rule itself, so the
> choice — schedule stage 2, or record stage 1 as the final answer and say why — is a
> human's. Recorded under "Decisions waiting on a human" in `STATUS.md` rather than
> settled here, because an ADR that quietly answers its own open question is how a
> fencing decision changes without a review.

The ACK gate becomes epoch holdership for **that volume**, not a lease for the host: a
writer may ACK a FLUSH only while it can show it holds the volume's current epoch. The
lease stops being the fence and becomes what it is elsewhere in the design — a liveness
signal.

A GET per FLUSH is not viable, so the Agent caches the holdership with its own TTL,
counted on the monotonic clock exactly as the lease is today (§12.2). The caching TTL,
not the lease, then bounds how long a fenced writer can still ACK — and it is per
volume, so moving one volume no longer touches another.

## Alternatives considered

- **Refuse renewals for any DRAINING host.** One line, and it makes every drain an
  availability event for the volumes still on the host. Rejected as the permanent
  answer; stage 1 is the same mechanism with the blast radius cut to one promotion.
- **A `FENCED` host state.** Names the situation but does not change the granularity: a
  fenced host still cannot ACK for volumes nobody is moving.
- **Go straight to stage 2.** It needs the Agent, and the drain is broken for healthy
  hosts today. Stage 1 is deliberately throwaway.

## The tests that would enforce it

- A drain test where the source renews its lease during the pass: the window must
  reopen the refusal, and the promotion must not proceed on a re-armed lease.
- A test that the window **closes** on every exit path, including a failed or cancelled
  pass — a drain that dies with the window open leaves a host that cannot renew, which
  is worse than the bug being fixed.
- A DST scenario measuring how long the source cannot ACK for an unrelated volume,
  asserting it is bounded by one promotion and not by the length of the drain.
- For stage 2: a scenario where the source keeps a valid *lease* but has lost the
  *epoch*, asserting its FLUSH is refused — which is exactly the case stage 1 cannot
  express.

## Consequences

- Stage 1 adds a window that must be closed on every path; that is the only new failure
  mode it introduces, and it is what the tests above exist for.
- Stage 2 changes what `wal.Log` consults before ACKing a FLUSH: today a
  `LeaseChecker`, then a holdership checker with the same shape. The interface is
  already the right one (defined where it is consumed), so the change is the
  implementation behind it plus the Agent's cache.
- The §6.1/§12.6 story stays coherent: leases remain per host for liveness, and fencing
  stops borrowing them for a job whose unit is the volume.
