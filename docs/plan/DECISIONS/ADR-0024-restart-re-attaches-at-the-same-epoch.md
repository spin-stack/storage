# ADR-0024 — a restarted writer re-attaches at the same epoch

- **Status:** Accepted — 2026-08-01. The decision stands; its justification was replaced
  when ADR-0026 (2026-08-03) deleted the mechanisms the original rested on.

## Decision

An Agent process dies — `kill -9`, an OOM, a host reboot — and starts again. It
**re-attaches at the same epoch**: no bump, no Control Plane round trip, no
`FENCING_WAIT`; it resumes its own WAL, already under its data directory.

The Control Plane is not told, and does not need to be: the heartbeat has not necessarily
expired and `GetDesiredState` keeps listing the volume at epoch N. `controlplane.Place`
therefore treats a restart as not a placement at all and leaves the row untouched.

This is written down because the design does not cover it — §12.3's epoch bump lives inside
the protocol for promoting a volume to a **different** host after an expired heartbeat, and
nothing says what a writer does when it comes back as itself — and because the 2026-07-26
audit split on it.

## Why it is safe

**Nothing a second incarnation writes leaves the host until it publishes.**

- Two live Agents on one data directory are refused by an exclusive `flock` on
  `agent.lock`, which names the directory in the refusal because the operator's next action
  is to find the process holding it. `TestASecondAgentRefusesTheSameDataDir` proves it
  against two real processes. Without this, same-epoch re-attach would put two processes on
  one segment directory — and bumping the epoch would not have fixed that either, since a
  stale supervisor restart never goes through the Control Plane. It is a mutual-exclusion
  problem on the data directory, not an epoch policy.
- Two Agents on **different** hosts are caught at publish time by the compare-and-set on
  `HEAD`.

The original argument rested instead on four mechanisms of the remote WAL chain —
`recovery.DurablePoint`, `InstallBase`, `VerifyAgreement` and INV-21's divergent-PUT
failure. ADR-0026 deleted that chain. The decision survived it because nothing mid-session
leaves the host any more, so the window the old argument had to close no longer exists.

## Why bumping the epoch is worse, not merely unnecessary

A bump makes a restart depend on the Control Plane: a CAS on the epoch record, a
term-guarded `BumpVolumeEpoch`, a fresh lease grant. §23's "PostgreSQL caído" wants Agents
to survive a Control Plane outage; this would make the moment an Agent is most fragile —
coming back up — depend on the one component the design assumes can be down. It also drags
in `FENCING_WAIT`, which exists to bound a writer that may still be alive on *another* host
and buys nothing against a dead PID on this one, and costs a fresh epoch directory per
restart for a writer whose identity has not changed.

## Revisit before, not after

If the fleet ever runs two Agents per host — per-device sharding, a blue/green upgrade —
this decision must be revisited *before* that lands.
