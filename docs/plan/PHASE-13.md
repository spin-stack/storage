# PHASE 13 — Hardening

> **Roadmap §30.13.** Most of this phase needs real hardware (fault injection on real
> NVMe/network, the backend conformance suite, runbooks with measured times) and is
> therefore blocked on infra. Increment **13.1 is pulled forward** because it is pure
> Go, it touches every later increment, and retrofitting it gets more expensive with
> every phase.

## Increment 13.1 (pulled forward) — Typed lifecycles: enums + explicit state machines — **DONE ✓**

**Problem.** Everything the Control Plane persists that has a lifecycle travels as a
bare `string`: `hosts.state`, `volumes.state`, `volumes.durability`, `snapshots.state`,
`operations.kind`, `operations.phase`. Nothing stops `State: "ACTIVE "`, a typo, a state
from the wrong vocabulary (`SetHostState(h, "PUBLISHED")` compiles), or an illegal jump
(`PRIMARY_SUSPECTED → RECOVERY_REQUIRED`, skipping `FENCING_WAIT`, which §7 forbids
outright). Drain compounded it by inventing its own phase words (`DRAINING`/`DRAINED`)
instead of a reconciliation lifecycle. These are exactly the errors that stay invisible
until they corrupt an operator's mental model — or worse, a fencing decision.

**Scope-in.** New dependency-free package `internal/lifecycle`: one typed vocabulary per
lifecycle, each with its **transition table** taken from the architecture document:

| Type | § | States |
|---|---|---|
| `HostState` | §28.1 | ACTIVE, CORDONED, DRAINING, DEAD |
| `VolumeState` | §7 | ACTIVE, PRIMARY_SUSPECTED, FENCING_WAIT, RECOVERY_REQUIRED, RECOVERING, DETACHED |
| `AgentVolumeState` | §16 | DETACHED, ATTACHING, ACTIVE, SNAPSHOTTING, SELF_FENCED, FENCED, RECOVERY_REQUIRED, RECOVERING, FAILED |
| `SnapshotState` | §19 | CREATING, PUBLISHED, FAILED, DELETING |
| `OperationKind` | §8 | attach, detach, clone, resize, drain, recovery, flatten, gc |
| `OperationPhase` | §7 | PENDING, RUNNING, CANCELING, CANCELED, SUCCEEDED, FAILED |
| `Durability` | §14.8 | remote, local |

Each type gets `Valid`, `Parse…` (rejects anything else, so a bad DB row fails loudly at
the boundary instead of flowing inward), `…Values()` (exhaustive list — the DB CHECKs and
the tests are generated from it, so adding a state and forgetting the table is a test
failure), and, where the doc defines a lifecycle, `CanTransitionTo`/`Transition`
(`ErrInvalidTransition`) plus `Predecessors` — the set that may legally become this
state.

**Enforced in three places, like INV-22:**
1. **Compile time** — the struct fields and `metadata.Store` signatures take the typed
   values, so a state from the wrong vocabulary does not compile. The zero value is
   *not* a valid state, so a forgotten field is caught, not defaulted to ACTIVE.
2. **Store boundary** — `SetHostState` / `UpdateOperation` are *transition-guarded*: the
   allowed predecessors from the table become the SQL predicate
   (`AND state = ANY($n)`), so the check is atomic in Postgres (no read-modify-write
   race) and the same rule runs in `metadata/sim`.
3. **Database** — `CHECK` constraints on every one of those columns (in `schema.sql`),
   so no client, script, or manual `psql` can write a state that does not exist.

Drain drops its ad-hoc phases for the generic reconciliation lifecycle
(`RUNNING → SUCCEEDED | FAILED | CANCELING → CANCELED`, with `FAILED → RUNNING` for the
retry the reconciler performs); the drain-specific detail stays where it belongs, in
`current_state` JSON. `controlplane` promotion states become `VolumeState` values rather
than package-level string constants, and `wal.DurabilityMode` gets its single, tested
mapping from `lifecycle.Durability` so the two ends of the wire cannot drift.

**Tests first.** Table-driven per vocabulary: every allowed edge, and the forbidden ones
that matter (`PRIMARY_SUSPECTED → RECOVERY_REQUIRED` skips FENCING_WAIT; `PUBLISHED →
CREATING` would break snapshot immutability; a terminal operation phase cannot resurrect).
`Parse` rejects unknown/empty/whitespace input; the zero value is invalid; `…Values()` is
exhaustive with respect to the transition table. Store-level: an illegal transition is
refused with `ErrInvalidTransition` in **both** `sim` and `pg` (TestContainers), and the
DB rejects a bogus value even when the guard is bypassed.

**Gate.** Standard. No new invariant ID; this is the type-level enforcement of lifecycles
the doc already specifies. ADR-0009 records the convention.

---

## Remaining Phase 13 increments (blocked on infra)

- **13.2** — fault injection on real hardware (§25.3): NVMe/network faults, power loss, and
  the measured-time runbooks (§28.4).
- **13.3** — object-store backend conformance suite (§6.1, §25.4), blocking per backend
  version.
- **13.4** — **INV-19** fleet-mixed format gating (§27): a writer at format v+1 stays
  blocked until every `hosts.max_format_version ≥ v+1`. The last pending invariant.
