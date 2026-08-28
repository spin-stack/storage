# ADR-0017 — Committed capacity is derived from state, not carried as a ledger

- **Status:** Accepted 2026-07-26. Second term withdrawn 2026-08-05 (see below).
- **Implements/Extends:** §28.2 (capacity accounting), §7 (term-guarded mutations).

## Decision

**Stop carrying the number. Derive it.**

```
committed(host) = Σ size_bytes of volumes whose primary_host_id = host
```

It is a query over rows that already exist and are already term-guarded, so there is no
delta to apply and nothing to apply twice: a resumed pass computes the same answer as the
pass that crashed. The oversubscription bound stays a predicate of the write that assigns
a volume or records a plan, evaluated against the derived value.

The original decision had a second term — bytes an in-flight operation plan had reserved
on a destination — so a volume being moved was charged to its destination before it became
primary there. It went with the operations table (ADR-0026). It changed no number the
derivation ever produced, because nothing outside a test wrote an operations row; what it
removed is headroom for a move spanning two hosts, and V1 performs none. It comes back
with cross-host movement.

## Alternatives considered

- **A reservation table keyed by `(operation_id, volume_id)`.** Exact, and it adds a
  table, a row lifecycle and a cleanup policy — it blinds the ledger's failure mode
  instead of removing it.
- **The progress stages as the key, no new table.** Cheaper, but the progress write and
  the capacity write stay two writes; making them one needs a transaction boundary on
  `metadata.Store`, the first in that interface, honoured by both implementations.
- **Keep the ledger and accept the residual.** Across a crash, a stranger's change that
  nets to exactly one volume size is indistinguishable from this operation's own delta.
  The residual is silent over-commit or a wedged drain, in the exact operational case the
  ledger exists for.

## What it costs, and what holds it

Placement and the drain read a sum instead of a field; it is indexed
(`volumes (primary_host_id, volume_id)`) and runs at placement time, not on the data path.
`CommitHostCapacity` disappears as a mutation, and its sentinels fold into the write that
assigns the volume.

The rule is that the derivation has exactly one home — the `host_committed_bytes` view —
so a second query cannot quietly reintroduce the ledger. Enforced by
`TestCommittedBytesIsDerivedInOnePlace` (`internal/db/queries_guard_test.go`). A stored
number that SQL can recompute is a cache, never an authority.
