# ADR-0017 — Committed capacity is derived from state, not carried as a ledger

- **Status:** Accepted 2026-07-26
- **Date:** 2026-07-26
- **Deciders:** human (decided), implementer agent (proposed the options)
- **Implements/Extends:** §28.2 (capacity accounting), §7 (term-guarded mutations).
- **Closes:** TEST-GAPS "capacity accounting has no idempotency key".

## Context

`hosts.nvme_committed_bytes` is an incremental ledger: the drain adds on reserve and
subtracts on release. Every safeguard the last two waves added exists because a ledger
can be applied twice — the non-negative guard, the oversubscription bound, the
expected-value predicate, and the per-volume stages in the operation's progress.

The residual wave 3 named honestly is unfixable in that shape: across a crash, a
stranger's change that nets to exactly one volume size is indistinguishable from this
operation's own delta. Closing it with an idempotency key means a reservation row keyed
by `(operation_id, volume_id)`, or a transaction boundary `metadata.Store` does not
have.

## Decision

**Stop carrying the number. Derive it.**

```
committed(host) = Σ size_bytes of volumes whose primary_host_id = host
                + Σ size_bytes reserved by in-flight operation plans targeting host
```

Both terms are queries over rows that already exist and are already term-guarded. There
is no delta to apply, so there is nothing to apply twice: the accounting is a function
of state, and a resumed pass computes the same answer as the pass that crashed.

The second term is what makes it correct rather than merely simple: a volume being moved
must be charged to its destination *before* it becomes the primary there, or two
placements would both see room. The operation's recorded plan is what says so — and
after wave 3 it already carries a per-volume stage, so "reserved but not yet primary" is
readable, not inferred.

The oversubscription bound (wave 3) stays exactly where it is — a predicate of the write
that assigns a volume or records a plan — evaluated against the derived value.

## Alternatives considered

- **A reservation table keyed by `(operation_id, volume_id)`.** Exact, and it adds a
  table, a lifecycle for its rows, and a cleanup policy for terminal operations. It
  blinds the ledger's failure mode instead of removing it.
- **Use the progress stages as the key, without a new table.** Cheaper, but the progress
  write and the capacity write remain two writes: making them one needs a transaction
  boundary on `metadata.Store`, which would be the first in the interface and would have
  to be honoured by both implementations.
- **Keep the ledger and accept the residual.** The residual is silent over-commit or a
  wedged drain, in the operational case (a crash mid-evacuation) the ledger exists for.

## What this costs

- **A query where there was a column.** Placement and the drain read a sum over the
  volumes of a host instead of one field. It is indexed (`volumes(primary_host_id)`
  exists, and every FK referencing column carries an index by the schema rule), and it
  runs at placement time, not on the data path.
- **`nvme_committed_bytes` becomes derived**, so the column goes or becomes an explicit
  cache with a test that recomputation agrees with it — the same rule ADR-0014 applies
  to `charged_bytes` and §5.8 applies to watermarks: a stored number that S3 or SQL can
  recompute is a cache, never an authority.
- `CommitHostCapacity` disappears as a mutation. The wave-3 sentinels it carried
  (`ErrCapacityExceeded`, `ErrCapacityConflict`) fold into the write that assigns the
  volume.

## The tests that would enforce it

- The wave-3 crash tests, unchanged in intent: kill a pass at each boundary, resume, and
  assert the host's committed value moved by exactly one volume size. Under a derived
  number they should become uninteresting — which is the point, and worth saying in the
  commit that makes them pass trivially.
- A test that a volume in flight is charged to its destination before it is primary
  there, and to neither host twice.
- A property test: for any interleaving of moves and crashes, `committed(host)` equals
  the sum over the host's volumes and in-flight plans. A ledger cannot state that
  property; a derived value is that property.
- A contract case pinning that the oversubscription bound is still evaluated inside the
  write, against the derived value.

## Consequences

- Removes an entire class of bug rather than guarding it — the same move wave 3 made
  when it put the oversubscription bound inside the write instead of the reader.
- The plan becomes load-bearing for accounting: an operation whose plan is lost or
  malformed no longer just fails its own move, it makes the destination look emptier
  than it is. The plan is already recorded term-guarded and is already what
  `finishMovedVolume` trusts, so this concentrates trust rather than spreading it.
