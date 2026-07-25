# ADR-0009 — Typed lifecycle vocabularies; stored as TEXT with CHECK, not PG enums or ints

- **Status:** Accepted (Increment 13.1, pulled forward)
- **Date:** 2026-07-25
- **Deciders:** human (raised the gap), implementer agent
- **Implements/Extends:** §7 (failover states, reconciliation), §8 (schema), §14.8
  (durability), §16 (Agent machine), §19 (snapshot states), §28.1 (fleet states).

## Context

Everything with a lifecycle travelled as a bare `string`: `hosts.state`,
`volumes.state`, `volumes.durability`, `snapshots.state`, `operations.kind`,
`operations.phase`. Consequences observed in the code before this change:

- `SetHostState(h, "PUBLISHED")` — a snapshot state on a host — compiled fine.
- `drain.go` invented its own phase words (`DRAINING`, `DRAINED`) unrelated to any
  vocabulary in the document, so two operation kinds could never be reconciled by one
  reconciler.
- `rebuild.go` wrote `State: "REBUILT"`, a state that exists nowhere in §7 or §16.
- The §7 rule "promotion always passes through FENCING_WAIT" and the §19 rule "a
  PUBLISHED snapshot never changes" existed only as prose in comments.
- Nothing stopped a typo, a trailing space, or a state from a different vocabulary
  from reaching a row and then a runbook.

## Decision

`internal/lifecycle` owns one typed vocabulary per lifecycle, each with the
transition table from the document: `HostState` (§28.1), `VolumeState` (§7),
`AgentVolumeState` (§16), `SnapshotState` (§19), `OperationKind` + `OperationPhase`
(§7/§8), `Durability` (§14.8). Enforcement is three-layered, mirroring INV-22:

1. **Compile time** — struct fields and `metadata.Store` signatures are typed; the
   zero value is not a valid state, so a field nobody set is caught rather than
   defaulting to ACTIVE. (Go still lets an *untyped constant* like `"ACTIVE"` convert
   implicitly; layers 2 and 3 catch the rest.)
2. **Store boundary** — writes validate the value, and `SetHostState` /
   `UpdateOperation` are transition-guarded: the legal predecessors from the table
   become the SQL predicate (`AND state = ANY($n)`), so the rule is checked atomically
   inside the UPDATE (no read-modify-write race) and the same rule runs in
   `metadata/sim` for DST.
3. **Database** — `CHECK` constraints on every one of those columns, so no script,
   migration, or manual `psql` can persist a state that does not exist. A
   TestContainers test loops over every declared Go value and asserts the DB accepts
   it, so adding a state in Go without a migration fails CI.

Two behaviours changed as a result, deliberately:

- Drain drops its private phases for the shared reconciliation lifecycle
  (`PENDING → RUNNING → SUCCEEDED | FAILED | CANCELING → CANCELED`, with
  `FAILED → RUNNING` for the retry the reconciler performs). What a drain is *doing*
  stays in `current_state` JSON, where it belongs.
- `rebuild-metadata` records a rebuilt volume as `DETACHED` instead of the invented
  `REBUILT`: after a total PG loss the CP knows nothing about ownership, and
  re-attaching goes through the normal (fenced) path.

## Why TEXT + CHECK rather than PostgreSQL `ENUM` types

Native enums buy: 4-byte storage, a reusable named type, and type errors when two
different enum columns are compared in SQL. Against that:

- **Evolution is rigid.** `ALTER TYPE … ADD VALUE` cannot be used in the same
  transaction that then writes the new value — which is exactly how a versioned
  migration wants to work. Removing a value is worse: there is no `DROP VALUE`, so it
  means recreating the type and rewriting every dependent column. With a CHECK, both
  are a one-line constraint swap.
- **Duplicate Go vocabularies.** sqlc generates a Go type per PG enum, which would sit
  next to `lifecycle.HostState` — two vocabularies for one concept, exactly the
  problem this ADR removes. It is fixable with per-column `go_type` overrides, but
  that is config to maintain forever.
- **Ordering surprises.** Enum comparison follows declaration order, which invites
  `state > 'CORDONED'` style code and makes inserting a value "in the middle" a
  migration with `BEFORE`/`AFTER`.

The safety an enum adds over a CHECK — catching a cross-vocabulary comparison *inside
SQL* — is already covered at the Go layer, and we never join on state columns. So:
TEXT + CHECK.

## Why the stored representation stays a string, not an integer code

Smallint codes would save a handful of bytes per row and make renames free. They cost
more than they save here:

- **The DB is an operator surface.** The CP database is what a human reads during a
  failover at 03:00; `state = 3` needs a decoder ring, and every runbook (§28.4) and
  break-glass psql session gets worse.
- **The S3 layout must stay self-describing (§22.5, INV-20).** `descriptor.json` and
  the manifests exist so PostgreSQL can be rebuilt from the buckets after total loss.
  Numeric codes would make the code table itself part of the durable format — a §27
  format-version contract — for a lifecycle that is otherwise free to evolve.
- **CHECK constraints stop being meaningful.** `CHECK (state BETWEEN 0 AND 5)` accepts
  a wrong-but-in-range value; the enumerated string list does not.
- **The saving is noise** at fleet scale (thousands of hosts/volumes/operations, not
  billions of rows).

The split we keep: **numeric enums inside binary formats** — `format.RecordType`,
`ioclass.Class`, `wal.DurabilityMode` are ints because they live in fixed-size headers
and hot paths — and **string enums in Postgres and JSON**, where a human or a
recovery tool reads them. `wal.ModeFor` is the single, exhaustively tested mapping
between the two, so the vocabularies cannot drift.

## Consequences

- Adding a state is now a deliberate three-part change (Go constant + transition table
  + migration), and the drift test fails if any part is missing.
- Illegal lifecycle moves surface as `lifecycle.ErrInvalidTransition` at the store,
  not as a silently corrupted row.
- The §16 Agent machine is defined but not yet consumed: Phases 02/03 implement the
  document's machine instead of inventing one.
