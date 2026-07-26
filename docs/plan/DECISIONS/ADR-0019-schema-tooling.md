# ADR-0019 — Replace Atlas with pgschema for schema management

- **Status:** Proposed (needs human review — it changes how the database is built in
  tests, CI and eventually production)
- **Date:** 2026-07-26
- **Deciders:** human (to decide), implementer agent (proposes)
- **Supersedes the tooling half of:** ADR-0007 (Atlas + Postgres 18 + UUIDv7). The
  Postgres 18 and UUIDv7 decisions stand; only the migration tool changes.
- **Closes:** DEV-0011 (`task db:migrate:lint` cannot run on the pinned Atlas).

## Context

Two independent problems with Atlas Community showed up in the same week:

1. **`atlas migrate lint` is gated behind Atlas Pro** since v0.38. The task exists, is
   documented in CLAUDE.md, and has never succeeded for anyone — so every migration so
   far was authored without the unsafe-change review the command exists to provide
   (DEV-0011).
2. **`atlas migrate diff` refuses a schema containing a view.** The wave-4 increment
   wanted one: ADR-0017 derives a host's committed bytes from the volumes it holds plus
   its in-flight plans, and that derivation is now **inlined into four queries** because
   the tool could not express it once. That is duplicated logic in the exact place this
   project keeps saying rules must live once (the oversubscription bound, the placement
   limit, the fencing rules).

The second is the one that matters. A tool that cannot represent a normal PostgreSQL
object shapes the schema around the tool.

## Decision (proposed)

**Adopt [pgschema](https://github.com/pgplex/pgschema).** Apache 2.0, no paid tier,
PostgreSQL 14–18, and it supports views, materialized views, functions, triggers, RLS
and privileges — the objects Atlas Community would not diff.

It is *state-based*, like Atlas's declarative half: `internal/schema/schema.sql` stays
the desired state and remains the single source of truth. `pgschema plan` produces the
DDL for review; `pgschema apply` executes it.

### What changes concretely

| Today (Atlas) | With pgschema |
|---|---|
| `task db:migrate:diff -- <name>` writes a versioned file | `task db:plan` writes the reviewed DDL for the change |
| `atlas.sum` checksums the migration chain | no checksum chain (see below) |
| Tests apply the migration files in order | tests apply `schema.sql` directly — faster, and it tests the artefact that is actually authoritative |
| `task db:migrate:lint` (unusable) | `pgschema plan` shows destructive operations before apply |

### The two things we lose, and what replaces them

- **The ordered, checksummed migration chain.** pgschema keeps no history table.
  Replacement: **every plan is committed** under `migrations/` as the reviewable,
  ordered record of what was applied, and apply runs *that saved plan* rather than a
  plan recomputed at deploy time. Reviewing plan A and applying plan B is the failure
  mode a state-based tool invites, and committing the plan is what closes it.
- **`atlas.sum` integrity.** Replaced by the plan files being in git and by CI checking
  that `schema.sql` applied to an empty database produces a schema `pgschema plan`
  reports as having nothing left to do — a stronger check than a checksum, because it
  compares the real result rather than the file's bytes.

### Why not goose

goose is a mature versioned runner, and for a project that hand-writes DDL it is the
right tool. It is the wrong one here: it has no declarative desired state, so
`schema.sql` would stop being authoritative and become documentation that drifts —
which is precisely what ADR-0007 adopted a declarative tool to avoid. It also has no
diffing, so every change becomes hand-written DDL, and no lint either.

If the ordered-history property turns out to matter more than the declarative one, the
fallback is **pgschema to author the DDL, goose to apply it**, keeping both. Not
proposed now: two tools for one job needs a reason stronger than symmetry.

## Risks, stated

- **pgschema is young** (~1k stars, sponsored by Bytebase) against Atlas's maturity. It
  is Apache 2.0 with no paid tier, which is the specific failure we are leaving; but a
  young tool can abandon or break. Mitigation: the artefacts are plain SQL — `schema.sql`
  and committed plans — so leaving pgschema costs a runner change, not a data migration.
- **No migration history table** means "what is applied here?" is answered by diffing,
  not by reading a row. Acceptable while nothing is deployed; before the first
  deployment this needs a runbook answer.
- We lose `atlas migrate lint`'s ruleset — which we never had, since it needs a licence.

## What this unlocks (the reason to do it now)

Views become expressible, so the derivations this project keeps inlining can live once:

- `host_committed_bytes` — ADR-0017's derivation, currently repeated in four queries;
- the lineage charge of ADR-0014 (`charged_bytes` per lineage) when it lands;
- the "volumes on a host with their in-flight plans" join the drain and placement both
  need.

sqlc generates from views like any other relation, so the Go side does not change shape.
**Re-evaluating where a view actually helps is a follow-up to this ADR, not part of it**
— the tool change should land first and be boring.

## The work

1. Pin `pgschema` in `Taskfile.yml`, install via `task tools`.
2. Replace `db:migrate:*` with `db:plan` / `db:apply` / `db:verify`, keeping the names
   descriptive of what they do rather than mirroring Atlas's.
3. Point the test harness (`internal/testinfra`, the `migrations` embed) at `schema.sql`.
4. Add the CI check: apply `schema.sql` to an empty PostgreSQL 18, then assert a plan
   against it is empty.
5. Fold the existing migration files into the initial state (nothing is deployed —
   CLAUDE.md's "formats change in place until the spine ships" applies to the schema
   too), or keep them as history with a note that they are no longer the apply path.
6. Delete DEV-0011.
