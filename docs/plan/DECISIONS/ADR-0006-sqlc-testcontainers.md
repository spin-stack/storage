# ADR-0006 — SQL via sqlc; Postgres integration tests via TestContainers

- **Status:** Accepted (Phase 07 / Increment 7.1)
- **Date:** 2026-07-25
- **Deciders:** human owner + tech-lead agent
- **Extends:** ADR-0001, §7, §8

## Context

Phase 07 introduces the PostgreSQL-backed Control Plane metadata (§8). The human
directed that **all SQL go through sqlc** (type-safe generated Go from hand-written
SQL) and that **Postgres be tested with TestContainers** (a real Postgres per test
run). The sibling `spin` project already uses exactly this stack, giving us proven
conventions to mirror.

## Decision

1. **sqlc** (v1.30.x) generates the DB layer from SQL. Layout mirrors `spin`:
   - Schema: `internal/schema/schema.sql`.
   - Queries: `internal/db/queries/*.sql`.
   - `sqlc.yaml` (v2, engine postgresql, `sql_package: pgx/v5`, `emit_json_tags`,
     `emit_empty_slices`, `emit_result_struct_pointers`, uuid→`google/uuid`).
   - Generated package: `internal/db` (package `db`). `task generate` runs `sqlc generate`.
   - Migrations under `migrations/` (forward-only).
2. **pgx/v5** is the driver (matches `spin`).
3. **TestContainers** (`testcontainers-go` + `modules/postgres`, matching `spin`'s
   v0.40.0) runs a real Postgres for the `pg` metadata adapter's integration tests.
   These are tagged so they only run where Docker is available (CI/dev), not in the
   pure-unit lane.
4. **Two implementations of `metadata.Store` behind one interface** (the simulable
   pattern, like `simio`):
   - `metadata/sim` — hand-written in-memory, **deterministic**, used by the DST
     harness. The fencing protocol (§12) is *proven* here under simulated partitions
     and clock drift; a real Postgres cannot be deterministic under those conditions.
   - `metadata/pg` — thin adapter over the sqlc-generated `db` package; the production
     path, verified by TestContainers integration tests.
   Both satisfy the same contract tests where determinism is not required.

## Consequences

- No hand-written SQL string building in Go; all queries are sqlc-checked against the
  schema at generate time.
- The DST fencing proof does not depend on Docker/Postgres (runs everywhere); the real
  PG path is exercised by TestContainers where Docker is present.
- `task generate` (sqlc) becomes part of the dev loop; generated code is committed
  (as `spin` does) so builds don't require sqlc.
- The simulable-interfaces lint (INV-01) is unaffected: pgx/network access lives behind
  the `metadata` interface, and the `pg` adapter is the sanctioned place for it.

## Alternatives considered

- **Hand-written SQL + `database/sql`:** rejected by the human; loses compile-time query
  checking.
- **Real Postgres for DST:** impossible — DST needs determinism under injected
  partitions/drift; that is exactly what the in-memory `sim` provides.
