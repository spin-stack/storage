# ADR-0007 — Atlas-versioned migrations, Postgres 18, UUID/UUIDv7 identity columns

- **Status:** Accepted (Phase 07 / Increment 7.1) — **decision 1 superseded by
  ADR-0019** (2026-07-26)
- **Date:** 2026-07-25
- **Deciders:** human owner + tech-lead agent
- **Extends:** §8

> **Superseded in part.** Decision 1 (Atlas owns migrations) no longer holds:
> **pgschema** replaces Atlas as of ADR-0019. `internal/schema/schema.sql` is still the
> declared state and still the single source of truth — that half of the decision is
> what ADR-0019 preserves — but there is no `atlas.hcl`, no versioned migration chain,
> no `atlas.sum`, and no `db:migrate:*` task; `migrations/` holds reviewed plans rather
> than the apply path, and tests build the database from `schema.sql` directly.
> **Decisions 2, 3 and 4 stand unchanged:** Postgres 18 everywhere, `uuid` identity
> columns, UUIDv7 (INV-22), and `string` ids at the Go boundary.

## Context

Three directions from the human, all aligned with the sibling `spin` project:
1. Manage schema changes with **Atlas** (versioned migrations) from the start, so the
   schema can evolve safely.
2. Use **Postgres 18** from the start, enabling native **UUIDv7**.
3. Identity (`_id`) columns should be `uuid`, not `text`.

The original schema (transcribed from doc §8) used `text` ids, which is inconsistent
with the on-disk WAL format — `RecordHeader.VolumeID` is a 16-byte UUID — and forgoes
native uuid indexing/validation.

## Decision

1. **Atlas** owns migrations (mirroring `spin`): `internal/schema/schema.sql` is the
   declared desired state; `atlas migrate diff` generates versioned files in
   `migrations/` with an `atlas.sum` checksum. `atlas.hcl` defines `local`/`ci`/
   `postgres` envs, all using `dev = docker://postgres/18/dev`. Taskfile targets:
   `db:migrate:diff`, `db:migrate`, `db:migrate:status`, `db:migrate:validate`.
   Migrations are committed and embedded (`migrations` package) so tests and tooling
   apply the real migration files, not a shortcut.
2. **Postgres 18** everywhere: Atlas dev DB, TestContainers image (`postgres:18-alpine`),
   and the production target.
3. **UUID identity columns.** `volume_id`, `host_id`, `snapshot_id`, `operation_id`,
   `request_id`, and the host/snapshot foreign keys are `uuid`. `volume_id` is exactly
   the on-disk `VolumeID [16]byte`. IDs are **UUIDv7** (time-ordered → better B-tree
   locality), generated app-side with `google/uuid.NewV7()` for values that must match
   the durable format; Postgres 18's native `uuidv7()` is available for any DB-side
   generation.
4. **Go boundary representation stays `string`.** The `metadata.Store` interface uses
   string ids (they flow as strings in S3 keys, logs, and the API); the `pg` adapter
   parses `string ↔ uuid.UUID` at the boundary (sqlc maps `uuid` → `google/uuid.UUID`).
   The DB is strongly typed; the service boundary is ergonomic. (Revisit to
   `uuid.UUID`-typed ids if a caller needs compile-time id typing.)

## Consequences

- Schema evolution is versioned and checksum-verified from commit 1; `db:migrate:diff`
  produces the next migration from a schema edit.
- Atlas and TestContainers both require Docker (already required for the integration
  lane); Atlas is installed via `atlasgo.sh` (pinned in the `tools` task).
- `uuid` columns cost nothing now and are painful to change after real data exists —
  hence done up front, like the format decision (ADR-0005).

## Alternatives considered

- **Hand-rolled migration files / golang-migrate:** rejected — Atlas gives declarative
  diffing + lint + checksums and matches `spin`.
- **`text` ids:** rejected — inconsistent with the on-disk UUID and weaker typing.
- **`uuid.UUID` throughout the Go layer:** deferred — string-at-boundary is simpler and
  ids are already string-shaped in S3 paths; easy to tighten later.
