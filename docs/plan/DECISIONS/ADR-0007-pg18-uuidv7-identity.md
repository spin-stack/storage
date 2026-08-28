# ADR-0007 — Postgres 18 and UUIDv7 identity columns

- **Status:** Accepted 2026-07-25 (Phase 07). The tooling half — how a schema change is
  applied — was superseded by **ADR-0019** (pgschema), which also carries the rejection
  of a versioned migration runner. What is below stands unchanged.
- **Extends:** §8

## Decision

1. **Postgres 18 everywhere** — TestContainers (`postgres:18-alpine`), the schema
   verification lane, and the production target. It is what makes native `uuidv7()`
   available on the database side.
2. **Identity columns are `uuid`, and every id is a UUIDv7** (INV-22). Time-ordered ids
   give B-tree locality on insert, and the same id names the volume in the catalog, in
   `descriptor.json` and in every object key. Minted app-side by `internal/ids`;
   Postgres 18's `uuidv7()` is available for DB-side defaults. Enforced two ways:
   `forbidigo` on the v4 generators, and a CHECK on the version nibble per uuid column.
3. **The Go boundary stays `string`.** `metadata.Store` takes string ids — they are
   already string-shaped in object keys, logs and the API — and the `pg` adapter parses
   `string ↔ uuid.UUID` at the edge. The database is strongly typed; the service
   boundary is ergonomic.

## Alternatives rejected

- **`text` id columns:** weaker typing, no uuid indexing, and they admit a non-v7 id.
- **`uuid.UUID` throughout the Go layer:** deferred, not refused — string-at-boundary is
  simpler, and it is easy to tighten if a caller ever needs compile-time id typing.
