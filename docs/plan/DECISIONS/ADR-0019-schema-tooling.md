# ADR-0019 — Replace Atlas with pgschema for schema management

- **Status:** Accepted 2026-07-26, implemented.
- **Supersedes the tooling half of ADR-0007.** Postgres 18 and UUIDv7 stand; only the
  migration tool changes.

## Why Atlas Community was dropped

Two independent problems, both external to this repository:

1. **`atlas migrate lint` is behind Atlas Pro since v0.38.** The task existed and had
   never succeeded for anyone, so every migration was authored without the unsafe-change
   review the command exists to provide.
2. **`atlas migrate diff` refuses a schema containing a view.** ADR-0017's committed-bytes
   derivation was therefore inlined into four queries — duplicated accounting logic in the
   one place this project insists a rule must live once.

The second is the one that decided it: a tool that cannot represent a normal PostgreSQL
object shapes the schema around the tool.

## Decision

**Adopt [pgschema](https://github.com/pgplex/pgschema)** (Apache 2.0, no paid tier,
PostgreSQL 14–18; views, functions, triggers, RLS, partial indexes, partitioning). Pinned
as `PGSCHEMA_VERSION` in `Taskfile.yml`, installed by `task tools` with `go install`, so
`go.sum` gives the pinning Atlas's `.sha256` fetch was providing.

It is state-based: `internal/schema/schema.sql` stays the single source of truth,
`pgschema plan` produces DDL for review, `pgschema apply` executes it.

The Go side did not move, and that is what made the swap boring: `sqlc.yaml` already
generated from `schema.sql` and never from the migration chain, so replacing the tool that
plans against that file changed nothing about what sqlc reads.

Two properties of Atlas are given up, and both are replaced:

- **The ordered, checksummed chain.** pgschema keeps no history table. Every plan is
  committed under `migrations/` and **apply runs that saved plan, never one recomputed at
  deploy time** — reviewing plan A and applying plan B is the failure a state-based tool
  invites.
- **`atlas.sum` integrity.** Replaced by `task db:verify` in `ci:full`: apply `schema.sql`
  to an empty Postgres 18 and assert the plan against the result is empty. Stronger than a
  checksum, because it compares the real result rather than the file's bytes.

**Tool constraint:** every invocation must pass `--plan-host` explicitly. Without it
pgschema downloads and runs an embedded PostgreSQL of its own choosing at plan time, which
is neither pinned nor 18.

Nothing in `schema.sql` was simplified to suit the tool — the point of the ADR. pgschema
round-trips every construct in it, the `get_byte(uuid_send(...)) >> 4 = 7` UUIDv7 CHECKs
(INV-22) included.

## Alternatives considered

- **goose.** Mature versioned runner, and the right tool for a project that hand-writes
  DDL. It has no declarative desired state, so `schema.sql` would stop being authoritative
  and become documentation that drifts — exactly what ADR-0007 adopted a declarative tool
  to avoid. No diffing and no lint either. If the ordered-history property ever matters
  more than the declarative one, the fallback is pgschema to author, goose to apply.
- **Stay on Atlas and keep the derivation inlined.** That is the duplication this ADR
  exists to remove.

## Risks, stated

- pgschema is young (~1k stars, sponsored by Bytebase) and can be abandoned or broken.
  The artefacts are plain SQL — `schema.sql` plus committed plans — so leaving it costs a
  runner change, not a data migration.
- No history table means "what is applied here?" is answered by diffing, not by reading a
  row. Acceptable while nothing is deployed; before the first deployment it needs an
  answer.
