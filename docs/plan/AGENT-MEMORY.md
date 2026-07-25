# AGENT-MEMORY

Session handoff for the coding agent. This mirrors the personal agent-memory so the
context travels with the repo. To repopulate the memory system on a new machine, copy
the relevant bits into `~/.claude/projects/<slug>/memory/` (the `<slug>` is the project
path with `/` → `-`, e.g. `-home-aledbf-Trabajo-github-spin-stack-storage`).

## Project (type: project)
Building the remote-volumes-for-VMs system in `spin-stack/storage/`. Design source of
truth: `arquitectura_mvp_volumenes_remotos_v5.md` (v5.1) — never re-design; deviations
require an ADR (`docs/plan/DECISIONS/`) and doc↔code divergences go in `DEVIATIONS.md`.
All plan state lives in `docs/plan/` so a fresh session resumes from the repo alone.

**Confirmed stack:** Go 1.26, module `github.com/spin-stack/storage`. Taskfile +
golangci-lint (+ custom `simulable` analyzer). GitHub Actions. OTel v1.38. QEMU pinned
11.0.2. Execution: branch per phase off `main`, parallel tracks allowed after Phase 01;
merge to `main` `--ff-only` after human review of data-loss zones.

**Non-negotiables:** tests/DST/checkers before implementation; simulable interfaces from
commit 1 (no `time.Now()`/sockets/disk/S3 outside `internal/simio`, lint-enforced =
INV-01); gate between increments; human-review zones = formats/fencing/durability/GC.

## SQL conventions (type: feedback)
All SQL via **sqlc**; Postgres tested with **TestContainers**; migrations via **Atlas**;
**Postgres 18**; identity columns are **uuid** with **UUIDv7** (INV-22, enforced two
ways). Layout mirrors sibling `spin`: schema `internal/schema/schema.sql`, queries
`internal/db/queries/*.sql`, generated `internal/db`, Atlas `migrations/` (+ `atlas.sum`,
embedded via the `migrations` package). Driver pgx/v5. `metadata.Store` has two impls:
`metadata/sim` (deterministic, for DST) and `metadata/pg` (sqlc, TestContainers-verified).
See ADR-0006, ADR-0007. Atlas installed via `curl -sSL https://atlasbinaries.com/...` to
`~/.local/bin/atlas`.

## Current state (2026-07-25)
Phases 0/01/04/05/06/07/08/09/10/**11** complete on `main` (Phase 11 = cross-host
materialization + cordon/drain + capacity, merged `--ff-only` after review). Increment
**13.1 (typed lifecycles, ADR-0009)** is on branch `hardening/typed-lifecycles`.
21/22 invariants active; only INV-19 (fleet-mixed) pending — Phase 11 adds no new
invariant ID, it extends INV-08/09/10/11/16/17 to the host-move path. Phases 02/03 planned
(need infra). `task ci` green, coverage ≥ 90%, integration green on PG 18. See
`docs/plan/STATUS.md` for the full handoff and next steps.

## Gotchas
- WAL headers are 104 bytes (ADR-0005), not the doc's "96".
- Drain evacuates from the volume's durable prefix in S3, **not** from a source-taken
  snapshot (ADR-0008 / DEV-0002): the doc's "snapshot + restore" needs a live, cooperating
  source and the CP↔Agent RPC that Phases 02/03 will bring.
- Tooling is pinned in `Taskfile.yml` and installed by `task tools` into
  `./.tools/bin` (sqlc, Atlas, golangci-lint). Never run those binaries by hand —
  every workflow (generate, migrations, format, lint, coverage) is a task.
- Postgres `jsonb` round-trips by value, not byte-for-byte — compare parsed JSON in tests.
- Lifecycles are typed in `internal/lifecycle` (ADR-0009): never write a bare state
  string. Stored as TEXT + CHECK (not PG enums, not int codes); binary formats keep
  numeric enums. A new state = constant + transition table + Atlas migration, or the
  drift test fails.

- Two real bugs the tests caught and fixed: `Log.Discard/WriteZeroes` not feeding the
  remote batcher; the sim network letting a closed conn Send.
- Coverage: `-coverpkg=./...`; 90% floor excludes generated db / integration-only pg &
  migrations / cmd / dst harness (`hack/coverage.sh`).
