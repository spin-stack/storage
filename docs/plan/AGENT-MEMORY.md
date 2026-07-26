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
All SQL via **sqlc**; Postgres tested with **TestContainers**; schema via **pgschema**
(state-based, ADR-0019 — Atlas is gone); **Postgres 18**; identity columns are **uuid**
with **UUIDv7** (INV-22, enforced two ways). Schema `internal/schema/schema.sql` is the
single source of truth: sqlc generates from it and the integration lane builds its
database from it. Queries `internal/db/queries/*.sql`, generated `internal/db`.
`migrations/` holds the *reviewed plans* (`task db:plan`), not an apply path — there is
no chain to replay and no `atlas.sum`; `task db:verify` checks the applied result
instead. Driver pgx/v5. `metadata.Store` has two impls: `metadata/sim` (deterministic,
for DST) and `metadata/pg` (sqlc, TestContainers-verified). See ADR-0006, ADR-0007,
ADR-0019. pgschema is pinned in `Taskfile.yml` and installed by `task tools`.

## Current state (2026-07-25, REBASELINED)
**Read `docs/plan/REBASELINE.md` first.** A human review found the status docs
overstated: what exists is a set of well-tested library *models* plus a DST harness —
no `cmd/`, no `api/`, no Agent, no vhost-user path, nothing that serves a VM. Phase
states are now *model* / *integrated* / *production-verified*; nothing is integrated.
Eight deviations are open (DEV-0003…DEV-0010): unvalidated objects can raise the
durable point, fencing is fail-open, operations are not term-guarded, the store can
delete permanently and the GC does not mark, drain is not idempotent after promotion,
rebuild-metadata covers volumes only, and no metric is ever recorded. Features are
paused until the first four are fixed.

## Previous (pre-rebaseline) note
Phases 0/01/04/05/06/07/08/09/10/**11** complete on `main` (Phase 11 = cross-host
materialization + cordon/drain + capacity, merged `--ff-only` after review). Increment
**13.1 (typed lifecycles, ADR-0009)** is on branch `hardening/typed-lifecycles`.
21/22 invariants active; only INV-19 (fleet-mixed) pending — Phase 11 adds no new
invariant ID, it extends INV-08/09/10/11/16/17 to the host-move path. Phases 02/03 planned
(need infra). `task ci` green, coverage ≥ 90%, integration green on PG 18. See
`docs/plan/STATUS.md` for the full handoff and next steps.

## Infrastructure (2026-07-25)
`task build:qemu` builds the pinned QEMU 11.0.2 into `_output/` (vhost-user-blk +
storage daemon); `task backend:conformance` runs the §6.1 object-store suite against
RustFS in a container. The AWS SDK lives in exactly one file
(`internal/simio/real/s3.go`, ADR-0010) behind `objectstore.Store`; container
scaffolding is `internal/testinfra` (integration build tag).

## Gotchas
- WAL headers are 104 bytes (ADR-0005), not the doc's "96".
- Drain evacuates from the volume's durable prefix in S3, **not** from a source-taken
  snapshot (ADR-0008 / DEV-0002): the doc's "snapshot + restore" needs a live, cooperating
  source and the CP↔Agent RPC that Phases 02/03 will bring.
- Tooling is pinned in `Taskfile.yml` and installed by `task tools` into
  `./.tools/bin` (sqlc, pgschema, golangci-lint). Never run those binaries by hand —
  every workflow (generate, schema plan/apply/verify, format, lint, coverage) is a task.
- RustFS answers `If-Match` on a missing key with **NoSuchKey**, not 412; multipart
  ETags carry a `-N` suffix (never treat an ETag as a content hash); a LIST page caps
  at 1000 keys, so the S3 store paginates internally (§22.1 depends on it).
- Do not re-add spinbox's `CONFIG_CXL=n` QEMU debloat: it breaks the 11.0.2 link. The
  QEMU build dir lives in a BuildKit cache mount — bump `QEMU_CONFIG_REV` when the
  configure flags change, or the old configuration is silently reused.
- Postgres `jsonb` round-trips by value, not byte-for-byte — compare parsed JSON in tests.
- Lifecycles are typed in `internal/lifecycle` (ADR-0009): never write a bare state
  string. Stored as TEXT + CHECK (not PG enums, not int codes); binary formats keep
  numeric enums. A new state = constant + transition table + the CHECK in `schema.sql`,
  or the drift test fails.

- Two real bugs the tests caught and fixed: `Log.Discard/WriteZeroes` not feeding the
  remote batcher; the sim network letting a closed conn Send.
- Coverage: `-coverpkg=./...`; 90% floor excludes generated db / integration-only pg /
  cmd / dst harness (`hack/coverage.sh`).
