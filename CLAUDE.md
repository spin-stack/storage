# CLAUDE.md — Remote Volumes (storage)

Engineering conventions for this module. The **design source of truth** is
`arquitectura_mvp_volumenes_remotos_v5.md` (v5.1); the **plan, invariants, and ADRs**
live in `docs/plan/` (`PLAN.md`, `INVARIANTS.md`, `PHASE-0N.md`, `DECISIONS/ADR-*`,
`STATUS.md`). Read `docs/plan/STATUS.md` first to see where the work is.

Do not re-design against the doc. Any implementation decision that contradicts,
extends, or interprets it needs an ADR before merge; observed doc↔code divergences go
in `docs/plan/DEVIATIONS.md`.

## Stack

- **Go 1.26**, module `github.com/spin-stack/storage`. Conventions mirror the sibling
  `spin`/`spinbox` projects.
- Build/orchestration: **Taskfile** (go-task). Lint: **golangci-lint v2**.
- Observability: **OpenTelemetry** v1.38.x. QEMU pinned **11.0.2** (same in CI and prod).
- Layout: `internal/` (impl), `cmd/` (binaries), `api/` (proto), `integration/`,
  `hack/`, `deploy/`, `migrations/`, `internal/schema/`, `internal/db/` (generated).

## Commands

**Everything goes through Taskfile targets.** Tool versions (sqlc, pgschema,
golangci-lint) are pinned in `Taskfile.yml` and installed into `./.tools/bin` by
`task tools`; CI runs the same tasks. Never invoke `sqlc`, `pgschema`,
`golangci-lint`, `gofmt`, or a raw `go test -coverpkg` by hand — if something is
missing, add a task.

```
task tools              # install the pinned toolchain into ./.tools/bin
task ci                 # fast local gate: fmt + build + lint + test(-race) + dst
task ci:full            # everything CI runs, incl. the Docker-gated lanes (the merge gate)
task test               # unit/property tests, race detector
task test:integration   # Docker-gated TestContainers tests (-tags integration)
task lint               # golangci-lint + the custom simulable analyzer
task dst                # mandatory Deterministic Simulation Testing scenarios
task cover              # cross-package coverage; fails under 90% on production code
task fmt / fmt:check    # format (gofmt+goimports via golangci-lint v2) / verify
task generate           # sqlc generate
task generate:check     # fail if the committed sqlc output is stale
task db:dev:up / db:dev:down     # the pinned Postgres 18 the schema tasks work against
task db:plan -- <name>           # DDL for the current schema.sql change → migrations/
task db:apply PLAN=<file>.json   # apply a *saved* plan, never a recomputed one
task db:verify                   # apply schema.sql to an empty DB; assert the plan is empty
task build:qemu         # build the pinned QEMU (vhost-user-blk) into _output/
task qemu:verify        # assert the built QEMU is pinned + has vhost-user-blk-pci
task build:qemu:push    # publish the runtime image (CI does this into GitHub Packages)
task qemu:version       # print the pinned version — the single source CI tags from
task backend:conformance # §6.1 object-store conformance suite (blocking per backend)
```

**Infrastructure.** QEMU is built by its own workflow (`.github/workflows/qemu.yml`),
not by the per-push gate: the build takes tens of minutes, so it runs only when
`Dockerfile.qemu`, the Taskfile or the workflow changes, and publishes
`ghcr.io/<owner>/<repo>/qemu:<version>` plus the extracted binaries as an artefact.
The workflow calls the same Taskfile targets a developer runs, with the BuildKit cache
backend swapped (`QEMU_CACHE_FROM/TO`), so there is one definition of the build.

QEMU 11.0.2 is built from `Dockerfile.qemu` (modelled on
spinbox's, with `--enable-vhost-user-blk-server` and without its `CONFIG_CXL=n`
debloat, which breaks the 11.0.2 link). The object-store backend for tests is RustFS,
pinned by digest and started with TestContainers (`internal/testinfra`); the S3 SDK is
used in exactly one file (`internal/simio/real/s3.go`) behind `objectstore.Store`
(ADR-0010).

## Non-negotiable invariants (enforced, not aspirational)

See `docs/plan/INVARIANTS.md` for the full list + checkers. The two enforced by lint:

- **INV-01 — simulable interfaces (§25.1).** No `time.Now()`, sockets, or disk/net/S3
  syscalls outside `internal/simio`. Production code depends on the `simio` interfaces
  (clock/disk/network/objectstore) by injection; the only real implementations live in
  `internal/simio/real`. Enforced by the custom `simulable` analyzer + `depguard`/
  `forbidigo`. This is impossible to retrofit — never bypass it.
- **INV-22 — all UUIDs are v7.** Generate ids only via `internal/ids.New()`
  (`ids.NewAt(ms, r)` for deterministic DST ids). `forbidigo` forbids `uuid.New`/
  `NewString`/`NewRandom` outside `internal/ids`; every uuid column has a Postgres CHECK
  on the version nibble. Don't add a v4 id anywhere.

## Testing

- **Tests-first.** Write failing tests / DST scenarios / invariant checkers before the
  implementation. A test that is weakened or deleted to make a change pass is a stop
  signal.
- **Table-driven tests** for any repeated case shape: a `tests := []struct{...}` with a
  `name` and, where behavior varies, a `drive`/`mut func(...)` field, then
  `t.Run(tc.name, ...)`. Adding a case should be one struct literal. Don't force a table
  where cases have genuinely different setups/assertions.
- **DST (`internal/dst`).** Correctness properties are proven in the deterministic
  simulation harness: seeded, reproducible, with invariant checkers. Same seed →
  identical trace. New data-path/fencing behavior gets a scenario + checker, and the
  mandatory set stays green on every change. Checkers must be able to *catch* a
  violation (prove it with a planted bug), not merely run.
- **Property tests** (`pgregory.net/rapid`) for serialize/replay and other algebraic
  code (e.g. the WAL: truncate-at-every-byte + bit-flip → exact state XOR detected
  error, never silently wrong).
- **Coverage.** `task cover` reports cross-package coverage (measured with
  `-coverpkg=./...`, since Go's default under-counts cross-package exercise) and
  **enforces a 90% floor on production code**. Excluded from that floor: generated
  (`internal/db`), integration-only (`internal/metadata/pg`), `cmd/` mains, and the
  `internal/dst` harness. Don't chase unreachable `os`-error branches —
  that is what the sim models.

## Go style (Dave Cheney's practical Go)

- **Clarity first.** "Clear is better than clever." Code is read far more than written;
  optimize for the reader. Reduce nesting; keep the happy path left-aligned with guard
  clauses / early returns ("line of sight").
- **Errors.** Return errors, don't panic in library code. Add context by wrapping
  (`fmt.Errorf("...: %w", err)`), compare with `errors.Is`/`errors.As`, define sentinel
  `var Err... = errors.New(...)` for conditions callers branch on. Handle an error once.
- **Interfaces.** Accept interfaces, return concrete types. Keep interfaces small and
  define them where they are *consumed*, not where implemented. `simio` and
  `metadata.Store` follow this.
- **State.** Avoid package-level mutable state (there is none in the data path — time,
  randomness, and I/O are injected). Make the zero value useful where practical.
- **Naming.** Short names for short scopes, longer for longer scopes; no stutter
  (`wal.Log`, not `wal.WALLog`). Package names are lowercase, no underscores.
- **Concurrency.** Don't reach for goroutines/channels unless they simplify; leave
  concurrency decisions to the caller. Guard shared maps with a mutex; keep critical
  sections small.
- **Prefer composition** over inheritance-style embedding gymnastics; small, focused
  types.

## SQL: sqlc + pgschema + Postgres 18 (ADR-0006, ADR-0007, ADR-0019)

- **All SQL goes through sqlc.** No hand-built query strings. Schema (the desired
  state) is `internal/schema/schema.sql`; queries are `internal/db/queries/*.sql`;
  generated code (`package db`, pgx/v5) lands in `internal/db` and is committed. Run
  `task generate` after editing schema or queries.
- **Schema via pgschema, state-based (ADR-0019).** `schema.sql` is the declared state
  and the only source of truth: sqlc generates from it, the integration lane builds
  its database from it, and `task db:plan -- <name>` diffs it against a live database
  to produce the DDL. **`migrations/` is the record of reviewed plans, not the apply
  path** — nothing replays it, and `task db:apply` runs a *saved* plan file (pgschema
  fingerprints the database it was planned against and refuses a stale one).
  `task db:verify` replaces the old `atlas.sum` check by applying `schema.sql` to an
  empty database and asserting the resulting plan is empty; it is in `ci:full` and CI.
- **Postgres 18** everywhere (the `db:*` tasks' database, TestContainers
  `postgres:18-alpine`, prod). No task assumes a PostgreSQL on your machine —
  `task db:dev:up` starts the pinned one.
- **Identity columns are `uuid`** (UUIDv7), not text — `volume_id` is the same 16-byte
  id the on-disk WAL format carries. The `metadata.Store` interface uses `string` ids at
  the boundary; the `pg` adapter parses `string ↔ uuid`.
- **Indexes are part of the schema review.** Every FK *referencing* column carries an
  index (Postgres only indexes the referenced side), and a query with a filter +
  `ORDER BY` gets a composite index in that order. Both rules are enforced by
  integration tests (`TestPGEveryForeignKeyHasAnIndex`, and an `EXPLAIN` assertion for
  the drain's `ListVolumesByHost`). Indexes for queries that do not exist yet are
  listed as deferred in `schema.sql` with the trigger that should add them.
- **Term-guarded writes (§7).** Every Control-Plane mutation validates the CP `term`
  (`... WHERE (SELECT term FROM control_plane_leader) = $n`, or `INSERT ... SELECT WHERE
  EXISTS(term match)`), so a zombie CP affects 0 rows → `ErrStaleTerm`.
- **Metadata has two implementations** behind one interface: `metadata/sim` (in-memory,
  deterministic — for DST) and `metadata/pg` (sqlc adapter — verified by TestContainers).

## Formats before the first deployment

Nothing is deployed yet: no bucket holds objects anyone will read again, and there is no
fleet to keep in step. **Until the spine ships (DEV-0007, ADR-0018), on-disk and on-S3
formats change in place** — no v2 alongside v1, no migration, no compatibility shim. A
format problem is corrected, not worked around: ADR-0005 fixed the WAL header to its
real 104 bytes rather than versioning around the doc's error, and ADR-0014 changes the
snapshot manifest outright rather than stranding a class of snapshot that could never be
compacted.

This narrows scope, it does not lower the bar. Format changes stay a human-review zone
(below), every change still lands with its tests, and INV-19 (read-old / write-new,
`max_format_version`) becomes binding the moment two Agents can run different versions —
which is exactly when compatibility starts costing something real.

## Human-review zones (data-loss)

On-disk / on-S3 **formats**, **fencing/leases**, **durability** (ACK rules, FLUSH/FUA
ordering), and **GC**. Changes here get a human review of the increment spec *before*
implementation and of the diff before merge, plus an associated DST scenario. Never
touch durability/fencing/GC without one.
