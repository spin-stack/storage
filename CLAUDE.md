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

**Everything goes through Taskfile targets.** Tool versions (sqlc, Atlas,
golangci-lint) are pinned in `Taskfile.yml` and installed into `./.tools/bin` by
`task tools`; CI runs the same tasks. Never invoke `sqlc`, `atlas`, `golangci-lint`,
`gofmt`, or a raw `go test -coverpkg` by hand — if something is missing, add a task.

```
task tools              # install the pinned toolchain into ./.tools/bin
task ci                 # the gate: fmt + build + lint + test(-race) + dst
task test               # unit/property tests, race detector
task test:integration   # Docker-gated TestContainers tests (-tags integration)
task lint               # golangci-lint + the custom simulable analyzer
task dst                # mandatory Deterministic Simulation Testing scenarios
task cover              # cross-package coverage; fails under 90% on production code
task fmt / fmt:check    # format (gofmt+goimports via golangci-lint v2) / verify
task generate           # sqlc generate
task generate:check     # fail if the committed sqlc output is stale
task db:migrate:diff -- <name>   # author an Atlas migration from schema.sql
task db:migrate:validate         # check migrations against atlas.sum
task db:migrate:lint -- --latest N  # lint pending migrations for unsafe changes
```

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
  (`internal/db`), integration-only (`internal/metadata/pg`, `migrations`), `cmd/`
  mains, and the `internal/dst` harness. Don't chase unreachable `os`-error branches —
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

## SQL: sqlc + Atlas + Postgres 18 (ADR-0006, ADR-0007)

- **All SQL goes through sqlc.** No hand-built query strings. Schema (the desired
  state) is `internal/schema/schema.sql`; queries are `internal/db/queries/*.sql`;
  generated code (`package db`, pgx/v5) lands in `internal/db` and is committed. Run
  `task generate` after editing schema or queries.
- **Migrations via Atlas.** `schema.sql` is the declared state; `task db:migrate:diff --
  <name>` generates a versioned file in `migrations/` (checksummed by `atlas.sum`).
  Migrations are forward-only and committed; the `migrations` package embeds them so
  tests apply the real files. Install Atlas via `atlasgo.sh`.
- **Postgres 18** everywhere (Atlas dev DB, TestContainers `postgres:18-alpine`, prod).
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

## Human-review zones (data-loss)

On-disk / on-S3 **formats**, **fencing/leases**, **durability** (ACK rules, FLUSH/FUA
ordering), and **GC**. Changes here get a human review of the increment spec *before*
implementation and of the diff before merge, plus an associated DST scenario. Never
touch durability/fencing/GC without one.
