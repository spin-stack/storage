# CLAUDE.md — Remote Volumes (storage)

Design source of truth: `arquitectura_mvp_volumenes_remotos_v5.md` (v5.1).
Current state: `docs/plan/STATUS.md` — short by construction, see "Documents".

## Build it thin, end to end, before you build it deep

**This is the rule the rest of the file serves, and the one this project has broken most
often.** The smallest version that a real caller drives, end to end, comes first. Depth —
another invariant, another checker, another spec — comes after, on top of something that
already runs.

Every one of these was found *after* the component it lived in had unit tests, property
tests, DST scenarios and an invariant checker:

| Defect | Why nothing saw it |
|---|---|
| `cmd/volume-agent` never set `HostID`, so **no real Agent ever ran a durability scheduler** | one field in `main`; every test built the manager itself |
| `--data-dir` applied twice, so the WAL landed in `<dir>/<dir>/wal/...` | every test hands the manager a Disk spanning a whole filesystem, where the two paths agree |
| The queue loop never restarted, so **any real Linux guest hung** on its first block request | the fake front-end configured the device once; every real boot configures it twice |
| A promoted host served **zeros for its predecessor's whole volume** | the rule for "needs a read view" was written from the two cases that had tests |
| A clone read zeros; a clone had no descriptor | §20's promise was in a doc comment and in nothing else |
| The two binaries parsed the KEK file differently | nothing had ever run both binaries against one file |
| Three documents claimed a Linux-guest lane **no test performed** | the artefacts were built and verified; nothing booted them |

They are all at seams between components. A suite that is deep in every component and thin
at the seams cannot see any of them. Sophisticated machinery around an unwired path does
not make the path work — it makes the gap harder to notice.

So, in order:

1. **Make the thinnest real path run.** Real binaries, real caller, one volume, no
   sophistication. If a piece has no caller, it is not done, however well it is tested.
2. **Assert on what the outside observes** — bytes in the bucket, a socket that exists, a
   line the process printed, an exit code. Not on a flag the code sets.
3. **Then deepen**: faults, invariants, checkers, the next case.

A component with no caller is a **liability, not progress**. `CloneCrossHost` was deleted
for exactly this: 280 lines, fully tested, called only by its own test.

## The gate

Two gates, because one gate that demands depth of the first increment guarantees that
nothing thin ever ships.

**Increment 1 of a new path — the one that makes it run:**

- [ ] A command a human runs that shows the thing working end to end, real binaries.
      Paste its output in the PR.
- [ ] Full suite green.

That is all. No DST scenario, no new checker, no property test, no `STATUS.md` edit.
Those are increment 2. Exception: anything inside a human-review zone takes the deep gate
from the start — carve the thin path so it uses formats and publish paths that already exist.

**Every other increment:**

- [ ] New tests green, `task ci:full` green, mandatory DST set green.
- [ ] Serialize/replay property test with arbitrary truncations and bit corruptions, if
      an on-disk / on-S3 format changed (§25.2).
- [ ] **What did I delete?** "Nothing" is an allowed answer; skipping the question is not.

**Stop signals** — halt and escalate to a human: a `sleep`/magic timeout/infinite retry
instead of a simulable interface; a review-zone change with no DST scenario; "I did it
differently from the doc because it was simpler."

## Documents

**A document that must be verified against the tree before it can be trusted costs three
times and pays once.** Write fewer, keep them checkable, delete them when the code moves.

- **Default: the decision lives in the code, at the line that makes it**, including the
  alternative that was rejected and why. That is what the long comments are for.
- **`STATUS.md`** holds *what to do next* and *which thin paths have not been deepened yet*.
  Nothing else. Finished work is deleted from it — the history is `git log`. Hard cap: 120
  lines, enforced by a task. If it does not fit, something in it is not state.
- **A doc↔code divergence is fixed in the increment that finds it** — by fixing the code or
  by deleting the sentence from the doc. Only write a DEV entry if the fix does not fit in
  the increment, and closing it means deleting the entry.
- **ADR only when all three hold:** it spans components, it contradicts or extends the
  design doc, and getting it wrong is expensive (data loss, fencing, a format). Do not
  write an ADR to record that you thought about something.
- **Review-zone specs go in the PR description**, not in `docs/`. They get the same review
  and disappear on merge.
- **If an increment writes more lines of Markdown than of Go, it is the wrong increment.**
- Any doc that only narrates gets deleted; a doc that a task can verify may stay. Runbooks
  belong in the Taskfile, where CI runs them.

There are 25 ADRs, 10 spec documents and ~1.800 `§`/`INV`/`ADR`/`DEV` references in the
code. That is a symptom, not an asset. New code cites the *reason*, not a pointer.
`REFERENCE.md` earns its place only if `task` generates it from the tree; otherwise delete it.

## Principles

- Prefer the smallest solution that completely solves the current problem.
- Remove obsolete code instead of preserving backward compatibility.
- One component, one purpose. Every layer justifies its existence.
- Prefer proven libraries; reuse existing dependencies before adding one; verify a
  dependency's capabilities before writing custom code.
- No abstractions, configuration, fallbacks, compatibility layers or migrations for
  hypothetical needs.
- If removing code makes the system simpler without losing functionality, remove it.

## Non-negotiable invariants

Full list + checkers: `docs/plan/INVARIANTS.md`. The two enforced by lint:

- **INV-01 — simulable interfaces (§25.1).** No `time.Now()`, sockets, or disk/net/S3
  syscalls outside `internal/simio`. Production code takes the `simio` interfaces
  (clock/disk/network/objectstore) by injection; real implementations only in
  `internal/simio/real`. Enforced by the custom `simulable` analyzer +
  `depguard`/`forbidigo`. Impossible to retrofit — never bypass it.
- **INV-22 — all UUIDs are v7.** Only `internal/ids.New()` (`ids.NewAt(ms, r)` for
  deterministic DST ids). `forbidigo` forbids `uuid.New`/`NewString`/`NewRandom` outside
  `internal/ids`; every uuid column has a Postgres CHECK on the version nibble.

## Testing

- **Tests-first.** Failing test / DST scenario / checker before the implementation. A test
  weakened to make a change pass is a stop signal.
- **Test the seams, not only the parts.** `integration/e2e` runs the real binaries as
  processes; `integration/vhost` boots a real kernel. A change to anything a binary wires
  up belongs in one of those lanes, not only in a unit test that constructs the type itself.
- **Assert on what the outside observes.** A gate that returns the right error and does the
  wrong thing satisfies any assertion on `err`.
- **Prove the test can fail.** Plant the bug, watch it go red. Four assertions here proved
  nothing until that was done.
- **Table-driven tests** for repeated case shapes: `tests := []struct{...}` with `name` and,
  where behavior varies, a `drive`/`mut func(...)`. Adding a case should be one struct
  literal. Don't force a table where setups genuinely differ.
- **DST (`internal/dst`).** Seeded, reproducible, same seed → identical trace. New
  data-path/fencing behavior gets a scenario + checker; the mandatory set stays green.
  Checkers must be able to *catch* a violation, proven with a planted bug.
- **Property tests** (`pgregory.net/rapid`) for serialize/replay and algebraic code (the
  WAL: truncate-at-every-byte + bit-flip → exact state XOR detected error, never silently
  wrong).
- **Coverage.** `task cover` enforces 90% on production code, measured `-coverpkg=./...`.
  Excluded: `internal/db`, `internal/metadata/pg`, `cmd/` mains, `integration/`,
  `internal/dst`. Don't chase unreachable `os`-error branches — that is what the sim models.

## Go style (Dave Cheney's practical Go)

Clarity over cleverness; guard clauses and early returns, happy path left-aligned. Return
errors, don't panic in library code; wrap with `fmt.Errorf("...: %w", err)`, compare with
`errors.Is`/`errors.As`, sentinel `var Err... = errors.New(...)` for conditions callers
branch on, handle an error once. Accept interfaces, return concrete types; define
interfaces where consumed. No package-level mutable state — time, randomness and I/O are
injected. Short names for short scopes, no stutter (`wal.Log`). Leave concurrency decisions
to the caller.

## Stack

**Go 1.26**, module `github.com/spin-stack/storage`; conventions mirror `spin`/`spinbox`.
Taskfile (go-task), golangci-lint v2, OpenTelemetry v1.38.x, QEMU pinned 11.0.2 (CI and
prod). Layout: `internal/` (impl), `cmd/`, `api/` (proto), `integration/`, `hack/`,
`deploy/`, `migrations/`, `internal/schema/`, `internal/db/` (generated).

**Everything goes through Taskfile targets.** Tool versions are pinned in `Taskfile.yml`
and installed into `./.tools/bin` by `task tools`; CI runs the same tasks. Never invoke
`sqlc`, `pgschema`, `golangci-lint`, `gofmt` or a raw `go test -coverpkg` by hand — if
something is missing, add a task.

```
task tools              # install the pinned toolchain into ./.tools/bin
task ci                 # fast local gate: fmt + build + lint + test(-race) + dst
task ci:full            # everything CI runs, incl. Docker-gated lanes (the merge gate)
task test               # unit/property tests, race detector
task test:integration   # Docker-gated TestContainers tests (-tags integration)
task lint               # golangci-lint + the custom simulable analyzer
task dst                # mandatory DST scenarios
task cover              # cross-package coverage; fails under 90% on production code
task fmt / fmt:check    # format / verify
task generate           # sqlc generate
task generate:check     # fail if the committed sqlc output is stale
task db:dev:up / db:dev:down     # the pinned Postgres 18 the schema tasks work against
task db:plan -- <name>           # DDL for the current schema.sql change → migrations/
task db:apply PLAN=<file>.json   # apply a *saved* plan, never a recomputed one
task db:verify                   # schema.sql → empty DB; assert the plan is empty
task build:qemu         # build the pinned QEMU (vhost-user-blk) into _output/
task qemu:verify        # assert the built QEMU is pinned + has vhost-user-blk-pci
task build:qemu:push    # publish the runtime image (CI does this into GitHub Packages)
task qemu:version       # print the pinned version — the single source CI tags from
task fetch:kernel       # pinned guest kernel at _output/guest/vmlinux (ADR-0022)
task build:guest        # build the initramfs the guest lane boots (a static Go /init)
task guest:verify       # assert the lane's inputs: the initramfs + the pinned kernel
task backend:conformance # §6.1 object-store conformance suite (blocking per backend)
```

QEMU is built by `.github/workflows/qemu.yml`, not the per-push gate (tens of minutes); it
runs when `Dockerfile.qemu`, the Taskfile or the workflow changes, and publishes
`ghcr.io/<owner>/<repo>/qemu:<version>` plus the extracted binaries. It calls the same
Taskfile targets a developer runs, with the BuildKit cache backend swapped
(`QEMU_CACHE_FROM/TO`), so there is one definition of the build. The test object store is
RustFS, pinned by digest, started with TestContainers (`internal/testinfra`); the S3 SDK
lives in exactly one file (`internal/simio/real/s3.go`) behind `objectstore.Store` (ADR-0010).

## SQL: sqlc + pgschema + Postgres 18 (ADR-0007, ADR-0019)

- **All SQL goes through sqlc.** No hand-built query strings. Schema:
  `internal/schema/schema.sql`; queries: `internal/db/queries/*.sql`; generated pgx/v5 code
  in `internal/db`, committed. `task generate` after editing either.
- **State-based schema (ADR-0019).** `schema.sql` is the only source of truth.
  `migrations/` is the record of reviewed plans, not the apply path — nothing replays it,
  and `task db:apply` runs a *saved* plan (pgschema refuses one planned against a different
  database). `task db:verify` is in `ci:full`.
- **Postgres 18** everywhere; `task db:dev:up` starts the pinned one.
- **Identity columns are `uuid`** (v7) — `volume_id` is the same 16 bytes the WAL carries.
  `metadata.Store` uses `string` at the boundary; the `pg` adapter parses `string ↔ uuid`.
- **Indexes are part of schema review.** Every FK *referencing* column carries an index; a
  query with a filter + `ORDER BY` gets a composite index in that order. Both enforced by
  integration tests. Indexes for queries that do not exist yet are listed as deferred in
  `schema.sql` with the trigger that should add them.
- **Term-guarded writes (§7).** Every Control-Plane mutation validates the CP `term`, so a
  zombie CP affects 0 rows → `ErrStaleTerm`.
- **Two implementations** behind one interface: `metadata/sim` (in-memory, deterministic,
  for DST) and `metadata/pg` (sqlc adapter, verified by TestContainers).

## Formats before the first deployment

Nothing is deployed: no bucket holds objects anyone will read again, no fleet to keep in
step. **Until the spine ships (DEV-0007, ADR-0018), on-disk and on-S3 formats change in
place** — no v2 alongside v1, no migration, no shim. A format problem is corrected, not
worked around (ADR-0005 fixed the WAL header to its real 104 bytes; ADR-0014 changed the
snapshot manifest outright). This narrows scope, it does not lower the bar: format changes
stay a review zone, and INV-19 (read-old / write-new, `max_format_version`) becomes binding
the moment two Agents can run different versions.

## Human-review zones (data-loss)

Three. ADR-0026 deleted two of the four (2026-08-03).

- **On-disk / on-S3 formats.** WAL record and segment layout, `image/<vol>/manifest.json`,
  chunk sealing (`<nonce:12><ct><tag:16>`), the snapshot manifest, `descriptor.json`. Also
  needs the §25.2 property test.
- **Mutual exclusion at publish.** The compare-and-set on a volume's manifest and the
  create-only write of a snapshot's. This is *all* that is left of fencing, and the only
  thing stopping two hosts from silently overwriting each other's session with no error
  anywhere. DST arm: `two-hosts-cannot-both-publish-an-image`, planted bug: a backend that
  ignores preconditions — which is why `task backend:conformance` is blocking per backend.
- **The FLUSH/FUA ACK rule.** Capture the sequence, `fdatasync`, advance
  `durable_sequence` — and still the sentence a guest's `fsync` rests on. Widening what an
  ACK claims is a review-zone change even when the diff is three lines.

These get a human review of the spec (in the PR) before implementation and of the diff
before merge, plus a DST scenario.

**Not review zones, and saying so is the point:** the reconciliation loop, placement, the
catalog's state machines, and everything the Control Plane does with a term guard.
