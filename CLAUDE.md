# CLAUDE.md — Remote Volumes (storage)

Design source of truth: `arquitectura_mvp_volumenes_remotos_v6.md` (replaced v5.1 on
2026-08-11: QEMU owns the local CoW format via qcow2; this system owns immutable commits,
publication and recovery; v5 is in `git log`). Current state: `docs/plan/STATUS.md`.

## Thin end to end before deep

**The rule this project has broken most often.** The smallest version that a real caller
drives, end to end, comes first. Depth — another invariant, checker or spec — comes after,
on top of something that already runs.

Every serious defect so far lived at a seam between individually well-tested components:
a field never set in `main` (no real Agent ever ran a durability scheduler); `--data-dir`
applied twice; a queue loop that never restarted, hanging every real Linux guest; a
promoted host serving zeros for its predecessor's volume; two binaries parsing one KEK
file differently; a documented guest lane no test performed. A suite deep in every
component and thin at the seams cannot see any of these.

In order:

1. **Make the thinnest real path run.** Real binaries, real caller, one volume. A piece
   with no caller is not done, however well tested — it is a liability. (`CloneCrossHost`:
   280 fully-tested lines, deleted; only its own test called it.)
2. **Assert on what the outside observes** — bytes in the bucket, a socket that exists, a
   printed line, an exit code. Not a flag the code sets.
3. **Then deepen**: faults, invariants, checkers, the next case.

## The gate

**Increment 1 of a new path — the one that makes it run:**

- [ ] A command a human runs that shows it working end to end, real binaries. Paste the
      output in the PR.
- [ ] Full suite green.

Nothing else — DST scenarios, checkers, property tests and `STATUS.md` edits are
increment 2. Exception: anything inside a human-review zone takes the deep gate from the
start — carve the thin path through formats and publish paths that already exist.

**Every other increment:**

- [ ] New tests green, `task ci:full` green, mandatory DST set green.
- [ ] Serialize/replay property test (arbitrary truncations + bit flips) if an on-disk /
      on-S3 format changed (§25.2).
- [ ] **What did I delete?** "Nothing" is an allowed answer; skipping the question is not.

**Stop signals — halt and escalate to a human:** a `sleep`/magic timeout/infinite retry
instead of a simulable interface; a review-zone change with no DST scenario; "I did it
differently from the doc because it was simpler"; a test weakened to make a change pass.

## Documents

A document that must be verified against the tree before it can be trusted costs three
times and pays once. Write fewer, keep them checkable, delete them when the code moves.

- Default: the decision lives in the code, at the line that makes it, including the
  rejected alternative and why.
- `STATUS.md` holds only *what to do next* and *which thin paths are not deepened yet*.
  Finished work is deleted — history is `git log`. Hard cap 120 lines, enforced by a task.
- A doc↔code divergence is fixed in the increment that finds it: fix the code or delete
  the sentence. A DEV entry only if the fix does not fit the increment; closing it means
  deleting it.
- ADR only when all three hold: spans components, contradicts or extends the design doc,
  and getting it wrong is expensive (data loss, fencing, a format).
- Review-zone specs go in the PR description, not `docs/`; they disappear on merge.
- If an increment writes more lines of Markdown than of Go, it is the wrong increment.
- A doc that only narrates gets deleted; runbooks belong in the Taskfile, where CI runs
  them. New code cites the *reason*, not a `§`/`INV`/`ADR`/`DEV` pointer.

## Principles

- The smallest solution that completely solves the current problem.
- Remove obsolete code instead of preserving backward compatibility.
- One component, one purpose; every layer justifies its existence.
- Reuse existing dependencies before adding one; verify a dependency's capabilities
  before writing custom code.
- No abstractions, configuration, fallbacks, compatibility layers or migrations for
  hypothetical needs.
- If removing code makes the system simpler without losing functionality, remove it.

## Non-negotiable invariants

Each invariant is stated where its checker is (`internal/dst/checkers.go`). The two
enforced by lint instead:

- **INV-01 — simulable interfaces (§25.1).** No `time.Now()`, sockets, or disk/net/S3
  syscalls outside `internal/simio`. Production code takes the `simio` interfaces by
  injection; real implementations only in `internal/simio/real`. Enforced by the custom
  `simulable` analyzer + `depguard`/`forbidigo`. Impossible to retrofit — never bypass.
- **INV-22 — all UUIDs are v7.** Only `internal/ids.New()` (`ids.NewAt(ms, r)` in DST).
  `forbidigo` forbids `uuid.New*` outside `internal/ids`; every uuid column has a Postgres
  CHECK on the version nibble.

## Testing

- **Tests-first.** Failing test / DST scenario / checker before the implementation.
- **Test the seams.** `integration/e2e` runs the real binaries as processes; the guest
  lane boots a real kernel under the pinned QEMU. A change to anything a binary wires up
  belongs in one of those lanes, not only in a unit test that constructs the type itself.
- **Assert on what the outside observes.** A gate that returns the right error and does
  the wrong thing satisfies any assertion on `err`.
- **Prove the test can fail** — plant the bug, watch it go red.
- **Table-driven tests** for repeated case shapes; don't force a table where setups
  genuinely differ.
- **DST (`internal/dst`).** Seeded, same seed → identical trace. New data-path/fencing
  behavior gets a scenario + checker, proven able to catch a planted violation.
- **Property tests** (`pgregory.net/rapid`) for serialize/replay and algebraic code (the
  commit manifest and HEAD: truncate-at-every-byte + bit-flip → refused, never silently
  wrong).
- **Coverage.** `task cover` enforces a floor on production code (`-coverpkg=./...`); the
  number and why it moved are in `hack/coverage.sh`.
  What is excluded, and why each one is, is the `EXCLUDE` line in that script — one copy,
  because a list repeated here drifts and this one had. Don't chase unreachable `os`-error
  branches: the sim models those.

## Go style (Dave Cheney's practical Go)

Clarity over cleverness; guard clauses, happy path left-aligned. Return errors, don't
panic in library code; wrap with `%w`, compare with `errors.Is/As`, sentinels for
conditions callers branch on, handle an error once. Accept interfaces, return concrete
types; define interfaces where consumed. No package-level mutable state — time,
randomness and I/O are injected. Short names for short scopes, no stutter. Leave
concurrency to the caller.

## Stack

**Go 1.26**, module `github.com/spin-stack/storage`; conventions mirror `spin`/`spinbox`.
Taskfile (go-task), golangci-lint v2, OpenTelemetry v1.38.x, QEMU pinned 11.1.1 (CI and
prod). Layout: `internal/`, `cmd/`, `api/` (proto), `integration/`, `hack/`, `deploy/`,
`migrations/`, `internal/schema/`, `internal/db/` (generated).

**Everything goes through Taskfile targets.** Versions pinned in `Taskfile.yml`,
installed into `./.tools/bin` by `task tools`; CI runs the same tasks. Never invoke
`sqlc`, `pgschema`, `golangci-lint`, `gofmt` or raw `go test -coverpkg` by hand — if
something is missing, add a task.

```
task ci                 # fast local gate: fmt + build + lint + test(-race) + dst
task ci:full            # everything CI runs, incl. Docker-gated lanes (the merge gate)
task test / test:integration / lint / dst / cover / fmt / fmt:check
task generate / generate:check          # sqlc
task db:dev:up|down     # pinned Postgres 18
task db:plan -- <name>  # DDL for the current schema.sql change → migrations/
task db:apply PLAN=<f>  # apply a *saved* plan, never a recomputed one
task db:verify          # schema.sql → empty DB; assert the plan is empty
task build:qemu / qemu:verify / qemu:version / qemu:tools
task fetch:kernel / build:guest / guest:verify
task demo:stage1        # Stage 1 end to end: a Linux guest boots off our qcow2
task backend:conformance # §6.1 object-store conformance (blocking per backend)
```

QEMU is built by `.github/workflows/qemu.yml` (not the per-push gate), which calls the
same Taskfile targets a developer runs and publishes `ghcr.io/<owner>/<repo>/qemu:<ver>`.
Test object store: RustFS, pinned by digest, via TestContainers (`internal/testinfra`);
the S3 SDK is confined to `internal/simio/real/s3*.go` behind `objectstore.Store`, and
nothing else in the tree imports it.

## SQL: sqlc + pgschema + Postgres 18 (ADR-0007, ADR-0019)

- **All SQL through sqlc.** Schema: `internal/schema/schema.sql`; queries:
  `internal/db/queries/*.sql`; generated code committed. `task generate` after editing.
- **State-based schema.** `schema.sql` is the only source of truth; `migrations/` is the
  record of reviewed plans, not the apply path. `task db:verify` is in `ci:full`.
- **Identity columns are `uuid`** (v7) — the same id names the volume in the catalog, in
  `descriptor.json` and in every object key. `metadata.Store` uses `string` at the
  boundary; the `pg` adapter parses.
- **Indexes are part of schema review.** Every FK-referencing column carries an index; a
  filter + `ORDER BY` query gets a composite index in that order (both enforced by
  integration tests). Deferred indexes are listed in `schema.sql` with their trigger.
- **Term-guarded writes (§7).** Every CP mutation validates the `term`; a zombie CP
  affects 0 rows → `ErrStaleTerm`.
- **Two implementations** behind one interface: `metadata/sim` (DST) and `metadata/pg`
  (TestContainers).

## Formats before the first deployment

Nothing is deployed, so until the spine ships (DEV-0007, ADR-0018) on-disk and on-S3
formats change **in place** — no v2 alongside v1, no migration, no shim; a format problem
is corrected, not worked around. This narrows scope, not the bar: format changes stay a
review zone, and INV-19 (read-old / write-new, `max_format_version`) becomes binding the
moment two Agents can run different versions.

**The wire is not one of those formats, and its clock is already running.** An on-S3 format
is read by our own binaries, which ship together; `api/` is read by *spin*, another repo on
another release cadence, so "client and server are deployed together" stops being true the
day spin calls this API and not the day the spine ships. `task lint` runs `buf breaking
--against main` at `FILE` — the level that catches a **rename**, which is the change that
looks like a refactor and is not: the field number survives it and every generated accessor
and JSON key does not. So, in `api/`: a field number is permanent, a deleted number and
name are `reserved`, an enum value is never reinterpreted, and a change is additive or it
is a new version. If `buf breaking` fails, that is the answer and not the obstacle — the
order to try is *additive field → new RPC → deprecate and migrate → new package version*,
and only then a break somebody signed for. The generated code in `api/gen` is never edited;
`task generate` is the only way it moves.

## Human-review zones (data-loss)

Spec reviewed in the PR before implementation, diff reviewed before merge, plus a DST
scenario. Three zones (v6):

- **On-S3 formats.** Commit manifest, `HEAD`, `descriptor.json`, and the seal a layer
  carries out (`<nonce:12><ct><tag:16>` under the volume's DEK). All framed: digest over
  the bytes as stored + `format_version`. `HEAD` above all — the one mutable object; a
  turned bit in it is the whole volume. Needs the truncation/bit-flip property test.
- **Mutual exclusion at publish.** The CAS on `HEAD` — all that is left of fencing at the
  object store, and the last line against two hosts silently overwriting each other (the
  epoch is the fencing token; the CAS alone is not sufficient). `task
  backend:conformance` is blocking per backend, with a planted precondition-ignoring
  backend; its concurrency cases fork processes.
- **The commit contract.** `Commit() → SUCCESS` promises this state is reconstructible
  without the host. The ordering (`PUT layer`, `PUT manifest`, `CAS HEAD`, never the
  reverse) and §11's two invariants serve that sentence. Widening what a commit claims is
  a review-zone change even at three lines of diff.

**Not review zones, deliberately:** the reconciliation loop, placement, the catalog's
state machines, and everything the CP does with a term guard. Gone with v6 (subject
removed): the WAL layout, the FLUSH/FUA ACK rule, chunk addressing.