# Plan to close TEST-GAPS in parallel

70 findings remain open in `TEST-GAPS.md` (31 high, 32 medium, 7 low; all seven
criticals are closed). They are independent enough to work in parallel, but only if
the partition respects the rule `PLAN.md §5` already sets for this project: **two
in-flight increments must not share a hot source file.** Everything below follows from
that.

## Partition by file ownership, not by severity

Six packages. Each owns a disjoint set of production files, so two packages can never
touch the same code. A finding lands in the package that owns the file its fix must
change — not the file where it was noticed.

| Package | Owns (production) | Findings |
|---|---|---|
| **A — wal** | `internal/wal/{log,batch,uploader,crypto,durability,summary,wal}.go` | 12 |
| **B — recovery** | `internal/recovery/`, `internal/materialize/`, `internal/checkpoint/`, `internal/snapshot/` | 7 |
| **C — fencing** | `internal/lease/`, `internal/epoch/`, `internal/controlplane/promotion.go` | 14 |
| **D — metadata** | `internal/metadata/**`, `internal/db/queries/`, `internal/lifecycle/` | 12 |
| **E — objectstore** | `internal/gc/`, `internal/simio/**` | 11 |
| **F — drain + harness** | `internal/controlplane/{drain,crosshost,clone,rebuild}.go`, `internal/placement/`, `internal/dst/` | 14 |

Shared, read-only for everyone: `internal/wal/format/`, `internal/ids/`,
`internal/obs/`, `docs/`. A package that believes it must change a file it does not
own stops and reports it instead of reaching across — that collision is the Planner's
call, not an implementer's.

**Concurrency cap: 5.** Six packages, five at a time: A–E run first, F follows. F is
last on purpose — it owns the DST harness, and it is worth writing its scenarios
against the fixes the others just landed.

## Isolation and merge protocol

Each package runs in its **own git worktree** off `main`, so five agents never write
to one index. Nothing is pushed by an agent. When a package finishes:

1. its worktree branch is merged into `main` **one at a time**, `--ff-only` where
   possible, rebasing if `main` moved;
2. the full gate runs after *each* merge (`task ci`, `task cover`, and
   `task test:integration` when the package touched SQL) — a package is not done
   because its own worktree was green;
3. a package that cannot merge cleanly is handed back rather than force-resolved.

## The ritual every package follows

The same one that let the documentation drift when it was skipped:

1. **Failing tests first, in their own commit.** The commit message states which
   finding it covers and what the failure mode is. A test that passes on the first run
   is a signal that the finding was wrong — report that, do not delete the test.
2. **Then the fix**, in a separate commit, with the reasoning in the message.
3. **Gate green before finishing**: `task ci` and `task cover` (≥ 90% production
   floor). Never lower the floor; if new production code cannot be unit-covered,
   exclude it in `hack/coverage.sh` with the reason, the way `metadata/pg` is.
4. **Report, honestly**: which findings are closed, which turned out not to be real
   (with the evidence), which need a decision. Under-claiming is free; over-claiming is
   what this whole exercise exists to correct.

## Rules that are not negotiable

- **Assume the worst operational case.** Every fix here exists because the happy path
  was already covered. A test that only proves the good weather adds nothing.
- **Never weaken or delete a test to make a change pass.** If an existing test
  encodes the wrong behaviour — as `TestNoLeaseConfiguredStillAcks` did — replace it
  deliberately and say so in the message.
- **INV-01 holds**: no `time.Now()`, sockets, or disk/S3 syscalls outside
  `internal/simio`. The lint enforces it; do not add an exemption.
- **Data-loss zones** (on-disk/S3 formats, fencing/leases, durability, GC) get a DST
  scenario with the fix, and a human reviews the diff before merge.
- Tooling only through Taskfile targets.

## What "done" means for this plan

`TEST-GAPS.md` has no open high-severity entries, every closed one names the commit
that closed it, and `task ci` + `task cover` + `task test:integration` +
`task backend:conformance` are green on `main`. Medium and low entries that survive
are re-listed with a reason, not silently dropped.
