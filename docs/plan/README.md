# docs/plan — the map

Each row answers one question, and no two answer the same one.

| I want to know… | Read |
|---|---|
| What is done, what is partial, what is missing, what to do next | **`STATUS.md`** |
| What has to be built, in what order, to serve one volume end to end | **`BUILD-INVENTORY.md`** |
| What does `§14.4` / `INV-13` / `ADR-0017` / `DEV-0007` in this comment mean? | **`REFERENCE.md`** — one line each, no other file needed |
| What property must hold, and which checker proves it | `INVARIANTS.md` |
| Why was this decided, and what was rejected | `DECISIONS/ADR-NNNN-*.md` |
| What could still go wrong, and what forces a re-review | `RISKS.md` |
| The on-disk segment format | `WAL-SEGMENTS-SPEC.md` |
| What one increment in a human-review zone decided, and why | the other `*-SPEC.md` — see below |
| How the system is supposed to work | `../../arquitectura_mvp_volumenes_remotos_v5.md` (v5.1, Spanish) |

**The `*-SPEC.md` files are review artefacts, not a category.** `CLAUDE.md` asks for a
spec before implementing *only* inside a human-review zone; everywhere else the increment
is the plan. So each one exists to be read by a person before a risky change, and once
that change has landed its decisions belong in comments at the code.

**Two of them ask rather than decide, and are the only two that landed before their code:**
`CHUNK-ADDRESSING-SPEC.md` and `DELETION-AND-RECLAIM-SPEC.md` each end in a section headed
"The question for review", and until it is answered nothing implements them. Those questions
are open items in `STATUS.md`'s "Decisions waiting on a human" — which is where a reader
looks for what is *waiting*; this directory is where they look for what a thing is.

**Which of them the code still cites is a command, not a sentence:**

```
for f in docs/plan/*-SPEC.md; do
  printf '%2d %s\n' "$(grep -rl "$(basename "$f" .md)" --include='*.go' . | wc -l)" "$(basename "$f")"
done
```

Run on 2026-08-04 it answered **6** for `SHUTDOWN-PUBLISH-SPEC.md` (`internal/agent/volume.go`
and `loop.go`, `cmd/volume-agent/main.go`, `internal/dst/scenarios_agent.go` and two lane
tests), **1** for `VIEW-ADOPTION-SPEC.md`, and **0** for every other. The sentence this
replaced said one spec was cited by a `.go` file and named the wrong one — it was written
before the shutdown-publish increment landed and was never rechecked, which is precisely
the drift this directory exists to prevent. A count in prose is a second thing to
maintain; the command is the answer.

Treat a spec whose increment is done as history, which this directory keeps in git rather
than in the tree. Removed on that rule: `SNAPSHOT-LIFECYCLE-SPEC.md` and
`OBJECTIZATION-SPEC.md` (2026-08-03) — the first superseded by ADR-0026 increment 3, the
second describing a V2 object kind whose readers no longer exist — and
`DURABILITY-SCHEDULER-SPEC.md` and `RUNTIME-FENCING-SPEC.md` (2026-08-04), whose subjects
ADR-0026 deleted: there is no checkpoint scheduler and no lease-gated ACK for a spec to
govern. Nothing cited either from code. The decisions that outlived them are in the tree,
not in git: the fencing teardown is at `VolumeManager.Fence` and `fencedEpoch`
(`internal/agent/volume.go`), and the one guarantee `wal.Log` still owns about concurrency
is proven by its own tests.

The count that used to open this file ("Eight entries") had been wrong for a while, in the
direction that matters: the directory kept growing and the map did not. A number here is a
second thing to maintain, so there is no longer one — the table is the map.

Conventions, commands, the gate and the review zones are in **`CLAUDE.md`** at the repo
root, because they apply while writing code rather than while planning.

## The rules that keep these files honest

**The design source of truth is the architecture document.** This directory does not
re-design it. Any implementation decision that contradicts, extends or interprets it
needs an **ADR before merge**; any observed doc↔code divergence is a **DEV entry in
`STATUS.md`**, and an open one blocks the gate.

**`STATUS.md` is the only file that tracks state.** Every other file here is either
timeless (invariants, decisions, formats) or forward-looking (risks). That is deliberate:
the previous structure spread the answer to "is this done?" across six files — a phase
plan, a status table, a deviations log, a test backlog and a rebaseline note that said
not to trust the status table — and they drifted apart, which is exactly how a document
comes to claim more than the code delivers.

**History lives in git, not in the tree.** Completed phase plans, closed audit findings
and resolved deviations were deleted rather than archived; every closed finding named the
commit that closed it, so `git log` and `git show` recover the full record. A file kept
"just in case" gets read as current.

**Deleted 2026-07-26, recoverable from git:** `PLAN.md`, `PHASE-01..13.md`,
`REBASELINE.md`, `TEST-GAPS.md`, `TEST-GAPS-PLAN.md`, `DEVIATIONS.md`,
`SCHEMA-FEATURES.md`, `AGENT-MEMORY.md`. What was still live in them is in `STATUS.md`,
`REFERENCE.md` or `CLAUDE.md`.
