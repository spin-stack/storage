# docs/plan — the map

Eight entries. Each answers one question, and no two answer the same one.

| I want to know… | Read |
|---|---|
| What is done, what is partial, what is missing, what to do next | **`STATUS.md`** |
| What has to be built, in what order, to serve one volume end to end | **`BUILD-INVENTORY.md`** |
| What does `§14.4` / `INV-13` / `ADR-0017` / `DEV-0007` in this comment mean? | **`REFERENCE.md`** — one line each, no other file needed |
| What property must hold, and which checker proves it | `INVARIANTS.md` |
| Why was this decided, and what was rejected | `DECISIONS/ADR-NNNN-*.md` |
| What could still go wrong, and what forces a re-review | `RISKS.md` |
| The on-disk segment format | `WAL-SEGMENTS-SPEC.md` |
| How the system is supposed to work | `../../arquitectura_mvp_volumenes_remotos_v5.md` (v5.1, Spanish) |

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
