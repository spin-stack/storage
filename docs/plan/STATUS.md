# STATUS

Short snapshot + resume-from-here handoff. **Read this first** when picking up the work.

- **Date:** 2026-07-25
- **Where the work is:** everything is on branch **`main`** through **Phase 10**
  (34 commits). The per-phase branches (`phase-01/…` … `phase-10/…`) are all
  fast-forward-merged into `main` and are just history labels — `main` is authoritative.
- **Gate:** `task ci` green; `task cover` ≥ 90% production floor; `task test:integration`
  green on Postgres 18 (Docker).

## Progress: 9 of 13 MVP phases done; 21 of 22 invariants active

| Phase | Status | Invariants activated |
|---|---|---|
| 0 planning | ✓ | — |
| 01 skeleton (simio + DST harness + obs) | ✓ | INV-01, 02 |
| 02 guest layout / 03 vhost-user | **planned only** (need VM/QEMU infra) | — |
| 04 WAL/CoW format + property tests | ✓ | INV-03, 04, 05, 18 |
| 05 encryption (AES-256-GCM, DEK/KEK) | ✓ | INV-15 |
| 06 remote WAL (batching, idempotent PUT, summary) | ✓ | INV-07, 21 |
| 07 Control Plane + leases + **fencing** | ✓ | INV-06, 09, 10, 11, 22 |
| 08 recovery (S3 authority) + rebuild-metadata | ✓ | INV-08, 12, 20 |
| 09 pause-free snapshots + clone + resize | ✓ | INV-16 |
| 10 objectization + checkpoints + GC + I/O classes | ✓ | INV-13, 14, 17 |
| 11 cross-host + cordon/drain | not started | — |
| 12 warm standby + WAL compaction + flatten | not started | — |
| 13 hardening + fleet-mixed | not started | INV-19 (only one left) |

**Only INV-19 (fleet-mixed read-old/write-new format gating) is still pending** — Phase 13.

## How to resume
1. Read `docs/plan/PLAN.md` (phase map), `docs/plan/INVARIANTS.md` (21/22 active, each with
   its checker), and `CLAUDE.md` (conventions — read it before writing code).
2. Ritual per increment: Planner publishes the increment in `PHASE-0N.md` → Harness writes
   failing tests/DST scenarios/checkers → Implementer makes them pass → gate green → commit.
3. Branch per phase off `main`; **human review before merge** for data-loss zones
   (formats, fencing, durability, GC). Merge to `main` with `--ff-only` after review.
4. Commands: `task ci`, `task cover`, `task test:integration`, `task dst`, `task generate`
   (sqlc), `task db:migrate:diff -- <name>` (Atlas). Atlas installed via atlasgo.sh.

## Next candidates (all pure-Go / DST-provable except where noted)
- **Phase 11** — cross-host materialization (via S3) + cordon/drain + capacity accounting.
- **Phase 12** — warm standby (checkpoint hidration) + WAL-object compaction + chain flatten.
- **Phase 13** — hardening: real-HW fault injection, backend conformance suite, runbooks,
  and INV-19. Needs real infra.
- **Phases 02/03** — guest layout + vhost-user/QEMU 11.0.2. Need VM/mount infra (RISK-10).

## Session gotchas worth remembering (also in AGENT-MEMORY.md)
- **ADR-0005:** WAL headers are **104 bytes**, not the doc's "96" (field lists sum to 104).
- **ADR-0006/0007:** all SQL via sqlc; migrations via Atlas; Postgres 18; UUIDv7 enforced two
  ways (forbidigo forbids v4 outside `internal/ids`; DB CHECK on the version nibble).
- Two real bugs the tests caught: `Log.Discard/WriteZeroes` didn't feed the remote batcher
  (DISCARD never reached S3); the sim network let a **closed** conn still Send. Both fixed.
- Coverage is measured with `-coverpkg=./...` (default under-counts cross-package); the 90%
  floor excludes generated `internal/db`, integration-only `pg`/`migrations`, `cmd`, and the
  `dst` harness.
