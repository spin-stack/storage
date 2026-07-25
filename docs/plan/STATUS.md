# STATUS

Short snapshot + resume-from-here handoff. **Read this first** when picking up the
work, then `REBASELINE.md` — a human review on 2026-07-25 found this file claiming
more than the repository does, and the maturity model below is the correction.

- **Date:** 2026-07-25 (rebaselined)
- **Where the work is:** everything is on **`main`**, pushed to `origin`
  (`/home/aledbf/spin-storage.git`, a bare repo — the old bundle remote is gone).
- **Gate on `main`:** `task ci` green; `task cover` 90.2% (>= 90 floor);
  `task test:integration` green on Postgres 18; `task backend:conformance` green
  against the pinned RustFS; `task build:qemu` + `task qemu:verify` green.

## Maturity, not "done"

| State | Meaning |
|---|---|
| **model** | Library logic with unit/property/DST coverage. No integrated caller, no real I/O path. |
| **integrated** | Wired into a running binary through the real interfaces, exercised end to end. |
| **production-verified** | Real hardware/backends under fault injection, telemetry recorded, runbook times measured. |

**Nothing here is `integrated` or `production-verified` yet.** There is no `cmd/`, no
`api/`, no Agent, no vhost-user path, no deployment: no VM is served by this code.
Nine roadmap phases have merged increments — as models.

| Phase | State | Notes |
|---|---|---|
| 0 planning | done | — |
| 01 skeleton (simio + DST harness + obs) | **model** | obs is a registry only — nothing records metrics (DEV-0010) |
| 02 guest layout / 03 vhost-user | **not started** | the QEMU 11.0.2 build now exists (`task build:qemu`); the guest/vhost work does not |
| 04 WAL/CoW format + property tests | **model** | format review still pending (human-review zone) |
| 05 encryption (AES-256-GCM, DEK/KEK) | **model** | — |
| 06 remote WAL (batching, idempotent PUT, summary) | **model** | — |
| 07 Control Plane + leases + fencing | **model, with gaps** | fail-open lease + non-atomic promotion (DEV-0004); operations not term-guarded (DEV-0005) |
| 08 recovery (S3 authority) + rebuild-metadata | **model, with gaps** | unvalidated objects can raise the durable point (DEV-0003); rebuild covers volumes only (DEV-0009) |
| 09 snapshots + clone + resize | **partial model** | synchronous sealing, no chain link persisted (DEV-0007) |
| 10 objectization + checkpoints + GC + I/O classes | **partial model** | no segment objects, GC computes but does not mark (DEV-0006, DEV-0007) |
| 11 cross-host + cordon/drain + capacity | **partial model** | the materialized view is discarded, drain not idempotent after promotion (DEV-0007, DEV-0008) |
| 12 warm standby + compaction + flatten | **not started** | paused by the rebaseline |
| 13 hardening + fleet-mixed | **13.1 model** (typed lifecycles, ADR-0009) | 13.2–13.4 need infra; INV-19 still pending |

## Invariants: 21 checkers run, but read the caveats

`INVARIANTS.md` marks 21 of 22 active. The rebaseline narrows four of those claims —
INV-06 (fencing is opt-in, not structural), INV-08/09 (the durable point is derived
from unvalidated headers), INV-14 (the interface does expose permanent deletion and
the GC does not mark), INV-20 (volumes only). Each carries a caveat pointing at its
deviation. A checker that passes against a model is not a proof about a system.

## Infrastructure available (2026-07-25)
- **QEMU 11.0.2** — `task build:qemu` -> `_output/bin/` + firmware; `task qemu:verify`
  asserts the version and `vhost-user-blk-pci`. Unblocks the tooling half of 02/03.
- **Object store** — RustFS pinned by digest via TestContainers (`internal/testinfra`);
  `task backend:conformance` runs the §6.1 suite (ADR-0010). Every requirement passes
  except throttling, which is not exercisable on demand.
- **One S3 client** — `internal/simio/real.NewS3Store`, the only place the AWS SDK is
  configured, proven by the shared `objectstore` contract (`storetest`) against the
  real backend.

## What to do next (from REBASELINE.md — features are paused)
1. Object integrity + fail-closed durability (DEV-0003, DEV-0004 first half).
2. Staged, idempotent promotion and drain (DEV-0004, DEV-0008).
3. Term-guard the remaining CP mutations + a structural test (DEV-0005).
4. Reversible delete and a GC that marks (DEV-0006).
5. The spine: `api/`, `cmd/volume-agent`, `cmd/control-plane`, `cmd/volctl`, then
   Phases 02/03 against the QEMU build. Only then can anything become *integrated*.
6. Reopen 09/10/11 as integration work; then telemetry (DEV-0010).

## How to resume
1. Read `REBASELINE.md`, then `PLAN.md` (phase map), `INVARIANTS.md`, `CLAUDE.md`.
2. Ritual per increment: failing tests/DST/checkers **in their own commit**, then the
   implementation, then the doc update — the three-in-one commits are what let the
   documentation drift from the code.
3. Branch per phase off `main`; human review before merge for data-loss zones
   (formats, fencing, durability, GC); merge `--ff-only`.
4. Commands: `task tools` first, then `task ci`, `task cover`, `task test:integration`,
   `task backend:conformance`, `task dst`, `task generate`/`generate:check`,
   `task db:migrate:diff -- <name>`, `task build:qemu`. Tools are never invoked
   directly — versions live in `Taskfile.yml`.

## Session gotchas worth remembering (also in AGENT-MEMORY.md)
- **ADR-0005:** WAL headers are **104 bytes**, not the doc's "96".
- **ADR-0006/0007:** all SQL via sqlc; Atlas migrations; Postgres 18; UUIDv7 enforced
  in code and by DB CHECK.
- **ADR-0008:** drain moves a volume from its durable prefix in S3, fencing first.
- **ADR-0009:** lifecycles are typed (`internal/lifecycle`), enforced by Go types,
  transition-guarded UPDATEs, and DB CHECKs.
- **ADR-0010:** one S3 wrapper; RustFS answers `If-Match` on a missing key with
  `NoSuchKey` (not 412); multipart ETags carry `-N`; LIST pages cap at 1000 keys.
- Do not re-add spinbox's `CONFIG_CXL=n` QEMU debloat (breaks the 11.0.2 link); bump
  `QEMU_CONFIG_REV` when configure flags change.
- Coverage excludes generated `internal/db`, integration-only `metadata/pg`,
  `simio/real/s3.go`, `testinfra`, `migrations`, `cmd`, and the `dst` harness.
