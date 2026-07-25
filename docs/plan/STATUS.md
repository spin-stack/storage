# STATUS

Short snapshot + resume-from-here handoff. **Read this first** when picking up the work.

- **Date:** 2026-07-25
- **Where the work is:** Phases 0–11 are on **`main`** (Phase 11 merged `--ff-only`
  after review). **Increment 13.1 (typed lifecycles) is on branch
  `hardening/typed-lifecycles`.** The per-phase branches are history labels; `main` is
  authoritative.
- **Gate:** `task ci` green; `task cover` ≥ 90% production floor (90.1%);
  `task test:integration` green on Postgres 18 (Docker).

## Progress: 10 of 13 MVP phases done; 21 of 22 invariants active

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
| 11 cross-host + cordon/drain + capacity | ✓ (merged) | none new — extends INV-08/09/10/11/16/17 |
| 12 warm standby + WAL compaction + flatten | not started | — |
| 13 hardening + fleet-mixed | 13.1 typed lifecycles ✓ (branch); 13.2–13.4 need infra | INV-19 (only one left) |

**Only INV-19 (fleet-mixed read-old/write-new format gating) is still pending** — Phase 13.

## What Phase 11 added (branch `phase-11/cross-host-drain`)

- `internal/placement` — the §20 placement order (source → cached/warm-standby → any)
  bounded by the §28.2 oversubscription policy. Pure and order-independent.
- `metadata.Store` fleet surface — `ListHosts`, `SetHostState` (cordon/drain/dead),
  `CommitHostCapacity` (non-negative guard), `ListVolumesByHost`, `UpdateOperation`
  (visible operation progress). Both `sim` and `pg` (+ sqlc queries, TestContainers).
- `internal/materialize` — rebuild a volume on another host from the object store alone
  (snapshot manifest, checkpoint, or an epoch's durable prefix). Background class, and it
  refuses missing objects / sequence gaps / digest mismatches rather than producing a
  partial volume.
- `controlplane.CloneCrossHost` — clone onto a host that never had the data; capacity
  committed before the fetch, released on any failure.
- `controlplane.Drainer` — reconciled, resumable, cancelable host evacuation:
  materialize → fence (FENCING_WAIT + promote + epoch CAS) → final materialize →
  recovery point → release. Cancels only at a volume boundary.
- Two new mandatory DST scenarios: `cross-host-materialization`,
  `drain-moves-volumes-fenced`.
- **ADR-0008 / DEV-0002:** drain evacuates from the durable prefix, not from a
  source-taken snapshot (the doc's phrasing needs a live source + the Agent RPC).

## How to resume
1. Read `docs/plan/PLAN.md` (phase map), `docs/plan/INVARIANTS.md` (21/22 active, each with
   its checker), and `CLAUDE.md` (conventions — read it before writing code).
2. Ritual per increment: Planner publishes the increment in `PHASE-0N.md` → Harness writes
   failing tests/DST scenarios/checkers → Implementer makes them pass → gate green → commit.
3. Branch per phase off `main`; **human review before merge** for data-loss zones
   (formats, fencing, durability, GC). Merge to `main` with `--ff-only` after review.
4. Commands: `task ci`, `task cover`, `task test:integration`, `task dst`, `task generate`
   (sqlc), `task db:migrate:diff -- <name>` (Atlas). Atlas installed via atlasgo.sh;
   `sqlc` via `go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.30.0` (lands in `~/go/bin`).

## Next candidates (all pure-Go / DST-provable except where noted)
- **Merge `hardening/typed-lifecycles`** (increment 13.1, ADR-0009).
- **Phase 12** — warm standby (checkpoint hydration; `materialize.FromCheckpoint` is the
  building block) + WAL-object compaction + chain flatten.
- **Phase 13** — hardening: real-HW fault injection, backend conformance suite, runbooks,
  and INV-19. Needs real infra.
- **Phases 02/03** — guest layout + vhost-user/QEMU 11.0.2. Need VM/mount infra (RISK-10).
  These also unblock the Agent-side half of drain (quiesce + release lease).

## Session gotchas worth remembering (also in AGENT-MEMORY.md)
- **ADR-0005:** WAL headers are **104 bytes**, not the doc's "96" (field lists sum to 104).
- **ADR-0006/0007:** all SQL via sqlc; migrations via Atlas; Postgres 18; UUIDv7 enforced two
  ways (forbidigo forbids v4 outside `internal/ids`; DB CHECK on the version nibble).
- **ADR-0008:** drain moves a volume from its durable prefix in S3, fencing before the
  destination can write.
- **ADR-0009:** lifecycles are typed (`internal/lifecycle`) and enforced three ways —
  Go types, transition-guarded UPDATEs, and DB CHECKs. Stored as TEXT (not PG enums,
  not int codes); binary formats keep their numeric enums. Adding a state means
  touching the constant, the transition table, and a migration.
- Two real bugs the tests caught: `Log.Discard/WriteZeroes` didn't feed the remote batcher
  (DISCARD never reached S3); the sim network let a **closed** conn still Send. Both fixed.
- Coverage is measured with `-coverpkg=./...`; the 90% floor excludes generated `internal/db`,
  integration-only `pg`/`migrations`, `cmd`, and the `dst` harness. Phase 11 sits at 90.1%,
  so new code needs its error paths tested, not just its happy path.
- Postgres `jsonb` round-trips by value, not byte-for-byte: compare parsed JSON in tests.
