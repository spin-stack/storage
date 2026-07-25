# PLAN — Remote Volumes System (MVP)

> **Design source of truth:** `arquitectura_mvp_volumenes_remotos_v5.md` (v5.1).
> This plan does **not** re-design. It converts §30 (roadmap) into an executable,
> incremental plan. Any implementation decision that contradicts, extends, or
> interprets the architecture document requires an ADR before merge
> (`DECISIONS/`), and any observed doc↔code divergence is logged in `DEVIATIONS.md`.

- **Status file:** `STATUS.md` (current increment, blockers, next 3 steps).
- **Invariants:** `INVARIANTS.md` (extracted from §4/§5/§12/§14/§21/§27).
- **Phase detail:** `PHASE-0N.md` (only Phase 1 is expanded in Phase 0; the rest are
  expanded just-in-time by the Planner before the phase starts).

---

## 0. Foundational decisions (see ADRs)

| Decision | Value | ADR |
|---|---|---|
| Language | Go 1.26 | [ADR-0001](DECISIONS/ADR-0001-stack.md) |
| Module path | `github.com/spin-stack/storage` | ADR-0001 |
| Code location | new Go module in `spin-stack/storage/` | ADR-0001 |
| Build tooling | Taskfile (go-task), matching sibling projects | ADR-0001 |
| Lint / static analysis | golangci-lint + custom `simulable` analyzer | [ADR-0003](DECISIONS/ADR-0003-simulable-interfaces-lint.md) |
| CI | GitHub Actions (gates as workflows) | ADR-0001 |
| Test libs | `testify`, `gotest.tools/v3` (stack convention) | ADR-0001 |
| Observability | OpenTelemetry Go v1.38.x (already in stack) | ADR-0001 |
| Execution model | parallel tracks where dependencies allow | [ADR-0002](DECISIONS/ADR-0002-parallel-tracks.md) |

Everything else is decided as an assumption and recorded in the relevant ADR, per the
Phase 0 mandate.

---

## 1. Non-negotiable principles (derived from the doc)

1. **The plan lives in the repo, not in the conversation.** A fresh agent resumes by
   reading `docs/plan/` and the code alone.
2. **Tests and harness before implementation.** No increment writes production logic
   before (a) its failing tests, (b) its DST scenarios / property tests, (c) its
   invariant checkers if it introduces new invariants — exist and fail.
3. **Simulable interfaces from commit 1** (doc §25.1): zero `time.Now()`, direct
   sockets, or disk/network/S3 syscalls outside the clock/net/disk/objectstore
   interfaces. Enforced by an automated lint in CI, not by good intentions
   (ADR-0003).
4. **Gates between increments.** Increment N+1 does not start until N's gate is green.
5. **Deviation = record.** Code never silently wins an argument against the document.
6. **Verify, don't assume:** library versions, external APIs, QEMU 11.0.2 behavior,
   S3 semantics of the target backend — verified against docs or by execution;
   anything unverifiable is marked `Unverified:` in the plan.

---

## 2. Standard gate (Definition of Done for every increment)

An increment is **done** only when all of the following are green:

- [ ] New tests green.
- [ ] Full suite green.
- [ ] Mandatory DST scenario set green (fencing w/ writer partition, lost-PUT,
      crash around append/fdatasync/PUT/ACK, injected clock drift — §25.1).
- [ ] Active invariant checkers green (the ones this or prior increments activated).
- [ ] Simulable-interfaces lint green (ADR-0003).
- [ ] `PLAN.md` / `STATUS.md` / `INVARIANTS.md` updated.
- [ ] Zero open entries in `DEVIATIONS.md` (each resolved by a code fix or an ADR).
- [ ] **If the increment touches on-disk / on-S3 formats:** serialize/replay property
      test with arbitrary truncations and bit corruptions included (§25.2).

**Human-review-required zones** (a human reviews the increment spec *before* the
Harness agent starts, and the diff *before* merge): **on-disk/on-S3 formats,
fencing/leases, durability (ACK rules, FLUSH/FUA ordering), and GC.** These are the
data-loss zones.

---

## 3. Roles (multi-agent workflow — bounded)

Agents communicate **only** through `docs/plan/` files and code, never "from memory".
One increment → one Implementer.

- **Planner** — keeps PLAN/PHASE/STATUS in sync; the only role that may re-cut
  increments; writes no production code.
- **Harness/Test agent** — owns the DST harness, invariant checkers, and property
  tests; writes failing tests **before** the Implementer starts; has veto (if a
  checker cannot be expressed, the increment returns to the Planner).
- **Implementer** — implements exactly one increment within its scope-in; if it needs
  something out of scope, it stops and reports to the Planner (new increment or
  re-cut), never smuggles it in.
- **Adversary/Reviewer** — reviews each diff asking "which doc invariant could this
  break?" and "which failure is uncovered?"; proposes new DST scenarios; hunts
  simulable-interface-lint violations and I/O-class (§11) escapes.
- **Doc-sync agent** — at each increment close, compares implemented behavior against
  referenced §sections; every divergence goes to `DEVIATIONS.md` with a mandatory
  resolution. Open entries block the gate.

### Per-increment ritual

```
1. Planner publishes the increment in PHASE-XX.md (scope, tests, gate)
   → human review if it touches formats/fencing/durability/GC.
2. Harness agent: tests + DST scenarios + checkers, failing. Commit.
3. Implementer: makes the tests pass. Small commits. Nothing out of scope.
4. Adversary: review + extra scenarios for any gaps found.
5. Doc-sync: conformance vs the doc. DEVIATIONS.md clean or with ADRs.
6. Gate: full checklist green → STATUS.md updated → only then, next increment.
```

### Stop signals (halt any agent; record in STATUS.md; escalate to human)

- A test weakened or deleted to make a change pass.
- A `sleep`, magic timeout, or infinite retry instead of a simulable interface.
- Any code touching durability/fencing/GC without an associated DST scenario.
- "I implemented it differently from the doc because it was simpler" without an ADR.
- Two in-flight increments with the same source file as a hot zone.

---

## 4. Phase map (roadmap §30 → phases)

Each roadmap item becomes one phase, split into 3–10-day single-person increments.
Phase 14 (post-MVP) is out of MVP scope and listed for completeness only.

| Phase | Roadmap § | Title | State | Depends on |
|---|---|---|---|---|
| **01** | 1 | Skeleton + simulable interfaces + minimal DST harness + tracing/logging | **DONE** ✓ (1.1–1.4; INV-01/02 active) | — |
| 02 | 2 | Guest layout: three devices + OverlayFS | **expanded (planning); needs VM/mount infra** | 01 |
| 03 | 3 | vhost-user-blk raw backend + reconnection + inflight shmfd | **expanded (planning); needs QEMU 11.0.2** | 01 |
| 04 | 4 | CoW (64 KiB segments) + local WAL (real extents) + format v2 (crypto fields reserved) + WAL property tests | **DONE** ✓ (4.1–4.4; INV-03/04/05/18 active; format review pending) | 01 |
| 05 | 5 | Per-volume encryption (DEK/KEK, dev KMS) + DISCARD/WRITE_ZEROES | **DONE** ✓ (5.1–5.2; INV-15 active) | 04 |
| 06 | 6 | Remote WAL: on-demand batching + PUT idempotency + summary objects | not expanded | 04 (05 for ciphertext) |
| 07 | 7 | PostgreSQL + Control Plane (verified term) + reconciliation + leases + full fencing protocol under DST | not expanded | 06 |
| 08 | 8 | Recovery with S3 as authority + recovery-point + basic `rebuild-metadata` | not expanded | 06, 07 |
| 09 | 9 | Pause-free snapshots + same-host clones + online resize (grow) | not expanded | 08 |
| 10 | 10 | Objectization + checkpoints + GC mark-and-sweep + versioned/Object-Lock buckets | not expanded | 08 |
| 11 | 11 | Cross-host via full materialization + cordon/drain + capacity accounting | not expanded | 08, 09, 10 |
| 12 | 12 | Warm standby + WAL-object compaction + chain flattening | not expanded | 10, 11 |
| 13 | 13 | Hardening: real-hardware fault injection + backend conformance suite + measured-time runbooks | not expanded | all |
| 14 | 14 | *(post-MVP)* lazy loading, multi-queue, io_uring, selective FUA flush, tenant QoS, S3-based lease renewal | out of scope | — |

MVP "done" = Phases 01–13, evaluated against the §31 success criteria.

---

## 5. Dependencies and parallel tracks (ADR-0002)

Phase 01 is structural and blocks everything. After it, work fans into tracks that
touch mostly-disjoint files. **Hot-zone rule:** two in-flight increments must not share
a hot source file; the Planner assigns hot zones and refuses overlapping schedules.

```
                          ┌─────────────────────────── Phase 01 (blocks all) ──────────────────────────┐
                          │  simulable interfaces · DST harness · checker framework · OTel/logging       │
                          └───────────────┬───────────────────────┬───────────────────────┬─────────────┘
                                          │                       │                       │
        Track A (guest/data path)   Track B (durable/remote)   Track C (control plane)   Track D (S3 client subsystem, §24)
        02 layout ──► 03 vhost ──► 04 WAL/CoW ──► 05 crypto     06 remote WAL ──► 08 recovery   07 CP+fencing            (hedged GET, retry budget,
                                     │                            │ (needs 04)      (needs 06,07)  (needs 06)              circuit breaker — can start
                                     └──────────────► 06 ◄────────┘                                                        after 01, feeds 06/08/11)
                          09 snapshots/clone/resize (needs 08) · 10 objectization/GC (needs 08) · 11 cross-host (needs 08,09,10)
                          12 standby/compaction/flatten (needs 10,11) · 13 hardening (needs all)
```

Concretely allowed to run in parallel once Phase 01 is green:
- **Track A** (guest + data path): 02 → 03 → 04 → 05.
- **Track D** (S3 client subsystem, §24): independent library work; disjoint files;
  feeds 06/08/11. Can proceed alongside Track A.
- **Track B** (06) starts once 04 lands the WAL format; **Track C** (07) once 06 lands
  epoch objects; recovery (08) once both 06 and 07 land.

Hot zones to watch (Planner-owned, single-writer at a time):
- WAL record/object format (`internal/wal/format`) — Phases 04, 05, 06, 12.
- Lease/epoch state (`internal/lease`, CP epoch logic) — Phases 07, 08.
- Object-store keyspace/layout — Phases 06, 08, 10, 12.

---

## 6. Proposed package layout (matches spinbox conventions)

> Detailed in Phase 01, increment 1.1. Sketch only — not a design change; mirrors the
> sibling `spinbox` layout (`internal/`, `cmd/`, `api/`, `integration/`, `hack/`,
> `deploy/`).

```
storage/
├── go.mod                       # module github.com/spin-stack/storage (go 1.26)
├── Taskfile.yml                 # build/test/lint/dst targets
├── .golangci.yml                # + custom "simulable" analyzer (ADR-0003)
├── .github/workflows/ci.yml     # gates: build, test, lint, dst-mandatory, invariants
├── api/                         # gRPC/proto: Control Plane ⇄ Agent, client ⇄ CP
├── cmd/
│   ├── volume-agent/
│   ├── control-plane/
│   └── volctl/                  # CLI incl. rebuild-metadata, drain, snapshot...
├── internal/
│   ├── simio/                   # simulable interfaces: clock, net, disk, objectstore
│   │   ├── clock/  net/  disk/  objectstore/
│   │   ├── real/                # thin passthrough implementations
│   │   └── sim/                 # deterministic simulated implementations
│   ├── dst/                     # deterministic scheduler, seed, fault injection, checkers
│   ├── obs/                     # OTel tracing + structured logging + metrics registry
│   ├── wal/                     # WAL record/object format, batcher, replay (Phase 04+)
│   ├── cow/                     # segments, active map (roaring bitmaps) (Phase 04+)
│   ├── crypto/                  # AES-256-GCM, DEK/KEK, KMS client (Phase 05+)
│   ├── objectclient/            # S3 subsystem: hedged GET, retry budget, CB (Phase §24)
│   ├── agent/                   # Volume Agent state machine (Phase 03+)
│   ├── lease/                   # lease manager (monotonic clock) (Phase 07+)
│   ├── controlplane/            # reconciler, term, promotion (Phase 07+)
│   └── recovery/                # durable-point determination, rebuild-metadata (Phase 08+)
├── integration/                 # real-hardware fault injection, QEMU 11.0.2 (Phase 13)
├── hack/                        # backend conformance suite, dev bootstrap
└── deploy/{config,systemd}/
```

---

## 7. Change log of the plan itself

- **Phase 0 (this document):** initial plan generated from v5.1. Foundational
  decisions confirmed with the human (Go / module-in-storage / GitHub Actions /
  parallel tracks). Phase 01 fully expanded; Phases 02–13 mapped at high level.
  Awaiting human approval of PLAN.md + PHASE-01.md before any production code.
