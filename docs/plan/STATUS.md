# STATUS

Short snapshot. Update at every increment close.

- **Date:** 2026-07-24
- **Current phase:** **Phase 01 COMPLETE** (skeleton barrier). Plan approved by human.
- **Increments:** 1.1, 1.2, 1.3, 1.4 all done. **INV-01 + INV-02 active.**
- **Blockers:** none. Branch `phase-01/increment-1.1-skeleton-lint`. Phase 01 exit gate
  green; awaiting human steer on what to build next (see "Next 3 steps").

## Increment 1.4 — DONE

- `internal/obs`: OTel tracing + W3C context propagation across the `simio.network`
  boundary (`InjectContext`/`ExtractContext`), structured JSON logging keyed on
  request_id/operation_id/volume_id/host_id + trace_id/span_id, and the full §26.2
  metric taxonomy as a declarative `Catalog()` built into live OTel instruments.
- Tests: trace propagates across a CP→Agent boundary; nested CP→Agent→objectstore
  spans share a trace; structured log carries all correlation fields; metrics catalog
  well-formed (no dupes, load-bearing names present) and every entry registered.

## Phase 01 exit gate — GREEN

- Module builds; `task ci` = build + lint(golangci + simulable) + test(-race) + dst.
- INV-01 (simulable lint) + INV-02 (deterministic replay) active and green.
- Four `simio` interfaces (real + sim) contract-tested; DST harness + checker
  framework + planted-bug proof; OTel + logging + §26.2 registry.
- No `time.Now()`/socket/disk/S3 escape anywhere (lint proves it).

## Increment 1.3 — DONE

- `internal/dst`: seeded harness (`Run(seed, scenario, checkers...)`) driving the sim
  interfaces, deterministic event trace, and a `Checker` framework.
- Checkers wired: `MonotonicClockChecker` (§12.1) and `NoPermanentDeleteChecker`
  (INV-14 seed). `DefaultCheckers()` is the Phase-01 set that later phases append to.
- Mandatory scenario set (interface-level arms): lost-PUT idempotent retry (§14.5),
  crash-around-fdatasync (durable-prefix survives), clock-drift-beyond-skew (§12.1:
  monotonic unaffected), network partition/heal (§12/§23).
- **Planted-bug tests** prove the checkers actually catch violations and the failure
  reports the reproducing seed (Adversary requirement).
- `task dst` now runs the set for real; wired into CI. **INV-02 → active.**

## Increment 1.2 — DONE

- Four `simio` interfaces with real + deterministic-sim impls, all contract-tested
  against each other (67 tests, `-race` clean):
  - `clock` — monotonic + wall + timers; sim `Advance`/`SetSkew`/`PendingTimers`
    (quiescence). Surfaced and fixed a real registration race in `Sleep`.
  - `disk` — append/read/sync/truncate + crash model (unsynced lost); sim faults:
    short append, sync-loss, torn tail.
  - `objectstore` — Put(If-None-Match/If-Match CAS)/Get/Head/List/Delete; sim faults:
    lost-response (§14.5 idempotent-retry proven), throttle, eventual LIST.
  - `network` — message transport; sim partition/heal; real TCP framed. (Named
    `network` to avoid shadowing stdlib `net` — minor rename from the PLAN §6 sketch.)
- Determinism property test (seed of **INV-02**): sim components produce identical
  traces for identical seeds.
- **ADR-0004**: Phase-01 `real` objectstore is filesystem-backed; S3-SDK impl deferred
  to Track D (§24).

## Increment 1.1 — DONE

- Go module `github.com/spin-stack/storage` (go 1.26), Taskfile, `.golangci.yml` (v2),
  `.github/workflows/ci.yml`, stub package tree per PLAN §6.
- Custom `simulable` analyzer (`hack/analyzers/simulable`) with golden tests
  (violating/compliant/exempt) — all green; resolves aliased imports by type, ignores
  local same-named methods.
- Belt-and-suspenders `forbidigo`/`depguard` in golangci-lint (both layers proven to
  flag a planted `time.Now()`; removed after demonstration).
- `task ci` (build + lint + test + dst-placeholder) green locally.
- **INV-01 → active.** Gate proof done: a planted `time.Now()` turns lint red.

## What exists

- `PLAN.md` — full phase map (roadmap §30 → Phases 01–13), dependencies, parallel
  tracks, standard gate, roles, hot zones.
- `INVARIANTS.md` — 21 invariants (INV-01…INV-21) with checkers and activating phases;
  all `pending` (nothing built yet).
- `PHASE-01.md` — fully expanded into 4 increments (1.1 bootstrap+lint, 1.2 simulable
  interfaces, 1.3 DST harness+checkers, 1.4 observability).
- `DECISIONS/ADR-0001` (stack/layout/CI), `ADR-0002` (parallel tracks), `ADR-0003`
  (simulable-interfaces lint).
- `DEVIATIONS.md` (empty), `RISKS.md` (RISK-01…RISK-10), this file.

## Confirmed foundational decisions

Go 1.26 · module `github.com/spin-stack/storage` in `spin-stack/storage/` · Taskfile +
golangci-lint (+ custom `simulable` analyzer) · GitHub Actions · parallel tracks after
Phase 01. (ADR-0001/0002/0003.)

## Next 3 steps (needs human steer)

Phase 01 barrier is done, so parallel tracks (ADR-0002) are unlocked. Options:

1. **Track A — Phase 02** (guest layout: three devices + OverlayFS) and **Phase 03**
   (vhost-user-blk + QEMU 11.0.2 inflight shmfd): highest-value data path, but need
   real infrastructure (mounts, QEMU) **not present in this sandbox** — best done where
   that infra exists; RISK-10 (inflight-shmfd) is *Unverified* until then.
2. **Track A — Phase 04** (CoW 64 KiB + local WAL, real extents, format v2 w/ crypto
   fields + WAL property tests §25.2): **pure Go, fully buildable here**, and the
   correctness spine (activates INV-03/04/05/18). Strong candidate to continue now.
3. **Track D — S3 client subsystem** (§24: hedged GETs, retry budget, circuit breaker
   over the `objectstore` interface): pure Go, buildable here, feeds Phase 06/08/11.

Recommendation: continue with **Phase 04** here (pure Go, unblocks 05/06), and schedule
Phases 02/03 for an environment with QEMU/mounts.

## Open questions for the human (non-blocking; defaults recorded as assumptions)

- Property-test library (`pgregory.net/rapid` assumed, ADR-0001) — confirm at Phase 04.
- Dev backend for the conformance sim vs real (MinIO/RustFS single-node dev only per
  §6.2) — confirm at Phase 06/13.
