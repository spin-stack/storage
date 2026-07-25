# STATUS

Short snapshot. Update at every increment close.

- **Date:** 2026-07-24
- **Current phase:** Phase 01 (skeleton) — **in progress**. Plan approved by human.
- **Current increment:** **1.1 + 1.2 complete**; **1.3 next** (DST harness + checkers).
- **Blockers:** none. Working on branch `phase-01/increment-1.1-skeleton-lint`.

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

## Next 3 steps

1. **Increment 1.3** — deterministic DST harness + checker framework + fault hooks +
   the planted-bug test. Activates **INV-02**; Adversary-reviewed.
2. **Increment 1.4** — OTel tracing + structured logging + full §26.2 metric-name
   registry.
3. **Phase 01 exit gate** → reassess with the human: Track A (Phase 02: guest layout,
   OverlayFS) and Phase 03 (vhost-user/QEMU 11.0.2) need real infrastructure not present
   in this sandbox; Track D (S3 subsystem) is pure Go and can proceed here.

## Open questions for the human (non-blocking; defaults recorded as assumptions)

- Property-test library (`pgregory.net/rapid` assumed, ADR-0001) — confirm at Phase 04.
- Dev backend for the conformance sim vs real (MinIO/RustFS single-node dev only per
  §6.2) — confirm at Phase 06/13.
