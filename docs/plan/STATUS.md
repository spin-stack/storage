# STATUS

Short snapshot. Update at every increment close.

- **Date:** 2026-07-24
- **Current phase:** Phase 01 (skeleton) — **in progress**. Plan approved by human.
- **Current increment:** **1.1 complete** (module + CI + simulable lint); **1.2 next**
  (simulable interfaces).
- **Blockers:** none. Working on branch `phase-01/increment-1.1-skeleton-lint`.

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

1. **Increment 1.2** — define the four `simio` interfaces (clock, net, disk,
   objectstore) with real + deterministic-sim implementations and contract tests.
   Tests-first: contract suites run against both impls; sim-determinism property test.
2. **Increment 1.3** — deterministic DST harness + checker framework + fault hooks +
   the planted-bug test. Activates **INV-02**; Adversary-reviewed.
3. **Increment 1.4** — OTel tracing + structured logging + full §26.2 metric-name
   registry. Then Phase 01 exit gate → unlock Track A (Phase 02) and Track D (S3
   subsystem) per ADR-0002.

## Open questions for the human (non-blocking; defaults recorded as assumptions)

- Property-test library (`pgregory.net/rapid` assumed, ADR-0001) — confirm at Phase 04.
- Dev backend for the conformance sim vs real (MinIO/RustFS single-node dev only per
  §6.2) — confirm at Phase 06/13.
