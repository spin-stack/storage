# PHASE 01 — Skeleton, simulable interfaces, minimal DST harness, observability

> **Roadmap §30.1:** *"Skeleton with simulable interfaces (clock/net/disk/S3) +
> minimal DST harness + tracing/structured logging. No direct `time.Now()` from the
> first commit."*
>
> **Why this phase is first and blocks everything:** §25.1 and §29.7 are explicit —
> simulable interfaces and invariant checkers are *impossible to retrofit*. This phase
> buys the structural properties (INV-01, INV-02) and the checker framework so every
> later data invariant is a fill-in, not a rebuild. **No production data-path logic is
> written here.**

**Phase objective (one sentence):** stand up the Go module, the four simulable I/O
interfaces (real + deterministic-sim implementations), a deterministic DST harness with
a checker framework and fault-injection hooks, the CI gates, and OTel tracing +
structured logging + metrics registry — with nothing touching real time, sockets, or
disk outside `simio`.

**Human-review-required:** none of these increments touch formats/fencing/durability/GC,
so no pre-start human review is mandated. Increment 1.3 (harness) is reviewed by the
Adversary with extra care because every later data-loss proof rests on it.

**Invariants this phase activates:** INV-01 (simulable-interfaces lint) → `active`;
INV-02 (deterministic replay) → `active`. All data invariants stay `pending`.

---

## Increment 1.1 — Module bootstrap + CI + simulable-interfaces lint

**Objective:** create the `github.com/spin-stack/storage` Go module with the package
skeleton, Taskfile targets, GitHub Actions CI, and the **custom lint that fails the
build on any direct `time.Now()` / socket / disk / S3 syscall** outside `internal/simio`.

**Scope in:**
- `go.mod` (go 1.26), `Taskfile.yml` (`build`, `test`, `lint`, `dst`, `ci` targets),
  `.golangci.yml` extending the sibling config.
- Empty package tree per PLAN §6 (compilable stubs with doc comments, no logic).
- `.github/workflows/ci.yml` running: `build`, `test`, `lint` (incl. the custom
  analyzer), and a placeholder `dst` job (green with zero scenarios for now).
- The `simulable` static analyzer (`hack/analyzers/simulable`, a `go/analysis` pass)
  **plus** `forbidigo`/`depguard` rules as belt-and-suspenders: forbid `time.Now`,
  `time.Since`, `time.Sleep`, `time.After`, `net.Dial*`, `os.Open/Create/OpenFile`,
  raw `syscall.*` disk/net calls, and direct S3 SDK construction outside `internal/simio`
  and `internal/simio/real`.

**Scope out:** any interface *definitions* (that's 1.2); any real functionality.

**Doc sections implemented:** §25.1 (simulable interfaces mandate, enforcement),
§27.5 (magic/version discipline noted for later), PLAN §0/§6 (stack, layout).

**Tests first (Harness agent writes these failing):**
- `analyzer_test.go`: golden testdata packages — one with a deliberate `time.Now()`
  outside `simio` (analyzer must flag), one compliant (must pass). Uses
  `analysistest.Run`.
- `depguard`/`forbidigo` config test: a fixture file importing a forbidden symbol fails
  `task lint`; a compliant fixture passes.
- CI smoke: `task ci` exits non-zero when a seeded violation is present.

**Invariants activated:** INV-01 → `active` (this is its checker).

**Metrics added:** none (observability lands in 1.4).

**Gate (DoD):** standard gate (PLAN §2). Specifically: `task ci` green on the clean
tree; the two analyzer golden tests green; a deliberately-planted `time.Now()` turns CI
red (demonstrated in the PR, then removed).

**Rollback:** the whole increment is scaffolding; revert the branch. No runtime flag
needed (nothing runs yet).

---

## Increment 1.2 — Simulable interfaces: clock, net, disk, objectstore

**Objective:** define the four `simio` interfaces and ship two implementations each — a
thin real passthrough and a deterministic simulated one — with contract tests proving
both satisfy the same behavior and the sim is reproducible from a seed.

**Scope in:**
- `internal/simio/clock`: `Clock` with **monotonic** and **wall** sources
  (`Monotonic() time.Duration-like`, `Wall() Time`, `NewTimer`, `Sleep` via callback);
  real impl wraps the runtime; sim impl advances a virtual clock the harness controls.
- `internal/simio/net`: minimal message/stream transport abstraction (dial, send,
  recv, close) sufficient for CP⇄Agent and Agent⇄objectstore later; real (TCP/gRPC
  dialer) + sim (in-memory, deterministic delivery order, partitionable).
- `internal/simio/disk`: append/read/fsync/fdatasync/truncate/rename over a file
  abstraction; real (POSIX) + sim (in-memory with injectable partial writes,
  fsync-loss, truncation-at-byte).
- `internal/simio/objectstore`: `PUT (If-None-Match), GET, HEAD, LIST, tags/versions`;
  real (S3 SDK) + sim (in-memory, CAS-capable, injectable lost-response, throttle,
  LIST-after-PUT consistency knob).
- Fault-injection hooks on every sim impl, driven by the harness (1.3), not by wall
  time.

**Scope out:** the harness scheduler itself (1.3); any WAL/format/crypto logic; the
real objectstore's hedging/retry (that is the Track-D S3 subsystem, §24, later).

**Doc sections implemented:** §25.1 (the four interfaces), §12.1 (monotonic vs wall
clock separation — the security-critical distinction), §24 (objectstore surface: CAS,
LIST, HEAD), §14.5 (`If-None-Match:*` surface).

**Tests first:**
- **Contract tests** (table-driven, run against *both* impls): each interface's
  behavior spec — e.g. objectstore `PUT If-None-Match:*` twice ⇒ second is 412;
  disk `fdatasync` then crash ⇒ data present; clock monotonic never goes backward.
- **Determinism property test:** sim impls produce identical event logs for identical
  seeds (feeds INV-02).
- **Fault-injection unit tests:** each injectable fault (lost PUT response, partial
  write, throttle, partition, clock drift) is observable and reproducible by seed.

**Invariants activated:** none flip yet (INV-02 needs the harness in 1.3), but the
determinism property test is the seed of INV-02.

**Metrics added:** none.

**Gate:** standard gate. Both impls pass identical contract suites; sim determinism
property test green.

**Rollback:** interfaces are additive; nothing wired into a running binary. Revert
branch if needed.

---

## Increment 1.3 — Deterministic DST harness + checker framework + fault hooks

**Objective:** a deterministic simulation harness — seeded scheduler driving the sim
interfaces, fault injection at every decision point, an event trace, and a
**checker-framework** with at least one trivial checker wired end-to-end — plus the
first mandatory CI scenarios (as far as they can be expressed with no data path yet).

**Scope in:**
- `internal/dst`: seeded pseudo-random source (explicit seed in, no `Math.rand`-style
  global), a deterministic scheduler stepping the sim clock/net/disk/objectstore, an
  event recorder, and a scenario runner.
- **Checker framework:** a `Checker` interface (`Observe(step)`, `Check() error`) the
  runner invokes each step; a registry; failing a checker fails the scenario with the
  reproducing seed printed.
- One trivial but real checker wired: a "monotonic clock never regresses" checker and a
  "no simulated permanent-delete occurred" stub (asserts the framework plumbing, not
  yet INV-14's full scope).
- **Mandatory CI scenario set — as far as expressible now:** lost-PUT-response +
  idempotent retry (objectstore only), crash-around-fdatasync (disk only), injected
  clock drift beyond `max_clock_skew` (clock only), a network partition (net only).
  These exercise the interfaces and fault hooks; the data-path arms of these scenarios
  are added by later phases.
- `task dst` runs the set; CI `dst` job goes from placeholder to real.

**Scope out:** WAL/fencing/recovery scenario *content* (later phases fill the arms);
nightly high-volume seed sweep (§25.1 "incremental" — set up hook, don't gate on it).

**Doc sections implemented:** §25.1 (harness, checkers, mandatory PR scenario set),
§25.3 (fault-injection points enumerated), §12 (drift injection scaffold).

**Tests first:**
- Harness self-tests: same seed ⇒ identical trace (INV-02); different seed ⇒ different
  trace but same invariant outcome.
- A **planted-bug test**: a deliberately regressing fake component makes the trivial
  checker fail, and the runner prints the exact reproducing seed (proves checkers can
  actually catch violations — the Adversary owns this).
- Each mandatory scenario runs deterministically and is reproducible by seed.

**Invariants activated:** INV-02 → `active` (deterministic replay). The checker
framework is now the substrate all later data invariants plug into.

**Metrics added:** harness-internal counters only (scenarios run, seed, failures);
exposed via 1.4's registry once it exists.

**Gate:** standard gate + the planted-bug test must demonstrate a *red* checker with a
reproducing seed. Adversary review is mandatory here.

**Rollback:** harness is test-only; never linked into production binaries. Revert
branch.

---

## Increment 1.4 — Observability: OTel tracing + structured logging + metrics registry

**Objective:** wire OpenTelemetry tracing and structured logging keyed on
`request_id`/`operation_id`, and stand up the Prometheus-style metrics registry, so
that from day 1 a slow FLUSH is "open the trace", not "grep three hosts" (§26.1).

**Scope in:**
- `internal/obs`: OTel tracer provider (v1.38.x, matching the stack), a structured
  logger (fields: `request_id`, `operation_id`, `volume_id`, `epoch`, `host_id`,
  `trace_id`), and a metrics registry with typed helpers.
- Trace-context propagation plumbed through the `simio.net` interface so CP → Agent →
  objectstore → KMS context flows in simulation too.
- Register the metric **names** (no-op/zero) the doc §26.2 mandates, so later phases
  only start incrementing existing series (stable cardinality contract): the fencing,
  WAL, snapshot, objectization, recovery, agent, fleet, and S3-client families as empty
  registrations with documented labels.
- A test exporter (in-memory span/metric sink) for assertions.

**Scope out:** real OTLP export endpoints / collectors (deploy concern, later); actual
metric *values* (each later phase emits its own).

**Doc sections implemented:** §26.1 (tracing), §26.2 (metric taxonomy — names + labels
reserved), §26.3 (alert rules documented as comments next to registrations).

**Tests first:**
- Trace propagation test: a simulated CP→Agent→objectstore call chain yields one trace
  with correctly nested spans carrying `request_id`.
- Structured-log test: emitted records contain the required fields.
- Metrics-registry test: every §26.2 name is registered exactly once with the
  documented label set (guards against cardinality drift and typos later).

**Invariants activated:** none (observability is not an invariant), but INV-01 lint now
also guards that tracing/logging never smuggle in `time.Now()` outside `simio`.

**Metrics added:** the full §26.2 taxonomy, registered as zero-value/no-op. First real
values arrive with their owning phases.

**Gate:** standard gate. Trace propagation and metrics-registration tests green; lint
still green (obs code uses `simio.clock`, not `time`).

**Rollback:** obs is additive and off the data path; disable exporters via config,
revert branch.

---

## Phase 01 exit gate (all four increments done)

- [ ] Module builds; `task ci` green; CI has real `build`/`test`/`lint`/`dst` jobs.
- [ ] INV-01 and INV-02 checkers `active` and green in CI.
- [ ] Four `simio` interfaces with real + sim impls passing identical contract suites.
- [ ] DST harness reproducible-by-seed; planted-bug test proves checkers catch
      violations and print the reproducing seed.
- [ ] Mandatory PR scenario set (interface-level arms) green.
- [ ] OTel tracing + structured logging + full §26.2 metric-name registry in place.
- [ ] `PLAN.md`, `STATUS.md`, `INVARIANTS.md` updated (INV-01/02 → active).
- [ ] `DEVIATIONS.md` clean or every entry resolved by fix/ADR.
- [ ] No `time.Now()`/socket/disk/S3 escape anywhere (lint proves it).

Only when this gate is green does **Track A (Phase 02)** and **Track D (S3 subsystem
groundwork)** start, per PLAN §5 and ADR-0002.
