# STATUS

Short snapshot. Update at every increment close.

- **Date:** 2026-07-25
- **Current phase:** **Phases 01, 04, 05 COMPLETE** (pure-Go). Phases 02/03 planned
  (need VM/QEMU infra). **Phase 06 (remote WAL) is next.**
- **Active invariants:** INV-01, INV-02, INV-03, INV-04(bounds), INV-05, INV-15, INV-18.
- **Branches:** `phase-01/...` (Phases 0/01, 02/03 plans) merged-forward into
  `phase-04/wal-cow` (Phase 04). Nothing merged to `main` yet — **pending human review**,
  especially the WAL format (ADR-0005 / DEV-0001, header size 104≠96).
- **Blockers:** none for pure-Go work; format review recommended before merge to main.

## Phase 05 — COMPLETE (encryption at rest)

- 5.1 `internal/crypto`: AES-256-GCM DEK.Seal/Open, nonce derived from
  (vol,epoch,seq) — never stored/reused (§15.2); KMS + DevKMS wrap/unwrap; injected
  randomness. Tamper fails closed; nonce-uniqueness property (rapid).
- 5.2 encrypted WAL end-to-end: `Log.EnableEncryption` seals payloads (WAL file =
  ciphertext, `KeyID`/`AuthTag` set, plaintext CRC per §14.1); live reads stay
  plaintext in host memory; replay decrypts + verifies. `format.DecodeRecord` gates
  the payload CRC on `KeyID`. DISCARD/WRITE_ZEROES accounting (`discarded_bytes_total`),
  crypto-shred test. **INV-15 active** (checker + DST scenario).

## Phase 04 — COMPLETE (WAL/CoW correctness spine)

- 4.1 format v2 (record+object headers, little-endian, CRC32C, crypto fields reserved,
  golden-bytes lock, deterministic S3 key). **Format decision ADR-0005 / DEV-0001.**
- 4.2 serialize/replay + §25.2 property test (INV-05): truncation at every byte + bit
  corruption → exact state XOR detected error, never silently wrong.
- 4.3 write-path Log: watermarks (INV-03), unflushed bounds + backpressure (INV-04),
  interval-map read view (§13.2, memory ~ working set), no PUT on WRITE (INV-18).
- 4.4 active map: roaring-bitmap presence + location table over 64 KiB segments
  (§13.3); memory-bounded over a 1 TiB universe (10k scattered segments < 1 MiB).

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

## Next steps

Pure-Go phases that build here, in dependency order:

1. **Phase 05** — per-volume encryption (AES-256-GCM, DEK/KEK, dev KMS) + DISCARD/
   WRITE_ZEROES end-to-end. Flips on the reserved `KeyId`/`AuthTag` fields (no format
   change). Activates INV-15 (nothing leaves the host in clear). Human-review zone
   (crypto/durability-adjacent). **Next.**
2. **Phase 06** — remote WAL: on-demand batching + PUT idempotency + summary objects
   over the locked object format. Activates INV-07 (ordering), INV-21 (PUT idempotency),
   full INV-04.
3. **Track D** — S3 client subsystem (§24) can proceed in parallel (disjoint files).

Deferred (need infra): Phases 02/03 (VM/mounts, QEMU 11.0.2).

**Recommended before more phases:** human review + merge of the WAL format
(ADR-0005 / DEV-0001) to `main`, since everything downstream builds on it.

## Open questions for the human (non-blocking; defaults recorded as assumptions)

- Property-test library (`pgregory.net/rapid` assumed, ADR-0001) — confirm at Phase 04.
- Dev backend for the conformance sim vs real (MinIO/RustFS single-node dev only per
  §6.2) — confirm at Phase 06/13.
