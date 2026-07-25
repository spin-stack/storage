# ADR-0003 — Enforcement of simulable interfaces (the day-1 lint)

- **Status:** Accepted (Phase 0)
- **Date:** 2026-07-24
- **Deciders:** human owner + tech-lead agent
- **Implements:** §25.1, INV-01

## Context

§25.1 mandates, from the first commit, that there be **no direct `time.Now()`, sockets,
or disk/network/S3 syscalls outside** the clock/net/disk/objectstore interfaces, and
that this be verified by an automated check in CI — "not by good intentions." §29.7
stresses this is impossible to retrofit. This ADR fixes *how* the check is built, since
the mechanism is an architectural commitment the whole codebase must live under.

## Decision

Enforcement is **layered**, so a gap in one layer is caught by another:

1. **Package boundary.** Only `internal/simio` (interface definitions) and
   `internal/simio/real` (thin passthrough implementations) may import the forbidden
   real primitives. Everything else depends on the `simio` interfaces via dependency
   injection.
2. **`depguard` (golangci-lint).** A `depguard` ruleset forbids importing `time` (except
   for types), `net`, `os` (file ops), `syscall`, and the concrete S3 SDK packages from
   any package outside the allow-listed `simio`/`simio/real` paths.
3. **`forbidigo` (golangci-lint).** Forbids specific call expressions that slip past
   import rules: `time.Now`, `time.Since`, `time.Sleep`, `time.After`, `time.Tick`,
   `net.Dial*`, `os.Open`, `os.Create`, `os.OpenFile`, and raw disk/net `syscall.*`.
4. **Custom `simulable` analyzer** (`hack/analyzers/simulable`, a `go/analysis` pass,
   runnable standalone and as a golangci-lint plugin). It catches what config-based
   linters cannot express cleanly: e.g. constructing a real clock/dialer/objectstore
   outside the allowed packages, or method-value escapes. Ships with `analysistest`
   golden fixtures (a violating package and a compliant one).
5. **CI gate.** `task lint` (which runs all of the above) is a required GitHub Actions
   job. A planted violation must turn CI red; this is demonstrated in the Phase 01 / 1.1
   PR and is the checker for **INV-01**.

## Consequences

- Test code and the sim implementations get the deterministic clock/net/disk/objectstore
  by injection; there is no ambient global time or I/O anywhere in production packages.
- The real implementations are a small, audited surface (`simio/real`) — the only place
  the forbidden primitives live.
- Adding a new real primitive (e.g. a KMS client) means extending `simio` with an
  interface and its real/sim pair, never calling the SDK inline. The lint enforces this.

## Assumptions (decided; revisit via ADR if false)

- golangci-lint's `depguard` + `forbidigo` are expressive enough for layers 2–3; the
  custom analyzer covers the rest. *Verified* incrementally in Phase 01 / 1.1 against
  the golden fixtures. If `forbidigo` proves too coarse, the custom analyzer absorbs its
  rules.

## Alternatives considered

- **Convention + code review only:** rejected outright by §25.1 ("not by good
  intentions").
- **Custom analyzer only (no golangci rules):** more code to maintain and slower to
  author; the layered approach gets 80% from config and reserves the analyzer for the
  hard cases.
