# ADR-0004 — Objectstore "real" implementation is filesystem-backed in Phase 01

- **Status:** Accepted (Phase 01 / Increment 1.2)
- **Date:** 2026-07-24
- **Deciders:** tech-lead agent (assumption per Phase-0 mandate)
- **Extends:** §24 (S3 client as a subsystem), ADR-0001

## Context

The doc (§24) describes the production object-store client as an S3/S3-compatible
subsystem (AWS SDK for cloud; MinIO/RustFS on-prem) with hedged GETs, a retry budget,
and a circuit breaker. Increment 1.2's scope is the `simio` **interfaces** with a real
and a deterministic-sim implementation, contract-tested against each other — not the
full S3 subsystem (that is Track D, §24, scheduled after Phase 01).

Pulling the AWS SDK now and testing it would require network/credentials and would not
run in the pure-Go contract suite; it would also pre-empt Track D's design.

## Decision

For Phase 01, `internal/simio/real.ObjectStore` is **filesystem-backed** (one file per
key under a root directory, ETag = SHA-256 of content, `If-None-Match`/`If-Match`
enforced via stat/read). It satisfies the full `objectstore.Store` contract without a
network and doubles as the local/dev-mode backend (§6.2, "MinIO/RustFS single-node
only for dev" — a filesystem store is an even simpler dev target).

The **S3-SDK-backed** implementation — with the §24 subsystem features (hedged GETs,
retry budget, circuit breaker, connection pooling) and the §6.1 backend conformance
suite — lands in **Track D** as its own increment, behind the same `objectstore.Store`
interface. No consumer changes when it arrives.

## Consequences

- Phase 01 (and the DST harness) run entirely in-process, deterministically, with no
  network dependency.
- `If-Match` CAS (§12.4) and reversible-delete semantics (§21.3) are exercised now
  against both the sim and the filesystem-backed real store, so the interface is
  proven before the SDK impl exists.
- **Follow-up (Track D):** implement the S3-SDK store + conformance suite; verify
  `If-Match`/`If-None-Match` on the target MinIO/RustFS versions (marked *Unverified*
  until then, per §12.4 and RISK-05).

## Alternatives considered

- **AWS SDK now, tested against a local MinIO container in CI:** heavier CI, network
  flakiness, and it front-runs the Track D design. Deferred, not rejected — that is
  exactly what Track D builds.
