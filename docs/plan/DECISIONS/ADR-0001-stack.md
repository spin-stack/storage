# ADR-0001 — Stack, module layout, build tooling, CI

- **Status:** Accepted (Phase 0)
- **Date:** 2026-07-24
- **Deciders:** human owner + tech-lead agent
- **Supersedes:** —

## Context

The architecture doc (v5.1) must be turned into a buildable system. The doc §14.1
already expresses formats as Go structs and cites Go-ecosystem libraries (roaring
bitmaps, an S3 client with pooling, OpenTelemetry). The surrounding `spin-stack`
monorepo (`spin`, `spinbox`, `erofs-snapshotter`, `runs-on-spin`, `devcontainer-cli`)
is uniformly Go. `spinbox` in particular sets the concrete conventions:
`go 1.26`, module path `github.com/spin-stack/<name>`, `Taskfile.yml` (go-task),
`.golangci.yml`, GitHub Actions (`.github/workflows/{ci,release}.yml`), gRPC +
protobuf, OpenTelemetry v1.38.x, `testify` + `gotest.tools/v3`, QEMU via
`digitalocean/go-qemu`/`go-libvirt`, and an `internal/ cmd/ api/ integration/ hack/
deploy/` layout.

## Decision

1. **Language:** Go 1.26.
2. **Module:** a new module `github.com/spin-stack/storage`, rooted at
   `spin-stack/storage/` (alongside the architecture doc), part of the monorepo.
3. **Build tooling:** `Taskfile.yml` (go-task), mirroring sibling projects, with at
   least `build`, `test`, `lint`, `dst`, `ci` targets.
4. **Lint / static analysis:** `golangci-lint` extending the sibling `.golangci.yml`,
   **plus** a custom `simulable` analyzer enforcing §25.1 (see ADR-0003).
5. **CI:** GitHub Actions; the standard gate (PLAN §2) is encoded as CI jobs
   (`build`, `test`, `lint`, `dst`, invariant checkers). QEMU-in-CI is pinned to
   **11.0.2**, identical to production (§4, §16) — wired in Phase 03/13, not Phase 01.
6. **Testing:** `testify` + `gotest.tools/v3`; property tests via a Go property library
   (candidate: `pgregory.net/rapid`) — final choice recorded in the Phase 04 ADR when
   the WAL property tests land.
7. **Observability:** OpenTelemetry Go v1.38.x (already vendored across the stack).
8. **Core libraries (doc §C / "apalancamiento en librerías maduras"):** roaring bitmaps
   for the active map (§13.3), a mature S3 client with connection pooling for the
   objectstore subsystem (§24), OTel for tracing/metrics. Exact import paths and
   versions are pinned in `go.mod` during Phase 01/04/06 and **verified** at that time
   (not assumed here).

## Assumptions (decided, per Phase-0 mandate; revisit via ADR if false)

- `pgregory.net/rapid` is the property-test library. *Unverified* until Phase 04; if it
  proves unsuitable, a follow-up ADR swaps it.
- The roaring-bitmap library is `github.com/RoaringBitmap/roaring/v2` (or the current
  maintained major). *Unverified:* exact version pinned and benchmarked in Phase 04.
- The S3 client is the AWS SDK for Go v2 for cloud, with the sim/real split (ADR-0003 /
  §24) isolating it behind `internal/simio/objectstore`. MinIO/RustFS CAS
  (`If-Match`/`If-None-Match`) support is **verified by the backend conformance suite**
  (§6.1) before any backend is enabled, not assumed here.

## Consequences

- Cohesion with the monorepo: shared tooling, reviewers, and CI patterns.
- The custom lint (ADR-0003) is a hard prerequisite of Phase 01 increment 1.1.
- Any change to language, module boundary, or CI platform requires a superseding ADR.

## Alternatives considered

- **Rust** for the data path (better p99 control, SPDK/vhost affinity): rejected for the
  MVP — diverges from the doc and the entire monorepo, forces re-expressing every
  format, and multiplies the "implementation surface" risk (§29.7). May be revisited
  post-MVP for a hot inner loop behind the same interfaces.
- **Separate repo:** rejected — loses shared tooling and cross-project cohesion for no
  MVP benefit.
