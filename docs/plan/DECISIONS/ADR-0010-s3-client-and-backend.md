# ADR-0010 — S3 client: aws-sdk-go-v2 behind one wrapper; RustFS as the certified dev backend

- **Status:** Accepted (Track D / §24 groundwork, Phase 13.3 conformance pulled forward)
- **Date:** 2026-07-25
- **Deciders:** human (asked for the verification and the wrapper), implementer agent
- **Implements/Extends:** §6.1 (backend requirements + conformance suite), §24 (S3
  client as a subsystem), §25.4. Supersedes the follow-up recorded in ADR-0004.

## Context

ADR-0004 deferred the S3-backed `objectstore.Store` to Track D and marked the
`If-Match`/`If-None-Match` support of the target backends as *Unverified*. Two
questions had to be answered before building on it: is the official AWS SDK the right
client for a non-AWS backend, and does RustFS actually implement the conditional
writes the fencing and idempotency protocols stand on?

## Decision

**Client: `aws-sdk-go-v2`, wrapped.** It is the only Go client that exposes both
conditional writes as first-class request fields (`IfNoneMatch`, `IfMatch` on
`PutObject`) — which §12.4 and §14.5 require — and it is the SDK the cloud
deployment needs anyway. The alternative (minio-go) is friendlier for S3-compatible
backends but would leave the AWS path on a second client, i.e. two clients to
certify instead of one.

The SDK is used in exactly one file, `internal/simio/real/s3.go`. `S3Config` is the
whole configuration surface (bucket, endpoint, region, credentials, path-style,
checksum mode, request timeout); everything else in the system keeps depending on
`objectstore.Store`, as INV-01 requires. There is no second place where somebody can
pick a different path-style or checksum setting, which is how "variants" appear.

Three behaviours the wrapper owns, each pinned by a test:

- **Error translation.** `PreconditionFailed → ErrPreconditionFailed`;
  `NoSuchKey`/`NotFound` → `ErrNotFound`. An `If-Match` against a key that does not
  exist answers **`NoSuchKey`, not 412** — verified — so the epoch store reads that
  as "not initialised yet" rather than "someone else advanced it".
- **LIST pagination.** A page is capped at 1000 keys. Recovery derives the durable
  point from a full prefix listing (§22.1), so the wrapper follows continuation
  tokens internally; a one-page `List` would silently shorten the contiguous prefix
  and declare ACKed data lost.
- **Checksum mode.** The SDK sends CRC trailers (`aws-chunked`) by default, the
  classic source of breakage against S3-compatible backends. The certified backend
  accepts the default; `ChecksumWhenRequired` is the one-field escape hatch, and a
  test proves both modes work.

**Backend: RustFS single-node, pinned by digest, for development and tests only.**
§6.1 forbids single-node RustFS/MinIO in production (that stays MinIO multi-node with
erasure coding or Garage RF=3); what it gives us is a fast, reproducible container
for every S3-shaped scenario.

## Verification (not documentation)

`task backend:conformance` runs the §6.1 list against the pinned image via
TestContainers. All of it passes:

| Requirement (§6.1) | Result |
|---|---|
| `If-None-Match: *` create-only | ✓ creates once, `PreconditionFailed` after; exclusive under 16 concurrent writers |
| `If-Match` CAS on ETag | ✓ advances on match, refuses stale; exactly one winner in a 16-way race |
| HEAD after a "lost" PUT | ✓ same ETag/length, retry refused rather than duplicated |
| Versioning | ✓ enabled, delete produces a delete marker, prior version still readable |
| Object Lock | ✓ GOVERNANCE retention **enforced**: the permanent delete is refused |
| LIST after PUT | ✓ read-after-write, keys sorted, paginates past 1000 |
| Throttling | ✗ not exercisable on demand — modelled in the sim (`InjectThrottle`); the real-backend arm belongs to Phase 13.2 fault injection |

Edge cases probed for the same reason: create-only on a **versioned** bucket still
refuses (otherwise retried uploads would stack versions and INV-21's argument would
collapse), multipart ETags carry a `-N` suffix and CAS still works with them, range
GETs, zero-byte objects, and delete-of-missing.

## Consequences

- `internal/simio/real/s3.go` is integration-only production code, excluded from the
  unit-coverage floor the same way `metadata/pg` is, and proven by the *shared*
  `objectstore` contract (`storetest`) running against a real backend — the same
  assertions the sim and the filesystem store must satisfy. A divergence between what
  DST simulates and what S3 does surfaces there.
- The conformance suite is blocking per backend version (§6.1): bump
  `RUSTFS_IMAGE` in the Taskfile and re-run before enabling a new one.
- Still open for Track D proper: hedged GETs, the retry budget, and the circuit
  breaker of §24 — they belong inside this wrapper, behind the same interface.
