# STATUS

Short snapshot + resume-from-here handoff. **Read this first** when picking up the
work, then `REBASELINE.md` — a human review on 2026-07-25 found this file claiming
more than the repository does, and the maturity model below is the correction.

- **Date:** 2026-07-26. The spine exists and **a guest write now reaches the WAL**:
  increment 3.1 serves `vhost-user-blk` to the pinned QEMU 11.0.2 and `internal/blockdev`
  puts `wal.Log` behind that seam, both verified by execution rather than by simulation.
  All 78 audit findings are closed; the two entries left in `TEST-GAPS.md` came from
  elsewhere. Schema tooling moved from Atlas to pgschema (ADR-0019).
- **Where the work is:** everything is on **`main`**, pushed to `origin`
  (`/home/aledbf/spin-storage.git`, a bare repo — the old bundle remote is gone).
- **Gate on `main`:** `task ci:full` green — that is now the merge gate and it includes
  the Docker lanes (`task ci` stays the fast local loop). `task cover` 92.1% (>= 90);
  `task test:integration` green on Postgres 18; `task backend:conformance` green
  against the pinned RustFS; `task build:qemu` + `task qemu:verify` green.

## Maturity, not "done"

| State | Meaning |
|---|---|
| **model** | Library logic with unit/property/DST coverage. No integrated caller, no real I/O path. |
| **integrated** | Wired into a running binary through the real interfaces, exercised end to end. |
| **production-verified** | Real hardware/backends under fault injection, telemetry recorded, runbook times measured. |

**One path is now `integrated`; nothing is `production-verified`.** The spine exists —
`api/` over Connect, an Agent that pulls, two `cmd/` binaries — and a real QEMU guest
boots off a device whose bytes come from a `wal.Log`, writing records through the same
interfaces production would use. That is the *write* half of one volume on one host.
Everything downstream of it — FLUSH's ACK path, the uploader, checkpoints, truncation —
is still exercised only by tests, and there is no deployment. The other phases remain
models.

| Phase | State | Notes |
|---|---|---|
| 0 planning | done | — |
| 01 skeleton (simio + DST harness + obs) | **model** | metrics are recorded by the paths that own them (DEV-0010 closed); wiring continues with each new path |
| 02 guest layout | **not started** | needs guest mounts / a VM; nothing in the durability chain depends on it |
| 03 vhost-user | **3.1 integrated** | a real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`task test:integration:qemu`). FLUSH is *not* exercised by a guest (no kernel in the lane) and 3.2 reconnection / 3.3 inflight-shmfd are untouched — RISK-10 stays open |
| 04 WAL/CoW format + property tests | **write path integrated**, rest model | a guest's WRITE lands as a WAL record with 0 PUTs (`internal/blockdev`); the WAL is a directory of segments so truncation reclaims. FLUSH/uploader/checkpoint are still model-only. Format review still pending (human-review zone) |
| 05 encryption (AES-256-GCM, DEK/KEK) | **model** | — |
| 06 remote WAL (batching, idempotent PUT, summary) | **model** | — |
| 07 Control Plane + leases + fencing | **model** | fail-closed lease + resumable promotion + term guards (DEV-0004/0005 closed) |
| 08 recovery (S3 authority) + rebuild-metadata | **model** | objects validated before they count as durable; rebuild includes the catalog (DEV-0003/0009 closed) |
| 09 snapshots + clone + resize | **partial model** | synchronous sealing, no chain link persisted (DEV-0007 — needs the spine) |
| 10 objectization + checkpoints + GC + I/O classes | **partial model** | GC now marks reversibly (DEV-0006 closed); still no segment objects (DEV-0007) |
| 11 cross-host + cordon/drain + capacity | **partial model** | drain is idempotent across crash boundaries (DEV-0008 closed); the materialized view is still not persisted (DEV-0007) |
| 12 warm standby + compaction + flatten | **not started** | paused by the rebaseline |
| 13 hardening + fleet-mixed | **13.1 model** (typed lifecycles, ADR-0009) | 13.2–13.4 need infra; INV-19 still pending |

## Invariants: 21 checkers run; the rebaseline caveats are closed

The four claims the review narrowed — INV-06 (fencing was opt-in), INV-08/09 (durable
point from unvalidated headers), INV-14 (permanent deletion was reachable, GC did not
mark) — are now true in code, and INV-20 covers volumes *and* the snapshot catalog.
What stays true regardless: these are proofs about libraries, not about a system that
serves a block device.

## Infrastructure available (2026-07-25)
- **QEMU 11.0.2** — `task build:qemu` -> `_output/bin/` + firmware; `task qemu:verify`
  asserts the version and `vhost-user-blk-pci`. Unblocks the tooling half of 02/03.
- **Object store** — RustFS pinned by digest via TestContainers (`internal/testinfra`);
  `task backend:conformance` runs the §6.1 suite (ADR-0010). Every requirement passes
  except throttling, which is not exercisable on demand.
- **One S3 client** — `internal/simio/real.NewS3Store`, the only place the AWS SDK is
  configured, proven by the shared `objectstore` contract (`storetest`) against the
  real backend.

## Test coverage of failure modes

`TEST-GAPS.md` is the backlog from a six-way audit of the suite (2026-07-25), each
finding checked by an adversary before it was accepted: 78 gaps confirmed, 7 of them
critical. **All seven criticals are closed**; eight fixes in total — a rejected WAL append that left its bytes behind,
the uploader comparing an ETag against a SHA-256, the GC not treating the durable
prefix as a root, a drain recording an epoch boundary below the durable point, nine of
eleven DST checkers that had never been shown to catch anything, recovery not chaining
across epochs, a checkpoint publishing more than S3 could prove, and logs that
restarted at sequence 1 after a promotion. The rest are listed there by severity.

Three of the seven criticals were one root cause (the GC), which is the shape to
expect: the suite covered the happy path thoroughly and the operational worst case
barely at all.

## What happened since the rebaseline

Four waves of parallel increments closed all 78 audit findings (`TEST-GAPS.md` records
each with the commit that closed it, plus the two later entries that did not come from
the audit and what each is waiting on). The ones worth carrying in your head, because
they were real data-loss paths and not tidying:

- a reopened WAL restarted at sequence 1 — duplicate sequences and reused GCM nonces;
- the publishers never consulted the epoch object at all, so a fenced host could
  publish a checkpoint into another host's epoch and advance `published`;
- `AdvancePublished` accepted a value below its own past, so one stale listing walked
  the published point backwards under WAL that had already been discarded;
- a drain finished volumes it never moved, writing create-only boundaries for them;
- `BumpVolumeEpoch` was a blind increment, so n promoters each got an epoch;
- the GC's epoch ceiling licensed destroying a superseded epoch's objects that a
  pre-promotion manifest still named (ADR-0012).

**Every design decision that was blocking the spine is now taken:** ADR-0011 (term
claimed in S3), 0012 + amendment (GC anchors; Phase 12 is born with a by-key index),
0013 (device pressure, Proposed), 0014 (soft per-lineage quota, content-addressed
snapshots, squash), 0015 (fencing wait is a monotonic dwell), 0016 (fencing
granularity), 0017 (capacity is derived, not a ledger), 0018 (the spine: Agent first,
Connect RPC in `api/`, Agent pulls).

## What to do next

Tracks A and C are done; **Track B is the live one**, and it is down to its last step.
Three deviations are open — **DEV-0007** (the spine's second half, below), **DEV-0011**
and **DEV-0012** — and the two latter both wait on a human decision rather than on code.

### ~~Track A — Control Plane~~ **done** (wave 4)
ADR-0015/0016/0017 landed. Two consequences to carry forward: a drain now costs one
dwell **per volume** (ADR-0016's own bound; stage 2 removes it), and stage 1 needs the
reconciler to poll faster than the revocation window or the drain retries without
converging — it corrupts nothing, it just does not finish.

### Track B — the spine (ADR-0018), Agent first
1. ~~`api/` + toolchain~~ **done**: protobuf served over Connect, `buf` pinned,
   `generate:proto:check` in the gate.
2. ~~`cmd/volume-agent` skeleton~~ **done**: pull reconciliation, heartbeat carrying
   device total/used/backlog, epoch-qualified watermark reports, a lease armed only by
   a successful heartbeat and anchored to the instant the request left.
3. ~~The three gaps that blocked the data path~~ **done**: `disk.Usage()` with statfs
   (and a device budget in the simulator, the knob ADR-0013 needs), the aggregate
   remote backlog on `metadata.Host`, and `GetVolumeKeys` as its own RPC — authorised
   per request against the volume's current primary, which a list response could not
   express. Known gap recorded there: the response carries no DEK **version**, because
   no column holds one and `0` means plaintext in the WAL.
4. ~~vhost-user-blk~~ **done for 3.1** — see the phase table.
5. ~~Put `wal.Log` behind the `Backend` seam~~ **done** (`internal/blockdev`, `047ed37`).
   Three things it settled, all of them things the specification did not say:
   **virtio-blk cannot express FUA** (a Linux guest gets WRITE+FLUSH instead, which is
   the same contract `Log.WriteFUA` implements, so that entry point is unreachable from
   this transport); **`VIRTIO_BLK_F_CONFIG_WCE` must stay unoffered**, or a guest
   switches the device to write-through and stops sending FLUSH while believing every
   WRITE is durable — which under §14.4 it is not; and **the wire has no ENOSPC**, so all
   three refusals complete as `VIRTIO_BLK_S_IOERR` with the distinction carried in
   wrapped sentinels, because a lapsed lease, a backpressure bound and a full device have
   different remedies.
6. **Next — the second half of the chain: FLUSH → verified object → checkpoint →
   truncate, driven by a guest.** None of it has ever been driven by one. Two blockers,
   both separable and neither deep:
   - **No guest in the lane can emit a FLUSH.** SeaBIOS's INT 13h has no flush verb, and
     `-kernel` direct boot is unavailable: `task build:qemu` extracts no
     `linuxboot_dma.bin`, and no kernel image is pinned. The fix is a Taskfile change plus
     a kernel pinned by digest, the way RustFS already is.
   - **`wal.Log` has no mutex** (`TEST-GAPS.md`). Today that is harmless — one virtqueue,
     one `queueLoop` goroutine, every `Backend` call serialized. Putting the checkpoint in
     the lane is exactly what makes it concurrent for the first time, so this is a
     prerequisite and not a follow-up.

**That second half is the definition of done for DEV-0007** — and what turns Phases 06
and 10 from models into something a guest has exercised.

### ~~Track C — WAL segmentation~~ **done** (`f7f68b3`)
The WAL is a directory of segments (`WAL-SEGMENTS-SPEC.md`, human-reviewed before the
code), so `TruncateLocal` reclaims on an active volume instead of freeing bytes only
when the checkpoint had reached the end of the log. One deviation came out of it:
**DEV-0011** — a segment's space is charged as it is used, not reserved at creation, so
the out-of-space state is still reached mid-record rather than at a segment boundary.
It waits on ADR-0013's device budget.

### Two decisions waiting on a human, both in review zones
- **ADR-0013 (device pressure) is still `Proposed`.** It carries DEV-0011 and the
  `SetLimits` the segment code has no way to receive today.
- **DEV-0012 — a self-fenced log still accepts WRITEs and still serves reads.** §16
  scopes SELF_FENCED to durable ACKs and the code implements exactly that; refusing more
  would extend the doc, which is a fencing-zone ADR. Worth deciding before increment 3.2,
  since a reconnecting front-end is the first caller that can observe a fenced log.

### After those
Quota/lineage accounting (ADR-0014, CP-side: `lineages`, `charged_bytes`), then Phase 12
— segments, the by-key index all three of ADR-0012/0014 need, and squash — then the
rest of Phase 13 (real-hardware fault injection, runbooks with measured times, INV-19,
which becomes binding as soon as two Agents can differ).

## How to resume
1. Read `REBASELINE.md`, then `PLAN.md` (phase map), `INVARIANTS.md`, `CLAUDE.md`.
2. Ritual per increment: failing tests/DST/checkers **in their own commit**, then the
   implementation, then the doc update — the three-in-one commits are what let the
   documentation drift from the code.
3. Branch per phase off `main`; human review before merge for data-loss zones
   (formats, fencing, durability, GC); merge `--ff-only`.
4. Commands: `task tools` first, then `task ci`, `task cover`, `task test:integration`,
   `task backend:conformance`, `task dst`, `task generate`/`generate:check`,
   `task db:plan -- <name>` / `task db:apply` / `task db:verify`, `task build:qemu`.
   Tools are never invoked directly — versions live in `Taskfile.yml`.

## Session gotchas worth remembering (also in AGENT-MEMORY.md)
- **ADR-0005:** WAL headers are **104 bytes**, not the doc's "96".
- **ADR-0006/0007/0019:** all SQL via sqlc; **pgschema**, not Atlas — `schema.sql` is the
  declared state, `migrations/` is the record of reviewed plans and nothing replays it;
  Postgres 18; UUIDv7 enforced in code, by a `uuidv7` domain, and by DB CHECK.
- **ADR-0008:** drain moves a volume from its durable prefix in S3, fencing first.
- **ADR-0009:** lifecycles are typed (`internal/lifecycle`), enforced by Go types,
  transition-guarded UPDATEs, and DB CHECKs.
- **ADR-0010:** one S3 wrapper; RustFS answers `If-Match` on a missing key with
  `NoSuchKey` (not 412); multipart ETags carry `-N`; LIST pages cap at 1000 keys.
- Do not re-add spinbox's `CONFIG_CXL=n` QEMU debloat (breaks the 11.0.2 link); bump
  `QEMU_CONFIG_REV` when configure flags change.
- Coverage excludes generated `internal/db`, integration-only `metadata/pg`,
  `simio/real/s3.go`, `testinfra`, `migrations`, `cmd`, and the `dst` harness.
