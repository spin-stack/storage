# TEST-GAPS — failure modes the suite does not cover

Produced 2026-07-25 by a six-way audit of the test suite (one agent per subsystem),
each finding then checked by an adversary whose default was that the claim is wrong.
78 gaps survived that check. This file is the backlog; it is not a wish list of "more
tests" — every entry is a concrete sequence of events with a bad outcome.

The audit's premise, and the one to keep: **assume the worst operational case**. The
happy path is already covered by 280+ tests. What was missing is what happens when a
disk tears a write, a response is lost twice, an operator runs two things at once, a
backend throttles mid-sweep, or a clock moves backwards.

Worked in two waves under `TEST-GAPS-PLAN.md` (packages A–E, then F1–F5), each in its
own worktree over a disjoint set of files. **Twelve entries are still open**, and
every one of them is listed below with what it is waiting on. Findings that turned out
to be already covered are recorded as such rather than counted as work.

## Closed — wave 1 (packages A–E)

| Finding | Commit |
|---|---|
| A rejected append left its bytes in the log: replay resurrected a record the guest was told failed (duplicate sequence, different content) or stopped at the tear and silently dropped everything after it | `5890963` |
| The uploader compared an ETag against a SHA-256, so one lost PUT response would report a byte-perfect object as divergent and wedge the volume forever on real S3 | `8428b4a` |
| The GC did not treat the durable prefix as a root: a sweep delete-markered every ACKed WAL object no checkpoint enumerates, moving the durable point to zero | `8dd3de3` |
| A drain could record an immutable epoch boundary of 0 below what the volume had durable, losing it permanently | `8dd3de3` |
| Nine of eleven DST checkers had never been shown to catch anything | `b54e66e` |
| Recovery and materialization never chained across epochs: a volume promoted twice rebuilt with only its newest epoch's writes, reported as complete | `a0d59a6` |
| `checkpoint.Create` published the log's durable watermark instead of what S3 proves, authorising local truncation over data that existed nowhere else | `a0d59a6` |
| Every log started at sequence 1, so a promoted writer would have written records colliding with the previous epoch's | `a0d59a6` |
| A KeyID-0 DEK sealed ciphertext but marked the object plaintext: the FLUSH ACKed, the object was unreadable at recovery, and Decrypt aborted a whole Recover on a key version it merely did not hold | `4ce470e`, `bd0b0fd` |
| `wal_durable_gap_bytes` measured what was un-fdatasynced instead of what no verified S3 object covers, so the RPO gauge read 0 while the backlog grew unbounded; nothing bounded that backlog | `4ce470e`, `bd0b0fd`, `58ccf6b` |
| A reopened WAL restarted at sequence 1 and served an empty view — duplicate sequences, re-issued object spans, and reused AES-GCM nonces for the same (volume, epoch, sequence) | `4ce470e`, `bd0b0fd` |
| A WRITE carrying FlagFUA completed with no fdatasync, no PUT and no lease check | `4ce470e`, `bd0b0fd` |
| A remote-mode FLUSH with a lease but no uploader ACKed and advanced durable over an empty bucket | `4ce470e` |
| WAL records never carried their VolumeID: a file replayed under the wrong volume applied to the wrong guest's extents with nothing to contradict it | `15f7117` |
| The uploader published two versions of one sequence span — the key embeds the content hash, so divergent objects land on different create-only keys | `4ce470e` |
| `materialize` replayed WAL objects with no integrity validation and had no prefix floor; `prefixFloor` swallowed every read error and fell back to floor 1 | `21e743d` |
| `checkpoint.Create` was not idempotent across a crash between Publish and AdvancePublished, and could not tell a byte-identical checkpoint from a foreign one | `21e743d` |
| A summary that over-claimed bricked recovery of its epoch; a mis-keyed summary was trusted | `21e743d` |
| The lease was anchored to the response instant, the epoch read was not atomic, and a promotion did not fence against the host it was fencing | `960753d` |
| A promotion granted a lease to a host that was unknown, DEAD or CORDONED; a missing `host_leases` row made the fencing wait zero | `960753d` |
| `Promote` had no expected-from-epoch guard; a grant delivered after a Revoke re-armed the lease | `960753d` |
| Only the sim was tested: sim and pg disagreed on error identity, malformed ids were coerced to SQL NULL, `RecordOperation` reported a stale term as `recorded=false` | `534da98`, `691cc21` |
| A heartbeat-shaped `UpsertHost` overwrote `state` and `nvme_committed_bytes`, un-cordoning a draining host and zeroing the ledger | `534da98` |
| `UpdateVolumeWatermarks` let a late epoch-N report lower what epoch N+1 had published | `534da98` |
| The GC widened its sweep on doubt: an unreadable anchor, a skewed clock, or a publication racing the sweep | `fbeed00` |
| The in-process stores' create-only PUT and If-Match CAS were check-then-act; a rewrite destroyed the marked version; the filesystem store wrote without atomicity or fsync | `91977b8`, `e6411b6` |
| The production S3 store could operate on an unversioned bucket, had no Restore, and did not map AWS's 409 `ConditionalRequestConflict` | `63d6c96` |

## Closed — wave 2 (packages F1–F5)

| Finding | Commit |
|---|---|
| A drain finished volumes it never moved: a concurrent failover made it author a create-only epoch boundary for a volume it had fenced nobody for, and release capacity it never reserved — twice, across a crash between the release and the progress record | `13bae4b` |
| An operation id presented for a different host drained the wrong plan and billed the wrong host | `13bae4b` |
| A duplicate drain re-cordoned a repaired, ACTIVE host; a resumed pass derived the previous epoch by subtracting 1; a stale-term pass leaked the destination's reservation silently | `13bae4b` |
| A pass that failed while a cancellation was pending took `CANCELING → FAILED` and erased the operator's request | `13bae4b` |
| No DST scenario crashed a drain at its own boundaries, and none asked whether the fenced source could still ACK | `d177a34` |
| `BumpVolumeEpoch` was a blind increment: n promoters reading one epoch each got an epoch and each wrote its own host into `primary_host_id`, leaving the row naming a host that holds neither the epoch object nor a lease | `6415fdd` |
| The epoch object named no holder, so a promoter that crashed between the PostgreSQL bump and the S3 CAS left two hosts passing a number-only check into one WAL namespace | `6415fdd` |
| The fencing deadline compared PostgreSQL's `now()` with the CP's own wall clock: a container clock that jumped forward shortened the wait by exactly the offset | `6415fdd` |
| Nothing could revoke a host lease, and a routine heartbeat re-armed the lease of a host that had just been fenced | `6415fdd` |
| The §7 volume failover states were unreachable through the Store, and the snapshot guard had no atomicity test | `6415fdd` |
| A fenced writer's late PUT raised a superseded epoch's durable prefix above the successor's recovery point, so `materialize.FromEpoch` rebuilt a state the live volume never had | `bbd18e8` |
| The prefix walk advanced by object, so overlapping spans under-reported the durable point, and the object order depended on the backend's listing | `bbd18e8` |
| INV-21 was unenforceable at recovery: two validated objects covering one sequence were both replayed, the winner decided by SHA-prefix sort order | `bbd18e8` |
| No DST checker existed for boundary monotonicity or for a durable point that goes backwards under a lagging LIST | `e1b0cc1` |
| `rebuild-metadata` read the authoritative epoch object and discarded it for any row that already existed, so a PITR-rewound volume stayed permanently unattachable while the rebuild reported success | `9d9d3e7` |
| 17 of 21 mandatory scenarios ignored the seed; no seed-driven fault reached fencing, promotion or the recovery point | `9780fd1`, `3f7a362` |
| A full device was not modelled anywhere, so the WAL's ENOSPC path was never exercised | `9d2284b` |
| Planted bugs emitted a literal event instead of breaking production behaviour, so a checker could pass while catching nothing real — 7 of 11 are now behavioural and the count is pinned | `a0c8565` |
| The CP term had no anchor outside the database it is restored from: a rewound `control_plane_leader` re-issued a term a live leader was still using, and both passed every §7 guard (ADR-0011, accepted) | `0265627` |

## Open

Twelve entries. Each names what it is waiting on; none is waiting on someone finding
the time to write a test.

### Needs a decision (1)

- **A GC sweep cannot see an anchor its listing has not caught up to** _(gc-objectstore)_
  - With an eventually consistent LIST, a freshly published manifest is GET-visible and
    LIST-invisible while the WAL objects it anchors — older by construction (§21.1) —
    are already listed and already past grace. The sweep marks live data. Re-listing
    does not help, and neither would having Mark consume Reachable's listing: both
    listings miss the same anchor.
  - waiting on: a root the sweep can read **by deterministic key** rather than by
    listing (a snapshot catalog query, or a snapshot index in the volume descriptor).
    That is a Control-Plane / on-S3-format decision, not a GC one.
  - bounded today: `TestListSeesAFreshPut` certifies strongly consistent LIST per
    backend and is blocking (§6.1). The precondition is now stated in the `gc` and
    `recovery` package docs.

### Needs an increment owning files no wave-2 package owned (5)

- **`CommitHostCapacity` enforces no oversubscription bound** _(drain-placement-dst)_
  - Two operations that read the fleet before either reserved anything choose the same
    destination and both commit, pushing it past `MaxOversubscription × NVMeTotalBytes`
    with neither caller having made a mistake. `Choose` evaluates the bound correctly:
    this is not a placement bug, it is a check-then-act between the read and the write.
  - shape: keep the non-negativity guard and add
    `AND ($2 <= 0 OR nvme_committed_bytes + $2 <= floor(max_oversubscription * nvme_total_bytes))`
    — the `$2 <= 0` disjunct is what stops a *release* bouncing off the bound on a host
    already above it — surfaced as a distinct `ErrCapacityExceeded`, with callers
    passing `placement.Policy.Limit` rather than a second copy of the rule.
  - needs one increment owning `internal/db/queries/hosts.sql` **and** `drain.go` /
    `crosshost.go`.

- **A second concurrent drain of one host is neither refused nor serialized** _(drain-placement-dst)_
  - The harm is closed — capacity is released once and no second destination is
    reserved — but two drains still run, both promote the same volumes, and the loser's
    reservation is released by nobody.
  - waiting on: a way to find live drain operations for a host. There is only
    `GetOperation` by id; this needs `ListOperationsByHost`, or a unique partial index
    on live drain operations per host.

- **A healthy host still cannot be evacuated** _(drain-placement-dst)_
  - The drain refuses to promote a source whose lease is still live, and
    `RevokeHostLease` now exists — but nothing wires them together, so draining a host
    that keeps heartbeating waits forever.
  - waiting on: the drain calling the revoke, term-guarded, as part of its fencing
    step, with `drain-source-cannot-ack-afterwards` extended to a healthy source.

- **`applyCapacity` cannot see a third party whose changes cancel out** _(drain-placement-dst)_
  - The drain proves its own delta landed by comparing the ledger against a recorded
    value, which is blind to another writer whose net effect between passes is exactly
    one volume size.
  - waiting on: an expected-value predicate in the reserving query — the same increment
    as the oversubscription bound.

- **`wal.Log` has no out-of-space state** _(wal-durability)_
  - The DST scenario asserts everything observable — backpressure before the device
    fills, no phantom sequence, sticky failure, a clean replay — but a caller cannot
    distinguish a full device from a transient I/O error, and no metric says it.
  - waiting on: a sticky `Degraded()` set when an append fails for want of space and
    cleared by a successful append after truncation, plus a `wal_out_of_space` gauge on
    the existing recorder. `internal/wal` was owned by no wave-2 package.

### Known-weaker coverage, deliberately (6)

- **Four checkers still have only a literal planted-bug proof**: `watermark-order` and
  `no-truncate-above-published` need a fault seam in `internal/wal` (`Log` enforces both
  internally and no simulated I/O reaches the check), `promotion-fencing-wait` needs a
  fault double for `metadata.Store`, and `background-yields` sits in `internal/ioclass`,
  which is pure in-process arbitration. `TestPlantedBugCoverageIsNotSilentlyWeakened`
  pins the behavioural count at 7 so this cannot quietly get worse.
- **Watermarks are not epoch-qualified at the store.** The monotonic floor covers the
  regression the finding named, but a fenced epoch-N writer reporting a *higher* durable
  sequence than epoch N+1 published would still be accepted. It needs a caller that
  knows its epoch, and there is none yet (DEV-0007).
- **The uploader has no backoff** between retries, so a coordinated throttle exhausts
  the budget faster than the backend recovers. The context half is closed.
- **The uploader detects an identical span, not a partial overlap** (`{1-3}` vs
  `{2-5}`), and its check is a LIST, so a lagging listing blinds it. Recovery is now the
  backstop; whether the write path should also detect it is a `wal` owner's call.
- **Publishers still call the number-only `epoch.Verify`** (`recovery`, `checkpoint`,
  `snapshot`, `dst/scenarios.go`), so `VerifyHolder` is defence in depth rather than the
  gate everywhere.
- **A durable point cannot regress from a real backend race in the sim**, only from a
  planted bug, because `sim.ObjectStore` cannot un-list a settled key.

## Interpretations that deserve a second look

- `recovery.VerifyAgreement` allows an overlap when the shared records are
  byte-identical and fails only on divergence (`ErrAmbiguousSequence`). The strict
  alternative — any overlap is fatal — would make a restarted writer's harmless
  re-batch permanently unrecoverable. This interprets §14.5 and may warrant an ADR; the
  strict rule is a two-line change in `agree`.
- Giving a superseded epoch a ceiling means its objects **above** that ceiling are no
  longer reachable roots, so the GC will mark them (reversibly, INV-14). They are
  unadopted orphans and marking them is correct, but it is a real semantic change.
- The §7 volume states record that a fence is in progress; they do **not** give mutual
  exclusion, because a self-transition is legal and two CPs can both reach
  FENCING_WAIT. Exclusion comes from the two compare-and-sets. Said here because the
  state names invite the opposite assumption.
- `RenewHostLease` refuses only a DEAD host. CORDONED and DRAINING keep serving what
  they hold, and refusing their renewals would stop their ACKs mid-evacuation.
