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
own worktree over a disjoint set of files, then wave 3 (G1–G5). **Seven entries are
still open**, and every one of them is listed below with what it is waiting on.
Findings that turned out to be already covered are recorded as such rather than
counted as work; two of the open entries are new, found by the harness while proving
that a checker could catch a real bug.

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

## Closed — wave 3 (packages G1–G5)

| Finding | Commit |
|---|---|
| `CommitHostCapacity` enforced no oversubscription bound: two operations reading the fleet before either reserved both committed, and nothing re-checked §28.2 at the write | `016b257` |
| A resumed drain proved its own capacity delta landed with a read then a write, so a third party's change in between passed as its own | `016b257` |
| A second concurrent drain of one host was neither refused nor serialized (`ListOperationsByHost`, chosen over a partial index so 'live' stays the transition table's answer) | `e857cc6` |
| A healthy host could not be evacuated at all: the drain now observes `last_renewal`, records it, and only then revokes — the order is the fix, since Promote refuses a zero instant | `b9a0f76` |
| `wal.Log` could not tell a full device from any other I/O error, and no metric said so (`Degraded()`, `wal_out_of_space`, orthogonal to `Fenced()`) | `c83bf1b` |
| The uploader retried a coordinated throttle with no backoff, exhausting its budget inside one throttling window | `e348fed` |
| The uploader could publish a span overlapping one it had already published (a foreign writer's overlap stays recovery's, argued in the code) | `781c976` |
| INV-03 and INV-13 were unreachable by any injected fault, so their checkers had nothing to catch (`OrderPolicy`, strict by default and by omission) | `273a3ae` |
| **`Log.AdvancePublished` accepted a value below its own past**: one stale listing walked the published point backwards under WAL that INV-13 had already authorised discarding | `dde33d9` |
| The publishers never consulted the epoch object at all — not 'number-only', *nothing* — so a fenced host could publish a checkpoint into another host's epoch and advance `published` | `94e9ec7` |
| The drain wrote the epoch boundary anonymously, so a resumed pass could record a create-only boundary for an epoch an overtaking promotion had moved on | `2a6e62e` |
| The GC's epoch ceiling — a permanent number computed from a listing — licensed destroying a superseded epoch's objects that a pre-promotion manifest still named (ADR-0012) | `0c2b4a4` |
| Four checkers were still proven against a fabricated event; all thirteen are now behavioural, with controls, and `InjectStaleListing` un-lists a settled key so a durable point can regress from a real race | `a0c8565`, `069fe8e` |
| The WAL classified a full device by matching an error message, because the disk interface declared no sentinel | `58f398b` |

## Open

Seven entries. Three are new: two found by the harness while proving that a checker
could catch a real bug, and one found while answering how a node protects itself from a
full device. Each names what it is waiting on.

### Needs a decision (3)

- **A lagging replica read makes the fencing wait elapse early** _(fencing-promotion, new)_
  - `Promote` takes the `host_leases` row as the whole authority for FENCING_WAIT. A
    read served by a replica behind by more than `lease_ttl + max_clock_skew` reports a
    `last_renewal` old enough that the wait looks over, and an epoch is granted over a
    live writer. The clock-offset check added in wave 2 cannot see it: both clocks
    agree, it is the *data* that is old.
  - waiting on: a decision between reading leases from the primary explicitly (a
    deployment guarantee the adapter would have to state and enforce) and a monotonic
    dwell measured from when *this* promoter first observed the lease. The second is
    the one that does not depend on how the database is deployed.

- **A heartbeat will re-arm the lease a drain revokes** _(fencing-promotion, new)_
  - `RenewHostLease` deliberately accepts CORDONED and DRAINING, because both still
    serve what they hold. Nothing renews leases outside promotion today, so the drain's
    revoke works — but once the Agent heartbeat exists (DEV-0007), a source that keeps
    heartbeating re-arms the lease the drain just revoked and the wait never elapses.
  - waiting on: an ADR in the lease/fencing zone. Either the CP refuses renewals for a
    host it is draining (which flips a wave-2 contract test that exists on purpose), or
    a host state means "fenced" distinctly from "cordoned".

- **A GC sweep cannot see an anchor its listing has not caught up to** _(gc-objectstore)_
  - Narrowed, not closed, by ADR-0012: the epoch ceiling no longer licenses destruction,
    and the reproduction that decided it — a hole in the listing *below* a durable point
    marks ACKed data with no manifest involved at all — showed no index would have
    helped. Strongly consistent LIST stays a precondition, certified per backend by
    `TestListSeesAFreshPut` (§6.1, blocking).
  - revisit when: compaction/objectization (Phase 12) makes a manifest the *sole* anchor
    of the WAL objects a segment replaced. That is the point where there is a format
    worth indexing.

### Needs an increment owning files no package owned (3)

- **`TruncateLocal` reclaims nothing on an active volume** _(wal-durability, new)_
  - It records `truncatedUpTo` and calls `file.Truncate(0)` only when `upTo >= local`,
    so the space below a published checkpoint is freed only when the checkpoint reached
    the end of the log — which on a volume under continuous write never happens. The
    WAL is a file that grows and is emptied when the volume goes idle. Every other
    defence against a full device is downstream of this one, and none of them helps
    while the reclaim path is a no-op.
  - waiting on: ADR-0013 (Proposed). The fix is to segment the WAL so truncation
    unlinks whole segments, which is an on-disk format change and needs the format
    review before the code.

- **Capacity accounting has no idempotency key** _(drain-placement-dst)_
  - The expected-value predicate closes the read→write window, not a crash: if the drain
    dies before its reservation and a stranger's change nets to exactly one volume size,
    the resumed compare-and-set fails, the re-read shows `before + delta`, and the pass
    concludes its own delta landed.
  - waiting on: a reservation row keyed by `(operation_id, volume_id)` — a schema
    decision, deliberately not taken by an implementer.

- **Watermarks are not epoch-qualified at the store** _(metadata-cp)_
  - The monotonic floor covers the regression the original finding named, but a fenced
    epoch-N writer reporting a *higher* durable sequence than epoch N+1 published would
    still be accepted. It needs a caller that knows its epoch, and there is none yet.
  - waiting on: DEV-0007 (the Agent spine).

### Known-weaker coverage, deliberately (1)

- **`internal/snapshot` does not check holdership before publishing a manifest.**
  `Snapshotter.Create` builds a create-only manifest from the live log, so only the host
  holding the volume can take one (§19) — but nothing enforces that at the object store.
  The gate is ready (`recovery.VerifyPublisher`); it is a pre-check only, since `Publish`
  is the last step of `Create`.

## Interpretations that deserve a second look

- `recovery.VerifyAgreement` allows an overlap when the shared records are
  byte-identical and fails only on divergence (`ErrAmbiguousSequence`). The strict
  alternative — any overlap is fatal — would make a restarted writer's harmless
  re-batch permanently unrecoverable. This interprets §14.5 and may warrant an ADR; the
  strict rule is a two-line change in `agree`.
- ~~Giving a superseded epoch a ceiling means its objects above that ceiling are no
  longer reachable roots, so the GC will mark them.~~ **Reversed by ADR-0012.** That
  reading was right about what the volume contains and wrong about what may be
  destroyed: an object above the ceiling is either a snapshot object a pre-promotion
  manifest still names, or the evidence of a boundary that was itself computed from a
  listing. Every WAL object of a closed epoch is now a root.
- The §7 volume states record that a fence is in progress; they do **not** give mutual
  exclusion, because a self-transition is legal and two CPs can both reach
  FENCING_WAIT. Exclusion comes from the two compare-and-sets. Said here because the
  state names invite the opposite assumption.
- `RenewHostLease` refuses only a DEAD host. CORDONED and DRAINING keep serving what
  they hold, and refusing their renewals would stop their ACKs mid-evacuation.
