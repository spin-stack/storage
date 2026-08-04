# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-08-03 · **Branch:** everything is on `main`. `git ls-remote origin`
  (`/home/aledbf/spin-storage.git`, bare) reports `88d309a` for `refs/heads/main`, and
  local `main` is ahead of it — the ADR-0026 increment 5 and invariants commits are
  unpushed.

  This line has been wrong twice, in opposite directions, and both times because it was
  written from memory. The rule this file needs is not "check before writing", which was
  already the rule — it is that **a claim about another system belongs next to the command
  that produced it**. Here that command is `git ls-remote origin`.
- **Gate:** `task ci:full` green, 2026-08-03, production coverage 90.3%.
  Green *on a developer machine, and nowhere else*. **CI has never run**: `origin` is a
  local bare repo, so the GitHub workflows have never executed on a runner. Treat every
  green claim here as reproducible-by-you, not as defended by a gate. Nothing is
  *missing* for that — ADR-0022 pins the kernel, ADR-0025 decided how a runner obtains
  QEMU — what is missing is a run.
- **Where this is going:** storage integrates into **spin** (`github.com/aledbf/spin`),
  which already has a control plane and a per-host runner — **ADR-0021**. spin imports
  storage, never the reverse; `cmd/control-plane` and `cmd/volume-agent` are test
  harnesses that must stay runnable end to end and will not be deployed.

## Pick up here

**ADR-0026 is implemented, all six increments.** V1 accepts an RPO of one session: a
volume is uploaded once when it stops, a snapshot is an `fsync` plus a copy frozen at a
§19 sequence, and a clone starts where its data already is. Roughly half the system was
deleted to get there — the remote durability chain, promotion and failover, checkpoints
and truncation, GC, the io-class scheduler, the lease-gated ACK — and the sections below
record each increment next to what it removed.

**What a guest can do today, driven by the real binaries in `integration/e2e`:**

1. boot off a vhost-user-blk device the Agent binds, write, and `fsync` — the ACK is
   local `fdatasync` and puts **zero** objects in the bucket (INV-18, asserted with a real
   kernel in the loop);
2. stop the Agent and have the volume's image appear in the object store, sealed
   (INV-15), CASed over the manifest it booted from (INV-10);
3. start again and read its own bytes back, on a fresh data directory so only the image
   can answer;
4. be snapshotted **while still serving** — `control-plane -snapshot-volume` writes a
   catalog row, the Agent finds it in its desired state, freezes at a sequence and
   publishes; the socket is still there when the manifest lands;
5. be cloned from that snapshot with `control-plane -clone-snapshot`, which asks
   `placement.Choose` and lands the clone on the host that took the snapshot.

**What is not there, in the order it matters:**

- **Nothing is deployed and CI has never run.** Every claim above is one machine's word.
- **No metric reaches anywhere.** `cmd/volume-agent` passes `Recorder: nil` deliberately —
  there is no OTLP exporter, and wiring one is a deploy concern nobody has landed. The
  metrics that *are* recorded (the WAL's watermarks and device state, the lease, and since
  2026-08-03 §19's two snapshot histograms) are proven by tests through `obs.NewTestProvider`
  and observed by nobody in production. **The §26.2 catalog is also stale**: about
  three quarters of its entries named mechanisms ADR-0026 withdrew; the catalog and §26.2
  were trimmed together on 2026-08-03 (~~DEV-0022~~).
- **A rebuilt catalog cannot tell you who was serving what.** `-rebuild-metadata` brings
  back volumes and snapshots from the bucket (INV-20, since 2026-08-03) but no placement,
  because no object records one. After losing the database you know what exists, not who
  was running it.
- **A host that dies mid-session loses everything written since the volume attached.**
  That is ADR-0026's accepted trade, not a defect, and it is what "what would reverse it"
  in that ADR is for.

## Maturity, not "done"

| State | Meaning |
|---|---|
| **model** | Library logic with unit/property/DST coverage. No integrated caller, no real I/O. |
| **integrated** | Wired into a running binary through the real interfaces, exercised end to end. |
| **production-verified** | Real hardware/backends under fault injection, telemetry recorded, runbook times measured. |

**The V1 path is integrated end to end; nothing is production-verified.** The spine
exists — `api/` over Connect, an Agent that pulls, two `cmd/` binaries — and a real QEMU
11.0.2 guest boots off a device the *Agent* binds and owns, writes, `fsync`s, and reads
its own bytes back after a stop and a restart. Snapshot and clone are driven by the real
Control Plane binary in the same lane.

What is *not* verified is everything about running it: no deployment, no CI run, no
exporter for the metrics, no fault injection against real hardware, and no measured
runbook times. "Integrated" here means the seams are exercised by processes rather than
by tests constructing the types themselves — which is the bar this project kept missing —
not that anything has met a load.

## How far is "functional" — and why the phase table does not answer that

Two axes run through this document and they are easy to confuse.

**The 13 phases below are the design doc's decomposition of the whole product.** They are
not a schedule and not a queue: phase 12 is *not started* and is not on the path to
anything working, phase 13 needs hardware that does not exist yet, and most of the rest
say **model** — the library is written and DST-covered, with no integrated caller. Reading
the table top to bottom gives the impression of being stuck near the end. Nothing is
stuck near the end; the table is a map of the product, not a progress bar.

**The queue is `BUILD-INVENTORY.md`, and it has nine increments** (0 through 8). It answers one
question — what has to exist for *one volume on one host* to work end to end with the
real binaries — and it is where "how much is left" is actually measured:

| Increment | State |
|---|---|
| 0 — binaries runnable and debuggable | **done** (`5bf31d4`) |
| 1 — a volume can exist | **done** (`5953c73`) |
| 2 — KEYSTONE: the per-volume runtime | **done** (`4f2852a`, `cf021cc`) |
| 3 — checkpoint and truncate | **done** (scheduler, ADR-0023, and as of `b5bd268` its §12.2 checker) |
| 4 — warm restart, same host | **done.** Its two data pieces were the durable point and the published point having no producer; increment 5's `InstallBase` supplies both, and `fetchBase` calls `recovery.DurablePoint`. The written decision it also asked for is **ADR-0024** — same-epoch re-attach, with the four mechanisms it rests on named so a change cannot silently invalidate it. Writing it surfaced **DEV-0014** (two Agents, one data dir), which predates the decision. |
| 5 — cold restart, seed the read view from S3 | **done** — this was the real correctness hole |
| 6 — the DEK arm | **done.** `dek_key_id` end to end (column + CHECK, proto, `metadata.Volume`, descriptor, `GetVolumeKeys`, provisioner, clone, rebuild-metadata) plus `-kek-file`/`-kek-id` on the Agent, `crypto.DevKMS`, and the unwrap at attach. Every object a served volume puts in the bucket is now ciphertext, and INV-15 is reachable — see below. **Corrected 2026-08-02 (DEV-0019):** "done" was half true. The write half was; the *read* half handed the guest that ciphertext back on every restart, because `fetchBase` recovered with no key. Fixed, with the mandatory DST arm that crosses encryption with a restart — which is the arm whose absence let this row be written. |
| 7 — a guest that can issue FLUSH | **mostly done** (`e8bbdab`, `cef9881`) |
| 8 — the e2e lane and a gate that can notice regressions | **done.** `integration/e2e` runs both binaries as processes in `ci:full` and in CI; CI builds them and the workflow runs the lane. The QEMU-in-CI question is decided and implemented (**ADR-0025**): the guest lane is a container job on the published runtime image, which skips with a notice when that image is absent — see below. |

So: **the build order is done.** The
milestone matching the target slice's literal wording was the end of increment 6, and the
first one a **merge gate can defend** was the end of increment 8. Both are in. The
remaining honesty caveat is narrower than it was: the workflows have still never *run*,
because `origin` is a local bare repo — but `ci:full` now includes every lane except the
QEMU guest one, and CI runs the same tasks.

What that milestone is *not*: multi-host, warm standby, compaction, or anything measured
on real hardware. Those are the phases below, and they start after the slice works.

## Where each phase actually is

| Phase | State | What is true, and what is not |
|---|---|---|
| 01 skeleton (simio + DST + obs) | **model** | Simulable interfaces, the DST harness and the metric catalog all exist and are enforced by lint. Metrics are recorded by the paths that own them; wiring continues with each new path. |
| 02 guest layout (3 devices + OverlayFS) | **not started** | Needs guest mounts / a VM. Nothing in the durability chain depends on it. Spec below. |
| 03 vhost-user | **3.1 integrated + served by the Agent** | A real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`task test:integration:qemu`). A **Linux** guest boots the lane too (`task build:guest`). Since the keystone the *Agent* binds a socket per volume and serves `blockdev.Device` behind it, so there is now a device to issue FLUSH against. The lease adapter landed with the keystone's review-zone half, so a FLUSH is the full §14.4 remote path and not local-mode. 3.2 reconnection and 3.3 inflight-shmfd untouched; RISK-10 open. |
| 04 WAL/CoW format | **write path integrated**, rest model | A guest's WRITE lands as a replayable WAL record with **0 PUTs** (`internal/blockdev`); the WAL is a directory of segments so truncation reclaims (`WAL-SEGMENTS-SPEC.md`). FLUSH, the uploader and checkpoints are integrated too, and increment 3's scheduler is what finally reclaims a byte. Format review still pending (human-review zone). |
| 05 encryption (AES-256-GCM, DEK/KEK) | **integrated** | Increment 6 wired it end to end: `-kek-file`/`-kek-id` on the Agent, the unwrap at attach, and every object a served volume PUTs is ciphertext. The *read* half was integrated and wrong until 2026-08-02 — see DEV-0019, and note that this row said "model / —" while a real defect lived in the path it declined to describe. |
| 06 remote WAL (batching, idempotent PUT, summary) | **integrated** | A real Linux guest drives the whole chain: `TestAGuestSurvivesCheckpointAndTruncation` boots a kernel, writes, `fsync`s, takes a checkpoint, truncates, reboots and reads back — proven against a planted bug (deleting the bucket between the two boots). The previous entry here, "No guest has ever driven a PUT", was true when written and outlived that by two increments. `WriteSummary` remains the one piece with no producer. |
| 07 Control Plane + leases + fencing | **model**, provisioning integrated | Fail-closed lease, resumable promotion, term guards. **A volume can now be created** (`controlplane.Provisioner`, `control-plane -seed-volume`): row + wrapped DEK + descriptor, verified against Postgres 18. |
| 08 recovery (S3 authority) + rebuild-metadata | **model** | Objects are validated before they count as durable; the rebuild includes the snapshot catalog. |
| 09 snapshots + clone + resize | **partial model** | The clone chain is persisted and a clone reads through its parent (DST arm + planted bug). Sealing is still synchronous (DEV-0007). |
| 10 objectization + checkpoints + GC + I/O classes | **partial model** | The GC marks reversibly; there are still no segment objects (DEV-0007). |
| 11 cross-host + cordon/drain + capacity | **partial model** | The drain is idempotent across crash boundaries. `CloneCrossHost` is gone: the destination Agent builds its own view now (DEV-0007). The drain's bulk pass still materializes on the Control Plane. |
| 12 warm standby + compaction + flatten | **not started** | Born with the by-key index ADR-0012 and ADR-0014 both need. |
| 13 hardening | **13.1 model** | Typed lifecycles (ADR-0009). 13.2 (real-hardware fault injection + measured runbooks), 13.3 (backend conformance per version) and 13.4 (**INV-19**, the last pending invariant) need infra. |

**Invariants:** 21 of 22 active, each with a checker proven to catch a planted bug.
INV-19 is pending and becomes binding the moment two Agents can run different formats.
See `INVARIANTS.md`.

**Test backlog:** the 2026-07-25 six-way audit found 78 gaps (7 critical); **all 78 are
closed**, each naming the commit. The one item still open is below and did not come
from that audit.

---

# Open work

**How this region is written while five tracks run at once (from 2026-08-03).**
`PARALLEL-PLAN.md` splits the remaining 31 increments into five tracks that share one
working tree, and every increment in every track ends by appending a section here. They
would all append at the same place — the end of this log — which is a five-way conflict on
every merge, for a file where a conflict is pure noise: nobody's paragraph contradicts
anyone else's. So this region **ends with one empty subsection per track**, and a track
appends only inside its own. The alternative, a `STATUS-<track>.md` per track, was rejected
because CLAUDE.md makes this the *only* file that tracks state — five files would have to
be merged into it at integration anyway, having lost the single place a reader looks first.
The tables at the head of the file are deliberately **not** part of this: they are counters,
every branch would bump the same integers, and they are recounted once, at integration, by
track A.

Three deviations, one gap, and one latent correctness hole found by the 2026-07-26
audit. The build order for all of it is `BUILD-INVENTORY.md`.

**Landed 2026-07-27** (increments 0 and 1, plus the guest lane): the object store is
reachable from both binaries and signs its requests — it never did, `s3.New` resolves no
credential chain, so every request had been going out unsigned; a bad `-host-id` fails on
the flag instead of writing zero rows forever; a failing reconciliation cycle now says so
with its backoff; and a volume can be provisioned. (An earlier revision of this line also
claimed "a Linux guest boots the lane". It did not: see DEV-0018.) See
`BUILD-INVENTORY.md` for what each increment covered.

**Landed 2026-07-28** (ADR-0022): the guest kernel is fetched into
`_output/guest/vmlinux` and pinned by sha256, instead of being read out of
`../spinbox/_output/`. It can come from a sibling checkout or from a mirrored image, and
either way it is rejected unless it hashes to the pin. `guest:verify` now also asserts
`CONFIG_VIRTIO_BLK`/`PVH`/`BLK_DEV_INITRD`/`SERIAL_8250_CONSOLE` by reading the config
the kernel embeds — all four proven to fail by planting them, along with a wrong hash, a
non-ELF, and every source missing. Running the gate for it surfaced DEV-0013 — `task
lint` had been red since the day before and nobody had run it — which is resolved and now
lives in git rather than here.

## The durability scheduler's two loose ends

**A false fencing witness, found and fixed.** A resumed log reports `durable = 0` until
its base arrives (increment 5's lazy recovery), and a checkpoint taken in that window
compares the object store's real durable point against 0 and raises
`ErrDurablePointMismatch` — which **ADR-0023 reads as "another writer is in this epoch"
and acts on by fencing**. A healthy host would have fenced itself out of its own volume on
every restart. `wal.Log.BasePending` now gates the scheduler, and it is covered by a test
in each package. It was a DST run that tripped over it, which is the argument for the arm
below.

**~~The DST arm is not done.~~ Done 2026-08-01.**
`scenarioTruncatedVolumeSurvivesARestart` now drives the real scheduler on every seed:
after the restarted Agent reads back what truncation reclaimed, it writes again and
`VolumeManager.Checkpoint` publishes and reclaims behind it.

The first attempt broke the planted bug and the cause is worth keeping, because it is a
trap the next scenario can fall into: **phase 1's hand truncation is the point of the
scenario, not scaffolding.** Moving it after the restart left the local segments intact,
so the resumed Agent replayed them and read the real data back even with an empty base —
no zeros, no violation, a planted bug that silently proved nothing. The truncation must
happen *before* the Agent starts, because the on-disk state a restart has to survive is
the whole premise.

The ordering also matters and is not arbitrary: the checkpoint must come *after* the
read, because the read is what waits for the base, and a checkpoint taken while the base
is pending is the false fencing witness above.

**~~The planted bug the spec asked for does not exist.~~ Replaced 2026-08-01.** It
proposed "a scheduler that truncates to `durable` rather than `published`". That is
unreachable: `StrictOrder.AllowTruncate` refuses `upTo > published` at the source, so the
mistake cannot be made through the API. The invariant is enforced where it should be, and
a planted bug the API rejects proves nothing about the checker.

The replacement is the other half of the same §12.2 sentence — *"deja de ACKear
durabilidad, **deja de publicar checkpoints/manifests**"*. Two obligations, one lease; the
first has had a checker since 7.2 and **the second never did**, though the gate is one
`if` at the top of `checkpointOnce`. `CheckpointLeaseChecker` +
`scenarioLapsedLeaseStopsPublishing` now watch it, and `plantedProofs` counts 16
behavioural proofs.

Three things make it a proof rather than a formality:

- **The verdict comes from the object store, not the gate.** It counts checkpoint objects
  before and after the attempt against `lease.Manager.Valid()`. A gate that returned the
  right error and published anyway would satisfy any assertion on `err`.
- **The bug is the wiring, not a fault.** `Lease: func() bool { return true }` — the lease
  question asked once at start-up instead of resolved per call, which is what
  `applyLease` does and what `volume.go` warns against. Same category as DEV-0012: not a
  broken algorithm, a question that stopped being asked.
- **The publish genuinely succeeds under it.** The epoch object names this host, so
  §12.4's `VerifyPublisher` lets it through — being fenced by one's own monotonic clock is
  precisely the case where nobody has taken the epoch away yet. The `if` is the only thing
  there, and until now nothing proved it was.

The scenario also found something worth writing down: §14.4 uploads (step 4) *before* it
checks the lease (step 5), so a lapsed-lease FLUSH leaves its objects in S3 and refuses
only the ACK. The store can then prove a longer durable prefix than the volume ever
ACKed — which is exactly the material a fenced host would publish, and why "there was
nothing new to publish" is not the reason the honest arm stays quiet. The same planted
wiring trips `DurableAckLeaseChecker` too, asserted alongside it, because one cached
answer loses both obligations.

## The guest lane in CI

**Decided 2026-08-02 — ADR-0025.** The lane runs inside the published QEMU runtime image
as a container job, rather than installing its dynamic dependencies on a bare runner,
because that keeps one definition of the dependency set in `Dockerfile.qemu`. It is
implemented: `.github/workflows/ci.yml` has a `guest-lane-image` job that probes for
`ghcr.io/<repo>/qemu:<version>` and a `guest-lane` job gated on it, which skips with a
notice rather than failing the gate when the image has not been published yet.

What is still true is narrower, and it is the same caveat as everywhere else on this
page: **no CI workflow has ever executed**, because `origin` is a local bare repo. The
job is written and reviewed, not observed.

## ~~DEV-0007~~ — the spine's second half *(the chain closed 2026-08-02)*

ADR-0018's definition of done is one volume, one host, a real QEMU guest running
**write → FLUSH → verified object → checkpoint → truncate**. The write is done. The rest
has never been driven by a guest, and two blockers stand in front of it — both separable,
neither deep:

1. ~~No guest in the lane can emit a FLUSH~~ **cleared 2026-07-27** (`e8bbdab`,
   `cef9881`). **Corrected 2026-08-02 (DEV-0018): no test did this.** The claim was that a real Linux
   guest boots the lane in ~1.1 s under TCG, running a static Go
   `/init`, and reports a verdict before powering itself off. `task build:guest` builds
   the initramfs; `task guest:verify` checks it and the kernel.
   **The blocker was never the firmware.** The audit said `-kernel` was impossible
   because the extract stage omits `linuxboot_dma.bin`; it does, and it does not matter —
   spinbox's kernel is an ELF with Xen PVH notes (`CONFIG_PVH=y`) and QEMU enters it
   through `pvh.bin`, which was being extracted all along. Proven by booting with the
   blob deleted. The real reason was simply that no kernel image existed.
   **Closed 2026-08-02.** `TestALinuxGuestIssuesFLUSH` boots the pinned kernel against a
   `vhost-user-blk` device the Agent serves over `wal.Log`, and a real kernel's `fsync`
   makes a record durable in a verified object. Getting there needed the DEV-0018 fix:
   a guest re-initialises the device when the firmware hands off to the OS, and the queue
   loop stayed parked on the first kick.
2. ~~`wal.Log` has no mutex~~ **cleared 2026-07-26** (`7afff77`). `Log` grew its own
   lock rather than the Agent being declared its single owner: `Log` is what owns the
   invariants, so that is where the guard belongs. Two mutexes — `mu` for state, held
   only for local work and **never across an object-store PUT**, and `flushMu`
   serializing durable steps. The constraint on `mu` is load-bearing: holding it across
   the upload would put S3 latency in the guest's WRITE path (§5.3, INV-18) by the back
   door and blind the Agent's reporting for the length of an S3 stall — when the gap
   those accessors report is the RPO that is growing. Both halves are pinned by tests
   proven against that planted bug.

**ADR-0018's definition of done is now met, by a test.**
`TestAGuestSurvivesCheckpointAndTruncation` boots one volume **twice**, through the real
`agent.VolumeManager` with production's wiring (`hostio` for the socket and the kernel
objects, `real.Disk`, a filesystem object store):

1. the guest writes eight scattered blocks and calls `fsync` — one FLUSH, answered out of
   §14.4, so the records are in verified objects;
2. the Agent publishes a checkpoint and truncates to it — measured, not assumed: **7 of 8
   segments reclaimed**, which is the moment the object store holds the only copy;
3. the Agent and the guest are both restarted, and the guest reads the same ranges back —
   out of a read view rebuilt from S3, because there is nowhere else left.

Two things the writing of it turned up, both about making the test able to fail:

- The guest's eight writes arrived as **one** 32 KiB record: the page cache merges
  adjacent dirty blocks into a single virtio request, and a WAL segment is sealed by the
  append that would overflow it — so nothing sealed and the truncation reclaimed nothing.
  They are scattered at a 64 KiB stride now, which the kernel cannot coalesce across.
- The first planted bug was **invalid**: pointing both Agents at a different bucket left
  them consistent and the test passed. Deleting the bucket between the two boots is the
  real one, and the guest then reports `read-back mismatch at 1048576`.

`guestinit` gained a `spin.mode=verify` cmdline mode so the second boot reads without
writing. Doing it in one boot would prove nothing: the data would still be in the page
cache and in the local WAL.

**The clone's read chain closed 2026-08-02** (`CLONE-CHAIN-SPEC.md`). §20 said a clone
"reuses the parent snapshot's already-durable objects, with no data copy"; nothing made
that true. The clone's Agent recovered against the *clone's* volume id, which finds
nothing — every object the parent wrote is under the parent's — so the base installed
empty and **the clone read zeros for everything its parent ever wrote**. A volume
advertised as a copy, delivered blank.

`volumes.parent_snapshot_id` now carries the link, `Clone` sets it, the descriptor carries
it for §22.5, `DesiredVolume` carries it and the parent's volume id to the Agent (which
cannot look either up — ADR-0021), and `fetchBase` materializes the parent snapshot and
layers the clone's own recovery over it.

Four things the implementation turned up:

- **`fetchBase` only ran when a volume was *resuming*.** A clone has no local segments, so
  the parent view was never fetched at all. It is keyed on `resuming || has a parent` now,
  and those are genuinely two different reasons to need a base.
- **`SetBase` after the fact is wrong, and `cow` was right to refuse it.** An unlayered map
  discards its tombstones as it replays, so giving it a base later would uncover every
  range the clone was told to DISCARD. The layering has to happen at construction —
  `recovery.RecoverOver`.
- **`Clone` wrote no descriptor**, so rebuild-metadata could not see a clone at all. It
  writes one now, reported-not-rolled-back, exactly as provisioning does.
- **The rebuild needs two passes.** A clone's parent snapshot belongs to a *different*
  volume that may not have been rebuilt yet, and the graph is genuinely circular
  (snapshots reference volumes, a cloned volume references a snapshot). Its test mints the
  clone's id *first* so the sort order is the one that breaks a single pass — minting the
  parent first made the test pass for the wrong reason, which is how it was written the
  first time.

**And the clone work found a bigger one, on the failover path.** The rule for "does this
volume need a read view from the object store?" was *resuming, or a clone*. A volume
**promoted to this host** (§12.3 — a drain, a failover) is neither: no local segments, no
parent snapshot, and everything it owns written by a previous epoch on another machine.
The Agent skipped the base fetch entirely and served **zeros for its predecessor's whole
volume**, with no error anywhere.

INV-09 was never violated in the object store — `recovery.DurablePrefix` finds the data
and the drain proves it does. What nothing checked is whether the **Agent on the
destination ever asks**, and it did not. The invariant held and the guest still got
nothing.

The rule is now "there is an object store", which covers all four cases — resumed, cloned,
promoted, and genuinely new (which recovers an empty view, the right answer, for one
LIST). `scenarioAPromotedHostReadsThePreviousEpoch` drives the real promotion sequence,
recovery point (§12.5) included, and putting the old rule back turns it red on the first
seed.

Two tests had to learn the ordering that used to apply only to resumed volumes: a
checkpoint is declined while the base is pending (ADR-0023's false-witness guard), so a
read comes first. A real Agent satisfies that by itself, because the guest reads.

**`CloneCrossHost` is deleted (2026-08-02).** It materialized the parent snapshot **on the
Control Plane** and returned the view, which its only caller — its own test — discarded.
That is the wrong machine: the bytes landed in the Control Plane's memory, warming nothing
on the destination, and ADR-0021 puts the data path in the Agent. It had no production
caller at all.

What replaced it is not new code. With the chain link persisted and the base rule fixed,
the destination's **own Agent** builds its view from the object store — a cloned volume
through its parent snapshot, a promoted one through the epoch chain. The advisory §28.2
pre-check went with it: its stated purpose was to refuse before starting a cold
materialization, and there is no longer one to protect. The authoritative bound is
unchanged, because it was always a predicate of `CreateVolume`.

Two claims in its tests were about `Clone` rather than about cross-host, and were ported
rather than dropped: a clone from an **orphan snapshot** (its volume is gone) fails and
creates nothing, and a **refused clone charges nothing** — ADR-0017's structural claim,
that the destination is charged when the row naming it exists.

**The drain's bulk pass has the same defect and is deliberately left alone.** It calls
`FromEpoch` and discards the view, with a comment saying "this is where the cold RTO is
spent (§22.3)" — but it spends it on the Control Plane. Its final pass is different and
must stay: it uses `Progress.UpTo` for `guardDurableFloor` and the recovery point, and the
materialization is the *proof* INV-09 rests on. Changing what that proves is a
fencing/durability review-zone decision, not a cleanup.

**Snapshot sealing — superseded 2026-08-03 by ADR-0026 increment 3.** `internal/snapshot`
and its `Capture`/`Seal` split went with the checkpoint chain in increment 4;
`SNAPSHOT-LIFECYCLE-SPEC.md` described a package that no longer exists and is deleted. §19's split
survives in a smaller form — `wal.Log.Freeze` is the capture, `image.PublishSnapshot` is
the seal — and two of that spec's findings carried over intact: the manifest is
create-only so a retry converges on its own manifest and refuses a different one at the
same id (`image.ErrSnapshotExists`), and the pause is measured where the capture is, not
around the whole operation.

**§19's two mandatory metrics are still unrecorded.** `internal/obs` registers
`snapshot_pause_duration_seconds` and `snapshot_publish_duration_seconds`; the code that
observed them was deleted with `internal/snapshot`, and neither `ensureSnapshot` nor
`snapshot` observes them. It is the one piece of §19 that increments 3 and 3b did not
close, and a metric nobody records is a metric that is missing during the first incident
that needs it.

**Objectization, lazy loading and the cold-RTO work are withdrawn, not deferred to a spec.**
`OBJECTIZATION-SPEC.md` described segments feeding `recovery` and `materialize`, both of
which are gone (increment 4); the spec is deleted with them. Bounded replay is not a property V1 has: there is no
mid-session replay to bound. §22.4's lazy loading is the same. Under ADR-0026 the cold path
is one manifest and its chunks, and what shortens it is placement (increment 5), not a new
object kind.

## DEV-0011 — a segment's space is charged as used, not reserved at creation

`WAL-SEGMENTS-SPEC.md` asks that creating a segment be charged against the device budget
*before* the first append, so out-of-space is reached at a segment boundary rather than
mid-record. `segments.create` charges only the 64-byte header; the rest is charged append
by append. Reserving the whole segment means growing the file to full size, and a segment
whose tail is zero-filled cannot be replayed by reading to the end — the zeros decode as
a bad magic. That needs `fallocate` in `simio/disk` plus a durable write offset in the
segment: a format change of its own.

**Not data loss** — a refused append already leaves no partial record, and ENOSPC at
segment creation is latched exactly like ENOSPC while appending
(`TestAFullDeviceAtASegmentBoundaryLeavesNoStub`). **Waits on ADR-0013**, where the Agent
knows a volume's share of the device budget.

## ~~DEV-0019~~ — a restarted encrypted volume served its guest ciphertext *(resolved 2026-08-02)*

**The most serious correctness defect this repository has shipped**, and it survived the
increment that was supposed to own it. `internal/agent/volume.go`'s `fetchBase` passed a
literal `nil` `*wal.Encryption` into `recovery.RecoverOver` for a volume whose DEK `start`
had unwrapped four lines earlier and handed to the WAL. `recovery.ApplyRecord` decrypted
only `if enc != nil`, so with `nil` it folded the **undecrypted GCM ciphertext** into the
read view.

Nothing anywhere could notice. `crypto.Seal` returns ciphertext of exactly the
plaintext's length and stores the GCM tag separately in the record header, so the
overwrite covered the right extent with the right number of bytes; the plaintext CRC is
never re-consulted on that path; and every watermark, every object and every existing
zeros-after-restart check agreed the volume was healthy. With `-kek-file` set, every range
served from a recovered base was ciphertext handed to the guest as its own data — which is
everything truncation reclaimed, everything a promoted host inherits (§12.3), and
everything a clone reads through its parent (§20).

**Why nothing saw it** is the whole point, and it is the CLAUDE.md table's pattern
exactly: both halves were covered and neither covered the seam. `internal/dst`'s
`agent-encrypts-what-leaves-the-host` encrypts but never restarts; its
`truncated-volume-survives-a-restart` restarts but runs with `keyID 0`, plaintext, and no
KMS in its deps. The bug lived in the argument one well-tested component passed another.

**Fixed on three levels, smallest first:**

1. `recovery.ApplyRecord` now refuses (`recovery.ErrSealedWithoutKey`) a record carrying
   `KeyID != 0` when it holds no key. `KeyID 0` is not a key version — it is the on-disk
   marker for a cleartext payload (§14.1, `wal.ErrUnversionedKey`) — so "sealed record,
   no key" is a contradiction rather than a mode. This makes the whole class
   unrepresentable in recovery, in materialization and in any future caller, which is
   why it was chosen over fixing the two call sites alone.
2. `Volume` carries its `enc`, and `fetchBase` passes it.
3. `parentView` re-binds the **same DEK to the parent's id** before materializing it.
   Passing this volume's `enc` would have been a second bug: a clone inherits the
   parent's DEK and its version (`controlplane.Clone`) so the chain stays readable, but
   `crypto.deriveNonce`/`crypto.aad` bind the volume id and `Encryption.Decrypt` opens
   with its own `VolumeID`, so the clone's binding fails `Open` on every parent record.

**Proven by** `encrypted-volume-survives-a-restart` (mandatory DST set — encryption *and*
a restart, the cross that did not exist), `TestPlantedBugRestartServesCiphertext`, whose
planted arm replays the volume's own sealed objects the pre-fix way and watches
`DurableRangeChecker` catch it, and two tests in `internal/recovery`. The checker grew a
second violation shape for this: `ForeignBytesAfterRestart`, because zeros are what a
*missing* base looks like and this is what a base built *wrongly* looks like.

One consequence worth keeping: restarting an Agent **without** `-kek-file` used to be the
cheapest route to the silent defect and now costs the volume its reads instead
(`TestRestartWithoutTheKEKRefusesRatherThanAnswering`). One missing flag must not cost a
guest its data.

## DEV-0020 — a clone chain deeper than one link cannot be materialized

Found while fixing DEV-0019, and **not** fixed with it — it is a different defect and it
predates that one. `materialize.FromSnapshot` resolves only the objects the parent's
manifest lists, all under the parent's own volume id, so for `chain_depth > 1` the
grandparent's extents are never fetched. A clone of a clone reads its grandparent's
ranges as whatever the base underneath says — today, nothing.

Unrelated to encryption: the same hole is there for a plaintext volume. It becomes
reachable the moment anything creates a clone of a clone; `controlplane.Clone` already
increments `ChainDepth` past 1 without complaint. §19/§20 need to say whether a chain is
walked at materialization or flattened at clone time before this is implemented, so it
is recorded here rather than decided in code.

## ~~DEV-0012~~ — a self-fenced log still accepts WRITEs and still serves reads *(closed 2026-08-02: not a divergence)*

**Closed by reading the document rather than by changing the code.** §12.2, line 641, says
a SELF_FENCED Agent *"puede seguir sirviendo reads de su caché mientras QEMU siga
conectado, **según política**"*. The line **grants** the behaviour and delegates the
choice. So this was never a doc↔code divergence: it was the policy §12.2 asks for, unwritten.

It is written now, and in the code rather than here — at `wal.Log`'s `fenced` field, where
a reader who wonders why `Write` does not consult it will actually be standing. The
policy, in short: `fenced` stops every *durable* operation and nothing else; nothing a
fenced host writes is ever ACKed durable or published (INV-06/09/10 hold with `Write`
ungated, because they are properties of the durable path); the accepted exposure is a
stale read reaching a guest plus device pressure; and stopping a guest's I/O is the
*Agent's* job through the Control Plane's refusal, which is the right authority for "you
no longer own this volume" — a host's own lease clock is not.

**Two fencing triggers, and the distinction is what the record kept losing.** The Control
Plane refusing a volume's report tears the runtime down — log, socket and device — and
that is what the keystone closed. The Log self-fencing on its own lease check stops the
durable path and leaves the runtime up. Five places said "Resolves DEV-0012" without
saying which half; they now say.

**`Log.Fenced()` has no production caller and, under this policy, should not.** It exists
as proof — the DST scenarios and the INV-06 unit tests read the transition through it
rather than through a flag they set themselves — and as the seam a stricter policy would
consult on the day one is chosen. Recorded here so it is not mistaken for the
CloneCrossHost pattern on a later sweep.

**What would reopen this:** a guest observing a fenced log *across a reconnection gap*
(increment 3.2), where the front-end reattaches to a log whose authority has changed
without the Control Plane having said so. That is the case the original entry was worried
about, and it is still the one worth watching.

## Gap 1 — a GC sweep cannot see an anchor its listing has not caught up to

Narrowed, not closed, by ADR-0012: the epoch ceiling no longer licenses destruction, and
the reproduction that decided it — a hole in the listing *below* a durable point marks
ACKed data with no manifest involved — showed no index would have helped. Strongly
consistent LIST stays a precondition, certified per backend by `TestListSeesAFreshPut`
(§6.1, blocking). **Waits on Phase 12's segment format**, which is born with a per-volume
index readable by deterministic key.

## Removed 2026-08-02: `cow.ActiveMap`

87 lines and their test, plus the `github.com/RoaringBitmap/roaring/v2` dependency they
were the only reason for (and two transitive ones with it). `ActiveMap`, `SegmentIndex`,
`SegmentRange`, `Location`/`LocationKind` and the `SegmentSize` constant had **no user
outside their own two files** — every path in the tree uses `cow.IntervalMap`. The same
category as `CloneCrossHost`: tested, plausible, and called by nothing.

One fact left with it, so a future reader can find it: `SegmentSize` was where §4/§13.1's
**64 KiB CoW granularity** appeared in code. It survives in
`arquitectura_mvp_volumenes_remotos_v5.md` and in `REFERENCE.md`'s §13.1 row, and
the objectization spec planned a different value (128 MiB) and a different structure — so
the constant was not just unused, it was a value nothing intends to keep. (That spec is
gone too, with the object kind it described.)

Production coverage 90.7% → 90.6%; the file was at 100%, so removing it lowers the
average slightly. That is the floor working as intended rather than a regression.

## Clean shutdown is observed, since 2026-08-02

`integration/e2e`'s `TestBothBinariesShutDownCleanly` sends SIGINT to each binary and
asserts it exits within the timeout, exits zero, and prints its parting line. Before it,
`testinfra.Process.Stop` had **no caller in the tree** — every lane either waited for a
process that exits on its own or SIGKILLed one — so `signal.NotifyContext`, the Control
Plane's `-shutdown-grace` and the Agent's `defer volumes.Close()` were code nothing had
asked to run.

**Worth recording, because it is the second time this exact mistake was made in this
repository:** the test first asserted that a second Agent could claim the `--data-dir`
afterwards, on the reasoning that this proved `volumes.Close()` had run. It proves
nothing — the kernel releases a flock when a process exits, however it exits — and the
assertion passed with `defer volumes.Close()` deleted. Planting the bug is what found it.
The assertion that survives is the one a supervisor can actually tell apart, and its own
plant (dropping `os.Interrupt` from `NotifyContext`, which is the bug the Control Plane
really had) turns the lane red.

## The ADRs, trimmed 2026-08-02: 25 → 21

CLAUDE.md's test is that an ADR is justified only when all three hold — it spans
components, it contradicts or extends the design doc, and getting it wrong is expensive.
Four failed it, and all four had drifted besides:

- **ADR-0001 (stack)** — a Phase-0 inventory of Go/Taskfile/golangci, all of it now in
  `CLAUDE.md`'s Stack section, which is where someone writing code actually looks. It
  also still recommended roaring bitmaps for an active map that no longer exists.
- **ADR-0002 (parallel tracks)** — a *process* decision: a Planner role scheduling
  parallel increment tracks against a hot-zone list, citing a `PLAN.md` deleted on
  2026-07-26. This repository does not run that process. RISK-09 went with it.
- **ADR-0004 (filesystem object store for Phase 01)** — a staging decision, explicitly
  superseded by ADR-0010 when the S3 subsystem landed. The situation it describes is
  over.
- **ADR-0006 (sqlc + TestContainers)** — the rule itself is in `CLAUDE.md`'s SQL
  section. The one thing that was *only* here — why `metadata.Store` has two
  implementations, and why one real PostgreSQL for both is impossible — moved to a
  comment on the interface in `internal/metadata/metadata.go`, which is where the
  decision is made.

The citations were removed *before* the files, in one pass across code, `Taskfile.yml`,
`.github/workflows/ci.yml`, `CLAUDE.md`, `api/buf.gen.yaml` and the surviving ADRs, so
no window existed where the tree cited a document that was not there. Verified by
`comm` against `REFERENCE.md`: every ADR the code cites resolves, and every ADR file has
a row.

**ADR-0013 is deliberately untouched** and remains the open question under "Decisions
waiting on a human".

## Misdirected `§` citations, corrected 2026-08-02

Following a citation from the code now lands on text that supports the comment. Two
patterns, both found by sampling references against the document rather than by reading
the comments:

- **`§30.3` was doing work it cannot do.** It is a *roadmap item* ("vhost-user-blk with
  raw backend + reconnection + inflight shmfd"), and fifteen comments cited it for the
  single-queue decision and the 128 queue depth — including `features.go`'s "§30.3 is
  explicit about a single queue", which it is not, and a **runtime error string** in
  `device.go`. Those facts live in §4's decision table (`| Queues | Una |`,
  `| Queue depth | 128 |`) and, for multi-queue as a non-objective, in §3. The four
  places where the roadmap item genuinely *is* the subject keep the citation.
- **`§26.4` does not exist.** §26 has 26.1, 26.2 and 26.3 and stops. Both citers meant
  §23's "PostgreSQL caído", which is the passage that actually says Agents keep serving
  while the Control Plane is unreachable. No DEV was opened: a mistyped citation is not
  a doc↔code divergence, and an open DEV would block the gate for a typo.

Also corrected: `.github/workflows/ci.yml` said the QEMU lane "runs on a developer
machine only" and that STATUS.md carried an open decision about it — contradicted by the
`guest-lane` job seventy lines above it in the same file.

## The e2e lane proves a durable ACK, since 2026-08-02

It used to stop at `os.Stat(sock)` and two log lines. That left the closure governing
every durable ACK —

```go
Lease: func() bool { return loop != nil && loop.LeaseValid() }   // cmd/volume-agent
```

— able to answer `false` for ever, or `true` for ever, with all five tests green. **The
QEMU lane did not cover it either**, which is the part that was not written down anywhere:
`integration/vhost` builds an `agent.VolumeManager` *in process*, with a filesystem-backed
store and a lease it grants itself, so nothing in the tree exercised the binary's flags,
its S3 credentials or its Control-Plane-driven lease on the data path.

`TestAGuestMakesTheDeploymentWriteADurableObject` now boots the pinned kernel against the
socket a real `volume-agent` bound, with the real Postgres and RustFS the lane already
starts. The guest's `fsync` is the load-bearing assertion and the object under
`wal/<volume-id>/` is the corroborating one. The QEMU guest helpers moved to
`internal/testinfra` so both lanes share one command line.

**Two things planting the bug taught, both of which contradicted the increment's own
spec:**

1. **The object in the bucket is necessary and not sufficient.** §14.4 uploads at step 4
   and verifies the lease at step 5, so an object lands even when the ACK is refused. The
   spec had listed it as the primary observable; it would have passed with the lease
   closure returning `false` for ever. The guest's `fsync` returning `EIO` is what
   actually catches it.
2. **A plant must reach the binary.** The first two attempts ran `go test` directly and
   tested a stale `_output/bin/volume-agent`, so the plant appeared not to fire and looked
   like a defect in the fencing chain. `task test:e2e` rebuilds; `go test` does not.

**A latent trap this found:** a Unix socket path is bounded by `sun_path`, 108 bytes, and
`bind(2)` reports only `EINVAL` when it overflows. `t.TempDir()` embeds the test's name, so
`<tmp>/<TestName><digits>/001/run/<uuid>.sock` was 112 bytes for a test whose name was
long enough — this one. The lane was one long test name away from failing for a reason no
error message explains. The fixture now takes its socket directory from a name that has
nothing to do with the test. **The same trap exists in production**: an operator passing a
long `-vhost-socket-dir` gets `bind: invalid argument` and nothing else. Not fixed here —
it is a `hostio.Listen` error-wrapping change, and it belongs with its own increment.

## §14.8 is implemented end to end, since 2026-08-02

Two halves, and the second was undesigned in any document — finding it is what this
increment was mostly worth.

**The mode the Control Plane sets now governs the ACK.** `internal/agent` never read
`DesiredVolume.durability` and `Log.SetDurabilityMode` had no production caller, so every
volume ran in the default `remote` mode whatever the catalog said. An unset durability is
**refused** rather than guessed: `wal.ModeFor` already promised "an unknown value is an
error, never a silent fallback to remote", and the Agent keeps that promise instead of
answering for the Control Plane. Every fixture in the tree now states the mode it means,
which is the cost of that decision and worth paying.

**And "S3 is asynchronous" now has something doing it.** In `local` mode `durableStep`
returns before the upload block, and that block is the only place in the tree draining
`batcher.Pending()` — the tests said so without meaning to, since `rpo_test.go` builds a
local log "with no remote path at all". So a `local` volume put nothing in the object
store, ever: `durable` stayed 0, no checkpoint could publish, `TruncateLocal(published)`
reclaimed nothing, and the WAL grew for the life of the volume. That is the
pre-increment-3 failure, and it would have applied to every `local` volume the moment the
first half landed. `agent.drainOnce` runs in the durability scheduler that already exists,
under the lease and io-class gates that already exist.

**The lease split is the decision worth reading twice.** Uploading is *not* gated: an
object is create-only under a deterministic key and INV-21 hard-fails a divergent PUT, so
writing one asserts nothing about who owns the volume — and a fenced host holds the only
copy, so refusing to upload would turn a fencing event into data loss. Advancing
`durable_sequence` *is* gated, on the same monotonic check §14.4 step 6 uses: it is the
claim, INV-03 orders it, INV-13 truncates against it, and a promoted successor reads it.
§14.8 frees the FLUSH *ACK* from the lease in local mode; it does not free the watermark.

Two things the tests caught in the implementation, both fixed in the contract rather than
in the test:

- The first `DrainPending` returned what *that call* uploaded, so a drain refused for a
  lapsed lease left the volume stuck below its own objects until the guest happened to
  write again. It returns the highest sequence a verified object *covers*, which is what
  its doc comment had claimed all along.
- The DST arm's first version emitted a `durable-ack` event when nothing had been ACKed,
  and with the lease flag taken from the closure under test. Both are false entries in a
  trace: the event exists only when durable actually advanced, and `LeaseValid` carries
  ground truth, because the checker's job is to compare the claim against reality.

`local-volume-drains-without-claiming` is in the mandatory set, under the INV-06 checker
that already governs the other path to `durable`. Its planted bug is the reading this
increment rejected — a lease resolved once at construction instead of per call, which is
the same shortcut `CheckpointLeaseChecker` plants.

## The SELF_FENCED rule is cited where it lives, since 2026-08-02

The sentence the durability scheduler enforces — *"deja de ACKear durabilidad, deja de
publicar checkpoints/manifests"* — is at **line 641 of §12.2** ("Ciclo del lease, lado
Agent", lines 619-642). Nineteen places cited **§12.6**, which is "Escalabilidad del
fencing (por qué el lease es por host)" and contains no publication rule at all.

It propagated the way these do. `INVARIANTS.md`'s INV-06 row contradicted itself — the
prose said §12.6 while its own section column said §12.2 — and
`DURABILITY-SCHEDULER-SPEC.md` wrote "§12.6 (line 641)", carrying the correct line number
under the wrong section heading all the way into the scheduler, its tests, its DST
checker and two error strings an operator would read.

Corrected in code, tests, checkers, `INVARIANTS.md`, `STATUS.md`, the spec and ADR-0023.
Deliberately untouched: `internal/lease`, `internal/metadata`, `internal/lifecycle`,
ADR-0016 and `REFERENCE.md`, where §12.6 is cited for what it actually says — why the
lease is per host. `task dst` produces the same trace on the same seed, which is the
point: nothing changed except where a reader is sent.

## ~~`placement.Policy` is implemented and the clone path does not call it~~ *(closed 2026-08-03, increment 5)*

Found while accepting ADR-0026, and it matters more under it than it did before.

`placement.Policy.Choose` implements §20's three-step order exactly — source host first,
then a host with the snapshot cached, then any host with capacity — and its **only
production caller is the drain** (`controlplane/drain.go`). `controlplane.Clone` takes the
destination host as a parameter, so nothing consults the policy when a clone is placed.

Under ADR-0026 that stops being a missed optimization. A cross-host clone now pays a full
download with no warm standby and no lazy loading to shorten it, so "start the clone where
the data already is" carries most of the boot-time story. The policy is written and
correct; wiring the clone path to it is a small increment and it is not done.

Two properties to preserve when it is: same-host is a *preference* (`Choose` already falls
through when the source host is full, cordoned or gone — making it mandatory would couple
scheduling to a host with no obligation to be up), and the locality is *time-bounded* (the
source host holds the data only while it still holds the volume).

## ~~DEV-0021~~ — the recovery-point floor was fictional *(closed 2026-08-02 by deletion)*

Found while executing ADR-0026's increment 1, and it is the finding, not the deletion.

§22.1 describes a **summary object**: a strongly consistent record of what a writer ACKed,
used to cross-check the contiguous prefix and, crucially, as the **floor a new epoch's
recovery point may not drop below**. `recovery` read it on every checkpoint and
`boundaryFloor` raised the floor from it.

**Nothing ever wrote one.** `wal.Log.WriteSummary` had no production caller — only tests
and DST scenarios. So in production `readSummary` always returned `ok=false`, the
cross-check was a permanent no-op, and **the boundary floor was only ever the prior
boundary**. A `WriteRecoveryPoint` that dropped below what the previous writer ACKed would
have been accepted, silently.

**Three tests asserted the protection and all three passed only because they fabricated
the input themselves** — `TestBoundaryMustNotDropBelowWhatThePreviousWriterAcked`,
`TestDurablePointRejectsLyingSummary`, `TestFromEpochRefusesLyingSummary`, plus the DST
scenario `durable-point-under-a-lagging-list`. Removing the writer made all four fail
immediately, which is how it was found: the mechanism they proved existed only inside
them. That is CLAUDE.md's fourth "prove the test can fail" case, and this is its fourth
instance.

**Closed by deletion, not by a fix.** Under ADR-0026 there is no promotion, so there are
no epoch boundaries for a floor to protect; `wal.Summary`, `SummaryKey`,
`recovery.readSummary`, `ErrSummaryOverclaims`, `SummaryOverclaim`, the `boundaryFloor`
summary read and all four proofs are gone. `durablePrefix` returns one number again,
because the second existed only to tell a lying summary apart from a superseded epoch.

**If ADR-0026 is ever reversed**, this is a hole that comes back with it, and it should
come back with a writer before it comes back with a test.

## ADR-0026 increment 2 is done — a volume is its image

**A volume is now uploaded when it stops, and booted from what it uploaded.** That is V1's
whole durability contract, and `internal/agent` no longer imports `internal/recovery`:
increment 4's largest deletion is now unblocked rather than theoretical.

`internal/image` is the format ADR-0026 chose — a manifest plus content-addressed chunks —
with `cow.Ranges` under it (the flattened extents of a layered view, which the map did not
expose). Chunks are sealed, and the nonce rule is the part to read twice: **it is drawn,
never derived, and a chunk is sealed exactly once, ever**, because a chunk whose key
already exists is skipped rather than re-sealed. Those two facts together make reuse
impossible, and skipping is also what keeps dedup — one mechanism, not a trade.

**The compare-and-set on the manifest is the entirety of V1's fencing.** Two incarnations
both publishing is a silent lost update, and one CAS on one key is all that stands
against it. `internal/image`'s tests assert both directions: a second publisher is refused
*and* the first host's bytes are still what the volume holds, because a refusal that had
already replaced the manifest would satisfy an assertion on the error alone.

**What went from the DST set, and why each.** `truncated-volume-survives-a-restart` and
`encrypted-volume-survives-a-restart` proved guest-visible properties through a replay of
WAL objects; both properties are now proven by
`a-stopped-volume-comes-back-from-its-image`, which is encrypted precisely because that
seam is where the same property last broke (DEV-0019).
`a-promoted-host-reads-the-previous-epoch` went with its mechanism: V1 performs no
promotion.

**Two test doubles stopped modelling anything when the boot path moved**, and neither
would have been noticed without a test going red:

- `agent`'s `unreachableStore` overrode `List` and `Get` — the two methods the replay
  called. `image.Load` asks `Head` first, which fell through to a real store, answered
  `ErrNotFound`, and the Agent read that as "no image yet", installed an empty base and
  served the guest **zeros**. It now fails every read method.
- `dst`'s `hidingStore` hid a *listing*. The image path never lists; it reads a manifest
  by key. The planted bug for the restart arm passed while proving nothing until the
  double hid `Head` and `Get` too.

That is the fifth and sixth assertion in this repository that proved nothing until
something moved underneath it. The pattern is always the same: a double written against
the methods one implementation happened to call.

**Not done in increment 2, on purpose:** the e2e observable with real binaries — a guest
writing, the Agent stopping, the object appearing, a second Agent reading it back. The
unit and DST levels cover the seam; the lane does not yet.

## ADR-0026 increment 4.1 — the durability scheduler is gone

`internal/agent/durability.go` and its tests: the checkpoint timer, `checkpointOnce`,
`TruncateLocal`, and the §14.8 drain. Under ADR-0026 a volume's WAL lives one session and
S3 receives the volume at stop, so there is nothing to reclaim mid-session and nothing to
drain asynchronously. **The drain landed in this same session and was removed by the ADR
accepted after it** — that churn is the cost of having accepted the ADR late, and it is
cheaper than the alternative, which was building increments 3-8 on the withdrawn premise.

`internal/agent` no longer imports `internal/checkpoint`, and `parentView` no longer
imports `internal/materialize`: **a clone reads its parent's *image*** with the same
operation a boot uses, rather than materializing a snapshot from a checkpoint plus the
WAL objects after it. The two remaining production users of the durability chain are now
`controlplane`'s drain, promotion and rebuild — increment 4.6.

Two DST scenarios went with the scheduler (`lapsed-lease-stops-publishing`,
`local-volume-drains-without-claiming`) and the `checkpoint-lease` checker with them; the
planted-bug ratchet is 15 → 14 with the reason recorded next to the constant, which is the
one shape of decrease that test allows.

**The guest lane became the e2e observable increment 2 was missing.**
`TestAGuestSurvivesCheckpointAndTruncation` is now
`TestAGuestSurvivesAStopAndComesBackFromItsImage`: a real Linux kernel writes and
`fsync`s, the Agent stops (which publishes), **the local WAL is deleted outright**, and a
second Agent and a second boot read the same range back. Removing the WAL is a stronger
statement than the truncation it replaced — the second boot has nothing but the image.
Green against real QEMU.

## ADR-0026 increment 4 — the remote durability chain is gone

**Deleted, with the document sections that described them, in one commit each way so no
commit exists where the tree cites something that is not there:**

`internal/recovery` (890), `internal/materialize` (293), `internal/checkpoint` (215),
`internal/epoch` (200), `internal/snapshot`, and `controlplane`'s `drain.go` (807),
`promotion.go` (438) and `rebuild.go` — none of which had a production caller outside each
other and DST. With them: `scenarios_drain.go` (801), `scenarios_recovery.go` (623), and
most of `scenarios.go`.

**§12 shrank to one paragraph.** It described a protocol governing *every durable ACK*;
V1 ACKs nothing against S3. What survives is the single obligation that can still lose
data silently — two incarnations must not both publish — and its mechanism, the
compare-and-set on the manifest. §21 and §22 became notes pointing at V2, kept as headings
rather than deleted so the citations that survive in `descriptor`, `clone` and `election`
still resolve to something that explains itself.

**Four checkers were retired with their subjects**, because a checker that cannot fire
proves nothing: `PromotionWaitChecker` (INV-11, no promotion), `NoLostAckedWriteChecker`
(INV-09, no failover), `TruncateBelowPublishedChecker` (INV-13, no truncation) and
`ImmutableSnapshotChecker` (INV-16, whose package went). The planted-proof ratchet is
14 → 8 with the reason recorded. `INVARIANTS.md` needs a pass of its own against this;
it has not had one.

**ADR-0017's behavioural tests went with the drain** and its text now says so: the
decision — committed capacity is derived, not a ledger — is unchanged and still enforced
structurally by `TestCommittedBytesIsDerivedInOnePlace`, but the interleaving of moves and
crashes it quantified over is empty in V1.

**A correctness bug this deletion exposed, fixed here.** `-race` in `ci:full` caught
`fetchBase` and `Volume.publish` racing on the image ETag — and looking at it found worse
than a race: `stop()` published without waiting for the base to be installed. The base is
everything the volume held before this session, so publishing early writes an image with
that data missing, and the CAS installs it **over the manifest the base came from**. It
would replace a volume's history with a partial view of it. `stop()` now waits for the
fetch, and a volume whose base never resolved does not publish at all: its view is not a
subset of the truth, it is a different thing.

**New finding, not resolved: `descriptor` has writers and no reader.** `provision` and
`clone` write `descriptor.json`; its readers were `gc.Reachable` and
`controlplane.RebuildMetadata`, and both are gone. It is the volume's only
self-describing anchor in the object store, so deleting it is not obviously right — but a
thing only ever written is the pattern this sweep exists to remove. §22.5 records it too.

## ADR-0026 increment 4.5 — one ACK contract, and the WAL's remote half is gone

`durableStep` is `fdatasync` then ACK. `uploader.go`, `batch.go`, `EnableRemote`,
`LeaseChecker`, the durability modes, the remote-gap accounting, `ErrSelfFenced`,
`ErrNoLease` and `ErrNoUploader` are gone, and so is `internal/ioclass` — whose only
reader was the scheduler deleted in 4.1.

**Increment 4.6's open question is answered.** `VolumeManagerDeps.Lease` had one reader
left after the ACK gate and the scheduler went: the constructor guard that checked the
dependency existed. So the lease **disappears from the Agent's data path** rather than
shrinking; the `Loop` still renews it for the Control Plane's liveness view.

**Two meanings were sharing one flag.** `fenced` meant "a durable step found the lease
invalid" *and* "a failed rollback left the tail unknown". The first went with the gate;
the second is `broken`, with `ErrLogBroken`, and `blockdev` stops calling it a loss of
authority — nothing took the volume away, the log simply cannot describe what it holds.

**The method, recorded because two attempts failed on it.** Delete a function when its
*subject* is gone, not when its *setup* mentions the removed API.
`guest-device-acks-durability-only-on-flush`, the ENOSPC arm and the WAL crash boundaries
all kept their subjects and lost only their setup lines and the assertions that named
uploads. Two tests **hung rather than failed** — they waited on a FLUSH that blocks on an
upload — which is worth knowing before touching this again.

**INV-18 is now a property of the wiring rather than a discipline.** The guest lanes still
count PUTs, and they now assert **zero on the FLUSH path too**: with a real kernel, a real
filesystem and a real object store in the loop, nothing on a guest's I/O path can reach S3
at all. The e2e observable moved with it — the artefact is the image published when the
Agent stops, not an object a FLUSH left behind.

**§6.1 was the one piece that was not deletion.** Its conformance test used the Batcher
and Uploader as a byte source to prove a real backend's create-only PUT and `If-Match`.
Those are properties of the *backend*, so the test survives with `internal/image` as its
source — a suite exercising a byte source production no longer uses would certify the
wrong thing.

**And the coverage floor was met by writing tests, not by moving the gate.** It fell to
89.9% because ~2 000 lines of well-covered production code left. `internal/storecfg` and
`internal/simio/real/s3_bootstrap.go` had no unit tests at all, and both hold real
decisions: the mutual exclusion of `-s3-bucket` and `-object-store-dir` (guessing which an
operator meant is not a thing to be clever about), and **which two error codes mean "this
bucket is already ours"** — too strict and every run after the first refuses to start, too
loose and we version and write into a bucket somebody else owns. Both proven against
planted bugs.

Six checkers have now been retired with their subjects across 4 and 4.5; the planted-proof
ratchet is 8 → 6, each step with its reason beside the constant.

## ADR-0026 increment 3 — a snapshot is a frozen view, since 2026-08-03

**★ on-S3 format.** A snapshot is `image/<vol>/snapshots/<snap>.json`, a manifest naming
chunks under the same `image/<vol>/chunks/` prefix the volume's own image uses. Nothing is
copied: a snapshot of a volume that has not changed since the last one uploads **zero new
chunks**, which is what makes §2's frequent-clone case affordable.

**`wal.Log.Freeze` is §19's three steps in one operation** — `fdatasync`, capture
`N = local`, and swap `l.view` for a fresh layer over the old one. The seal is a pointer:
`cow.NewIntervalMapOver` never writes through to its base, so the view handed back is
immutable by construction rather than by a rule someone has to remember. That is where §2's
"pausa de I/O por snapshot ~0" comes from — there is no queue to drain and no quiesce.

**Two mechanisms, because they are two objects with different lives.** The volume's
manifest moves and is written with a CAS; a snapshot never moves and is written
create-only (`ErrSnapshotExists`, §5.2/INV-16). And a snapshot does **not** advance the
volume's image: coupling them would make taking a snapshot change what a restart reads.

**A live defect closed on the way.** `parentView` loaded the parent's *live image*, so a
clone of a still-running parent would read writes made after the point it claims to
descend from. It loads the snapshot now. The DST arm is the observable that shows the
difference: write A, snapshot, write B over it, and a clone reads **A** while the source VM
is still up. The planted bug is taking the snapshot after the later writes — not an
approximation of "did not freeze", but byte-for-byte what that implementation publishes.

**What is not done, and it is the honest half of this row.** `VolumeManager.Snapshot` has
**no production caller**: nothing in the Control Plane can ask a host to take a snapshot.
`DesiredVolume` carries no pending-snapshot field and `VolumeReport` carries no taken-id,
so production can *consume* snapshots (the clone path does) and cannot *produce* them. The
trigger is its own increment — a proto field each way, plus the volume column the Control
Plane sets and clears — and it is listed below with the other callerless components rather
than left implied by this section.

## ADR-0026 increment 3b — a snapshot can be asked for, since 2026-08-03

Increment 3 left `VolumeManager.Snapshot` with no production caller. It has one now, and
it is not a call: **a snapshot request is desired state**. `DesiredVolume` carries
`pending_snapshot_id`, `VolumeReport` carries the id, the sequence and an error back, and
the loop closes when the Control Plane stops sending an id it has recorded. ADR-0021 is
what forces that shape — the Agent cannot be *asked* anything, it can only be told — and
it is also what makes the protocol convergent for free: a host that missed the report,
restarted or came back from a partition simply does the same thing again.

**No schema change.** `snapshots` already had `source_host_id`, `target_sequence`,
`manifest_key` and the CREATING state; what was missing was two queries and the wiring
between them. `ListPendingSnapshots` joins through `volumes.primary_host_id` rather than
filtering on `snapshots.source_host_id` — the request names a *volume*, only the host
serving it can freeze it, and `source_host_id` is stamped on completion by the host that
actually did (which is what §20's placement rule 1 reads later).

**The uploading is on a goroutine and the request is not.** An Agent that blocked its
reconcile loop for the length of an upload would miss the heartbeat that renews its lease
and be fenced for doing what it was told. An in-flight snapshot reports nothing at all —
a half-taken snapshot is not a fact the catalog can hold — so the row stays CREATING and
the request arrives again, which the per-volume map makes free.

**A stale writer's report is dropped, not applied.** It runs after the epoch check: a host
the fleet has moved past froze a view of a volume it no longer writes, and stamping its
sequence would publish a snapshot of a superseded state. The row stays CREATING, so the
volume's current writer still takes it. A *failed* snapshot is recorded FAILED rather than
retried forever, because the failures that heal are already retried inside the session.

**Observable, with real binaries** (`integration/e2e`,
`TestASnapshotOfALiveVolumeIsPublished`): a real Linux guest writes and `fsync`s, an
operator runs `control-plane -snapshot-volume <id>`, and the manifest appears in the
bucket **while the Agent is still serving the volume** — the socket is still there when it
lands. Everything else in that lane publishes at stop, so a snapshot that only appeared
after the Agent exited would be the stop path wearing a different name.

**Proven able to fail, three ways.** Dropping the one line that puts the id in the desired
state times the e2e lane out (60s, no object). Removing the once-guard makes the manifest
be written six times for six requests. And the "stops being reported" property needed
*both* of its mechanisms broken to go red — Status answering only about the pending id,
and the map being pruned — which is written next to the test rather than left as an
implied stronger claim.

## ADR-0026 increment 5 — a clone starts where its data already is, since 2026-08-03

`controlplane.Clone` asks `placement.Policy.Choose` instead of being handed a host, and
`control-plane -clone-snapshot` is the caller that makes either of them run in production.
Both were previously reachable only from tests: `Choose`'s one production caller was the
drain, deleted in increment 4, and `Clone`'s was never written.

**Step 1 of §20 is now a fact rather than a guess.** The snapshot's `source_host_id` is
stamped by the host that took it (increment 3b), and that host still holds the data on
local NVMe — which under ADR-0026 is most of the boot-time story, because a cross-host
clone pays a full download with no warm standby and no lazy loading to shorten it. The
table-driven test pins both halves: the clone lands on the source host, and it lands
*elsewhere* when that host is cordoned or full. The fallback host is deliberately emptier
than the source, so a policy that merely balanced would pick it in every row and the table
would prove nothing.

**The bound is computed where the decision is taken.** `Choose` is pure and advisory
(ADR-0017), so `Clone` hands `CreateVolume` the same `Limit` it admitted against, and the
ceiling is a predicate of the statement that places the bytes rather than a check some
steps before it.

**A snapshot that is not PUBLISHED is refused.** Its objects may still be uploading, or
the upload failed; cloning it produces a volume that reads zeros for everything its parent
wrote — DEV-0007's shape, reached through the catalog instead of through a missing field.

**The e2e lane found a real race in its own first version.** It waited for the snapshot
manifest to appear in the *bucket* and then cloned, and the clone was refused: the object
lands a reconcile cycle before the catalog row leaves CREATING. It now waits on the
desired state — the pending id arriving and then clearing — which observes the whole round
trip and is the honest definition of "the snapshot exists".

## The invariants were rewritten against the code, 2026-08-03

`INVARIANTS.md` had gone on describing checkers, scenarios and packages ADR-0026 deleted.
A row naming code that no longer exists is worse than a missing row — it is the reason a
reader believes a property is proven — so every row was checked against a symbol that
exists, and `withdrawn` was added as a state distinct from `pending`: pending means the
checker was never written, withdrawn means the *mechanism* is gone. Each withdrawn row
says what survives of it and what would bring it back, which is the part deleting the rows
would lose.

**The bookkeeping itself was lying, and that is the finding.** `plantedProofs` claimed six
behavioural proofs; two were fiction. `effective-single-writer` claimed one while **nothing
emitted the event its checker reads** — the scenario that did went with promotion — and
`watermark-order` claimed one that was never written. `TestEveryCheckerHasAPlantedBugProof`
could not see either, because it asserted the *map* matched the checker list rather than
asserting the proofs exist. That is the same failure mode as a checker that cannot fire,
one level up.

Closed three ways. `plantedBug` now records what it actually proved and a `TestMain`
asserts, after the package runs, that every default checker was exercised — guarded on
`-run` being empty, so a single-test invocation is not spuriously red, which is how a check
like this gets switched off. `watermark-order` is reclassified `proofLiteral` with a proof
that runs: the ordering is enforced at the source (`ErrWatermarkOrder`), so no fault in the
simulated disk, store or clock can make production emit an out-of-order triple. And
`effective-single-writer` got a real driver, below.

**INV-10 has a scenario again, and it is the only fencing left.**
`two-hosts-cannot-both-publish-an-image` runs two real `VolumeManager`s on separate data
directories against one object store: both serve the same volume, both write different
bytes, both stop. Exactly one image may exist afterwards. The observable is the manifest's
ETag rather than an error, because `publish()` logs its refusal and returns nothing — a
failed publish must not block a teardown. **Its planted bug is a backend, not a code
change:** an object store that ignores preconditions. Every claim about fencing in V1 rests
on `If-Match` meaning what it says, and a store that quietly accepts a stale ETag turns the
invariant off with nothing in this tree failing — which is exactly why
`task backend:conformance` is blocking per backend (§6.1).

Three checkers were deleted with the subjects they watched: `PromotionWaitChecker` (nothing
promotes), `NoLostAckedWriteChecker` (there is no failover) and `ImmutableSnapshotChecker`
(INV-16 is structural now — `image.PublishSnapshot` is create-only). The event kinds and
`Event` fields only they read went with them.

## ~~DEV-0022~~ — the §26.2 metric catalog described a system that was withdrawn *(closed 2026-08-03)*

`internal/obs.Catalog()` is the §26.2 taxonomy, declared up front in Phase 01 so later
phases would start incrementing existing series rather than inventing names. About three
quarters of its entries now name mechanisms ADR-0026 removed: the WAL-remote block
(batches, PUT latency and retries, the small-batch ratio, the durable gap, object counts),
`fencing_wait_duration_seconds`, the whole objectization/compaction/GC block,
`recovery_duration_seconds`, `standby_checkpoint_lag_bytes`,
`bytes_downloaded_before_boot`, and the io-class pair.

It was the same shape as the invariants file before 2026-08-03: a declaration a reader
takes for a plan. **Closed by the owner's decision to cut both together** — the catalog
*is* §26.2, so `internal/obs.Catalog()` and the architecture document were trimmed in one
commit rather than left to diverge.

**Six entries are recorded today**: `wal_local_sequence`, `wal_durable_sequence`,
`wal_published_sequence` (permanently 0 — see the callerless list), `wal_unflushed_bytes`,
`wal_out_of_space`, `lease_remaining_seconds`, `lease_renewal_failures_total`, and since
2026-08-03 `snapshot_pause_duration_seconds` and `snapshot_publish_duration_seconds`.
**None of them reaches a collector**: `cmd/volume-agent` passes `Recorder: nil` on purpose,
because there is no exporter and `obs.NewTestProvider` in production would look like
observability from the outside without being it.

## INV-20 is active again — the catalog can be rebuilt from the bucket, since 2026-08-03

`descriptor.Write` had two callers and no reader since increment 4 deleted the previous
`RebuildMetadata`. The owner's call was to write the reader rather than stop writing the
descriptors, and the result is much smaller than what was deleted: a volume's state in S3
is one descriptor plus one manifest, so the rebuild reads two objects per volume with no
epoch chain to walk and no contiguous prefix to reassemble. `control-plane
-rebuild-metadata` is the caller.

**It needs no key material.** The manifest is structural (§15.3), so `ReadSnapshotManifest`
was added rather than reusing `LoadSnapshot` — which would download and decrypt every
chunk to learn a sequence number that is in the manifest. An operator with the bucket and
no KEK can still rebuild the catalog.

**Three passes, because the schema is circular.** `volumes.parent_snapshot_id` references
`snapshots`, and `snapshots.volume_id` references `volumes`. So: volumes without their
parent link, then snapshots, then the clones again with the link — which the catalog's own
upsert was already built for (`parent_snapshot_id = COALESCE(existing, excluded)`, never
cleared by a converging write). Proven by planting the one-pass version, which fails on
the parent-snapshot lookup.

**What it deliberately does not restore.** Placement — no object records a primary host,
and inventing one would make a rebuilt catalog claim somebody is writing when nobody is.
Snapshot lineage depth, which is not in any manifest. And `source_host_id`, so §20's
placement rule 1 falls through to steps 2 and 3 for a clone taken after a rebuild, which
is what those steps are for.

**The assertion is not "the rows came back".** A rebuild that recreated a volume with the
right wrapped DEK and the wrong version produces rows that look perfect and a volume
nothing can open, and the failure would surface at the guest's first read. So the DST arm
goes the whole way: a new Agent, a data directory that has never seen the volume, key
material only from the rebuilt catalog, and the bytes the original guest wrote. Its
planted bug is that off-by-one version.

**The §26.2 catalog was trimmed with it (DEV-0022, closed).** About three quarters of its
entries named mechanisms ADR-0026 withdrew; `internal/obs.Catalog()` and §26.2 of the
architecture document were cut together, in the same commit, because the catalog *is*
§26.2. `wal_published_sequence` went for a smaller reason worth writing down: nothing
publishes in V1, so it was a gauge that could only ever read 0 — one an operator has to
learn to ignore. One entry was *added*, `image_publish_duration_seconds`, and it is
recorded: publishing at stop is the only moment anything leaves the host, so its duration
is the cost of a whole session rather than one step among many.

## The decisions stop describing deleted machinery, 2026-08-03

Same pass as the invariants, one layer up. Six of the 22 ADRs described mechanisms
ADR-0026 removed, and an ADR is read as a *current* decision — the file's whole purpose is
to be the bridge for a reader comparing code against the design doc, so one describing
code that is gone misleads more than a missing one would.

**Withdrawn, with what survives named in each:** ADR-0008 (a drain moves from the durable
prefix — there is no drain and no durable prefix; what survives is the *shape* of the
question, that an evacuation moves from something the destination can verify itself),
ADR-0012 (GC anchors — `internal/gc` is deleted, and what survives is enforced at
construction: `real.NewS3Store` refuses an unversioned bucket), and ADR-0014 (quota and
squash — no compaction, no lineage ledger; the distinction it drew between a *soft*
allocation control and ADR-0013's *hard* device budget is the part to keep).

**Amended, because the decision outlived its justification:**

- **ADR-0023** — the object store is still a fencing witness the data path acts on, but
  the witness moved from a checkpoint mid-session to the manifest's compare-and-set at
  stop. That is the same principle at the only moment V1 writes anything.
- **ADR-0024** — same-epoch re-attach still holds, for a simpler reason than the one it
  was argued on. Three of its four mechanisms (`DurablePoint`, `VerifyAgreement`, the
  divergent-PUT hard fail) are deleted; what makes it safe now is that **nothing leaves
  the host mid-session**, so a second incarnation has nothing to overwrite until it stops,
  where the CAS catches it. `InstallBase` is the one piece unchanged and still
  load-bearing.
- **ADR-0015** — nothing promotes, so nothing waits out the dwell. `fencing_started_at`
  stays in the schema on purpose: the reason to *store* the instant is that a Control
  Plane restarting mid-fence has no memory of having observed anything, which is the
  failure a future promotion must be rebuilt on.
- **ADR-0016** — the lease is liveness only; the per-host granularity stands and is still
  what `Loop` renews. The window this ADR bounded is currently empty because a revocation
  stops nothing on the data path.

**And one more thing with no caller went with them.** `wal.Log.BasePending` existed so the
durability scheduler could not act on a *false* ADR-0023 witness — a resumed log reports
`durable = 0` until its base lands, and a checkpoint in that window would conclude another
writer held the epoch and fence a healthy host on every restart. With no mid-session
publication there is no such window, and the method had only its own test.

**ADR-0013 is still `Proposed` and is the most-cited ADR in the tree after ADR-0026** (32
citations). It carries DEV-0011 (a segment's space charged as used rather than reserved),
and ADR-0014's amendment above leans on it for the hard limit. It is a decision waiting on
a human, not a divergence.

## Components with no production caller

CLAUDE.md's rule is that a component with no caller is a liability rather than progress,
and `CloneCrossHost` was deleted for exactly it. These are the remaining ones. They are
listed here, in the file that tracks state, because until now each was recorded only
inside the spec or ADR that built it — which is how a thing stays "done" while nothing
calls it.

- **`wal.TruncateLocal` and `wal.AdvancePublished` have no production caller.** The
  durability scheduler that called them went in increment 4.1, so `published_sequence`
  is permanently 0 and nothing reclaims a segment mid-session — which is correct under
  ADR-0026, since the WAL lives one session. Both are kept deliberately: the rule
  `TruncateLocal` enforces (`ErrTruncateAboveDurable`) is the part that is easy to get
  wrong, and reclamation returns with any long-lived volume. Recorded so nobody reads
  INV-03's first `≤` as a live property.
- **`metadata.BumpVolumeEpoch` has no caller outside tests.** `controlplane.Promoter`
  went with the fencing half in increment 4.6; the store's compare-and-set on the epoch
  stayed, because it is what would grant one if promotion returns (ADR-0024). INV-11 is
  withdrawn, not pending.

## STOPPED — the data-path cleanup, superseded by ADR-0026

**ADR-0026 is accepted (2026-08-02).** The question it was blocked on — has anyone ever
asked for a VM to survive the loss of its host mid-session? — was answered by the owner:
**no, it is a wish rather than a request.**

So increments 12, 13 and 14 of the cleanup plan are **not paused, they are moot**: they
polish code the accepted ADR removes. Cleaning it would be the most expensive way to be
wrong.

§2 declares the primary use case ("flotas de VMs de desarrollo, CI y entornos efímeros")
and the SLO ("RPO 0 bajo el modelo de fallas probado por DST") in the same section, and
they pull against each other. The RPO-0 row is the single assumption that generates the
entire remote durability chain — `recovery`, `gc`, `materialize`, `checkpoint`, `epoch`,
`lease`, the remote half of `wal` and the fencing half of `controlplane`, on the order of
half the production tree and the half that is expensive to reason about. It has never
been checked against a stated requirement.

Increment 12 (`WriteSummary` and its unbounded accumulator) was specced and its branch
chosen — delete the writer and the accumulator — before the stop. It is subsumed: under
ADR-0026 the whole summary mechanism goes, not just its writer.

**What replaces the cleanup plan is `BUILD-INVENTORY.md`**, rewritten 2026-08-02 for
ADR-0026. Six increments, ordered by the real dependency graph, and **none of them is
started**.

The ordering rule is the one this repository keeps having to relearn: **build the new thin
path before deleting the old one.** Increment 2 (upload at stop, boot from the copy) and
increment 3 (snapshot as `fsync` plus a copy) come *before* increment 4 cuts `recovery`,
`materialize`, `checkpoint` and the remote half of `wal` — because deleting first leaves no
way to serve a volume.

**Increment 0's blocking half is done (2026-08-02).** §2's SLO table and §14.8 were
rewritten in place: the RPO is one session, the two durability modes collapse to one
contract, and §2 records that the withdrawn "RPO 0" row — not the use case — is what
generated the remote chain. The gate is unblocked.

**The rest of the document goes with its code, not before it.** §12, §21 and §22 still
describe promotion, objectization and mid-session recovery, and the code still cites
them — 57 references to §12.3 alone, all inside `controlplane/promotion.go` and its
neighbours. Sections leave in the same commit as the code that cited them, exactly as the
ADRs did in the earlier sweep, so no commit exists where the tree cites a section that is
not there. `REFERENCE.md` carries the command that checks it.

Rewriting the document from scratch was considered and rejected: the code cites it by
section number **2.106 times**, so a new document would have to preserve the numbering —
at which point it *is* the old one with sections removed, and writing it fresh only risks
losing what survives untouched (§14.1's record layout, §26.2's metric catalog, §23's edge
cases, §6.1's backend requirements).

**What is *not* paused:** anything outside the durability chain. The transport, the block
device, the read view, the guest lane and the documentation work are unaffected by
ADR-0026 either way.

## Decisions waiting on a human

- **ADR-0013 (device pressure) is still `Proposed`.** It carries DEV-0011 and the
  `SetLimits` the segment code has no way to receive today. Note the gap this leaves: 32
  citations across 19 non-test files treat it as decided, and its WAL segment format has
  landed. Either the review happened and the ADR should say so, or it did not.
- **The review-zone specs have no commit that precedes their implementation.** Five
  increments in a human-review zone (fencing, keys/format, on-S3 format ×3) have their
  `*-SPEC.md` landing in the same commit as the code it was supposed to gate. That is not
  proof the review did not happen — a commit records when a file entered the tree, not
  when a person read it — but the evidence CLAUDE.md's review-zone rule asks for is not
  in git, and only a human can say which it was.
- **ADR-0023 shipped without a DST scenario, and its own text says so.** "It needs its
  own DST arm" is still literally true, and it was left standing rather than tidied:
  `grep -rn 'ADR-0023' internal/dst/` returns exactly one line, and that line explains
  why `scenarios_agent.go` orders its checkpoint so ADR-0023 does **not** fire falsely —
  the opposite of an arm that proves it. The only proof of the decision is a unit test
  (`internal/agent/durability_internal_test.go`). CLAUDE.md lists "code touching
  durability, fencing or GC with no DST scenario" as a stop signal, so this is recorded
  as one: either the arm gets written, or the gap is accepted in writing.
- **ADR-0016's stage 2 has lost its blocker and has not been scheduled.** It was written
  as waiting on the Agent (DEV-0007), whose chain closed on 2026-08-02;
  `internal/controlplane/drain.go` carries the marker for where it lands. Stage 2 changes
  what `wal.Log` consults before ACKing a FLUSH — the durability rule itself — so the
  choice between scheduling it and recording stage 1 as the final answer is not one an
  increment should make on its way past.
- **DEV-0020**, above: whether a clone chain is walked at materialization or flattened at
  clone time is a §19/§20 question, not an implementation detail. **Subsumed by ADR-0026
  if that is accepted** — a chain that is only ever materialized at boot is a different
  problem.
- ~~**ADR-0026 — does V1 accept an RPO of one session?**~~ **Answered 2026-08-02: yes.**
  Recorded in the ADR with the reasoning; what remains is execution, not a decision.
- **The Phase 04 format review** (human-review zone) has never been signed off.

## Track A — the documents (open work, appended per increment)

*Only track A appends here* — it owns the architecture document, the head of this file,
`REFERENCE.md`, `RISKS.md` and `INVARIANTS.md`. The head tables are recounted once, at
integration, by this track; no other track edits them.

## Track B — the gate runs (open work, appended per increment)

*Only track B appends here* — it owns `.github/workflows/`, `Taskfile.yml`, `hack/`,
`internal/testinfra`, `integration/guestinit` and `integration/vhost/guest_test.go`. The
head tables are recounted once, at integration, by track A.

**Wave 0: the mandatory DST set cannot select nothing (2026-08-03, `d58c19c`).**
`go test -run TestNoSuchNameAtAll ./internal/dst/` exits 0 — "ok, no tests to run" — and
`task dst` selected its scenarios with `-run` regexes, so a renamed test or a mistyped
pattern turned the mandatory gate into a no-op that reported success. The set is now
pinned by **name** (`internal/dst/mandatory_set_test.go`, 17 entries) rather than by a
count, because a count is one integer every future branch bumps.

**And the fix found a second no-op underneath it.** `internal/dst`'s `TestMain` audits,
after the package runs, that every default checker was actually shown to catch a planted
bug — and it skips that audit whenever `-run` is non-empty, so a single-test invocation is
not spuriously red. `task dst` always passed `-run`. **The gate whose purpose is running
the DST proofs was the one place the "these checkers can fire" audit was switched off.**
Removing the filter entirely — rather than making it fail on an empty selection — costs
0.02s and re-enables it.

**Wave 0: the simulable analyzer sees the build-tagged surface (2026-08-03, `2d75842`).**
INV-01's authoritative layer ran without build tags, so every file under `integration/`
and `internal/testinfra` was invisible to it. Two things were not what the task assumed:
the analyzer's `-tags` flag is a deprecated no-op in `singlechecker` (tags reach it only
through `GOFLAGS`, which the `go list` child inherits), and there were **zero real
findings** — all 15 sites are build-tagged harnesses already exempt under DEV-0016, so the
exemption was ported to the analyzer rather than the code being fixed. The net new
coverage under `integration/` is therefore zero files; what it *did* buy is that tagged
code elsewhere — `internal/metadata/pg`'s integration tests, for one — is now governed by
the authoritative layer for the first time. The exemption is per **file**, not per
package, because `integration/vhost` holds host-side non-test code beside its harnesses,
and the fragment matching was changed to segment-anchored: plain `strings.Contains` would
have handed the exemption to any package merely *named* like an exempt one.

**Wave 0: the workflow calls `task ci:full` (2026-08-03, `784404f`).** The premise was
wrong in a way worth recording — CI's 13 steps were already set-equal to `ci:full`, so
nothing was missing; the defect was structural, two copies of the gate with nothing
holding them equal. Doing the permissions work found two real ones instead, both in the
guest lane's registry access: `guest-lane-image` logs in to ghcr.io with only
`contents: read`, so against a private package the lookup fails closed and the lane
disables itself **silently** — the exact failure ADR-0025's preflight exists to make
visible — and `guest-lane` is a container job, whose image is pulled before any step runs,
so no `docker login` step could ever authenticate it and it had no `container.credentials`.
Both fixed. None of it is executed: these workflows have never run.

## Track C — the agent data path (open work, appended per increment)

*Only track C appends here* — it owns `internal/agent`, `internal/wal`,
`cmd/volume-agent`, `integration/e2e` and `integration/vhost/{lifecycle,wal}_test.go`. The
head tables are recounted once, at integration, by track A.

**Wave 0: `integration/e2e/e2e_test.go` is nine files (2026-08-03, `a5b3024`).** Eight
future increments each land an assertion in what was one 820-line file, and each also
mutates the shared deployment fixture inside it — eight three-way merges, or one split
now. Behaviour-identical, and proven so rather than assumed: the set of `func Test*` and
of unexported helpers is byte-identical to the parent commit, every file kept its
`//go:build e2e`, and the nine tests were run verbosely to confirm **PASS and not SKIP** —
a dropped build tag or an orphaned helper surfaces as a silent skip, not a failure, so
the exit code alone would have proved nothing.

**The spec for the shutdown publish is written and waiting on a human**
(`SHUTDOWN-PUBLISH-SPEC.md`, review zone: durability). Writing it surfaced a second path
to the same loss that no auditor had found: `stop()` cancels the context `fetchBase` runs
under and *then* waits on `baseDone`, so a volume stopped while its base is loading is
dropped — correctly, since the view would be incomplete — because the Agent cancelled its
own read.

### C1 — the publish assertion is made against the store the manager wrote to (2026-08-03)

`TestAVolumeWhoseBaseFailedDoesNotPublish` listed a **freshly constructed**
`sim.NewObjectStore()` rather than the store the rig handed the manager, so it asserted
that an empty bucket was empty and passed whatever `publish()` did. It was green with
`if v.baseFailed { return }` deleted — proven, not assumed.

Two things made it possible and both are fixed. `newPublishRig` took an
`objectstore.Store` and kept the concrete one only if a type assertion happened to succeed
(`sto, _ :=`), so a caller passing a double silently got `rig.store == nil`; it now takes
`*sim.ObjectStore`, which makes that unrepresentable. A test that genuinely needs a double
takes the new `newPublishManager` and asserts through the double it built — which is what
`TestARepeatedSnapshotRequestIsTakenOnce` already did with `countingStore`.

The fixture changed too, because the old one could not have failed even pointed at the
right store: the volume never wrote anything, and an object store that answers nothing
also fails `image.uploadChunks`' Head, so a wrong publish would have died of the double
rather than of the missing guard. The volume is now a **clone whose parent snapshot was
never published** — the shape of base failure that nothing else refuses, because the clone
has no image of its own and its publish CASes with an empty ETag, which is create-only and
*succeeds*. The other shape (the volume's own manifest unreadable) is unfalsifiable here:
that volume has a manifest in the bucket, so create-only loses the CAS and the bucket is
identical with or without the guard.

It writes a block before it stops, and the assertion is the absence of
`image/<vol>/manifest.json` — reporting, on failure, the chunk count it named, which is the
fact that separates "published a partial view" from "correctly published nothing". Planted
bug (the `baseFailed` guard deleted, in a scratch copy of HEAD, never in the tree):
*"a volume whose base never resolved published image/<vol>/manifest.json naming 1 chunk(s)
at sequence 1"*.

### C4 — three things that described a mechanism ADR-0026 deleted (2026-08-03)

**The e2e assertion is deleted, not repointed** (`263c26e`). `TestTheDeploymentServesAVolume`
scanned the Agent's output for `"no durability scheduler"` and failed the lane on it. No
code emits that string — the only occurrences in the tree were two comments — so the loop
body had been unreachable since increment 4.1 removed the scheduler. Proven rather than
argued: the check was inverted in place to fail when the string is *absent*, the lane was
run against the real binaries, and it failed on that line alone with everything else green.
**What is no longer covered: nothing.** It existed because `-host-id` was missing from the
binary and `checkpointsEnabled` refused a scheduler without one; there is no scheduler to
refuse, and the host id's remaining consumer — the heartbeat that creates the host row —
is already blocking four lines earlier in `waitForHost`, where a wrong id fails on the
foreign key.

**Five `VolumeManagerConfig` fields had no reader outside the code setting them**
(`1313296`): `UploadAttempts`, `HostID`, `CheckpointBytes`, `CheckpointInterval`,
`CheckpointPoll`. `DataDir`, `SocketDir` and `Limits` have one and stay. The call sites went
with them, which is the part that read as live configuration: `cmd/volume-agent` carried
an eight-line comment saying `HostID` is what stops the NVMe filling, four DST scenarios
set `CheckpointPoll: 24 * time.Hour` so a poller they no longer have would not fire, and
`integration/vhost/lifecycle_test.go` minted a UUIDv7 per run for a field that discarded
it. Dropping the DST host ids removes four draws from the seeded source, so ids downstream
of them change in those traces; the mandatory set is green.

**`-data-dir` stopped promising checkpoints** (`8750e3c`) — it names the WAL and
`agent.lock`, which are the only things in there. Verified from `volume-agent -h`, not from
the source. No other flag help in that binary describes withdrawn behaviour.

## Track D — the catalog (open work, appended per increment)

*Only track D appends here* — it owns `internal/controlplane`, `internal/cpserver`,
`internal/metadata/**`, `internal/db`, `internal/schema`, `internal/lifecycle` and `api/`,
and it runs as one sequential lane. The head tables are recounted once, at integration, by
track A.

**D1: the per-volume durability mode is gone, end to end (2026-08-04).** ADR-0026 made
§14.8's local ACK the only contract; the enum outlived the mechanism by a whole increment,
in nine places — `lifecycle.Durability`, `volumes.durability` with its CHECK, the proto's
`Durability` enum and `DesiredVolume.durability`, `metadata.Volume.Durability`, the
descriptor's `durability` field, `controlplane.VolumeSpec.Durability`, the
`-seed-local-durability` flag, and about thirty fixtures. **The claim that nothing reads
it was checked before anything was deleted, not after**: `Durability.Remote()` had exactly
one caller and it was its own test; `DesiredVolume.GetDurability()` had exactly one caller
and it was `cpserver`'s own test; the Agent never looked at the field it was sent. The
three remaining reads were the validations of a value nobody consumed — `Valid()` in
`provision.validate`, and the default-then-validate in each of the two `CreateVolume`s.
`wal.DurabilityMode`, which `lifecycle.Durability`'s doc comment named as its data-path
counterpart, had already been deleted; the comment was the last thing describing it.

**−220 lines of hand-written code and SQL** (−347 counting the regenerated `api/gen` and
`internal/db`). The schema change is `ALTER TABLE volumes DROP COLUMN durability`
(`migrations/20260804020927_drop_volume_durability.{sql,json}`, planned against the dev
database and applied to it); `task db:verify` and `task test:integration` are green, so
the column is gone from both the declared state and the database the pg contract runs on.
Proto field 6 is `reserved`, not renumbered — a peer built before this change decodes 6 as
an enum, and a later field reusing the number is the one way a wire format lies to a
reader that is otherwise correct. The top-level `Durability` enum is deleted outright:
proto3 has no file-scope `reserved`, so the note lives in `DesiredVolume` where the
`reserved 6` is.

**D2: a volume's placement is changeable, so attach is no longer permanent (2026-08-04).**
`volumes.primary_host_id` was write-once. `CreateVolume` set it and its converging upsert
protected it with `COALESCE(existing, excluded)`; `BumpVolumeEpoch` is the only other
writer and has no production caller. So a volume could not be detached, could not be
re-placed, and the volumes `-rebuild-metadata` restores with no host — which it says out
loud on the way out — could never be given one. Three well-tested teardown paths had no
condition in production that reached them: `VolumeManager.Apply`'s gone-branch,
`VolumeManager.Fence`, and the `NOT_PRIMARY` report outcome. All three are now reachable.

`metadata.Store.SetVolumePrimaryHost(ctx, term, volumeID, primaryHostID)` places a volume
or clears its placement, in both implementations and one term-guarded statement, with
`cmd/control-plane -detach-volume` and `-attach-volume`/`-attach-host` as the callers.
Three decisions, each written at the code rather than here:

- **The §7 state moves with the ownership, in the same write** (`metadata.PlacedState`):
  no writer means `DETACHED`, a writer means `ACTIVE`. Two writes have a window and
  neither order is harmless — a volume left `ACTIVE` with no host is one
  `controlplane.RequestSnapshot` accepts (it checks only the state) and no Agent can ever
  take, so the snapshot sits `CREATING` for ever.
- **The epoch is not touched, in either direction.** An epoch is a fencing token granted
  by a compare-and-set to a writer that won it; a detach grants it to nobody. Nothing
  needs the bump either: `cpserver.applyReport` compares `primary_host_id` against the
  reporting host *before* it looks at the epoch, so a cleared volume answers `NOT_PRIMARY`
  — the outcome that makes the Agent fence and tear down — and `""` matches nobody
  because the RPC refuses an empty `host_id` at the boundary. And a bump would cost a
  re-attach its local data: the WAL lives at `<data-dir>/wal/<volume-id>/<epoch>`, so a
  new epoch is a fresh empty root and detach has to be reversible.
- **A straight hand-over A → B is refused** (`metadata.ErrAlreadyPlaced`); the caller
  detaches, observes the host has stopped, and places. This is what keeps the increment
  out of the mutual-exclusion review zone rather than dragging it in: a host serves what
  `GetDesiredState` lists and learns it lost a volume on its *next* poll, so one write
  naming a new owner would have two Agents serving one volume for a poll interval, both
  publishing an image over the same manifest with one losing its session to the CAS. The
  refusal does not make the two-step exclusion — an operator who re-places within a poll
  interval rebuilds the window by hand — it only guarantees no single catalog write opens
  it. Closing it properly is the §7 machine's job and is not done.

No schema change: the column was already nullable with an FK. The contract case
`VolumePlacementIsChangeableAndClearingIsIdempotent` asserts on `ListVolumesByHost` —
what `GetDesiredState` answers an Agent with — rather than on the column, so a store that
wrote the column and answered the listing from elsewhere would still fail. **Four planted
bugs, each watched go red:** dropping the sim's `checkTerm` (stale-term subtest and both
`everyMutation` sweeps), dropping the sim's hand-over guard (the A → B subtest), dropping
the sim's `v.State = state` (the DETACHED assertions), and replacing the SQL term
predicate with a tautology in the pg lane (stale-term subtest, `<nil>` instead of
`ErrStaleTerm`). `task ci` and the pg integration lane are green.

**Not proven end to end.** `integration/e2e` is track C's file set in this wave, so no lane
drives `-detach-volume` against a running Agent and asserts the socket disappears and the
image lands. That proof is the seam this repository keeps losing defects at, and it is
owed.

## Track E — observability (open work, appended per increment)

*Only track E appends here* — it owns `internal/obs`, `internal/vhost`,
`internal/blockdev` and `internal/cow`. The head tables are recounted once, at integration,
by track A.

### E1 — `internal/obs`'s logging and tracing half is deleted (2026-08-03)

`obs` had two halves and only one of them was connected. The metrics half has real
callers — the WAL's watermarks and `wal_out_of_space`, the Agent's lease counter and
gauge, §19's two snapshot histograms — all reaching an instrument through `obs.Recorder`.
The other half had none. `Tracer`, `NewTracer`, `InjectContext`/`ExtractContext`, the five
correlation context keys with `WithRequestID` and friends, `NewLogger`, `LoggerFrom` and
`Provider.RecordedSpans` were referenced **only inside `internal/obs` and its own tests**:
no RPC injected a header, no handler extracted one, no binary constructed a `Tracer`, and
not one line in the tree was logged through `LoggerFrom`. §26.1's CP → Agent → S3 → KMS
trace propagation was a package that propagated between two halves of its own test.

Deleted rather than wired. Wiring is not one line and it is not this package's to make:
the injection point is an interceptor in `api/`/`internal/cpserver` that does not exist,
and the extraction point is the Agent's loop. Keeping the machinery until they do means
"registered and unused" — the same finding as DEV-0010, which was about this very package's
metric catalog. Git holds it; the increment that grows the interceptor brings back the
functions it calls.

`Provider.Meter` went with them, for a related reason worth separating: it existed "for
ad-hoc instrument creation in tests" and was used by one test that made a counter and
added 1 to it. It handed out the ability to create a series outside `Catalog()`, which is
the one property `Recorder` exists to hold (§26.2). Three tests went with the code:
`TestTracePropagationAcrossBoundary`, `TestStructuredLogHasCorrelationFields`,
`TestNestedSpansShareTrace`, plus `TestMeterRecords`.

Net: −144 lines of production code, −64 of test. `go mod tidy` moved
`go.opentelemetry.io/otel/sdk` and `go.opentelemetry.io/otel/trace` from direct to
indirect requirements — nothing imports them any more.

**For track A, not edited here:** §26.1 of `arquitectura_mvp_volumenes_remotos_v5.md`
describes a mechanism the tree no longer contains — it needs the ADR-0026 treatment the
other withdrawn sections got, not a deletion. `REFERENCE.md:123` (`§26.1 | Distributed
tracing.`) resolves the *document* section and stays accurate as written; it is listed
only so track A decides deliberately rather than by omission.

### E3+E4 — the fake leaves the production surface, and four doc comments stop lying (2026-08-03)

Two commits, no behaviour change, `task ci` green on each.

**E3.** `vhost.RawDevice` — an in-memory Backend whose own comment said it exists "so the
unit tests can prove the transport" — lived in `internal/vhost/backend.go`, a production
file. It now lives in `internal/vhost/rawdevice_test.go`. The proof there was never a
production caller is that `go build ./...` still succeeds with it gone from the production
surface; `backend.go` is down to the `Backend` interface and `ErrOutOfRange`, and lost
three imports. An `internal/vhost/vhosttest` package was rejected: it buys cross-package
reuse nobody has asked for and puts the fake back on the production surface under another
name.

The item's premise that `hostio.RawFile` "is gone, surviving only in two doc comments" is
**wrong** — `internal/vhost/hostio/rawfile.go` is 180 lines and
`integration/vhost/qemu_test.go:226` calls `CreateRawFile` to serve a real kernel a
Backend with no WAL underneath it. It is the same shape of scaffolding as `RawDevice` and
it cannot make the same move: `integration/vhost` is a different package and Go has no way
to import another package's tests. Left where it is; the two doc comments were corrected
to describe it accurately instead of deleted. Nothing else matched
`fake|stub|noop|dummy` outside a `_test.go` file in the four packages.

**E4.** Every `pkg.Symbol`, `Err*` and `Test*` identifier in the doc comments of
`internal/{obs,vhost,vhost/hostio,blockdev,cow}`'s production files was grepped out and
looked up. Four did not resolve, and the interesting part is that three of them were one
thing: the deleted remote durability chain, still describing what a guest is promised.

- `wal.ErrSelfFenced` in `blockdev/doc.go`'s error list — gone with the lease-gated ACK
  (ADR-0026 4.5). Replaced by `wal.ErrLogBroken`, which `refuse` has branched on since.
- The same file's FLUSH paragraph promised "the guarantee against losing the host arrives
  only with a FLUSH" and a `remote` mode that "returns only after every covering object is
  verified in S3 and the lease is confirmed valid on the monotonic clock". `durableStep`
  is one `fdatasync`. The doc now says a FLUSH survives the process, the Agent and QEMU,
  and **not** the host. Narrowing a doc to what the code does is not an ACK-rule change and
  no code moved — but this comment *is* where the guest-facing promise is written down, so
  it is flagged for the human who reviews that zone.
- `TestAGuestWriteCompletesWhileAFlushIsUploading`, cited by `Device`'s comment as the
  proof a concurrent WRITE cannot be ACKed by a FLUSH, exists nowhere in the tree. Now
  cites `TestConcurrentRequestsDoNotRaceTheLog`, which does exist here, and says the
  sequence argument is wal's to prove rather than borrowing a name for it.
- `cow.ActiveMap`, promised by `internal/cow`'s package doc ("and, in Increment 4.4, the
  64 KiB segment active map"), was deleted 2026-08-02.

Each correction quotes the text it replaces, in the file and in the commit message.

**For other tracks, seen and not edited:**

- **Track C — `wal.Log.Flush`'s own doc comment (`internal/wal/log.go:575`) is stale in
  exactly the way `blockdev/doc.go` was**: it still lists "remote (default): upload +
  verify every covering object, VERIFY the lease… (§12.2, INV-06)" and a `local` mode,
  twenty lines above `durableStep`, which says both were deleted. Same for `Log`'s
  concurrency comment ("mu … is **never held across an object-store PUT**", "flushMu
  serializes durable steps (Flush, WriteFUA)" — `WriteFUA` is gone) and
  `ErrFUAOnWrite`'s "fdatasync, verified PUT, valid lease".
- **Track C — `wal.Log.AdvancePublished` has no production caller** (only
  `internal/dst/scenarios_wal.go:313`), so `published` never advances for a live volume
  and `TruncateLocal` can reclaim nothing. That is why `blockdev.go:163`'s operator-facing
  string still offers "restore the object store" as a remedy for a full device: it is
  wrong, but what replaces it depends on the truncation story. Left alone deliberately —
  the same phrase is in `internal/wal/degraded.go:15` and `internal/simio/disk/disk.go:21`.
- **Track A — `REFERENCE.md` still resolves `§14.4` as "**The order of operations in
  FLUSH/FUA.** Six steps" and `INV-07` as "ACK only after the six §14.4 steps | active",
  and `§14.8` as "Per-volume durability modes (`remote` / `local`)". `INVARIANTS.md`'s
  INV-07 row is already correct ("Six steps became two"); it is `REFERENCE.md`'s one-line
  resolution that still sends a reader to the deleted chain.

---

# Specs for work not started

Kept because they are the only record of the intended shape; everything else that was
planning-only has been deleted (git has it).

## Phase 02 — guest layout (§9, §5.5)

Boot a guest with three devices — `/dev/vda` EROFS read-only (shared base image),
`/dev/vdb` ext4 persistent, `/dev/vdc` ext4 ephemeral — assembled with OverlayFS
(`lowerdir=EROFS`, `upperdir=persistent/root-upper`, `workdir=persistent/root-work`), the
ephemeral mounted at `/var/lib/ephemeral` with binds for `/var/lib/docker` and
`/var/lib/containerd`.

**The safety-relevant part is fail-closed:** if the ephemeral mount fails, Docker and
containerd do **not** start and the persistent device is **not** used as a silent
fallback. That is what makes §5.5 ("the ephemeral may be lost") true. Its test is a fault
test that removes the ephemeral device and asserts they fail to start rather than fall
back.

Device features on `/dev/vdb`: `VIRTIO_BLK_F_FLUSH`, `VIRTIO_BLK_F_DISCARD`,
`VIRTIO_BLK_F_WRITE_ZEROES`, and resize via config-space update plus notification. Note
`VIRTIO_BLK_F_CONFIG_WCE` is **deliberately absent** — see the gotchas below.

## Phase 03 — the two remaining increments

- **3.2 Reconnection.** An Agent restart (deploy) must be a pause of seconds, not a VM
  restart: QEMU reconnects to the socket and the device resumes (§28.3). Note that **QEMU
  11.0.2 already reconnects on its own** after a backend protocol error, with no
  `reconnect=` on the chardev — this increment builds on that rather than adding it.
  Activates `vhost_reconnects_total`.
- **3.3 Inflight tracking via shmfd** ⚠️ *durability review zone.* In-flight requests live
  in the inflight shared-memory region so an Agent crash recovers them exactly once
  (§16). Needs a DST arm that kills the Agent with requests in flight, plus a real
  QEMU + real kill verified by checksums. **This is what RISK-10 is waiting for:** the
  backend does not advertise `INFLIGHT_SHMFD`, QEMU therefore never sends
  `GET_INFLIGHT_FD`/`SET_INFLIGHT_FD` (asserted in the lane), and nothing yet says how
  QEMU behaves when it does.

---

# How to resume

1. Read this file, then `CLAUDE.md` (conventions + the gate), `REFERENCE.md` (to resolve
   any `§`/`INV`/`ADR`/`DEV` the code cites) and `INVARIANTS.md`.
2. **Ritual per increment:** failing tests / DST scenarios / checkers **in their own
   commit**, then the implementation, then the doc update. Three-in-one commits are what
   let documentation drift from code.
3. Branch per phase off `main`; human review before merge for the data-loss zones
   (formats, fencing, durability, GC); merge `--ff-only`.
4. `task tools` first. Tools are never invoked directly — versions live in `Taskfile.yml`.

## Gotchas that cost time to rediscover

- **ADR-0005:** WAL headers are **104 bytes**, not the doc's 96.
- **ADR-0019:** pgschema, not Atlas. `schema.sql` is the declared state; `migrations/`
  is the record of reviewed plans and **nothing replays it**.
- **ADR-0010:** RustFS answers `If-Match` on a missing key with `NoSuchKey`, not 412;
  multipart ETags carry `-N`; LIST pages cap at 1000 keys.
- **virtio-blk cannot express FUA.** A Linux guest gets WRITE+FLUSH instead, which is the
  contract `Log.WriteFUA` implements — so that entry point is unreachable from this
  transport.
- **`VIRTIO_BLK_F_CONFIG_WCE` must stay unoffered**, or a guest switches the device to
  write-through and stops sending FLUSH while believing every WRITE is durable, which
  under §14.4 it is not.
- **The virtio wire has no ENOSPC** — three status values, one of them IOERR. All three
  refusals complete as IOERR with the distinction carried in wrapped sentinels, because a
  lapsed lease, a backpressure bound and a full device have different remedies.
- Do not re-add spinbox's `CONFIG_CXL=n` QEMU debloat (it breaks the 11.0.2 link); bump
  `QEMU_CONFIG_REV` when configure flags change.
- **The guest kernel is spinbox's, and it is an ELF with Xen PVH notes — not a bzImage.**
  QEMU enters it through `pvh.bin`. Do not go looking for `linuxboot_dma.bin`: that is
  the bzImage option ROM, this tree deliberately does not extract it, and a guest boots
  fine without it. storage never builds a kernel (ADR-0021).
- **The kernel lives at `_output/guest/vmlinux` and nowhere else** (ADR-0022). `task
  fetch:kernel` puts it there — from a sibling spinbox checkout, or from a mirrored
  image — and every source must match `GUEST_KERNEL_SHA256`. Do not add a second path:
  `SPINBOX_KERNEL` is a *source to copy from*, not the place anything looks.
- **`cd ../spinbox && task build:kernel` exits non-zero on this machine** — it fails
  writing the BuildKit cache under `/var/lib/spin-stack-buildkit-cache` (permissions) —
  **but it emits the artefact anyway**. Check for the file before believing the exit code.
- **A kernel is verified from itself, not from a config file next to it.** It carries its
  `.config` (`CONFIG_IKCONFIG=y`, gzipped after the `IKCFG_ST` marker), and
  `hack/guest-kernel.sh verify` reads it there. Note that `gzip -dc` on that stream exits
  non-zero on the kernel bytes trailing the config, so under `set -o pipefail` the
  extraction "fails" while having produced the whole config — judge it by its output.
- The guest init writes at **1 MiB**, not offset 0: anything that probes a block device
  writes to the first sector, so a pattern found at 0 proves nothing.
- Coverage excludes generated `internal/db`, integration-only `metadata/pg`,
  `simio/real/s3.go`, `testinfra`, `cmd/`, and the `dst` harness.
