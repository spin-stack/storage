# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-08-02 · **Branch:** everything is on `main` — `guest-kernel-pinning`
  merged `--ff-only` at `283f1dd`, then `checkpoint-lease-checker` at `b5bd268`, each
  after a full `task ci:full`. `origin` (`/home/aledbf/spin-storage.git`, bare) is level
  with `main`: `git ls-remote origin` reports `1a7f75d` for `refs/heads/main` and that is
  `HEAD`. Nothing is unpushed.

  This line has now been wrong twice, in opposite directions, and both times because it
  was written from memory. It claimed `origin` was seventeen commits *behind* at
  `be84619`; corrected, it then claimed `origin` stopped at `4f6e125` with one commit
  unpushed, while `origin` was in fact seventeen commits *ahead* of `4f6e125`. The rule
  this file needs is not "check before writing", which was already the rule — it is that
  a claim about another system belongs next to the command that produced it. Here that
  command is `git ls-remote origin`.
- **Gate:** `task ci:full` green, 2026-08-02.
  Green *on a developer machine, and nowhere else*: `task cover` 90.6%
  (floor 90 — the margin is thin because the binary wiring increments 0 and 1 added is
  not reached by unit tests), `task test:integration` green on PostgreSQL 18,
  `task backend:conformance` green against the pinned RustFS, `task build:qemu` +
  `task qemu:verify` + `task guest:verify` green — all of that is one machine's word.
  **CI has never run.** `origin` is a local bare repo, so the GitHub workflows have
  never executed on a runner. Treat every green claim here as reproducible-by-you, not
  as defended by a gate (`BUILD-INVENTORY.md`, increment 8). Two of the three reasons
  the guest lane could not run there are now gone (2026-07-28): the kernel is fetched
  and pinned rather than read out of a sibling checkout (**ADR-0022**), and
  `test:integration:qemu` skips loudly instead of hard-failing when `_output` has no
  QEMU. The third — how a runner obtains QEMU at all — was decided on 2026-08-02 by
  **ADR-0025** and implemented as a container job. So nothing is *missing*; what is
  missing is a run.
- **Where this is going:** storage integrates into **spin** (`github.com/aledbf/spin`),
  which already has a control plane and a per-host runner — **ADR-0021**. spin imports
  storage, never the reverse; `cmd/control-plane` and `cmd/volume-agent` are test
  harnesses that must stay runnable end to end and will not be deployed.
- **The road to something finished:** `BUILD-INVENTORY.md` — nine increments from here
  to one volume served end to end by real binaries, ordered by dependency, from an
  eleven-agent audit of what exists versus what does not. **Increments 0 and 1 are
  done, and increment 2 — the keystone — is done, review-zone half included**
  (`RUNTIME-FENCING-SPEC.md` records each decision). **Increment 5, view adoption, is
  done** (2026-08-01, `VIEW-ADOPTION-SPEC.md`): the seam is in `cow.IntervalMap`, `wal`
  can adopt a base lazily, the Agent resumes, and a DST arm restarts a truncated volume
  through the Agent on every seed. **Increment 3, checkpoint and truncate, is now
  done** (2026-08-01, `DURABILITY-SCHEDULER-SPEC.md` + **ADR-0023**):
  `internal/agent/durability.go` checkpoints at 256 MiB of WAL or two minutes (§21.1),
  behind a valid lease (§12.6) and the background io-class budget (INV-17), then
  truncates to *published*. **Local WAL is reclaimed for the first time in this
  repository's history** — before it, `published` stayed 0 for the life of the process.
  The DST arm drives the scheduler on every seed. One note below: the planted bug the
  spec asked for turned out to be unreachable, and this arm uses a different one.

## Pick up here

**The keystone landed on 2026-07-29, minus its review-zone half.**
`internal/agent/volume.go` holds a `Volume` runtime per volume — `{wal.Log,
blockdev.Device, vhost.Server}` on its own socket — and a `VolumeManager` that diffs the
desired state and owns them. `readDesiredState` no longer assigns a field nothing reads:
it hands the desired state to the manager, and the report that goes back carries what the
live logs actually observe. `cmd/volume-agent` grew `-vhost-socket-dir` and serves from
the manager instead of an empty `VolumeSet`.

Conventions fixed by it, both asserted by tests: the WAL root is
`<data-dir>/wal/<volume-id>/<epoch>` (the epoch is in the path so a promoted writer
cannot append into the segments of the epoch it replaced) and the socket is
`<socket-dir>/<volume-id>.sock` (no epoch — it is the guest's attachment point and
survives promotion).

**The three review-zone pieces were reviewed and answered on 2026-07-29 and are now
implemented** (`RUNTIME-FENCING-SPEC.md` records each decision next to the question it
answers):

1. **The lease adapter** — the host lease gates every volume's durable ACK. It is a
   function resolved per call, never a captured `*lease.Manager`, because `applyLease`
   allocates a new manager on a TTL change and a Log holding the old one would self-fence
   a healthy host and never recover. A store with no lease is refused at construction.
   `EnableRemote` is now called, so a FLUSH is the §14.4 path.
2. **Fencing tears the runtime down** — the safe side, throughout: log, socket and device
   all go, so neither reads nor writes are answered and the guest's I/O stalls rather
   than being served by a host with no authority. **Resolves DEV-0012.** The half that is
   easy to miss: the Control Plane refuses the *report* while `GetDesiredState` may keep
   listing the volume, so a fenced epoch is remembered and only a *higher* epoch — the
   Control Plane granting the volume again — restarts it.
3. **The blockdev mutex is gone.** `Log.Flush` captures its target under the same lock
   `Log.Write` appends under, which is the property that made the mutex redundant rather
   than load-bearing.

**One is closed, one is open, and both were named in that spec rather than implied:**

- ~~The fencing teardown has no DST arm.~~ **Closed 2026-08-01.** `internal/dst` now
  models the Agent: `scenarios_agent.go` drives the real `agent.VolumeManager` on the
  simulated clock, disk and socket, and `FencedVolumeChecker` watches every seed. Its
  planted bug is the Agent not acting on the refusal — DEV-0012 as it actually stood —
  and the event the checker reads is the manager's own answer to "do you still have a
  device for this volume?", not a hand-written one. INV-10 is now proven at both levels.
- **A FLUSH still blocks a guest's READs**, and the spec had named the wrong cause. It is
  not the blockdev mutex (removed, with a regression test): `vhost.Device.ProcessQueue`
  serves the ring serially under its own mutex, one request at a time. Concurrent
  dispatch means out-of-order used-ring completion and collides with increment 3.3's
  inflight tracking and RISK-10 — its own spec, its own review.

One thing the keystone's first test found and fixed on the way: `Reconcile` reported the
volume set it had read *before* reconciling, so every volume was one cycle late in the
Control Plane's view and a volume started and stopped inside one cycle was never reported
at all. The report now re-reads after `readDesiredState`; the heartbeat still uses the
earlier picture, and must, because it is anchored to the instant the lease was renewed
at (§12.2).

## Maturity, not "done"

| State | Meaning |
|---|---|
| **model** | Library logic with unit/property/DST coverage. No integrated caller, no real I/O. |
| **integrated** | Wired into a running binary through the real interfaces, exercised end to end. |
| **production-verified** | Real hardware/backends under fault injection, telemetry recorded, runbook times measured. |

**One path is integrated; nothing is production-verified.** The spine exists — `api/`
over Connect, an Agent that pulls, two `cmd/` binaries — and a real QEMU 11.0.2 guest
boots off a device whose bytes come from a `wal.Log`, writing records through the same
interfaces production would use, and since the keystone that device is one the *Agent*
binds and owns rather than one a test assembled. That is the **write** half of one volume
on one host.
Everything downstream — FLUSH's ACK path, the uploader, checkpoints, truncation — is
still exercised only by tests, and there is no deployment.

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
| 3 — checkpoint and truncate | **done** (scheduler, ADR-0023, and as of `b5bd268` its §12.6 checker) |
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

The replacement is the other half of the same §12.6 sentence — *"deja de ACKear
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

**Snapshot sealing is split (2026-08-02, `SNAPSHOT-LIFECYCLE-SPEC.md`).** §19 separates a
snapshot into "capturar atómicamente N (µs)" and, *in background*, making N durable and
publishing the manifest. `Create` did both in one blocking call. `Capture` and `Seal` are
now separate, `Create` is their composition, and a `Captured` value carries the `CREATING`
state the lifecycle vocabulary had and nothing ever produced.

No goroutine was added, deliberately: "in background" is the caller's property — the Agent
has io-class budgets to spend it against (INV-17) and spin's runner may own the lifecycle
(ADR-0021) — so a `go` statement in a library would be a policy decision taken in the
wrong place.

Two things it found:

- **A retried `Seal` failed.** The manifest is published create-only, so the second
  attempt got `ErrPreconditionFailed`. That is the exact case a background seal produces —
  die between the PUT and whatever records PUBLISHED — and failing is the worst of the
  three outcomes, because the manifest exists, is immutable (INV-16) and is a GC root
  (§21.3), and the caller would write FAILED beside it. It converges on its *own* manifest
  now, and refuses a different one at the same id (`ErrSnapshotConflict`): two snapshots
  claiming one id is not something a retry can reconcile.
- **§19's two mandatory metrics had never been recorded.** `internal/obs` has registered
  `snapshot_pause_duration_seconds` and `snapshot_publish_duration_seconds` since Phase 01
  and nothing observed either. `Capture` and `Seal` do now, and a test asserts it — a
  metric nobody records is a metric that is missing during the first incident that needs
  it.

The pause test also earned its keep: the old one measured `Create`, which captures *and*
seals, and passed because the simulated clock only advances when something works — proving
the pause was zero without proving where the work went. `TestCaptureIsTheWholePause` holds
the two apart: after a capture, nothing is durable, nothing is listed, and no manifest
exists.

**Objectization (segment objects) is specified and deliberately not implemented** —
`OBJECTIZATION-SPEC.md`. §21.1's steps 4–7 (publish the checkpoint, advance, truncate) are
done and proven end to end; steps 1–3 (build, upload and verify segments) **do not exist at
all**: no `segments/` prefix, no producer, no consumer. It is a feature, not a defect, and
the system is correct without it — what it buys is bounded replay, which is RISK-04's cold
RTO and a performance property.

It is a phase rather than the tail of an increment: a new on-S3 object kind with its own
§25.2 property test, a `Checkpoint` format change, `recovery`/`materialize` reading both
kinds (the code INV-08 and INV-09 rest on), and a third anchored kind for the GC (INV-14).
Doing all four in the session that closed five other DEV items is how a format change gets
merged without anyone reading it. The spec says what shape it should take and what to
assert first: *a view rebuilt from segments + tail is byte-identical to the same view
rebuilt from WAL alone.*

§22.4's lazy loading — what the cold RTO actually needs — is designed and not implemented.

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

## DEV-0012 — a self-fenced log still accepts WRITEs and still serves reads

§16 scopes SELF_FENCED to *durable ACKs*, and the code implements exactly that:
`Log.Flush` refuses once fenced, `Log.Write` never consults the flag and `Log.Read` has
no gate. So a fenced host keeps consuming device space for writes no FLUSH can cover, and
can serve a read after another writer was promoted — a split-brain read.

**No ACKed data is at risk** (INV-06/09/10 all hold: nothing a fenced host writes is ever
acknowledged durable or published); the exposure is a stale read reaching a guest, plus
device pressure. **Deliberately not decided in code:** refusing writes or reads *extends*
§16, which makes it a fencing-zone ADR. The counter-argument is that this is not the
WAL's job — the **Agent** should tear the device down when the lease lapses, and a log
that refuses I/O to a guest still attached is the worse failure. Decide before increment
3.2, since a reconnecting front-end is the first caller that can observe a fenced log
across a gap.

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
`OBJECTIZATION-SPEC.md` already plans a different value (128 MiB) and a different
structure — so the constant was not just unused, it was a value nothing intends to keep.

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

## Components with no production caller

CLAUDE.md's rule is that a component with no caller is a liability rather than progress,
and `CloneCrossHost` was deleted for exactly it. These are the remaining ones. They are
listed here, in the file that tracks state, because until now each was recorded only
inside the spec or ADR that built it — which is how a thing stays "done" while nothing
calls it.

- **`internal/snapshot` has no production caller.** `NewSnapshotter`/`Snapshotter.Create`
  are reached only from `internal/dst` and unit tests; no binary and no lane creates a
  snapshot. This is the honest state of phase 09, and it was written down only in
  `SNAPSHOT-LIFECYCLE-SPEC.md`.
- **INV-17 has no path through the real Agent.** `VolumeManagerDeps.IOClass` is set by
  nothing outside `internal/agent/durability_internal_test.go`, and
  `ioclass.Scheduler.Begin`/`End` have no production caller at all — so even with a
  scheduler injected, `HighActive()` would be 0 and the background budget would never
  see a foreground request to yield to. The invariant is enforced against a condition
  nothing can produce. **Deciding between marking the data path and deleting the field
  with its gate is a durability-zone change** and gets its own increment.
- **§14.8's durability mode is not implemented end to end.** The Control Plane decides it
  (`-seed-durability`), stores it, and sends it on `DesiredVolume.durability`; a
  `cpserver` test asserts the field arrives. Nothing reads it: `Log.SetDurabilityMode` has
  no production caller, so every volume is served in the default `remote` mode whatever
  the catalog says. Found while planting increment 9's bug. The direction is the *safe*
  one — a `local` volume gets the stricter ACK rather than the looser — so this is a
  missing feature rather than a durability hazard, but a mode the Control Plane can set
  and the Agent silently ignores is worse than one that does not exist.
- **`controlplane.Promoter`/`BumpVolumeEpoch` are not wired into any binary.**
  `NewPromoter` appears only in tests and DST. ADR-0024 already says so; this file did
  not. Failover therefore exists as a model, and nothing an operator can run performs it.

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
- **DEV-0012**, above.
- **DEV-0020**, above: whether a clone chain is walked at materialization or flattened at
  clone time is a §19/§20 question, not an implementation detail.
- **The Phase 04 format review** (human-review zone) has never been signed off.

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
