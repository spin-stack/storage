# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-08-04 · **Branch:** everything is on `main`. `git ls-remote origin`
  (`/home/aledbf/spin-storage.git`, bare) reports `9f5e5e4` for `refs/heads/main`, and
  `git rev-list --count origin/main..HEAD` reports **1** — the exporter's caller
  (`fa0c834`) is unpushed. Both numbers move under you while a parallel wave is running:
  four lanes commit into this one working tree, so read them as "run the command", not as
  a fact this file keeps up to date.

  This line has been wrong twice, in opposite directions, and both times because it was
  written from memory. The rule this file needs is not "check before writing", which was
  already the rule — it is that **a claim about another system belongs next to the command
  that produced it**. Here that command is `git ls-remote origin`.
- **Gate:** the last recorded `task ci:full` green is **wave 2, 2026-08-04, production
  coverage 90.0%** — the floor exactly, with no slack (the integration log at the end of
  the open-work region has the run). While the wave is open the tree is not continuously
  green: `task ci` on 2026-08-04 during this increment stopped at `fmt:check` on
  `internal/metadata/metadatatest/contract.go`, another lane's file.
  Green *on a developer machine, and nowhere else*. **CI has never run**: `origin` is a
  local bare repo (`git remote -v` → `/home/aledbf/spin-storage.git`), so the GitHub
  workflows have never executed on a runner. Treat every
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
- **A metric can leave the Agent, and no lane has watched one arrive.** Since 2026-08-04
  (`fa0c834`) `cmd/volume-agent` takes `-otlp-endpoint` (`main.go:88`), builds a real
  exporter before anything records (`main.go:133`), and hands the Recorder to the manager
  (`main.go:230`), which hands it to each `wal.Log` at the one place a Log is built
  (`internal/agent/volume.go:642`). What is recorded through it: the WAL's watermarks and
  `wal_out_of_space` (`internal/wal/log.go:239-244`, `degraded.go:151`), the lease pair
  (`internal/agent/loop.go:286,292`), `image_publish_duration_seconds`
  (`internal/agent/volume.go:229`) and §19's two snapshot histograms (`volume.go:1332,1338`).
  **`cmd/control-plane` still records nothing** — no exporter, no Recorder, verified by
  `grep -n 'otlp\|Recorder' cmd/control-plane/main.go` returning nothing — and **no test
  starts the real binary and asserts a series arrives at a collector**, so "a metric leaves
  the process" is proven of the exporter and of the wiring, not of a deployment. The §26.2
  catalog was trimmed to what exists on 2026-08-03 (~~DEV-0022~~); it declares **25** entries
  today (`internal/obs/metrics.go:47`) and **nine** have a producer — the nine named
  above, counted by grepping each catalog name outside `_test.go` and `metrics.go`.
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
| **withdrawn** | ADR-0026 removed the mechanism. Not a maturity at all, and it earns a word of its own for the same reason `INVARIANTS.md` needed one: "model" reads as *written, waiting for a caller*, and a reader who goes looking finds no package. The row says what survives and what would bring it back. |

**The V1 path is integrated end to end; nothing is production-verified.** The spine
exists — `api/` over Connect, an Agent that pulls, two `cmd/` binaries — and a real QEMU
11.0.2 guest boots off a device the *Agent* binds and owns, writes, `fsync`s, and reads
its own bytes back after a stop and a restart. Snapshot and clone are driven by the real
Control Plane binary in the same lane.

What is *not* verified is everything about running it: no deployment, no CI run, no
collector any binary's metrics have been observed arriving at, no fault injection against
real hardware, and no measured
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

**The queue is `BUILD-INVENTORY.md`, and it is on its second road.** The first — increments
0 through 8, "FLUSH uploads a verified object; a checkpoint publishes and the local WAL
truncates" — landed in full and was then **withdrawn by ADR-0026 along with the SLO that
required it**; that table used to stand here, describing a scheduler, a checkpoint chain
and a durable-prefix recovery none of which are in the tree. It is deleted rather than
banner-ed, because unlike the sections below it recorded no defect and no decision, only a
queue that no longer exists. `git log` has it, and `BUILD-INVENTORY.md`'s own header says
the same thing in one line.

**The road today is ADR-0026's six increments, and all six are in.** Each row was checked
by finding the code, not by reading `BUILD-INVENTORY.md` back:

| Increment | State, and where it is |
|---|---|
| 0 — the architecture document says what was decided | **done.** §2's SLO table reads "RPO: **una sesión**" (`arquitectura_mvp_volumenes_remotos_v5.md:92`) and §14.8 is "El contrato de ACK (V1)" (`:801`). The rest of the document's ADR-0026 sweep is A1, in track A's section below. |
| 1 — the free deletions | **done.** `internal/gc` is gone (no such directory) and so is the summary object: `grep -rn 'WriteSummary\|SummaryKey\|ReadSummary' --include=*.go .` is empty. |
| 2 — a volume is uploaded when it stops, and booted from what it uploaded | **done.** `image.Publish` CASes the manifest (`internal/image/image.go:180`), `image.Load` is what a boot reads (`:256`), and the Agent calls it in `fetchBase` (`internal/agent/volume.go:755`). |
| 3 — a snapshot is an `fsync` and a copy | **done.** `wal.Log.Freeze` (`internal/wal/log.go:678`) is §19's capture; `image.PublishSnapshot` writes the manifest create-only (`internal/image/image.go:142`). |
| 3b — a snapshot can be *asked* for | **done.** `pending_snapshot_id` in `DesiredVolume` (`api/spin/storage/v1/control_plane.proto:176`), the report back at `:241`, `cpserver.applySnapshotReport` (`internal/cpserver/cpserver.go:287`), `control-plane -snapshot-volume` (`cmd/control-plane/main.go:82`). |
| 4 — cut the old path | **done.** `internal/recovery`, `internal/materialize`, `internal/checkpoint`, `internal/epoch`, `internal/snapshot`, `internal/ioclass`, `controlplane/drain.go` and `controlplane/promotion.go` are all absent from the tree; `Log.durableStep` is `fdatasync` then advance, with no upload and no lease (`internal/wal/log.go:607-624`). |
| 5 — a clone starts where its data already is | **done.** `controlplane.Clone` takes a `placement.Policy` and asks it (`internal/controlplane/clone.go:62`), and `control-plane -clone-snapshot` is the production caller (`cmd/control-plane/main.go:88`, calling `controlplane.Clone` at `:272`). |

**So the build order is done twice over, and what is left is not in that file.** The
remaining work is `PARALLEL-PLAN.md`'s 31 increments across five tracks — five real
defects in the data path, two lifecycle verbs V1 lacks, about fifteen surfaces with no
caller, and a gate that has never executed a guest-backed proof. The per-track sections at
the end of the open-work region are where those land.

The honesty caveat is unchanged: the workflows have still never *run*, because `origin` is
a local bare repo — `ci:full` includes every lane except the QEMU guest one, and CI is
written to run the same tasks.

What is *not* in either road: multi-host, warm standby, compaction, or anything measured
on real hardware. Those are the phases below, and most of them are V2 under ADR-0026
rather than "later".

## Where each phase actually is

**Recounted 2026-08-04 against the tree, not against another document.** Every row below
was checked by finding the symbol; where a phase's subject was deleted by ADR-0026 the row
says **withdrawn** rather than a maturity, because "model" reads as "written, waiting for a
caller" and that is the opposite of what happened to it. A fourth state was needed for
exactly the reason `INVARIANTS.md` needed `withdrawn`: three of these rows described
integrated machinery that is not in the tree at all.

| Phase | State | What is true, and what is not |
|---|---|---|
| 01 skeleton (simio + DST + obs) | **integrated** | Both binaries run on `simio/real` (clock, disk, network, object store); `internal/dst` holds an 18-scenario mandatory set pinned by name (`internal/dst/mandatory_set_test.go:34`). Telemetry got its caller on 2026-08-04: `-otlp-endpoint` → a real exporter → the Recorder the manager hands to each `wal.Log` (`cmd/volume-agent/main.go:88,133,230`; `internal/agent/volume.go:642`). Nine of the catalog's 25 entries have a producer, and **no lane has observed a series arrive at a collector**. |
| 02 guest layout (3 devices + OverlayFS) | **not started** | `grep -ril 'erofs\|overlayfs' --include=*.go .` is empty. Nothing V1 does depends on it. Spec below. |
| 03 vhost-user | **3.1 integrated + served by the Agent** | A real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`integration/vhost/qemu_test.go:355`), and a **Linux** guest boots the lane (`integration/vhost/guest_test.go:43`). The *Agent* binds a socket per volume and serves `blockdev.Device` behind it (`internal/agent/volume.go:648,883`). A FLUSH is `fdatasync` and nothing else — the "full §14.4 remote path" this row used to claim went with ADR-0026 (`internal/wal/log.go:607`). 3.2 reconnection and 3.3 inflight-shmfd untouched: `INFLIGHT_SHMFD` is deliberately not advertised (`internal/vhost/features.go:93`), RISK-10 open. |
| 04 WAL/CoW format | **integrated** | A guest's WRITE lands as a replayable WAL record with **0 PUTs**, and so does its `fsync` — asserted with a real kernel and the real binary in the loop (`integration/e2e/guest_test.go:75`, INV-18). The WAL is a directory of segments (`internal/wal/segment.go`). What this row used to add — "the uploader and checkpoints are integrated too, and increment 3's scheduler is what finally reclaims a byte" — is withdrawn: nothing reclaims mid-session and `TruncateLocal`/`AdvancePublished` have no production caller (see the callerless list). Format review still pending (human-review zone). |
| 05 encryption (AES-256-GCM, DEK/KEK) | **integrated** | `-kek-file` on the Agent (`cmd/volume-agent/main.go:92`), the unwrap at attach, and every chunk a volume publishes is sealed (`internal/image/image.go`, INV-15). The *read* half was integrated and wrong until 2026-08-02 — see DEV-0019, and note that this row said "model / —" while a real defect lived in the path it declined to describe. |
| 06 remote WAL (batching, idempotent PUT, summary) | **withdrawn** — ADR-0026 | There is no remote WAL: no batcher, no uploader, no summary object (`grep -rn 'WriteSummary\|SummaryKey' --include=*.go .` is empty; `Batcher`/`Uploader` survive only as words in `integration/backend/edgecases_test.go`). What the guest lane proves now is the replacement — write, `fsync`, stop, and the image answers on a second boot (`integration/vhost/lifecycle_test.go:64`). Idempotence survives as INV-21, structurally: a chunk key is its plaintext digest and an existing key is skipped. |
| 07 Control Plane + leases + fencing | **partial: provisioning, snapshots, clone and placement integrated; fencing is one CAS** | Term-guarded writes are in every mutating query (`grep -c control_plane_leader internal/db/queries/*.sql`). The lease is liveness only — granted and renewed by `Heartbeat` (`internal/cpserver/cpserver.go:62`), consulted by nothing on the data path. **Promotion and `FENCING_WAIT` are gone** (increment 4.6): the only fencing left is `image.Publish`'s compare-and-set plus the Agent tearing a refused runtime down (`internal/agent/volume.go:931`). |
| 08 recovery (S3 authority) + rebuild-metadata | **rebuild integrated; recovery withdrawn** | `internal/recovery` does not exist. The object store is the **boot** authority, not the recovery authority (INV-08): one manifest and its chunks. `controlplane.RebuildMetadata` + `control-plane -rebuild-metadata` (`cmd/control-plane/main.go:93`) bring volumes and snapshots back from two objects per volume — and deliberately not placement. |
| 09 snapshots + clone + resize | **snapshot and clone integrated; resize has no caller** | A snapshot of a live volume is requested through desired state and published while the Agent serves (`integration/e2e/snapshot_test.go:28`); a clone of it is placed and boots (`integration/e2e/clone_test.go:21`). **Resize is metadata only**: `metadata.Store.ResizeVolume` is implemented by both stores and called by nothing but `metadatatest/contract.go` — no flag, no RPC, and nothing tells a guest its device grew. |
| 10 objectization + checkpoints + GC + I/O classes | **withdrawn** — ADR-0026 | All four subjects are deleted: `internal/gc`, `internal/checkpoint`, `internal/ioclass`, and the segment-object plan (`OBJECTIZATION-SPEC.md`, removed 2026-08-03). What survives of GC is enforced at construction — `real.NewS3Store` refuses an unversioned bucket (`TestRequireVersioning`, `internal/simio/real/s3_versioning_test.go:36`) — and it is all that INV-14 has left. |
| 11 cross-host + cordon/drain + capacity | **cordon and capacity integrated; drain withdrawn** | `controlplane/drain.go` is deleted and nothing calls a drain; `HostDraining` survives as a lifecycle state (`internal/lifecycle/lifecycle.go:132`) with no producer. What is real: a host cordons itself out of the fleet when its device fills, through the heartbeat (`internal/cpserver/cpserver.go:14,339`), and admission counts what a host is *using* rather than only what it was promised (`internal/placement`). Cross-host movement of a volume does not exist. |
| 12 warm standby + compaction + flatten | **not started, and V2 under ADR-0026** | Nothing in the tree; ADR-0014 (its quota/squash half) is itself withdrawn. Listed because §2 still names it as a target for a later version. |
| 13 hardening | **13.1 integrated (typed lifecycles); the rest needs infra** | `internal/lifecycle` is the typed state machine (ADR-0009) and the Control Plane uses it. 13.2 (real-hardware fault injection + measured runbooks) and 13.3 (backend conformance per version) need hardware nobody has; 13.4 is **INV-19**, which stays pending on purpose until two Agents can run different formats. |

**Invariants — recounted against `INVARIANTS.md` on 2026-08-04: 14 active, 6 withdrawn,
2 pending, 22 total.** This line said "21 of 22 active" and had been wrong since ADR-0026
retired six mechanisms and their checkers with them. The two pending are **INV-14** (GC —
its subject is deleted, so it cannot fire) and **INV-19** (format read-old/write-new, not
binding until two Agents can run different versions). Every *active* checker has been shown
to catch a planted bug, and `internal/dst`'s `TestMain` now enforces that claim rather than
recording it. See `INVARIANTS.md`.

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

## ~~The durability scheduler's two loose ends~~ *(the scheduler was deleted 2026-08-03, ADR-0026 increment 4.1)*

> **Withdrawn, kept for the two traps.** `internal/agent/durability.go`, `checkpointOnce`,
> the checkpoint timer, `CheckpointLeaseChecker` and both DST arms named below are gone —
> there is no mid-session publication for a lease to gate and nothing to truncate. Read
> everything below in the past tense. It is kept because the two findings are about how a
> test lies, not about the scheduler: a healthy host fencing itself on a *false* witness,
> and a planted bug that silently proved nothing because the scenario's setup was moved.
> `wal.Log.BasePending`, the guard the first one produced, went with the scheduler
> (see "The decisions stop describing deleted machinery").

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

## ~~DEV-0007~~ — the spine's second half *(the chain closed 2026-08-02; its subject was withdrawn 2026-08-03)*

> **Read this whole section in the past tense.** ADR-0026 removed the chain it closes —
> checkpoints, truncation, the drain, the epoch/promotion path and §14.4's six-step ACK —
> so every "is" below was true on 2026-08-02 and is not now. Two renamings, so the names
> here resolve: `TestAGuestSurvivesCheckpointAndTruncation` is now
> `TestAGuestSurvivesAStopAndComesBackFromItsImage` (`integration/vhost/lifecycle_test.go:64`),
> which deletes the local WAL between the two boots instead of truncating it; and the
> "read view rebuilt from S3" is `image.Load` (`internal/image/image.go:256`), not
> `recovery`. What is *not* superseded is the middle of it: the clone-chain link
> (`volumes.parent_snapshot_id`), the four findings under it, and the rule that decides
> whether a volume needs a base at all — all still live, and the reason this section is
> banner-ed rather than deleted.

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

> **The defect and its lesson stand; two of the three fixes have moved.** `internal/recovery`
> was deleted on 2026-08-03 (increment 4), so `recovery.ApplyRecord` and
> `recovery.ErrSealedWithoutKey` — fix 1 below — no longer exist. What replaced them is
> narrower and holds the same line: the boot path is `image.Load(ctx, store, v.enc, id)`
> with the volume's own key, and the comment above that call names DEV-0019 as the reason
> the argument is `v.enc` and not `nil` (`internal/agent/volume.go:749-755`). Fix 3, the
> parent's DEK re-bound to the parent's id, survives in `parentView`
> (`internal/agent/volume.go:809`). The mandatory DST arm that crosses encryption with a
> restart is now `a-stopped-volume-comes-back-from-its-image`, which is encrypted for
> exactly this reason.

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

## DEV-0020 — a clone chain is flattened by copying, and nothing decided that

**Still open, and its mechanism is not the one this entry was opened with (rewritten
2026-08-04).** As recorded on 2026-08-02 it said `materialize.FromSnapshot` resolves only
the parent's own objects, so a clone of a clone read its grandparent's ranges as zeros.
`internal/materialize` was deleted on 2026-08-03 (increment 4) and the shape changed with
it. What the code does now, checked line by line:

- **A clone's base is one manifest, and the chain is still not walked.** `parentView`
  calls `image.LoadSnapshot(store, penc, parentVolume, snapshotID)`
  (`internal/agent/volume.go:836`), which reads a single object under the *parent's*
  prefix. Nothing follows a link from there to a grandparent.
- **But a published image is self-contained, because publishing flattens.** `image.Publish`
  and `image.PublishSnapshot` both upload `view.Ranges()`
  (`internal/image/image.go:180,142`), and `cow.IntervalMap.Ranges` reports "the base's
  ranges minus this layer's tombstones, plus this layer's own extents"
  (`internal/cow/ranges.go:24`). So the first time a clone stops or is snapshotted, its
  **parent's whole dataset is re-sealed and re-uploaded under the clone's own prefix.**

The consequence is the inverse of the one recorded here originally: a depth-2 clone does
not read zeros — it reads a copy — and the cost is a full duplicate of the inherited data
per link, which is exactly what §20's "reuses the parent snapshot's already-durable
objects, **with no data copy**" says must not happen. **Nothing in the tree exercises
`chain_depth > 1`** (`grep -rn ChainDepth --include=*_test.go` reaches 1 and stops), so
read the paragraph above as what the code says, not as a proven property.

Why it is still one entry and still open: `controlplane.Clone` sets
`ChainDepth: parent.ChainDepth + 1` with no ceiling and no refusal
(`internal/controlplane/clone.go:91`), and §19/§20 have never said whether a chain is
walked at read time or flattened at clone time. Removing the flattening to save the
storage — `PARALLEL-PLAN.md`'s C11 — turns a cost defect back into the silent-zeros
correctness defect this entry was opened for, because the flattening is the only thing
making depth 2 readable. **They are one decision, and it is a human's** (on-S3 format;
listed below under "Decisions waiting on a human").

## ~~DEV-0012~~ — a self-fenced log still accepts WRITEs and still serves reads *(closed 2026-08-02: not a divergence)*

> **Half of this has no subject any more (2026-08-03, increment 4.5).** A log cannot
> self-fence: the lease left the data path with the remote chain, so there is no lease
> check inside `wal.Log` and no `ErrSelfFenced`. The `fenced` field the policy below was
> written at is now `broken`, and it means the one thing that survived — a failed rollback
> left the tail unknown — with `ErrLogBroken` and `Log.Broken()`
> (`internal/wal/log.go:107-114,258`). **The other trigger is entirely live and is now the
> whole of INV-10's Agent half:** the Control Plane refusing a report tears the runtime
> down — log, socket and device — through `VolumeManager.Fence`
> (`internal/agent/volume.go:931`), and only a higher epoch brings it back
> (`fencedEpoch`, `:385-391,483`). The paragraph below that distinguishes the two triggers
> is the reason this section is kept.

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

## ~~Gap 1~~ — a GC sweep cannot see an anchor its listing has not caught up to *(no sweep since 2026-08-02)*

> **Its subject is deleted.** `internal/gc` went in ADR-0026 increment 1 and ADR-0012 is
> withdrawn, so nothing sweeps and nothing anchors. Two things below outlive it and are
> why the paragraph stays: strongly consistent LIST is still a precondition on any backend
> we accept, certified by `TestListSeesAFreshPut`
> (`integration/backend/conformance_test.go:294`, blocking per backend); and the reversible
> half is enforced at construction instead — `real.NewS3Store` refuses an unversioned
> bucket (`internal/simio/real/s3_versioning_test.go:36`), which is all INV-14 has left.

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

> **What a clean Agent shutdown *means* changed on 2026-08-04**, after the
> SHUTDOWN-PUBLISH review: the Agent now has its own `-shutdown-grace`
> (`cmd/volume-agent/main.go:90`), and the grace bounds **one publish attempt** rather than
> the teardown — a host that cannot publish keeps its data directory and retries instead of
> exiting. The test below is unchanged and still asserts what it always did, on the path
> where publishing succeeds. Track C's section has the increment.

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

## ~~The e2e lane proves a durable ACK~~ — what it proves now is a published image *(2026-08-02, restated 2026-08-03)*

> **The ACK this section is about no longer exists.** Under ADR-0026 a FLUSH is
> `fdatasync` and nothing else, so the `Lease: func() bool {...}` closure quoted below is
> gone from `cmd/volume-agent` (`grep -n Lease cmd/volume-agent/main.go` finds only the
> TTL config and a comment) and the lane's assertion was inverted rather than deleted:
> `TestAGuestMakesTheDeploymentWriteADurableObject` now asserts the guest's `fsync`
> publishes **zero** objects (`integration/e2e/guest_test.go:75`) and that the image
> appears when the binary *stops*. Everything below about *how the lane was made able to
> fail* — the object being necessary and not sufficient, a plant having to reach the
> rebuilt binary, and the 108-byte `sun_path` trap — is unchanged and is why this is
> banner-ed rather than cut.

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

## ~~§14.8 is implemented end to end, since 2026-08-02~~ *(both halves withdrawn 2026-08-03)*

> **The two durability modes are one contract now, and §14.8 is that contract.** There is
> no `remote`/`local` pair to select between: `wal.ModeFor`, `SetDurabilityMode`, the
> `durability` column and `agent.drainOnce` are all gone (`grep -rn
> 'ModeFor\|DurabilityMode' --include=*.go .` is empty; the column's absence is reasoned in
> `internal/schema/schema.sql`, "There is no durability column", line 116). §14.8 in the document is now "El contrato de ACK
> (V1)" (`arquitectura_mvp_volumenes_remotos_v5.md:801`) — `fdatasync`, then ACK. Kept for
> one finding that outlived its mechanism, in the last paragraph but one: **a lapsed-lease
> FLUSH left its objects in S3 and refused only the ACK**, so an object in the bucket was
> never proof of a claim. That is the same reasoning the guest lane now uses in the
> opposite direction, and it is why "the object appeared" is not an assertion this
> repository accepts on its own.

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

## ~~The SELF_FENCED rule is cited where it lives, since 2026-08-02~~ *(the rule has no enforcer since 2026-08-03)*

> **Nothing enforces the sentence any more** — no durability scheduler, no lease on the
> data path, no checkpoint or manifest published mid-session — and
> `DURABILITY-SCHEDULER-SPEC.md`, one of the files corrected by this pass, was retired on
> 2026-08-04. Kept for the mechanic it describes, which is not about fencing at all: a
> *wrong section number* propagated from one document into code, tests, a checker and two
> operator-facing error strings, and was found by sampling citations against the document
> rather than by reading the comments. That is still the only method this repository has
> for catching it, and `REFERENCE.md`'s header carries the command.
>
> **The line numbers below no longer resolve.** §12 was reduced to one paragraph with the
> code that cited it (2026-08-02, ADR-0026): there is no §12.2 in the document today, and
> line 641 is inside §12's surviving text about the compare-and-set
> (`arquitectura_mvp_volumenes_remotos_v5.md:626`). Do not follow them; they are recorded
> as what the citations *were*.

The sentence the durability scheduler enforced — *"deja de ACKear durabilidad, deja de
publicar checkpoints/manifests"* — was at **line 641 of §12.2** ("Ciclo del lease, lado
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

**Recounted 2026-08-04, because this paragraph said "six" above a list of nine and one of
the nine had been cut.** The catalog declares **25** entries (`internal/obs/metrics.go:47`)
and **nine** have a producer, found by grepping each name outside `_test.go` and
`metrics.go`: `wal_local_sequence`, `wal_durable_sequence`, `wal_unflushed_bytes`
(`internal/wal/log.go:239-244`), `wal_out_of_space` (`degraded.go:151`),
`lease_remaining_seconds` and `lease_renewal_failures_total`
(`internal/agent/loop.go:286,292`), `image_publish_duration_seconds`
(`internal/agent/volume.go:229`), and §19's `snapshot_pause_duration_seconds` /
`snapshot_publish_duration_seconds` (`volume.go:1332,1338`). `wal_published_sequence` is
**not** among them: it was removed from the catalog with the rest, because nothing
publishes in V1 and a gauge permanently at 0 is one an operator has to learn to ignore.

**They now reach a collector if one is configured**, since 2026-08-04 (`fa0c834`):
`cmd/volume-agent -otlp-endpoint` builds a real exporter and the Recorder reaches each
`wal.Log` (`main.go:88,133,230`, `internal/agent/volume.go:642`). `cmd/control-plane` still
records nothing, and no lane has yet watched a series arrive from a running binary — so
the sentence this paragraph used to end on, "observed by nobody", is still true of
production and no longer true of the code.

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

**ADR-0013 is still `Proposed` and is now the most-cited ADR in the tree, ahead of
ADR-0026** — 72 citations in all `.go` files, 54 of them outside `_test.go`, against
ADR-0026's 73 and 49 (`grep -rhoE 'ADR-[0-9]{4}' --include=*.go . | sort | uniq -c | sort -rn`,
2026-08-04; the figure here was "32" and had never been recounted). It carries DEV-0011 (a
segment's space charged as used rather than reserved), and ADR-0014's amendment above leans
on it for the hard limit. It is a decision waiting on a human, not a divergence.

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
- **`metadata.Store.ResizeVolume` has no caller outside tests** (added to this list
  2026-08-04). Both stores implement it, the term guard and the shrink refusal are
  contract-tested, and nothing asks for a resize: no flag on `cmd/control-plane`, no RPC,
  and no path that does what §17 describes — propagate the grow through the device's config
  space and notify the guest (`arquitectura_mvp_volumenes_remotos_v5.md:550`), which is an
  objective in §3's list and is not built. Recorded here rather than deleted because the
  refusal (`ErrShrinkNotAllowed`) is the part that is easy to get wrong later.
- **`wal.Log.Broken()` has no production caller, and its own doc comment says so**
  (`internal/wal/log.go:257`). It is read by the DST harness and a concurrency test as
  proof of a transition, which is the same deliberate exception `Log.Fenced()` was before
  the rename. Listed so a later sweep does not read it as the `CloneCrossHost` pattern.

**This list is not exhaustive, it is not the authority any more, and saying both is the
point.** `PARALLEL-PLAN.md`'s audit counted about fifteen surfaces with no caller; the four
above are the ones verified by grep while recounting this file on 2026-08-04. On the same
day track B made the question computable — `task deadcode` walks the call graph from the
binaries and fails on a finding nobody has explained (`hack/deadcode.sh`, `833541d`) — for
exactly the reason this section keeps needing a recount: a list a human maintains about
code a human is changing reports success by not being updated. **Run the task; read this
list for the two entries the task cannot see**, which its own header names: symbols reached
through reflection (`TruncateLocal`) and packages no binary imports (`metadata`'s two store
implementations, where `BumpVolumeEpoch` and `ResizeVolume` live).

## ~~STOPPED~~ — the data-path cleanup, superseded by ADR-0026 *(and the ADR is now executed)*

> **This section was written while the work was ahead of it and reads as if it still is.**
> All six ADR-0026 increments landed on 2026-08-02/03 — the head of this file recounts them
> against the code — so "none of them is started" below is the opposite of true, and the
> ordering rule it states was followed rather than planned. Two figures in it are also
> stale and are corrected here rather than in place, because the paragraphs are the record
> of a decision taken at a moment: **§12.3 is cited 22 times across 15 files today, none of
> them `controlplane/promotion.go`, which is deleted**; and the code carries 846 `§`
> citations, not 2.106 (both counted 2026-08-04 with the commands in `REFERENCE.md`'s
> header — and both moved between two runs an hour apart while four lanes committed, which
> is why that file replaced its counts with the command). What is *not* superseded is the reasoning: why the ADR was
> accepted, and why the new thin path was built before the old one was cut.

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
  `SetLimits` the segment code has no way to receive today. Note the gap this leaves: **54
  citing lines across 23 non-test files** treat it as decided (recounted 2026-08-04; it
  said 32), it is now the most-cited ADR in the tree, its WAL segment format has landed,
  and since 2026-08-04 a host **cordons itself** on the device measurement it defines
  (`internal/cpserver/cpserver.go:14`). Either the review happened and the ADR should say
  so, or it did not.
- **The review-zone specs have no commit that precedes their implementation.** Five
  increments in a human-review zone (fencing, keys/format, on-S3 format ×3) have their
  `*-SPEC.md` landing in the same commit as the code it was supposed to gate. That is not
  proof the review did not happen — a commit records when a file entered the tree, not
  when a person read it — but the evidence CLAUDE.md's review-zone rule asks for is not
  in git, and only a human can say which it was.
- ~~**ADR-0023 shipped without a DST scenario**~~ **— moot since 2026-08-03, and left
  standing for one line of it.** The checkpoint it governed is deleted, and
  `grep -rn 'ADR-0023' internal/dst/` now returns **nothing**; the unit test this entry
  named as the only proof (`internal/agent/durability_internal_test.go`) went with the
  scheduler. What the ADR was amended to say is that the object store is still a fencing
  witness the data path acts on — the manifest's compare-and-set at stop — and *that* has a
  mandatory arm, `two-hosts-cannot-both-publish-an-image`. Nothing is waiting on a human
  here any more; it stays as a `~~closed~~` line because "an ADR whose DST arm was never
  written" is the shape worth being able to find again.
- **ADR-0016's stage 2 has lost its blocker, and has now lost its subject too.** Stage 2
  changed what `wal.Log` consults before ACKing a FLUSH; under ADR-0026 a FLUSH consults
  nothing but `fdatasync`, so there is no window for it to bound. The marker this entry
  pointed at (`internal/controlplane/drain.go`) is deleted. Stage 1 survives and is real —
  the bounded revocation window, `RenewalsBlockedUntil`, term-guarded
  (`internal/metadata/metadata.go:213,444`) — with **no production caller**, like the rest
  of the promotion path. The decision a human still owns is the small one: record stage 1
  as the final answer, or keep stage 2 alive for the durability tier ADR-0026 says would
  reverse it.
- **DEV-0020**, above: whether a clone chain is walked at read time or flattened at clone
  time. **Not subsumed by ADR-0026 — sharpened by it.** The code answers "flattened, at
  every publish, by copying the parent's whole dataset under the clone's prefix", nobody
  decided that, and `PARALLEL-PLAN.md`'s C11 wants the copy gone. Removing it without a
  chain read turns a storage cost back into silent zeros, so the two are one decision, and
  it is an on-S3 format decision.
- ~~**ADR-0026 — does V1 accept an RPO of one session?**~~ **Answered 2026-08-02: yes.**
  Recorded in the ADR with the reasoning; what remains is execution, not a decision.
- **The Phase 04 format review** (human-review zone) has never been signed off.
- ~~**`SHUTDOWN-PUBLISH-SPEC.md` is written and unreviewed**~~ **Reviewed and decided
  2026-08-04: a host that cannot publish refuses to release its data-directory lock** —
  the opposite of what the spec recommended. Since a flock is released by process exit,
  that can only mean the Agent does not exit: it holds the directory, keeps the local WAL,
  and retries indefinitely. `-shutdown-grace` becomes the timeout of one attempt rather
  than a budget after which data is abandoned. `ErrSuperseded` is the single exception
  that still exits, because retrying it would overwrite a newer image with an older one.
- ~~**The descriptor's on-S3 format changed without a spec**~~ **Reviewed 2026-08-04:
  kept.** D1 removed the `durability` field from `descriptor.json`; the field had no
  reader, §25.2's property test was updated with the shape and is green, and a descriptor
  already in a bucket still parses and still verifies its digest. The protocol breach —
  a format change with no preceding spec — is recorded rather than excused: the next one
  gets its spec first.
- **`internal/blockdev/doc.go` now states a narrower FLUSH promise** (E4). Flagged by its
  own author for whoever reviews the ACK-rule zone.

## Integration log — the wave owner's notes

**Wave 1 (2026-08-04) is green: `task ci:full` exit 0, production coverage 90.2%.** Four
lanes, fourteen commits, and the parallelism held everywhere except one place worth more
than the fourteen: **track E committed two files it did not own, from a stale copy, and
wholesale reverted the fix track C had just landed** — an assertion that could not fail,
restored to being unable to fail. Track C noticed and restored it (`96c1cbd`); nothing but
that noticing stood between the wave and shipping the defect it had just removed. Track D
had anticipated exactly this and committed by explicit pathspec, saying so. That is now
the rule in `PARALLEL-PLAN.md`, along with the four files no track owned.

**The margin to watch: production coverage is 90.2% against a 90% floor**, after nine
deletions of tested production code. The next deletion-heavy increment trips it, and the
answer is to write the tests the surviving code lacks — not to move the floor.

**Wave 2 (2026-08-04): `task ci:full` exit 0, production coverage 90.0% — the floor
exactly, with no slack left.** Twelve commits across four lanes. The lane that mattered
delivered: an Agent that cannot publish now holds its data directory and retries, and the
verifier confirmed it against a real TCP proxy in front of RustFS rather than an injected
error — SIGTERM with the store unreachable, the Agent does not exit, prints the holding
line twice, a second real `volume-agent` process is refused **by the kernel**, and only
after the proxy comes back does the image appear and the process exit 0.

**E2 was written up as done and was not.** The exporter and provider were proven against
a real OTLP collector, but nothing called either: both binaries still passed
`Recorder: nil`, and no `-otlp-endpoint` flag existed. By CLAUDE.md's own rule that is not
done, however well tested — it is the `CloneCrossHost` shape. **Closed by the integration
owner** (`internal/agent`, `cmd/volume-agent`): the Agent constructs the provider before
anything that records, hands the Recorder to the manager, and the manager hands it to each
`wal.Log` at the one place a Log is built.

That last hop was a second defect hiding behind the first: **`wal.Log.SetRecorder` had no
production caller at all**, so the four metrics a Log owns — the watermarks, the unflushed
bytes, `wal_out_of_space` — could not be recorded even once a collector existed. An
exporter would have shipped an empty series set and looked like working observability.
`TestAServedVolumeRecordsItsWALMetrics` drives the real manager and asserts on the
collected series; planting the missing `SetRecorder` call turns it red with `collected:
map[]`.

**Three ownership gaps again, and the wave got lucky in all three rather than protected.**
`internal/simio/real` and `go.mod` (E), `internal/placement` (D), and
`internal/dst/mandatory_set_test.go` — a hand-maintained registry that *must* be edited in
the same commit as any new mandatory scenario, and which no lane owns.

## Track A — the documents (open work, appended per increment)

*Only track A appends here* — it owns the architecture document, the head of this file,
`REFERENCE.md`, `RISKS.md` and `INVARIANTS.md`. The head tables are recounted once, at
integration, by this track; no other track edits them.

### A1 — the architecture document stops contradicting itself (2026-08-04)

`98e38b6` (the document), `725b75e` (`REFERENCE.md` + `RISKS.md`).

**Four lines stated a withdrawn contract in the present tense** and are corrected where
they stood: the v5.1 changelog bullet announcing the dual durability mode, §4's two ACK
rows, §8's `durability TEXT NOT NULL DEFAULT 'remote'` (the declared schema dropped that
column, with the reasoning at `internal/schema/schema.sql:96-102`), and §31's criteria
3/3b. **Four sections got a banner instead of a rewrite** — §23, §29, §31, §32 — matching
what §12, §21, §22 and `INVARIANTS.md` already do, and each banner names which parts of
its own section are still true.

**§17 got line fixes rather than the banner the audit suggested, deliberately.** Its
*WRITE normal* and *DISCARD* blocks are exactly what the code does; only the FLUSH
pointer (to §14.4's six steps) and the closing "every FLUSHed write survives host loss"
were false — and that last one contradicted §2 and §14.8 inside the same file. A banner
saying "this section is V2" would have been as wrong as the two lines were.

**What is still stale and was deliberately left**, so the next increment has an inventory
rather than a rediscovery: §5.8 (S3 as recovery authority — INV-08 is boot authority now),
§11 (I/O classes; `internal/ioclass` is deleted), §14.2/§14.3/§14.4/§14.5 (the remote
batch and its rules), §16 (the state machine's `SELF_FENCED`/`RECOVERING` arms), §24 (the
S3 client subsystem), §30's roadmap items 6, 8, 10, 11 and 12. None of them contradicts
§14.8 in the way §17 and §23 did; all of them describe V2 in the present tense.

**Two claims in files this track does not own were checked and are wrong**, both about
tests: `integration/vhost/lifecycle_test.go`'s doc comment still describes checkpoints,
truncation and §21.1 above a function renamed `TestAGuestSurvivesAStopAndComesBackFromItsImage`,
and `integration/vhost/guest_test.go:38` still says a guest's FLUSH is answered by "our
§14.4 ACK path — the object verified and the lease valid at the instant of the ACK", when
`integration/e2e/guest_test.go:75` asserts that same `fsync` publishes **zero** objects.
The bodies of `## ~~DEV-0007~~` and `## ~~DEV-0022~~` in this file's open-work region name
the old test too. Track C owns the first, track B the second, and the owner the third.

**`task ci` was red before and after this change**, at `fmt:check` on
`internal/metadata/pg/pg.go` — another lane's uncommitted gofmt alignment. This increment
changed three Markdown files. What it is accountable to are the four self-checks in
`REFERENCE.md`'s header (dangling `§`, and every `ADR-`/`INV-`/`DEV-` the code cites
resolving), which were run and are all empty; adding the missing `DEV-0022` row is what
made the last one so.

### A2 — the head is recounted, the body stops using the present tense (2026-08-04)

`30ea4b2` (STATUS.md), `ad77035` (README + two spec deletions), `<this commit>` (this
entry).

**Every claim in both head tables was re-derived from the code and cites the file and line
it was checked against.** That is the whole method, and the audit that produced this item is
the argument for it: the phase table listed checkpoints, GC, the drain, promotion and the
remote WAL as integrated, and none of those packages exists. A table edited from another
table is what produced that. The invariant line is now **14 active, 6 withdrawn, 2
pending** — counted from `INVARIANTS.md`'s explicit states, against "21 of 22 active".

**Three rows needed a state the table did not have**, so `withdrawn` was added to the
maturity legend, matching `INVARIANTS.md`. "Model" reads as *written, waiting for a
caller*; a reader who goes looking for `internal/recovery` finds nothing at all, and those
are not the same claim.

**The old BUILD-INVENTORY table (increments 0-8) was deleted rather than banner-ed** — the
only deletion in this increment. It recorded a queue, not a defect or a decision, and
`git log` has it. Everything else got a banner and kept its account: the durability
scheduler's two traps, the e2e lane's "an object in the bucket is not proof of a claim",
§14.8's two halves, DEV-0007's clone-chain findings, DEV-0019's three-level fix, DEV-0012's
two fencing triggers, Gap 1's LIST precondition.

**DEV-0020 was rewritten, not banner-ed, because it is open and it described the wrong
defect.** `materialize.FromSnapshot` is gone; nothing walks a clone chain, and
`image.Publish`/`PublishSnapshot` upload `view.Ranges()`, which flattens the base into the
layer (`internal/cow/ranges.go:24`) — so a depth-2 clone reads a *copy* of its
grandparent's data, not zeros, and pays a full duplicate per link. Nothing in the tree
exercises `chain_depth > 1`, and the entry now says so rather than asserting the inference.
C11 and this are one decision, which is what `PARALLEL-PLAN.md` concluded independently.

**Four counters were recounted with the command beside them**, all four wrong: ADR-0013's
citations (54 non-test lines, not 32 — it has overtaken ADR-0026 as the most-cited),
`internal/obs.Catalog()` (25 declared, 9 with a producer, under a paragraph that said
"six" above a list of nine, one of which had been cut), the architecture document's
citations (846, not 2.106), and §12.3's (22 across 15 files, none in the deleted
`promotion.go`). Two of them moved between two runs an hour apart while other lanes
committed.

**`README.md`'s reachability claim was wrong and is now a loop that answers itself.** Six
`.go` files cite `SHUTDOWN-PUBLISH-SPEC.md`; one cites `VIEW-ADOPTION-SPEC.md`; every other
spec is cited by no code at all. `DURABILITY-SCHEDULER-SPEC.md` and
`RUNTIME-FENCING-SPEC.md` are deleted under that file's own rule — ADR-0026 removed the
scheduler and the lease-gated ACK they reviewed, and the decisions that outlived them are
in `internal/agent/volume.go` where CLAUDE.md says they belong.

**`task ci` exit 0** (fmt, build, lint, race tests, dst) immediately before the first
commit; an earlier run the same afternoon was red at `fmt:check` on
`internal/metadata/metadatatest/contract.go`, another lane's file, and that lane fixed it.
The four self-checks in `REFERENCE.md`'s header were run and are all empty. **Left for
whoever holds the counters:** `CLAUDE.md` says "25 ADRs and 10 spec documents" and the
tree has 22 and 7 — that file is outside this track's ownership.

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

**B9: a guest that stays alive (2026-08-03, `0ee78a6`).** `RunLinuxGuest` ended in
`cmd.Run()`, so **no test in this repository had ever had a live guest concurrent with any
other event** — every snapshot, restart and clone in the lanes happened over a device whose
guest had already powered off, and the e2e lane's "live" snapshot is requested thirteen
lines after the guest is gone. `testinfra.StartLinuxGuest` returns while QEMU runs (on the
same `Process` the binaries use, so the console is waitable line by line, with no second
copy of the pump), and `RunLinuxGuest` is now the thin blocking wrapper — its `ctx`
parameter went with the rewrite, because a guest that is killed by the test's own cleanup
has no use for one; the five call sites in `integration/{e2e,vhost}` are the only edits
outside this track's files. `integration/guestinit` grew `spin.mode=hold`: write, fsync,
print `GUESTINIT-ALIVE <n>`, repeat, and stop when the **host** sends `GUESTCTL-STOP` down
the other direction of the serial line — a channel rather than a signal to QEMU, because
killing the VM proves nothing about the guest and leaves the volume mid-write. An
unrecognised `spin.mode=` is now an error instead of falling back to the write path.

`TestAGuestStaysAliveWhileTheHostWatchesItWrite` is the deliverable: it asserts liveness
three ways — the WAL's durable watermark moves *after* the host read it, a heartbeat
numbered above any seen before that arrives afterwards, and the guest answers the stop by
powering off and reporting `GUESTINIT-PASS`. **Both planted bugs go red**: a `hold()` that
returns after one iteration fails at "the WAL to grow under a running guest" (the first
heartbeat and the first watermark still arrive, which is why one observation would not
have been enough), and a guest deaf to the console keeps printing heartbeats until `Stop`
gives up. What this unblocks is track C's snapshot-mid-write, an Agent restart under an
attached guest, and RISK-10's reconnect path; none of them are written here.

**B8a: "components with no production caller" is computed (2026-08-04).** The list under
that heading above was hand-written, and in two consecutive waves it was wrong — it went
stale inside one increment, and wave 2 added an OTLP exporter and a provider nobody listed.
`task deadcode` (`hack/deadcode.sh` + `hack/deadcode-allow.txt`, on the pinned
`golang.org/x/tools/cmd/deadcode` — the module already required x/tools, so the pin is
go.mod's and `tools:deadcode` refuses to install when the two disagree) answers it from the
call graph instead. Roots are the binaries and only the binaries — `./cmd/...` plus
`integration/guestinit`, which is PID 1 in the guest; `-test` was rejected because it makes
every test's own subject reachable and answers a question nobody asked.

**Its output today: 78 reported, 61 explained by the allowlist, 17 unexplained, exit 1.**
The 17 are `internal/simio/real`'s entire network implementation (9 — a real `Listen`/
`Dial` whose only caller is the simio contract test; the transport is Connect over HTTP),
`internal/lifecycle`'s Agent volume-state machine (7 — §16's `AgentVolumeState`, tested and
referenced by nothing outside its own file), and `lease.Manager.Revoke`, a verb nothing
performs since ADR-0026 removed the lease-gated ACK. They belong to tracks E and D, so this
track reports them rather than deleting them. **What would make it blocking in `ci:full`:
that number reaching zero** — each finding deleted, or in the allowlist with its reason.
Until then it stays out of the gate deliberately: a step that is red the day it lands is a
step someone removes.

**The allowlist is the part that had to be built carefully**, because an allowlist is where
findings go to be forgotten. Every entry carries its reason on the same line and the task
**fails** on an entry with no reason and on an entry that no longer matches a finding —
which caught its own author twice within minutes: `lifecycle.SnapshotStates` is called by
`SnapshotState.Valid` and was never dead, and `package integration/guestinit` cannot be
reported because it is a root. Neither would have been noticed by a human writing a list.

**Two blind spots, both stated in the script, both real.** RTA marks every method of a type
that reaches `reflect` as live, so `wal.TruncateLocal` — the flagship entry of the
hand-written list — is *not* reported (`-whylive` answers "reachable only through
reflection"); and a package no binary imports is not in the program at all, which is where
`metadata.BumpVolumeEpoch`, the other entry, lives. A second pass names those packages
(10, all explained) at package granularity. So the tool is a floor: absence from its report
is not evidence of a caller, and the hand-written list and the tool disagree in *both*
directions — which is the argument for having the mechanical one, not against it.

**Planted:** an exported `PlantedUnusedVerb()` in `integration/guestinit/main.go` (a root,
so the plant tests the analysis and not just the parser). Reported went 78 → 79 and
unexplained 17 → 18, with `integration/guestinit.PlantedUnusedVerb` at the top of the
unexplained list. Reverted.

**B8b: `guest:verify` fails when it cannot check (2026-08-04).** The lane's preflight had
four states in which it printed OK having proven nothing, and three of them were measured
on this machine before the change rather than argued from the source. `test -f` on the
initramfs: emptying `_output/guest/initramfs.cpio.gz` left `task guest:verify` green and
exit 0 — and `build:guest` did **not** rebuild it, because its `sources:`/`generates:`
fingerprint compares the sources, so a truncated artefact is "up to date". An empty
`GUEST_KERNEL_SHA256`: `task guest:verify GUEST_KERNEL_SHA256=` printed the full `OK:
Linux version 7.1.0 — PVH ELF, ... present`, verifying a kernel against no pin. A missing
`readelf`: the Xen PVH note check was wrapped in `if command -v readelf`, so a machine
without binutils skipped it silently. And a kernel with no embedded config printed
`warning: ... unchecked` and returned success, which is the case where all four
`CONFIG_*` assertions stop running.

All four now fail, and each message names the input and the task that produces a good one.
The initramfs is opened rather than stat'ed — gzip-tested, then listed for `./init`, whose
absence is a kernel panic ("no working init found") that reads like a kernel bug rather
than a missing build step. `fetch` still honours an empty pin, because bisecting a kernel
change needs to acquire an unpinned one; *verifying* against no pin is a contradiction.

**Planted, all four, and quoted here because a preflight is exactly the kind of check that
is never exercised:** zero-byte initramfs → `... is not a valid gzip stream — a truncated
or interrupted build; rebuild: task build:guest`; a valid archive holding only `dev/` →
`... contains no ./init — the kernel would mount it and panic with no PID 1`; empty pin →
`GUEST_KERNEL_SHA256 is empty: there is no pin to verify ... against`; `readelf` off the
PATH → `readelf is missing, so ...'s Xen PVH note cannot be checked`; and the config branch
planted by pointing `ikconfig` at a marker the kernel does not carry →
`has no embedded config (CONFIG_IKCONFIG=n), so CONFIG_VIRTIO_BLK ... cannot be checked`.
Each exits non-zero; with good inputs the target is green.

**One of these plants was the check crying wolf at its author**, which is worth recording
because it is the failure mode that gets a preflight disabled: the first `./init` matcher
was `grep -qx './init'`, and `cpio --list` prints the stored `./init` back as `init`, so a
perfectly good initramfs failed. Caught by running the good path, not by reading it.

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

### C2 — stop no longer cancels the read it then waits for (2026-08-04)

SHUTDOWN-PUBLISH-SPEC §5, implemented after the human review of 2026-08-04. `fetchBase`
ran under the serve context, and `stop()` cancels that context and *then* waits on
`baseDone` — so a volume stopped while its base was still loading cancelled its own read,
`fetchBase` recorded a failed view, and `publish()` correctly refused to write an image
missing everything the volume held before this session. The whole session was dropped with
one log line. It now runs under `context.WithCancel(context.WithoutCancel(ctx))`, released
by `stop()` *after* publish has used its result. `WithoutCancel` and not `Background`: the
values (a trace span) are worth keeping and only the cancellation is wrong for this work —
and not a child of `ctx` either, because that is the Agent loop's context, which SIGTERM
cancels before `Close()` runs at all.

**What bounds the fetch now: nothing this increment owns.** `-shutdown-grace` does not
exist yet (the next increment of the same spec), so a stop waits on the store's own
timeouts. That is the safe direction of the two — a stop that waits too long is visible
and recoverable, a stop that publishes an image with a hole in it is neither — and it is
recorded here rather than discovered by whoever meets it.

The proof is a new mandatory DST scenario, `a-volume-stopped-mid-fetch-still-publishes`,
and it asserts what a *later guest reads back*, not that a context survived. Three
sessions: one writes and stops so there is a base worth waiting for; the second writes and
is stopped with its manifest read **blocked inside the store**; the third is a fresh Agent
on a data directory that has never seen the volume, so only the bucket can answer — and it
must answer with both patterns. Determinism comes from the release of the blocked read
being triggered by the *listener closing*, which happens only after the teardown has
cancelled the serve context; no sleep and no timeout. The hold is a `gatedStore` in the
scenario rather than a new `sim.ObjectStore` injector: every injector there returns an
answer, which is a fetch that has already finished, and `sim.ObjectStore`'s methods ignore
the context by design, so it could not model the cancellation half at all.

Planted bug (restored `context.WithCancel(serveCtx)`, reverted): red on all six seeds —
*"volume … read zeros at 4096: the image published by the interrupted session is missing
what the interrupted session wrote"*, with the Agent's own line above it reading *"the
volume's image could not be loaded … the read of image/…/manifest.json was cancelled while
it was in flight: context canceled"*.

### C5 — the shutdown publish holds the data directory and retries (2026-08-04)

SHUTDOWN-PUBLISH-SPEC's "REVIEWED AND DECIDED", implemented as the owner decided it and
**not** as the spec below that block proposed: a host that cannot publish does not exit
non-zero, it refuses to let go. The mechanism is forced — a flock is released by the
kernel when the process exits, so "refuse to release the lock" can only mean "do not
exit", which in this code means `VolumeManager.Close` does not return. It quiesces every
volume, then retries their publishes in rounds, indefinitely, holding the directory and
the process.

`-shutdown-grace` (new, 60s, mirroring `cmd/control-plane`) is the bound on **one
attempt**, not on the wait, and nothing here abandons data on a timer. It is armed on the
injected clock rather than with `context.WithTimeout`, which reads the real one — a
deadline the DST harness cannot advance either never fires in simulation or fires by
wall-clock accident. Zero means unbounded, which is what every in-process caller passes,
so no test or scenario gained a timer.

**Where the loop lives, and why the other two places are wrong.** In the `VolumeManager`.
Not in `Volume`: the decision is about the *data directory*, which a Volume cannot even
name, and a per-volume loop inside `stop()` would publish one volume to completion before
attempting the next, stranding every other session behind the slowest one. Not in `main`:
`Close` joins its errors, so `main` would have to re-derive which volumes were left, and
anything that lives only in `main` is what spin's runner does not inherit when it takes
the manager without the loop (ADR-0021) — which is exactly how `HostID` went missing.

**Volumes retry together, not one at a time.** A round tries every still-unpublished
volume once, serially within the round, then backs off (2s doubling to 30s). Concurrent
publishing multiplies the bandwidth a stopping host takes from the ones still serving,
with no io-class scheduler left to bound it, and interleaves the per-volume lines an
incident reads. One-to-completion is worse: a failure specific to volume A means volume B
is never attempted and the operator hears nothing about it.

**Three failures do not retry.** `image.ErrSuperseded` — another writer published over us,
so retrying would replace a newer image with an older one, which is the one thing INV-10
exists to prevent, and holding buys nothing because the volume has moved to a host that
does not care what this directory holds. Exit **2**, meaning *do not restart*. The new
`agent.ErrNoReadView` — the fetch that would have completed the image is over and this
process will not attempt another, so holding would be a wait with no event that could end
it. And the reconciliation teardown (`remove`, i.e. a fence or a promotion) makes exactly
one attempt and never holds: it runs on the reconcile goroutine, where a retry loop is a
lease not renewed and every *other* volume on the host fenced.

**The heartbeat keeps running while it holds** (`Loop.Sustain`), because "up and stuck" and
"gone" must not look the same to the fleet — that legibility is the whole reason the owner
chose holding over exiting. It is a reduced cycle: heartbeat plus a report of the volumes
still held (a held volume is still this host's, so the report is accepted), and
deliberately no `GetDesiredState` — applying it would restart the runtimes the teardown
just stopped — and no fencing, since nothing is left to stop and the manifest's CAS is the
authority anyway.

The proof is `integration/e2e/hold_test.go`, with the real binaries: the object store is
made unreachable by a TCP proxy the test can break (not by stopping the container, which
would come back on a different port the Agent was never told about), the Agent is
SIGTERM'd, and it **stays alive** — the holding line appears twice, a second Agent on that
directory is still refused by the kernel, the bucket is still empty, and the host's
`last_heartbeat` keeps advancing in the catalog. Then the proxy is restored, nothing
touches the Agent, the image appears and only then does it exit 0.

Planted bug (teardown returns on the first failure, which is what this increment replaced):
red, *"agent-1 exited (exit status 1) having printed "agent is holding unpublished data and
will not release its data directory" 0 time(s), wanted 2"* — a regression test for the
defect itself. The three unit arms in `internal/agent/hold_test.go` were planted
separately: giving up on the first failure ("*the bucket still holds no manifest*"),
retrying `ErrSuperseded` ("*the teardown waited to retry a superseded publish*"), and
reclaiming the local WAL on the strength of a publish that did not happen ("*…/wal/…/1 is
empty after an abandoned publish*" — SHUTDOWN-PUBLISH-SPEC §6, pinned).

**A defect the lane found by running the binaries.** The holding line printed
`data_dir=.`. In production `VolumeManagerConfig.DataDir` *is* `"."` — the Agent's real
Disk is rooted at `--data-dir` so the process cannot write outside it — so every message
naming the directory named nothing, on the one line whose entire purpose is to tell an
operator which directory on which host is stuck. Fixed with `DataDirLabel`, the same
directory spelled the way the operator spelled it. No in-process test could have seen it:
they all hand the manager a Disk spanning a whole filesystem, where the two spellings
agree — the same blind spot that hid `--data-dir` being applied twice.

**Not done here, and deliberately.** No DST scenario for the hold: the harness advances
its clock on quiescence, so a loop that retries forever is a scenario that never ends, and
the property under test ("the process is still there and the lock is still refused") is
about a process and a kernel, which is `integration/e2e`'s job. The retry's *effects* on
the data path — what publishes, what refuses, what stays in the WAL — are covered by the
unit arms above and by the existing publish scenarios.

### C9 — a snapshot taken while a guest writes is one point, not a smear (2026-08-04)

§19's whole claim is that a snapshot is a **sequence number, not an event**, and until now
nothing tested it: every snapshot, restart and clone in these lanes happened over a device
whose guest had already powered off, and the e2e lane's "snapshot of a live volume" asks
for its snapshot thirteen lines after the guest has gone. `TestASnapshotOfAWritingGuestIsOnePointAndNotASmear`
(`integration/vhost/lifecycle_test.go`) is the first test in this repository where a real
Linux guest is writing to a device *while* something else happens to it.

**The assertion is on bytes, and the weaker ones prove nothing.** "The snapshot object
exists" is satisfied by a smeared snapshot — it exists too, with the wrong bytes in it, and
a clone of it boots a state its parent never had. "The sequence is non-zero", or below the
volume's, tests a number the same function writes into the manifest next to the bytes;
nothing ties the two together, so `Freeze` could return the live map with a perfectly
correct sequence and every sequence assertion in the tree stays green. And "the guest's
data is in the snapshot" is the opposite half — completeness — which a copy of everything,
including writes made after the freeze, passes perfectly.

**The shape of the test is forced by two facts, and they are worth writing down because
the obvious design does not work.** (1) `integration/guestinit`'s hold mode writes one
constant pattern to eight fixed blocks, so a volume it is writing to stops changing about
400 ms into the run — every later moment looks identical, and a snapshot taken at any of
them is indistinguishable from a smear. The changing byte therefore has to be *filler the
guest then overwrites*, which means the region must already be in the frozen view:
`image.uploadChunks` evaluates `view.Ranges()` once, up front, so a range that did not
exist at the freeze is never uploaded and could never carry a late write. The volume boots
from an image the test publishes with `image.Publish`, with the guest's whole write region
pre-filled. (2) Whether a write lands inside the freeze→upload window cannot be left to
timing, so the Agent's object store is a double that **suspends the upload at its first
chunk `Head`** and holds it there while the kernel boots and writes. That turns "the guest
wrote while the copy was being made" from a race into an ordering the test enforces. A
second, unwritten region below the guest's exists only so the frozen view is two chunks:
`uploadChunks` reads a chunk's bytes and only then calls the store, so with one chunk the
whole copy is already in memory before the first call and there is no moment to suspend
the upload *at*.

Planted bug — the natural error in `wal.Log.Freeze`, taking `l.view` without swapping a
fresh layer over it, so the "frozen" map is the live one — red on the bytes:

    the snapshot carries a byte the guest wrote after it was frozen: at volume offset
    2097152 it holds 0x41, and the point it was frozen at (sequence 0) held 0xf5

0x41 is `'A'`, the first byte of the guest's pattern, in a snapshot whose every byte should
be filler. The other half of the test is what keeps that from being vacuous: a guest that
wrote nothing — a moved offset in `guestinit`, a device swallowing requests — also leaves a
snapshot full of filler, so the volume's **own** image, published when it stops, is read
back and required to differ from the filler in the same region.

Two things the first run of it established. The gate is on chunk keys and not on any
`Head`, because loading a volume's base image Heads `manifest.json` once for the ETag its
publish will CAS against — a gate on the first Head of any kind held the read view instead
of the snapshot, and the assertion on *which* key it stopped at is what said so. And the
lane, not `integration/e2e`, is where this can live: suspending an upload deterministically
needs a store the test owns, and over RustFS the equivalent is a paused TCP proxy, which
trades the deterministic window for an S3 client timeout — with the snapshot then failing
and never being retried (`ensureSnapshot` starts one at most once per id).

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

**D4: admission counts what a host is using, not only what it was promised (2026-08-04).**
ADR-0013's second surviving gap (its amendment of 2026-08-03). The Agent has always
shipped the measured `UsedBytes` — a `statfs` of the filesystem holding `--data-dir` — and
`cpserver` has always stored it in `hosts.nvme_used_bytes`; **nothing read it**. So the
§28.2 bound protected against over-promising and not at all against filling, and under
ADR-0026 that is worse than when the ADR was written: a session's whole WAL stays local
until the volume stops, so the device holds everything every attached volume has written,
with no mid-session reclaim and no reservation covering a byte of it.

`placement.Policy` gains `MaxUsedRatio` (zero means `DefaultMaxUsedRatio` = ADR-0013 §3's
85%, never "unbounded" — a policy literal written before the field existed never decided
that a full device may keep receiving volumes), and `metadata.CapacityBound` gains
`UsedLimit`. Both arms travel with the write (ADR-0017): the SQL predicate now reads
`nvme_used_bytes` from the same `hosts` row lookup that already proved the host exists, in
both `CreateVolume` and `UpdateOperationPhase` — a drain's plan entry is a reservation too,
and it moves whole hosts' worth of data. `placement.Policy.Bound` is the only builder of a
bound, so the two ceilings are derived together; `cmd/control-plane` exposes
`-max-used-ratio` next to `-max-oversubscription`.

**The rule, and what it is not.** The measured arm charges the request *nothing*:
`used <= UsedLimit`, a gate, not an accounting. `used + size <= UsedLimit` was rejected
because it assumes a volume occupies its declared size the moment it is placed, which is
the assumption oversubscription exists to deny — a 1 TiB volume would be unplaceable on a
half-empty 2 TiB device. A single occupancy number over `max(committed, used)` was
rejected because the physical ceiling is *below* the promise ceiling by construction, so it
subsumes it and `MaxOversubscription` stops meaning anything. And the catalog cannot
predict what a volume adds physically anyway: what lands is what the guest writes.
`best()` still ranks on committed, deliberately — the measurement lags placement by a
heartbeat plus however long a guest takes to write, so a host handed ten volumes still
measures empty and a used-bytes ranking would keep choosing it.

**Two traps, both decided rather than tripped.** `used` includes the other tenants of that
filesystem, which is the point (`agent.DiskUsage`): no truncation of ours frees them. And
"the host has not measured yet" is *not* a third state — total and used come from one
`statfs` in one heartbeat, and `DiskUsage.Usage` returns an error rather than a zero when
it fails, so the existing `NVMeTotalBytes <= 0` guard already refuses the unmeasured host.
Reading `used == 0` as "unknown, refuse" would have refused the emptiest host in the fleet.

**Four planted bugs, each watched go red.** Dropping the `&& h.NVMeUsedBytes <=
p.UsedLimit(h)` from `Admits` (`Admits = true, want false` in three table cases, and
`Choose = "h-source"` where a source host at 95% must fall through); `if false &&` on the
sim's arm and `>= -1 *` on each of the two SQL predicates (`CreateVolume onto a device at
900/1024 GiB = <nil>, want ErrCapacityExceeded`, and the same for the plan entry, in both
the sim and pg lanes). `task ci` and `go test -tags integration ./internal/metadata/pg`
are green.

**D4b: the bound was a predicate of the write and still not a bound (2026-08-04).** Found
while writing D4's verification, and it is the reason that case exists. ADR-0017 moved the
§28.2 ceiling into the statement that places the bytes, and the claim written next to it —
"two operations that chose the same destination against the same fleet read produce one
reservation and one `ErrCapacityExceeded`" — **is false in PostgreSQL**. READ COMMITTED
fixes a statement's snapshot before the statement runs, and the derived committed value is
an aggregate over rows the statement does not lock, so two `INSERT`s that overlap in time
each evaluate the bound against a fleet the other is not in yet. Measured before anything
was written: two `psql` sessions, one 100-byte ceiling, two 100-byte volumes, 200 committed
afterwards. Nothing in the repository could have caught it — every capacity case was
sequential, and a sequential case cannot tell a predicate of the write from a check in
front of it.

`internal/db/queries/hosts.sql` gains `LockHostPlacement`
(`pg_advisory_xact_lock(hashtextextended(host))`) and `pg.Store.placing` runs every bounded
write as *two* statements in one transaction: the lock, then the write. The lock has to be
its own statement — an advisory lock taken inside the INSERT would change nothing, because
that statement's snapshot is already taken — and READ COMMITTED is what makes it work: the
loser blocks on the lock and the INSERT it then runs takes a fresh snapshot containing the
winner's row, so it refuses itself. Rejected: SERIALIZABLE (a retry loop in every caller of
the Store for a two-row hot spot) and `SELECT ... FOR UPDATE` on the host row (locks the
wrong rows — every heartbeat writes that one, and the counted rows are in `volumes` and
`operations`). An unbounded write is not a placement decision and pays nothing: no
transaction, no lock.

The contract case is `placements racing for the last slot leave the host inside its
ceiling` — five rounds of 32 concurrent `CreateVolume`s onto a host with room for one,
asserting the host's committed bytes afterwards rather than which caller won. Rounds and
racers because a scheduler is not an oracle. **Planted in both lanes:** replacing `placing`
with `boundRefused` in Go followed by the write reddens the pg lane 5 runs out of 5
(`round 0: 2 of 32 racing placements landed, want exactly 1`); moving the sim's
`boundLocked` out of its critical section reddens the sim lane 16 runs out of 20 — the
sim's window is a mutex hand-off, so its detection is high but not certain, while the lane
where the defect is real detects every time.

`internal/metadata/pg` (`-tags integration`, the whole package) and `task dst` are green.
`task ci` is **not** green in this tree, for a reason this branch did not cause:
`cmd/volume-agent/main.go` is unformatted and `internal/agent` hangs in a publish retry
loop, both track C's uncommitted work in progress. `task test` for every package this
branch touches is green, as is `task lint` apart from that one file.

**D5: a cordon on device pressure has an actor, a reason and hysteresis (2026-08-04).**
ADR-0013 §3's first row, the last of the three the amendment approved. Nothing cordoned on
pressure: `SetHostState` had no non-production caller at all, and no code path read a
device measurement to decide anything. `cpserver.Heartbeat` now does — the heartbeat
carries the only measurement of the device that exists, and reacting to it is the Control
Plane's alone (ADR-0013 §5: the Agent gets local defensive powers only, and moving volumes
stays here, because two actors evacuating one host is the class of bug two earlier waves
closed). The Agent-local half — refusing attaches at 85%, the reserve — is deliberately
not here; it is `internal/agent`'s and a later increment's.

**The hysteresis is a band, not a dwell** (`cpserver.CordonUsedRatio` = 0.70,
`UncordonUsedRatio` = 0.65). One threshold is a flapping cordon: a host sitting on the line
crosses it in both directions on consecutive heartbeats, and every crossing is a write, a
state every placement decision in the fleet reads, and a line in whatever an operator is
watching — placement becomes non-deterministic for reasons nothing records. A Schmitt
trigger fixes that with **no state at all**: the decision is a pure function of the host
row, because the previous decision is read back from the state and the reason it wrote.
Coming back requires freeing 5% of the device, which heartbeat jitter does not produce; the
5%–gap dead zone (a host cordoned while admission's 85% ceiling would still take it) is the
price, and the band is deliberately the *smallest* gap that is unambiguously real.
**A dwell was rejected**: "clear for N heartbeats" needs a per-host timestamp, which is
either a column written on every heartbeat of every host — the busiest RPC there is — or
memory a leader change discards, so a failing-over CP would hold hosts cordoned
indefinitely with each new leader restarting the count. It also puts a clock into a
decision that is otherwise two numbers already in the row (INV-01 makes every clock an
injected dependency). What it catches and the band does not is a device that frees 5% and
refills it between two heartbeats — not noise, a host doing exactly what the cordon is for.

**The reason is also the authority** (`lifecycle.CordonReason`, `hosts.cordon_reason`,
`migrations/20260804112514_host_cordon_reason.{sql,json}`, planned against and applied to
the dev database; `task db:verify` green). `CORDONED` stopped being evidence that a human
meant it, so an operator needs the cause — and, in the other direction, the automatic loop
must never clear a cordon set for a cause the fleet cannot see. One column serves both:
`OPERATOR` outranks `DEVICE_PRESSURE`, `DEVICE_PRESSURE` may only replace `''` or itself,
and `CordonReasons().OverwritableNames()` is that table as the `SetHostState` predicate —
in Go it would be read, compare, write, and an operator's cordon landing between the read
and the write would be cleared anyway. A second `cordoned_by` column was rejected: two
columns that must agree are two columns that can disagree. `SetHostState` gains the
parameter; `CordonNone` is not an actor and is refused, so no write is authorless. A table
constraint (`state = 'CORDONED' OR cordon_reason = ''`) keeps a reason from outliving its
cordon, because the next reader of a stale reason is the pressure loop deciding whether it
may act.

**Three planted bugs, each watched go red.** `UncordonUsedRatio = 0.70` (the hysteresis
gone) reddens `TestHeartbeatCordonsAndUncordonsAcrossTheBand` on a one-byte oscillation:
`after 769658139443/1099511627776 used: state = "ACTIVE" reason = "", want "CORDONED"` —
one byte off a 1 TiB device flips the fleet state. Adding `CordonOperator` to
`cordonOverwrite[CordonPressure]` reddens the contract case in both lanes
(`un-cordoning an operator's cordon: want ErrCordonHeld, got <nil>`), and passing
`CordonOperator.OverwritableNames()` in the pg params reddens it in the pg lane alone —
which is where the rule is actually enforced, since pg's Go check only diagnoses a 0-row
write. Dropping the reason clause from `pressureTarget` on top of the first reddens
`TestPressureNeverTouchesAnOperatorsCordon` (`a heartbeat cleared an operator's cordon`);
either alone leaves it green, which is the defence in depth working and is why the store
half has its own case.

No DST scenario: this is neither data path nor fencing nor GC, and it adds no checker (the
merge protocol allows one per window). `task ci`, `task cover` (90.0%) and
`go test -tags integration ./internal/metadata/pg` are green.

**D6: an operator can read the fleet, and a rebuilt volume can be placed (2026-08-04).**
Two findings, one increment. `cmd/control-plane` had a one-shot for every *write* an
operator needs — seed, snapshot, clone, rebuild, detach, attach — and none for looking, so
"which hosts are cordoned and why", "which volumes has nobody got" and "which snapshots
are stuck" were a psql session and four hand-written joins that lived nowhere. And the
reason the catalog could not answer them is structural: every read the Control Plane
serves is scoped to a host, because every one of them answers an Agent. A volume with no
primary matches no host id — not even `""`, which is `ErrInvalidID` at the boundary — and
`ListPendingSnapshots` reaches a snapshot *through* `volumes.primary_host_id`. After
`-rebuild-metadata`, which restores no placement, every volume in the catalog is in that
blind spot.

`metadata.Store` gains `ListVolumes` and `ListUnfinishedSnapshots` in both
implementations, with the contract case `FleetWideReadsSeeTheRowsNoHostOwns` stating the
contrast rather than assuming it (it detaches the fixture volume, then asserts the
per-host read has gone blind and the fleet-wide one has not). Unfinished is CREATING and
DELETING, defined once as `lifecycle.SnapshotState.Unfinished` and handed to the SQL as an
array — the `allowed_states` move — so the §19 vocabulary keeps one authority. It is
deliberately not "the states with no successor": PUBLISHED has one and is finished, while
DELETING has none and is not, because ADR-0026 deleted the reclaim that was supposed to
consume it. No index on `snapshots.state`: this is a fleet-wide scan an operator runs by
hand, and an index would be maintained by every snapshot write for its benefit (written in
the query).

`-fleet-status` prints hosts with state/reason/fill, volumes with host/state/epoch, and
the unfinished snapshots with the host that is supposed to take each — `-` when nobody is,
which is the column that says whether anything will ever happen. Text, not JSON, and a
flag rather than a subcommand tree: ADR-0021 says these binaries are test harnesses and
the operator interface belongs to `spin`, so the bar is "someone running this repository's
lanes can see the fleet". It takes no term and no `-holder-id`, and runs before the object
store is opened — a read guards nothing, and refusing to show the catalog because nothing
is leading (or because no bucket was named) removes the view exactly when it is the only
thing left. Ages are differences against `metadata.Store.Now`, the catalog's own clock.

The placing half: `-attach-host` is now optional, and `controlplane.Place` chooses with
`placement.Choose` when it is absent — §20's third rule, under the same two ceiling flags
`-clone-snapshot` uses. **Both, and neither direction is an accident**: an operator naming
a host is overriding the admission rule on information the catalog does not have (a volume
restored to the machine whose device still holds its bytes goes to a host cordoned for
exactly that fill), so a named host is honoured without an admission check — and not
silently, since the placement line carries `host_state` and `chosen_by`.

**Owed, and it is a real gap: the bound is checked by `Choose` and is not carried into the
write.** `SetVolumePrimaryHost` takes no `CapacityBound`, so two attaches racing onto one
host both evaluate the ceiling against a fleet the other is not in yet and both commit —
the same defect D4b closed for `CreateVolume` by making the bound a predicate of the
statement (and, in Postgres, by the advisory lock in front of it). Closing it is a
signature change to a store method in both implementations plus its contract; until then
this path is one human running one command from a shell.

**Also owed: nothing drives `-fleet-status` as a process.** The report is asserted on the
bytes it writes, against the sim store, in `cmd/control-plane/fleet_test.go`; the three
lines in `main` that reach it from the flag are covered by nothing, and `integration/e2e`
is track C's file set this wave. That is the seam this repository keeps losing defects at.

**Six planted bugs, each watched go red.** Dropping the state filter from the sim's
`ListUnfinishedSnapshots`: the printed report says `SNAPSHOTS NOT FINISHED (2)` and lists
a PUBLISHED snapshot; the contract case fails with `a PUBLISHED snapshot is still
outstanding`. The same tautology in SQL (`WHERE TRUE OR state = ANY(...)`, regenerated)
fails the pg lane identically. Making the sim's `ListVolumes` skip the unplaced:
`ListVolumes = [b1], want [b1 b2]`. Narrowing `Unfinished` to CREATING:
`"DELETING".Unfinished() = false, want true`. Replacing `Choose` with `hosts[0]` — the
fixture's admitted host sorts *last* so that this plant cannot pass — `placed on ...072,
want the one host that admits it (...074)`. Ignoring the named host: `Place returned
...074/ACTIVE, want the named host and the state that says it is out of service`.

No schema change (two new queries, no new object), so no migration and no `db:plan`. No
DST scenario and no new checker: this is neither data path nor fencing. `task ci`,
`task cover` (production 90.0%) and `go test -tags integration ./internal/metadata/pg` are
green.

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

### E2 — a metric leaves the process (2026-08-04)

`obs.NewProvider(name, exporter)` is the production Provider, and
`real.NewOTLPMetricExporter(ctx, endpoint)` is the OTLP/HTTP exporter behind it. Nine
metrics were being recorded and none of them left either binary: `obs` had only
`NewTestProvider`, so both mains passed `Recorder: nil` — honestly, with a comment saying
that passing the test provider "would export the metrics to memory and look like
observability from the outside".

The socket-opening half is in `internal/simio/real/otlp.go` and nowhere else. That is
INV-01, not tidiness: building the exporter inside `internal/obs` would have needed an
exemption in **both** `.golangci.yml` and the `simulable` analyzer, which is the widening
those two exist to prevent. `obs` takes an `sdkmetric.Exporter` and knows nothing about
transports.

**A nil exporter is a working Provider that exports nothing**, and an unset endpoint
returns exactly that nil — so a binary needs no conditional and an Agent with no collector
starts and runs as it does today. Two smaller decisions are written at the code: a
malformed endpoint is *refused* at startup (the exporter's own behaviour on a bad URL is
to keep its defaults and quietly export to localhost), and exporter retries are **off**,
because the export that matters is the one `Shutdown` flushes while a volume is stopping —
the default one-minute retry would add a minute to every Agent's shutdown when a collector
is down, delaying the publish that carries V1's whole RPO. Nothing is lost by dropping it:
OTLP metrics are cumulative, so the next successful export restates the totals.

**Verified against a real receiver, not against a constructor returning non-nil.**
`TestOTLPExporterDeliversARecordedMetricToACollector` stands up an OTLP/HTTP server,
records `lease_renewal_failures_total{host=host-a} += 3` through the Recorder production
code holds, and asserts the collector decoded the name, the value 3, the label, the
resource's `service.name=volume-agent`, and the POST path `/v1/metrics`. **Three planted
bugs, each watched go red:** dropping the reader in `NewProvider` so the exporter is
ignored (`the collector received no lease_renewal_failures_total; it saw map[]`), dropping
the `Shutdown` flush (same line), and pointing the exporter at `http://127.0.0.1:1`
(`Shutdown: failed to upload metrics: … connect: connection refused`, returned in under a
second — which is also the retries-off decision proving itself).

**The handoff — track C owns `cmd/volume-agent/main.go`, so this lane did not wire it.**
Five lines, and they are exactly these:

```go
otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
    "OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
// …after signal.NotifyContext:
exporter, err := real.NewOTLPMetricExporter(ctx, *otlpEndpoint) // (nil, nil) when unset
if err != nil {
    return err
}
metrics, err := obs.NewProvider("volume-agent", exporter)
if err != nil {
    return err
}
defer func() {
    // context.WithoutCancel: the flush must outlive the SIGTERM that started the shutdown.
    // Logged, never fatal — a collector that is down must not change the Agent's exit code.
    if err := metrics.Shutdown(context.WithoutCancel(ctx)); err != nil {
        slog.Error("flushing metrics", "error", err)
    }
}()
```

then `Recorder: metrics.Recorder(),` in `agent.Deps` in place of `Recorder: nil` and its
three-line comment. `cmd/control-plane` is the same change with `"control-plane"` as the
name. What this lane could *not* make one line is the `defer`: a periodic reader holds up
to a minute of samples, so a process that exits without flushing exports nothing at all —
which is the same defect this item removed, in a different place. It is deliberately
`metrics.Recorder()` and not `obs.NewRecorder(metrics.Metrics)` so the wiring is one
expression.

`go.mod` grew `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp` (v1.39.0,
matching the pinned core) and, through it, `go.opentelemetry.io/proto/otlp` v1.9.0; MVS
pulled `golang.org/x/crypto` 0.43.0 → 0.49.0 and added `golang.org/x/net`. `task ci` green;
`task cover` 90.1% against the 90% floor.

**Still not proven end to end**, and it is the seam this repository loses defects at: no
lane starts the real Agent binary with `-otlp-endpoint` pointed at a receiver and asserts a
series arrives. That belongs in `integration/e2e`, which is track C's file set this wave.
Until it exists, "a metric leaves the process" is proven for the exporter and the provider,
not for the binary.

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
