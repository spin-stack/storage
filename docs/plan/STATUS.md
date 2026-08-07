# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-08-06 · **Branch:** everything is on `main`, and `main` has not been pushed
  since wave 1. Run the two commands rather than reading a number here: `git ls-remote
  origin` (`/home/aledbf/spin-storage.git`, bare) for what the remote has, and
  `git rev-list --count origin/main..HEAD` for how far ahead this tree is. Both answers move
  under you while a parallel wave is running, because five lanes commit into this one
  working tree — which is why neither is written here. The wave that wrote them down had
  them stale before the wave ended.

  This line has been wrong twice, in opposite directions, and both times because it was
  written from memory. The rule this file needs is not "check before writing", which was
  already the rule — it is that **a claim about another system belongs next to the command
  that produced it**. Here that command is `git ls-remote origin`.
- **Gate:** `task ci:full` changed meaning in wave 4 and the claim has to be read with that
  in mind. It no longer skips the guest-backed proofs — it refuses to run without the pinned
  QEMU and the pinned kernel, and `ci:noguest` is the same lane list for a machine that has
  neither, ending by printing what it did **not** prove (ADR-0025, amended). So a green
  `ci:full` now means a guest booted. Run `task cover` for the coverage figure — it is not
  written here for the same reason the commit count is not. What is worth knowing about it
  is structural: since `f65b579` the floor compares the ratio rather than the one-decimal
  string `go tool cover` prints, which had been reporting OK for a tree at 89.9908%.
  While a wave is open the tree is not continuously green, and the usual reason is another
  lane's uncommitted `fmt:check`.
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
   `placement.Choose` and lands the clone on the host that took the snapshot;
6. be told **no** rather than fill the device: since wave 3 every Agent divides the
   filesystem behind `-data-dir` among `-max-volumes` and refuses to start without a share
   (`agent.Budget`), so a WRITE past the share is `wal.ErrBackpressure` — an error a guest
   understands — instead of an ENOSPC nothing planned for. `integration/e2e/budget_test.go`
   reads the budget off the Agent's own start-up line;
7. survive a stop that cannot reach the bucket: the Agent keeps its data directory locked,
   keeps the local WAL, retries, and does not exit 0 having lost the session
   (`SHUTDOWN-PUBLISH-SPEC.md`, reviewed; verified against a TCP proxy in front of RustFS).

**What is not there, in the order it matters:**

- **Nothing is deployed and CI has never run.** Every claim above is one machine's word.
- **A metric can leave the Agent, and no lane has watched one arrive.** Since `fa0c834`
  `cmd/volume-agent` takes `-otlp-endpoint`, builds the provider through
  `real.NewOTLPMetricExporter` *before* anything that records, and hands
  `telemetry.Recorder()` to the manager, which hands it to each `wal.Log` at the one place a
  Log is built (`Log.SetRecorder`, called from `internal/agent/volume.go`). Which catalog
  entries have a producer is a command, not a list — and this bullet has carried a wrong
  number twice:

  ```
  for n in $(sed -n 's/^\t\t{"\([a-z0-9_]*\)".*/\1/p' internal/obs/metrics.go); do
    grep -rl "\"$n\"" --include='*.go' . | grep -v _test.go | grep -v internal/obs/metrics.go |
      head -1 | sed "s|^|$n → |"
  done
  ```

  Read its output with one caveat that bit this recount: a metric name that is also a JSON
  tag matches its struct field, so `chain_depth` reports a "producer" in
  `internal/descriptor` and has none. **`cmd/control-plane` still records nothing** — no
  exporter, no Recorder, and `grep -n 'otlp\|Recorder' cmd/control-plane/main.go` returns
  nothing — and **no test starts a real binary and asserts a series arrives at a
  collector**, so "a metric leaves the process" is proven of the exporter and of the wiring,
  not of a deployment. The §26.2 catalog was trimmed to what exists on 2026-08-03
  (~~DEV-0022~~) and has grown since: wave 4 added the read-view trio, and `f65b579` gave
  them their producer on the durable step after they shipped with none.
- **A rebuilt catalog cannot tell you who was serving what.** `-rebuild-metadata` brings
  back volumes and snapshots from the bucket (INV-20, since 2026-08-03) but no placement,
  because no object records one. After losing the database you know what exists, not who
  was running it — and the answer to that is `-attach-volume` with no `-attach-host`, which
  asks `placement.Choose` instead of making an operator invent a host id per volume.
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
| 5 — a clone starts where its data already is | **done.** `controlplane.Clone` takes a `placement.Policy` and asks it (`internal/controlplane/clone.go:62`), and `control-plane -clone-snapshot` is the production caller (`cmd/control-plane/main.go:88`, calling `controlplane.Clone` below it). |

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
| 01 skeleton (simio + DST + obs) | **integrated** | Both binaries run on `simio/real` (clock, disk, network, object store); `internal/dst` holds a mandatory scenario set pinned **by name** in `pinnedMandatorySet` (`internal/dst/mandatory_set_test.go`), which is the only place its size is stated: it moved from 18 to 19 within twenty minutes of being written down here, so this row names the pin rather than a number. Telemetry got its caller on 2026-08-04: `-otlp-endpoint` → `real.NewOTLPMetricExporter` → the Recorder the manager hands to each `wal.Log` through `Log.SetRecorder`. Under half of `obs.Catalog()` has a producer — the command that says which is in "What is not there" above — and **no lane has observed a series arrive at a collector**. |
| 02 guest layout (3 devices + OverlayFS) | **not started** | `grep -ril 'erofs\|overlayfs' --include=*.go .` is empty. Nothing V1 does depends on it. Spec below. |
| 03 vhost-user | **3.1 integrated + served by the Agent** | A real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`integration/vhost/qemu_test.go:355`), and a **Linux** guest boots the lane (`integration/vhost/guest_test.go:43`). The *Agent* binds a socket per volume and serves `blockdev.Device` behind it (`internal/agent/volume.go:648,883`). A FLUSH is `fdatasync` and nothing else — the "full §14.4 remote path" this row used to claim went with ADR-0026 (`internal/wal/log.go:607`). 3.2 reconnection and 3.3 inflight-shmfd untouched: `INFLIGHT_SHMFD` is deliberately not advertised (`internal/vhost/features.go:93`), RISK-10 open. |
| 04 WAL/CoW format | **integrated** | A guest's WRITE lands as a replayable WAL record with **0 PUTs**, and so does its `fsync` — asserted with a real kernel and the real binary in the loop (`integration/e2e/guest_test.go:75`, INV-18). The WAL is a directory of segments (`internal/wal/segment.go`). What this row used to add — "the uploader and checkpoints are integrated too, and increment 3's scheduler is what finally reclaims a byte" — is withdrawn: nothing reclaims *mid-session*. **Something does reclaim, since wave 4**, and the row said otherwise until 2026-08-06: `wal.Log.InstallBase` unlinks the segments a restored image already holds, at attach, through the INV-13 rule — without it a volume started and stopped ten times on one host kept ten sessions of WAL under a bound nothing cleared. The exported `TruncateLocal`/`AdvancePublished` still have no production caller. Format review still pending (human-review zone). |
| 05 encryption (AES-256-GCM, DEK/KEK) | **integrated** | `-kek-file` on the Agent (`cmd/volume-agent/main.go:92`), the unwrap at attach, and every chunk a volume publishes is sealed (`internal/image/image.go`, INV-15). The *read* half was integrated and wrong until 2026-08-02 — see DEV-0019, and note that this row said "model / —" while a real defect lived in the path it declined to describe. |
| 06 remote WAL (batching, idempotent PUT, summary) | **withdrawn** — ADR-0026 | There is no remote WAL: no batcher, no uploader, no summary object (`grep -rn 'WriteSummary\|SummaryKey' --include=*.go .` is empty; `Batcher`/`Uploader` survive only as words in `integration/backend/edgecases_test.go`). What the guest lane proves now is the replacement — write, `fsync`, stop, and the image answers on a second boot (`TestAGuestSurvivesAStopAndComesBackFromItsImage`, `integration/vhost/lifecycle_test.go`). Idempotence survives as INV-21, structurally: a chunk key is its plaintext digest and an existing key is skipped. |
| 07 Control Plane + leases + fencing | **partial: provisioning, snapshots, clone and placement integrated; fencing is one CAS** | Term-guarded writes are in every mutating query (`grep -c control_plane_leader internal/db/queries/*.sql`). The lease is liveness only — granted and renewed by `Heartbeat` (`internal/cpserver/cpserver.go:62`), consulted by nothing on the data path. **Promotion and `FENCING_WAIT` are gone** (increment 4.6): the only fencing left is `image.Publish`'s compare-and-set plus the Agent tearing a refused runtime down (`internal/agent/volume.go:931`). |
| 08 recovery (S3 authority) + rebuild-metadata | **rebuild integrated; recovery withdrawn** | `internal/recovery` does not exist. The object store is the **boot** authority, not the recovery authority (INV-08): one manifest and its chunks. `controlplane.RebuildMetadata` + `control-plane -rebuild-metadata` (`cmd/control-plane/main.go:93`) bring volumes and snapshots back from two objects per volume — and deliberately not placement. |
| 09 snapshots + clone + resize | **snapshot and clone integrated; resize is out of V1** | A snapshot of a live volume is requested through desired state and published while the Agent serves (`integration/e2e/snapshot_test.go`); a clone of it is placed and boots (`integration/e2e/clone_test.go`). **Resize is gone, not pending** (2026-08-06, `446b61e`): `metadata.Store.ResizeVolume` grew a row and nothing else — a `Device`'s capacity is fixed by `blockdev.New`, `Apply` returns before reading the size, and the guest cannot be told at all, since a new capacity travels on a vhost-user backend request channel `internal/vhost` does not offer. What guards the decision is `metadatatest`'s `VolumeGeometryIsImmutable`, which reads a volume's size back after every mutation. |
| 10 objectization + checkpoints + GC + I/O classes | **withdrawn** — ADR-0026 | All four subjects are deleted: `internal/gc`, `internal/checkpoint`, `internal/ioclass`, and the segment-object plan (`OBJECTIZATION-SPEC.md`, removed 2026-08-03). What survives of GC is enforced at construction — `real.NewS3Store` refuses an unversioned bucket (`TestRequireVersioning`, `internal/simio/real/s3_versioning_test.go:36`) — and it is all that INV-14 has left. |
| 11 cross-host + cordon/drain + capacity | **cordon and capacity integrated; drain withdrawn** | `controlplane/drain.go` is deleted and nothing calls a drain; `HostDraining` survives as a lifecycle state (`lifecycle.HostDraining`, in `internal/lifecycle`) with no producer. What is real: a host cordons itself out of the fleet when its device fills, through the heartbeat (`cpserver.CordonUsedRatio`/`UncordonUsedRatio`, a Schmitt band rather than ADR-0013's single threshold, in `internal/cpserver/pressure.go`), and admission counts what a host is *using* rather than only what it was promised (`internal/placement`). Wave 4 retired the `operations` table and with it ADR-0017's second capacity term, which summed rows nothing ever wrote — `internal/schema/schema.sql` opens by saying there is no such table and why. Cross-host movement of a volume does not exist. |
| 12 warm standby + compaction + flatten | **not started, and V2 under ADR-0026** | Nothing in the tree; ADR-0014 (its quota/squash half) is itself withdrawn. Listed because §2 still names it as a target for a later version. |
| 13 hardening | **13.1 integrated (typed lifecycles); the rest needs infra** | `internal/lifecycle` is the typed state machine (ADR-0009) and the Control Plane uses it. 13.2 (real-hardware fault injection + measured runbooks) and 13.3 (backend conformance per version) need hardware nobody has; 13.4 is **INV-19**, which stays pending on purpose until two Agents can run different formats. |

**Invariants — `INVARIANTS.md`'s State column owns this, and the tally is a command:**

```
grep -oE '^\| \*\*INV-[0-9]+\*\* \| \*\*[a-z]+' docs/plan/INVARIANTS.md | sed 's/.*\*\*//' | sort | uniq -c
```

This line has carried a written total twice and been wrong both times — "21 of 22 active"
survived ADR-0026 retiring six mechanisms, and "14 active, 6 withdrawn, 2 pending" survived
INV-13 coming back on 2026-08-06 when `wal.Log.InstallBase` put a production caller behind
its rule. The two `pending` are **INV-14** (GC — its subject is deleted, so it cannot fire;
`DELETION-AND-RECLAIM-SPEC.md` is the shape it would return in) and **INV-19** (format
read-old/write-new, not binding until two Agents can run different versions). Every *active*
checker has been shown to catch a planted bug, and `internal/dst`'s `TestMain` enforces that
claim rather than recording it.

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
`scenarioLapsedLeaseStopsPublishing` now watch it, and `plantedProofs` grew a behavioural
entry. *(That checker and its scenario went with the durability scheduler in ADR-0026
increment 4.1; `wantBehavioural` in `internal/dst/planted_bug_test.go` is what counts them
now, and its comment carries every decrease with the reason. This paragraph is the record
of an increment, not a description of the tree — which is why the number it used to state
here was wrong within days and is gone.)*

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

**Decided 2026-08-02 — ADR-0025, amended 2026-08-05.** The lane runs inside the published
QEMU runtime image as a container job, rather than installing its dynamic dependencies on a
bare runner, because that keeps one definition of the dependency set in `Dockerfile.qemu`.
That half is unchanged.

**The skip is gone, and this section described it for a wave after it went.** The preflight
is `guest-inputs` (not `guest-lane-image`), and when either artefact — the QEMU runtime
image or the mirrored kernel — is unpublished it **exits 1**, writing the command that
publishes each into the run summary. `guest-lane` and `guest-e2e-lane` run with
`REQUIRE_PROOFS=1`; the `ci` job runs `ci:noguest`, which is what a runner with Docker and
no QEMU can honestly claim; and `gate` is the job to require on the branch, because a
*skipped* needed job leaves its dependents free to run and a workflow of green-and-grey
boxes reports as a pass. The reasoning is in ADR-0025's banner: trained-to-ignore-red is a
real cost, believed-to-be-proven is the larger one, and it is the one this repository has
already paid (DEV-0018).

**So the gate is red until two artefacts exist, deliberately** — the runtime image
(`qemu.yml` publishes it; it has `workflow_dispatch`) and the mirrored guest kernel (a human
with a spinbox checkout has to run `task fetch:kernel && task guest:kernel:push`). Track B's
log has both commands.

What is still true is narrower, and it is the same caveat as everywhere else on this
page: **no CI workflow has ever executed**, because `origin` is a local bare repo. The
jobs are written and reviewed, not observed.

## ~~DEV-0007~~ — the spine's second half *(the chain closed 2026-08-02; its subject was withdrawn 2026-08-03)*

> **Read this whole section in the past tense.** ADR-0026 removed the chain it closes —
> checkpoints, truncation, the drain, the epoch/promotion path and §14.4's six-step ACK —
> so every "is" below was true on 2026-08-02 and is not now. Two renamings, so the names
> here resolve: `TestAGuestSurvivesCheckpointAndTruncation` is now
> `TestAGuestSurvivesAStopAndComesBackFromItsImage` (`integration/vhost/lifecycle_test.go`),
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

## DEV-0023 — the design document still has resize, and V1 does not

Wave 5 deleted `metadata.ResizeVolume` and everything under it: V1 does not grow a volume,
because a row that grows is not a volume that grows and nothing on the Agent side ever
acted on a size change. The reasoning and the guard test are in `tracks/TRACK-D.md`, and
`metadata.go` carries the note where the verb used to be.

`arquitectura_mvp_volumenes_remotos_v5.md` still promises it, in five places and in the
present tense: §1's objective bullet (*resize online (grow)*), §7's client-facing operation
list, the `size_bytes` column comment (*mutable: resize grow*), the operation-kind enum, and
§9's propagation sentence (config space + notification + `resize2fs` in the guest). None
carries a marker.

**It is a DEV entry and not a struck-through decision, which is what the deleting lane
recorded.** CLAUDE.md is explicit: an observed doc↔code divergence is a DEV entry in this
file, and an open one blocks the gate. The lane's judgement — that the document describes
the product rather than V1's scope — is reasonable and is exactly the judgement the DEV
mechanism exists to make visible rather than resolve silently.

**Two ways to close it**, and it is track A's to do either way: mark those five places the
way §17, §21, §23 and §31 were marked when ADR-0026 withdrew them — a banner naming what is
V2 — or, if resize is meant to be V1, reopen it as an increment with the Agent half that
was always missing. What is not an option is leaving a document promising a verb whose
implementation was deleted this week.

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
  what `Loop` renews. The window this ADR bounded was *empty*; since wave 4 it is
  **deleted** — `BlockHostRenewals`, `UnblockHostRenewals`, `RevokeHostLease`,
  `metadata.ErrRenewalsBlocked` and the `hosts.renewals_blocked_until` column are all gone,
  and `internal/schema/schema.sql` carries the reasoning where the column stood. The ADR's
  own banner says so.

**And one more thing with no caller went with them.** `wal.Log.BasePending` existed so the
durability scheduler could not act on a *false* ADR-0023 witness — a resumed log reports
`durable = 0` until its base lands, and a checkpoint in that window would conclude another
writer held the epoch and fence a healthy host on every restart. With no mid-session
publication there is no such window, and the method had only its own test.

**ADR-0013 is still `Proposed` and is the most-cited ADR in the tree.** Which ADR leads and
by how much is a command, not a sentence here — it has been rewritten twice with a number
that was wrong within a wave:

```
grep -rhoE 'ADR-[0-9]{4}' --include='*.go' . | sort | uniq -c | sort -rn | head
```

It carries DEV-0011 (a segment's space charged as used rather than reserved), ADR-0014's
amendment leans on it for the hard limit, and since wave 3 an Agent's whole write-path bound
is derived from it (`agent.Budget`, `cmd/volume-agent`'s `-max-volumes`). It is a decision
waiting on a human, not a divergence.

## Components with no production caller

CLAUDE.md's rule is that a component with no caller is a liability rather than progress,
and `CloneCrossHost` was deleted for exactly it. **This section no longer answers "which
ones". `task deadcode` does, it runs inside `task ci`, and the reason for every symbol it
reports is on the line beside it** in one of two files:

- `hack/deadcode-allow.txt` — "unreachable, and that is correct forever": a test-only
  affordance, a reference model, a fake front-end's encoder.
- `hack/deadcode-pending.txt` — "unreachable, and that is a deletion nobody has finished".
  A ratchet over a **set**, not a count: a finding in neither file fails, and an entry the
  tool stops reporting also fails, so the line leaves in the same commit as the code.

**The hand-written list that stood here is deleted rather than corrected, and how it failed
is the argument.** Recounted 2026-08-06 it was wrong in both directions at once: it said
`published_sequence` "is permanently 0 and nothing reclaims a segment", which stopped being
true when `wal.Log.InstallBase` landed; it still listed `metadata.Store.ResizeVolume`, which
track D deleted; and it had never grown the ten entries the pending list now carries. A list
a human maintains about code a human is changing reports success by not being updated — the
same failure mode as a checker that cannot fire, one document over. **Do not add an entry
here that `task deadcode` can see.** There the reason is checked against a finding; here it
is checked by whoever happens to reread this paragraph.

**What the task cannot see is the only thing this section is still for.**
`hack/deadcode.sh`'s header names both blind spots, and an entry belongs here **only** if it
falls in one of them:

- **Reached through reflection**, so RTA marks it live and it is never reported:
  `wal.TruncateLocal` and `wal.AdvancePublished`. Neither exported method has a production
  caller, and that has not changed. **What changed in wave 4 is that the rules they carry
  now run in production.** `wal.Log.InstallBase`, called by the Agent's `fetchBase`, raises
  `published` to the sequence the recovered image covers and reclaims the segments below it
  through the same `truncateLocalLocked` and the same `StrictOrder.AllowTruncate` (INV-13)
  the exported method uses. So `published_sequence` is no longer permanently zero, and a
  volume that has been started and stopped ten times on one host no longer keeps ten
  sessions of WAL. The exported pair is kept because *mid-session* reclamation returns with
  any long-lived volume.
- **Not in the analysed program at all**: `metadata.BumpVolumeEpoch`. `metadata/sim` is a
  package no binary imports (the allowlist's coarse pass names the package, not the symbol),
  and `metadata/pg` is linked but its store methods are not built into the analysed program
  — `deadcode -whylive '…/internal/metadata/pg.(*Store).BumpVolumeEpoch' ./cmd/...` answers
  `not found in program`. It is the store's compare-and-set on the epoch, kept because
  ADR-0024 says it is what would grant one if promotion returns.
- **`wal.Log.Broken()`**, whose own doc comment says so. It is read by the DST harness and a
  concurrency test as proof of a transition — the same deliberate exception `Log.Fenced()`
  was before the rename — and the task does not report it either.

The entry this section held for `metadata.Store.ResizeVolume` is gone because the method is:
see "Decisions waiting on a human", where the question it was waiting on was answered.

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

- **ADR-0013 (device pressure) is still `Proposed`.** It carries DEV-0011. Note the gap
  this leaves: it is the most-cited ADR in the tree (the command is in "The decisions stop
  describing deleted machinery" above — do not trust a number written here), its WAL
  segment format has landed, a host **cordons itself** on the device measurement it defines
  (`cpserver.CordonUsedRatio`/`UncordonUsedRatio`, `internal/cpserver/pressure.go`), and
  since wave 3 an Agent that cannot derive a budget from it **refuses to start**
  (`agent.Budget`, `NewVolumeManager`'s `Budget.Share() <= 0` guard). Every one of those
  treats it as decided. Either the review happened and the ADR should say so, or it did not.
- **The review-zone specs have no commit that precedes their implementation.** Five
  increments in a human-review zone (fencing, keys/format, on-S3 format ×3) have their
  `*-SPEC.md` landing in the same commit as the code it was supposed to gate. That is not
  proof the review did not happen — a commit records when a file entered the tree, not
  when a person read it — but the evidence CLAUDE.md's review-zone rule asks for is not
  in git, and only a human can say which it was. **Wave 4 stopped adding to the pile:**
  `DELETION-AND-RECLAIM-SPEC.md` (`b132161`) and `CHUNK-ADDRESSING-SPEC.md` (`1586c32`) are
  each a commit that changes one file and no code. Their questions are two bullets below.
- ~~**ADR-0023 shipped without a DST scenario**~~ **— moot since 2026-08-03, and left
  standing for one line of it.** The checkpoint it governed is deleted, and
  `grep -rn 'ADR-0023' internal/dst/` now returns **nothing**; the unit test this entry
  named as the only proof (`internal/agent/durability_internal_test.go`) went with the
  scheduler. What the ADR was amended to say is that the object store is still a fencing
  witness the data path acts on — the manifest's compare-and-set at stop — and *that* has a
  mandatory arm, `two-hosts-cannot-both-publish-an-image`. Nothing is waiting on a human
  here any more; it stays as a `~~closed~~` line because "an ADR whose DST arm was never
  written" is the shape worth being able to find again.
- **ADR-0016 has lost both its stages, and the entry that stood here named a mechanism that
  no longer exists.** It said stage 1 "survives and is real — the bounded revocation window,
  `RenewalsBlockedUntil`, term-guarded"; wave 4 deleted all of it, because three writers
  with no caller, a column that could only ever be NULL and a predicate that could only ever
  be true are indistinguishable from a broken writer to the next reader. Stage 2 changed
  what `wal.Log` consults before ACKing a FLUSH, and a FLUSH consults nothing but
  `fdatasync`. **What a human still owns is one sentence:** close ADR-0016 at "the lease is
  per host and is liveness only", or keep stage 2 open against the durability tier ADR-0026
  says would reverse it. The schema carries the reasoning where the column was
  (`internal/schema/schema.sql`, the `hosts` table's comment).
- **DEV-0020**, above: whether a clone chain is walked at read time or flattened at clone
  time. **Not subsumed by ADR-0026 — sharpened by it.** The code answers "flattened, at
  every publish, by copying the parent's whole dataset under the clone's prefix", nobody
  decided that, and `PARALLEL-PLAN.md`'s C11 wants the copy gone. Removing it without a
  chain read turns a storage cost back into silent zeros, so the two are one decision, and
  it is an on-S3 format decision. **It now has its spec**, and the question is put in that
  spec's terms below.
- **Is `chain_depth` a structure or a label?** — `CHUNK-ADDRESSING-SPEC.md` §8, written in
  wave 4 and unreviewed. Today it is neither: `controlplane.Clone` increments it, nothing
  reads it, no ceiling refuses on it, and the flattening at `image.uploadChunks`'s
  `view.Ranges()` loop makes it describe a lineage rather than a read path. *Label* means
  keep flattening and pay a duplicate per link forever; *structure* means a chunk-key format
  change, a chain walk in the attach path, and a deletion design that counts references
  across prefixes. Every option in that spec is downstream of the answer, and it is an
  on-S3 format decision.
- **Must a clone be independent of its parent before the parent can be deleted, or should
  the delete make it independent?** — `DELETION-AND-RECLAIM-SPEC.md` §9, written in wave 4
  and unreviewed. This is the first thing in the repository that would remove an object.
  **The spec's own recommendation has a hole it asks the reviewer to close rather than
  discover:** refusing a delete while `parent_snapshot_id` is set is only a *temporary*
  refusal if something can clear that column, and nothing can — the upsert is
  `parent_snapshot_id = COALESCE(volumes.parent_snapshot_id, EXCLUDED.parent_snapshot_id)`
  and a rebuild reads the link back out of the descriptor. So the second decision is: clear
  the link when a clone's first publish makes it self-contained, or make the precondition
  "descends from this **and** has no image of its own". Free now, a migration later.
- ~~**Does V1 offer resize, or is §3's grow-only rule a promise with no verb behind it?**~~
  **Answered 2026-08-06 (`446b61e`): V1 does not resize a volume, and `ResizeVolume` is
  deleted.** It is recorded here rather than dropped because the *shape* is worth finding
  again: a store method that was term-guarded, grow-only and correct, and whose whole path
  did not exist — the desired state already carries `size_bytes` to every Agent, `Apply`
  returns at its epoch check before reading it, a `Device`'s capacity is fixed by
  `blockdev.New`, and a new capacity would travel as `VHOST_USER_BACKEND_CONFIG_CHANGE_MSG`
  on a backend request channel `internal/vhost` does not offer. The thirty correct lines
  were not neutral either: `descriptor.json` carries the same size and is written only at
  create and clone, so a resize that landed in the catalog and not in the bucket was the one
  way to make the two disagree about a volume's size with nothing to notice. What replaces
  it is a property both stores are held to (`metadatatest`'s `VolumeGeometryIsImmutable`),
  so a resize brought back as a store method *and nothing else* fails there.
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

## The tracks' running logs

Each track's increment-by-increment record lives in its own file, one per lane. They were
carved out of this file on 2026-08-04: five lanes appending to one region collided in every
wave, and in wave 3 a lane committed a stale copy and deleted 121 lines of another lane's
entries ten seconds after they landed. Nothing was permanently lost, and the only reason is
that the other lane happened to look again — which is a human-shaped control, i.e. not one.

| Track | Log | Owns |
|---|---|---|
| A — the documents | [`tracks/TRACK-A.md`](tracks/TRACK-A.md) | the architecture document, this file, `REFERENCE.md`, `RISKS.md`, `INVARIANTS.md` |
| B — the gate runs | [`tracks/TRACK-B.md`](tracks/TRACK-B.md) | `.github/workflows/`, `Taskfile.yml`, `hack/`, `internal/testinfra`, `integration/guestinit` |
| C — the agent data path | [`tracks/TRACK-C.md`](tracks/TRACK-C.md) | `internal/agent`, `internal/wal`, `cmd/volume-agent`, `integration/e2e`, `integration/vhost/{lifecycle,wal}_test.go` |
| D — the catalog | [`tracks/TRACK-D.md`](tracks/TRACK-D.md) | `internal/controlplane`, `internal/cpserver`, `internal/metadata/**`, `internal/db`, `internal/schema`, `internal/lifecycle`, `internal/placement`, `api/`, `cmd/control-plane` |
| E — observability | [`tracks/TRACK-E.md`](tracks/TRACK-E.md) | `internal/obs`, `internal/simio/real`, `internal/vhost`, `internal/blockdev`, `internal/cow`, `go.mod` |

The full ownership map and the merge protocol are in `PARALLEL-PLAN.md`.

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
