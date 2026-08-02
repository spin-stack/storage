# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-08-01 · **Branch:** everything is on `main` — `guest-kernel-pinning`
  merged `--ff-only` at `283f1dd`, then `checkpoint-lease-checker` at `b5bd268`, each
  after a full `task ci:full`. `origin` (`/home/aledbf/spin-storage.git`, bare) holds
  everything through `4f6e125`; only `b5bd268` is unpushed. (An earlier revision of this
  line claimed `origin` was seventeen commits behind at `be84619`. It was not — the claim
  was written without checking, and `git ls-remote` disagrees with it.)
- **Gate:** `task ci` green (2026-08-01, with the keystone and its review-zone half in). It had been red since
  `e8bbdab` until DEV-0013 was resolved on 2026-07-28, which nothing had noticed
  because nobody had run it.
  Green *on a developer machine, and nowhere else*: `task cover` 90.3%
  (floor 90 — the margin is thin because increments 0 and 1 added binary wiring that unit
  tests do not reach), `task test:integration` green on PostgreSQL 18,
  `task backend:conformance` green against the pinned RustFS, `task build:qemu` +
  `task qemu:verify` + `task guest:verify` green — all of that is one machine's word.
  **CI has never run.** `origin` is a local bare repo, so the GitHub workflows have
  never executed on a runner. Treat every green claim here as reproducible-by-you, not
  as defended by a gate (`BUILD-INVENTORY.md`, increment 8). Two of the three reasons
  the guest lane could not run there are now gone (2026-07-28): the kernel is fetched
  and pinned rather than read out of a sibling checkout (**ADR-0022**), and
  `test:integration:qemu` skips loudly instead of hard-failing when `_output` has no
  QEMU — it used to `deps: [qemu:verify]`, which made `task test:integration`
  unrunnable anywhere QEMU had not been built by hand, contradicting the task's own
  description. **What remains is QEMU itself**, below.
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

**The queue is `BUILD-INVENTORY.md`, and it has eight increments.** It answers one
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
| 6 — the DEK arm | **done.** `dek_key_id` end to end (column + CHECK, proto, `metadata.Volume`, descriptor, `GetVolumeKeys`, provisioner, clone, rebuild-metadata) plus `-kek-file`/`-kek-id` on the Agent, `crypto.DevKMS`, and the unwrap at attach. Every object a served volume puts in the bucket is now ciphertext, and INV-15 is reachable — see below. |
| 7 — a guest that can issue FLUSH | **mostly done** (`e8bbdab`, `cef9881`) |
| 8 — the e2e lane and a gate that can notice regressions | **done**, minus the QEMU-in-CI decision. `integration/e2e` runs both binaries as processes in `ci:full` and in CI; CI builds them and the workflow now runs the lane. The guest (QEMU) lane still skips on a runner — see below. |

So: **the build order is done**, and what is left is the QEMU-in-CI choice. The
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
| 03 vhost-user | **3.1 integrated + served by the Agent** | A real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`task test:integration:qemu`). A **Linux** guest boots the lane too (`task build:guest`). Since the keystone the *Agent* binds a socket per volume and serves `blockdev.Device` behind it, so there is now a device to issue FLUSH against — but only local-mode FLUSH until the lease adapter lands (`RUNTIME-FENCING-SPEC.md`). 3.2 reconnection and 3.3 inflight-shmfd untouched; RISK-10 open. |
| 04 WAL/CoW format | **write path integrated**, rest model | A guest's WRITE lands as a replayable WAL record with **0 PUTs** (`internal/blockdev`); the WAL is a directory of segments so truncation reclaims (`WAL-SEGMENTS-SPEC.md`). FLUSH / uploader / checkpoint are model-only. Format review still pending (human-review zone). |
| 05 encryption (AES-256-GCM, DEK/KEK) | **model** | — |
| 06 remote WAL (batching, idempotent PUT, summary) | **model** | No guest has ever driven a PUT. |
| 07 Control Plane + leases + fencing | **model**, provisioning integrated | Fail-closed lease, resumable promotion, term guards. **A volume can now be created** (`controlplane.Provisioner`, `control-plane -seed-volume`): row + wrapped DEK + descriptor, verified against Postgres 18. |
| 08 recovery (S3 authority) + rebuild-metadata | **model** | Objects are validated before they count as durable; the rebuild includes the snapshot catalog. |
| 09 snapshots + clone + resize | **partial model** | Sealing is synchronous and no chain link is persisted (DEV-0007). |
| 10 objectization + checkpoints + GC + I/O classes | **partial model** | The GC marks reversibly; there are still no segment objects (DEV-0007). |
| 11 cross-host + cordon/drain + capacity | **partial model** | The drain is idempotent across crash boundaries; the materialized view is still not persisted (DEV-0007). |
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
non-ELF, and every source missing. Running the gate for it surfaced **DEV-0013** (below),
which had been red since the day before; that is fixed too, so `task ci:full` is green
end to end again.

## ~~DEV-0013~~ — `task lint` was red from `e8bbdab` to 2026-07-28 *(resolved)*

**The gate had not been green since the guest init landed on 2026-07-27**, and the claim
at the top of this file said it was. `integration/guestinit/main.go` tripped the INV-01
lint layer four times: `syscall` (depguard), `os.OpenFile` and `os.Open` (forbidigo), and
an unchecked `syscall.Pause()` (errcheck) — plus the authoritative analyzer, twice. It
surfaced on 2026-07-28 on the first `task ci` run since; nothing in the kernel increment
touches Go, so it was not its doing.

**INV-01 was never violated — the rule just did not say what it meant.** `guestinit` runs
as PID 1 *inside the guest VM*: it is on the far side of the interface INV-01 governs, it
is never linked into any binary this repository ships, and its purpose is to be the real
world `simio` models. A block-device open it could simulate would prove nothing about a
kernel deciding a write must be durable, which is the one thing no other test here
reaches. That is a different reason from `internal/vhost/hostio`'s (ADR-0020), which is
host code that *could* be simulated and deliberately is not — so it is recorded as its
own exemption rather than folded into that one.

**Fixed (human-approved) in both enforcement layers**, since either alone would leave the
gate red: `exemptPathFragments` in `hack/analyzers/simulable/simulable.go`, and the
`exclusions` in `.golangci.yml`. `syscall.Pause()`'s result is now explicitly discarded
in the source rather than excluded in config.

**The exemption is narrow, and there is a fixture that proves it.**
`TestExemptGuestInit` asserts the guest program is clean; `TestIntegrationItselfIsNotExempt`
asserts a host-side package under `integration/` is still flagged — because what earned
the exemption is *"runs inside the guest"*, not *"lives under `integration/`"*, and an
exemption that widened to the directory would quietly unsimulate the lane that drives
QEMU.

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

## Increment 8: the lane that runs the deployment, and the three things it found

`integration/e2e` (`task test:e2e`, in `ci:full` and in CI) starts the **real binaries**
as processes against a real Postgres 18 and the pinned RustFS: `control-plane` elected,
`volume-agent` heartbeating, a volume provisioned through the real provisioning path,
picked up, and served behind a socket. `internal/testinfra` grew what that needs — a
process supervisor that tees output to `t.Log`, waits on a *line the process printed*
rather than on a sleep, and can SIGKILL — plus a Postgres helper built from `schema.sql`.

It found three defects on its first three runs, and none of them was reachable from any
in-process test:

- **`control-plane -seed-volume` stole the term from the running Control Plane.**
  `AcquireLeadership` increments unconditionally, for the same holder id too — so
  provisioning a volume left the *serving* CP holding a stale term, every write refused
  as `ErrStaleTerm` until someone restarted it. Provisioning must not take down the
  Control Plane: seeding now borrows the current term (`GetLeader`) and fails if there is
  nobody to borrow from.
- **The Agent was never given its own host id.** `cmd/volume-agent` built its
  `VolumeManagerConfig` without `HostID`, and `checkpointsEnabled` refuses a scheduler
  that cannot name the host publishing (§12.3–12.4). Every volume on every real Agent
  would have grown its WAL for ever — increment 3's whole point, defeated by a missing
  field in `main`. The lane read the log line saying so.
- **The whole build-tagged surface was unlinted** — DEV-0016 above.

Two ordering facts are now encoded rather than folklore: a volume cannot be provisioned
for a host the catalog has never seen (the Agent's heartbeat creates the host row, and
`volumes.primary_host_id` is a foreign key), and `-s3-create-bucket` belongs to exactly
one process — every later one must find the bucket rather than invent it.

`TestBothBinariesAgreeOnTheKEK` exists because of the regression that shipped *inside*
increment 6: the two binaries had separate KEK readers with different rules — a
hex-encoded key file was a working Control Plane and a dead Agent — and the id was a flag
on one side and a hash of the material on the other. Both now go through
`crypto.LoadKEK`/`crypto.KEKID`, the Agent's `-kek-id` flag is gone (derived, never
configured), and the planted bug — the Agent naming its KEK `"kek-1"` — fails the lane.

## Increment 6: what the DEK arm actually needed

It was never "call `EnableEncryption`". Everything else existed and was tested —
`crypto.DEK`, `crypto.DevKMS`, `wal.NewEncryption`, the provisioner minting and wrapping
a DEK — and the Agent still could not build an encryptor, because **nothing remembered
the DEK's version**. `wal.NewEncryption` refuses `KeyID 0` (0 is the WAL's plaintext
marker), the catalog had no column for a version, and `GetVolumeKeysResponse`'s own
comment said the field was absent because there was no honest value to put in it. The
version now runs catalog → descriptor → wire → KMS, and `volumes.dek_key_id` carries a
`CHECK (> 0 AND <= 2^32-1)` in both stores.

That it is bound as **GCM additional authenticated data** is what makes the whole thing
verifiable rather than merely copied: a wrapped DEK paired with the wrong version does
not unwrap at all. Three tests lean on that — the four-boundary round trip, the clone,
and the descriptor property test.

**Three things this turned up that were not on the list.**

- **`Clone` copied the parent's wrapped DEK without its version.** A clone shares the
  parent's key (§19) and would have been unopenable; the failure would have surfaced on
  the clone's first WRITE. Fixed, and asserted — after the assertion was found to prove
  nothing, because the fixture's parent was at version 1 and so was the hardcoded value.
  The fixture is now at 42, and the planted bug fails it.
- **`rebuild-metadata` had the same hole** (§22.5), which is why the *descriptor* carries
  the version and not only the catalog.
- **The descriptor had no test of any kind** — see DEV-0015.

**Fail closed, and a mode that is honest about itself.** An Agent with a KMS serves an
encrypted volume or serves nothing: falling back to plaintext would put guest data in the
bucket under a name that says otherwise, and §15.3's crypto-shredding guarantee does not
survive that. An Agent started *without* `-kek-file` runs unencrypted — that is the
dev/local mode the DST harness and the QEMU lane use — and says so in a warning at
startup.

**INV-15 now has a checker that has seen an Agent.**
`scenarioEncryptedWALNoPlaintextLeak` drives `wal.Log` directly, so it could only ever
prove the WAL encrypts *when handed a key*; nothing handed it one. The new arm serves a
volume through the real `VolumeManager` with a real `DevKMS`, then reads every object out
of the bucket looking for the guest's pattern. Its planted bug is leaving `-kek-file`
off: one flag, a supported mode, still a violation for a real volume.

## ADR-0024 and the mechanism it first credited to the wrong thing

Increment 4's last item was a written decision: does a restarted Agent re-attach at the
same epoch, or must the epoch be bumped? **ADR-0024 decides same-epoch**, and it is a
fencing review zone, so it landed with `scenarioCrashedFlushDoesNotCollideOnRestart`.

The scenario builds the state that makes the question interesting: §14.4 uploads at step
4 and checks the lease at step 5, so a writer that loses its lease mid-FLUSH leaves the
bucket holding a *longer* contiguous prefix than the guest was ever told was durable.
Here: the guest was told 4, the bucket holds 8. Re-attach at the same epoch, write
something different over that range, flush — if the resumed writer had numbered from the
last ACK it would re-issue sequences the bucket already has under a different content
hash, INV-21 would hard-fail the PUT, and the volume could never flush again.

**The first draft of the ADR credited S3 for preventing that, and was wrong.** It said
`recovery.DurablePoint` → `InstallBase` resumes the writer above the bucket. True, and
not sufficient: a listing that comes back one object short would resume *below* objects
that exist. So the scenario was run against exactly that fault — and **it still passed**,
which is what identified the real mechanism. `Resume` sets `local` from the last record
**on disk**, `InstallBase` only ever raises, and INV-13 forbids truncating above
`published`, which never exceeds what the bucket proves. Everything the dead incarnation
uploaded is still in a local segment. S3 raises the floor; the local WAL is what stops it
being lowered.

Both listings now run, and the proof is a plausible regression rather than a hypothetical
one: making `InstallBase` *set* rather than raise fails the short-listing arm while the
honest arm still passes — which is the argument for the second arm existing.

Two things came out of it beyond the ADR: **DEV-0014** (below), and a real race in the
harness — `simListener.Close` guarded a channel close with a `select`/`default`, which is
not a guard, and paniced under `-race` the first time a scenario ran two managers in one
simulation. Now a `sync.Once`.

## The guest lane in CI: QEMU is the input that is still missing

The kernel is solved (ADR-0022) and `task test:integration` no longer dies where QEMU is
absent, so the remaining reason CI cannot run the guest lane is QEMU itself, and it is
**not** the same problem the kernel had. `qemu.yml` already publishes both a runtime
image and the extracted binaries, so obtaining them is easy; the difficulty is that the
binaries are dynamically linked against what the runtime image provides
(`libglib2.0-0`, `libpixman-1-0`, `libcap-ng0`, `libseccomp2`, `libaio1`, `liburing2`,
`zlib1g` — the `runtime` stage of `Dockerfile.qemu`). Extracting them onto a bare runner
and executing them is therefore not enough.

Two shapes, and the choice has not been made:

- **Run the lane's tests inside the published runtime image** (Go toolchain added to it).
  One definition of the dependency set, which stays in `Dockerfile.qemu` where it
  already is.
- **Install the runtime libraries on the runner** and use `_output` as today. Smaller
  change, but the list above then exists in two places and drifts silently — the failure
  being a QEMU that will not start, in a lane whose whole purpose is to tell us something
  else.

Nothing here is a blocker for the keystone: the lane runs on a developer machine, which
is where it has always run.

## The hole: truncation makes a restart serve zeros

`Log.view` — the read view a guest is answered from — is rebuilt **only from local
segments**, and `TruncateLocal` unlinks exactly those. `view` is unexported and assigned
in one place (`NewLogAfter`); there is no setter, so `recovery.Recover` and
`materialize.From*` produce precisely the right object and **nothing can install it**.

Today this is latent, because nothing calls `TruncateLocal` in a running system. It stops
being latent the moment a durability scheduler exists: **a restart would then silently
serve zeros for every truncated range, with no error anywhere.** None of the nine
`Resume` tests truncates first, which is why the suite is green.

**Closed 2026-08-01, Agent included.** What an Agent restart used to do, measured: write, FLUSH
(object verified in the store), restart the Agent, read the same offset → **zeros,
silently**. The object is in the store and the segment is on disk; the Agent looks at
neither. The next *write* then fails loudly — `wal` refuses a fresh log over a directory
that already holds unreplayed segments — so **nothing is overwritten and no committed
data is destroyed**, but the volume is unusable until someone resumes it, and the read
that came first was a silent lie.

A layered
`cow.IntervalMap` gives the read view a base, `wal.ResumeAwaitingBase` installs one
lazily, and a read with no base fails with `ErrBaseUnavailable` instead of answering
zeros. and the Agent now resumes rather than creating whenever the segment directory
already holds files, recovering the base in the background. A restarted volume reads back
what was flushed, and refuses to read at all when the base cannot be rebuilt. See
`VIEW-ADOPTION-SPEC.md`.

**Reproduced 2026-08-01**, and it behaves exactly as described: six segments, flush,
publish, truncate (five files unlinked), restart — and `Read` at offset 0 returns zeros
where `0xAB` was written, ACKed durable and verified in the store. No error, no degraded
flag, no log line. **`VIEW-ADOPTION-SPEC.md`** carries the reproduction and the four
decisions the fix needs; it is a durability *and* format review zone, so it waits for a
human. Note it did not reproduce on the first attempt: `reclaim` unlinks only *sealed*
segments, so a test must use a small `SegmentBytes` and assert a file actually
disappeared — which is how nine tests missed it.

**Consequence for the build order: the checkpoint/truncate increment must not merge
without the view-adoption increment** (`BUILD-INVENTORY.md`, increments 3 and 5).

## DEV-0007 — the spine's second half *(the only thing on the critical path)*

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
   **What is left is the other side of the socket:** attaching a `vhost-user-blk` device
   backed by `blockdev.Device` over `wal.Log` — which the keystone provides. The guest
   currently fails with `GUESTINIT-FAIL opening /dev/vda: no such file or directory`,
   which is the init working as designed.
2. ~~`wal.Log` has no mutex~~ **cleared 2026-07-26** (`7afff77`). `Log` grew its own
   lock rather than the Agent being declared its single owner: `Log` is what owns the
   invariants, so that is where the guard belongs. Two mutexes — `mu` for state, held
   only for local work and **never across an object-store PUT**, and `flushMu`
   serializing durable steps. The constraint on `mu` is load-bearing: holding it across
   the upload would put S3 latency in the guest's WRITE path (§5.3, INV-18) by the back
   door and blind the Agent's reporting for the length of an S3 stall — when the gap
   those accessors report is the RPO that is growing. Both halves are pinned by tests
   proven against that planted bug.

Also part of DEV-0007, and untouched: snapshot sealing is synchronous rather than a
background lifecycle, clone persists no parent/read-chain link, objectization publishes
no segment objects, and cross-host materialization returns a view the caller discards.

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

## DEV-0018 — the Linux guest lane never existed, and the guest hangs on its first I/O

**A stop signal, recorded rather than worked around.** Two findings, and the second is
only visible because of the first.

**1. Three documents claimed a lane that no test performed.** `STATUS.md` (twice) and
`BUILD-INVENTORY.md` said increment 7 was mostly done because "a Linux guest boots the
lane in ~1.1 s under TCG, reports a verdict and powers off". The artefacts are real —
`task build:guest` builds the initramfs, `task fetch:kernel` pins the kernel, and
`task guest:verify` asserts both — but **no Go file in the tree referenced either of
them**. Every test under `integration/vhost` boots a 512-byte boot sector under SeaBIOS,
which reaches the backend through INT 13h, and INT 13h has no flush verb. So the FLUSH
those tests observe is one the *test* issued, never one a guest asked for — which is the
exact gap `integration/guestinit` was written to close.

It was found by trying to give CI that lane (ADR-0025) and asking what it would run.

**2. With the lane written, a real kernel hangs on its first block request.**
`TestALinuxGuestIssuesFLUSH` boots the pinned kernel with `guestinit` as PID 1. The
kernel boots, `virtio_blk` registers the device and reports the right capacity — so the
vhost-user handshake, the memory tables and GET_CONFIG are all correct — and then it
stops. The last line is always:

```
virtio_blk virtio0: [vda] 32768 512-byte logical blocks (16.8 MB/16.0 MiB)
```

No partition scan, no `Freeing unused kernel memory`, no init output, no panic. It sits
there until the timeout.

**What is ruled out so far:** the initramfs is a valid static `/init`; `VIRTIO_RING_F_INDIRECT_DESC`
*is* offered; the call (interrupt) eventfd *is* signalled by `queueLoop.drain` when
`ProcessQueue` reports completions with `notify`. What is not ruled out is where the
difference between SeaBIOS and Linux actually lies — SeaBIOS **polls** the used ring and
never waits for an interrupt, so every passing test in this lane is blind to a completion
path a sleeping driver depends on. That is the first place to look.

**One real fix already landed on the way:** the initramfs is built by an unprivileged
`cpio` and therefore contains no device nodes, so the kernel could not open an initial
console and handed PID 1 **no stdio at all** — every line `guestinit` printed went to a
closed descriptor. It now mounts devtmpfs and opens `/dev/console` explicitly. That was
masking the hang as silence.

The test is committed and **skipped, with the reason as its skip message**. Skipped rather
than deleted because the test is not what is wrong; skipped rather than left red because a
red gate everyone knows about stops being a gate. Deleting it would put the tree back
where it was — a guest built, verified, and booted by nothing.

## ~~DEV-0017~~ — the Agent wrote its WAL one level below where it was told *(resolved 2026-08-02)*

Found while scoping the DEV-0014 lock to a directory, which forced the question of what
`VolumeManagerConfig.DataDir` is a path *relative to*.

`cmd/volume-agent` roots its `real.Disk` at `--data-dir` — that is what keeps the Agent
from writing outside it — and then passed the same absolute path as `DataDir`. Since
`DataDir` is a path inside the Disk's namespace, every name was resolved twice: the WAL
landed under **`<data-dir>/<data-dir>/wal/<volume-id>/<epoch>`**.

Not data loss, and not even inconsistent — a restart reproduces the same path and finds
its own segments. What it breaks is everything outside the process: an operator looking in
`--data-dir` finds nothing, and any tooling that inspects the WAL is looking at an empty
directory next to a `/tmp/...` tree nested inside it.

**Nothing in-process could have seen it.** Every unit test and the whole DST harness hand
the manager a Disk spanning a full filesystem, where the two paths agree and the bug
cancels out. It took the e2e lane, where the Disk is rooted the way production roots it.

Fixed by passing `DataDir: "."` from the binary, with the convention now stated on the
field. `TestTheAgentWritesWhereItWasTold` asserts the doubled directory does not exist,
using the lock file as its witness because it is created at start-up — a WAL directory
would only appear on the first guest append.

## ~~DEV-0016~~ — the entire build-tagged surface was never linted *(resolved 2026-08-02)*

`task lint` ran `golangci-lint run ./...` with **no build tags**, so golangci-lint never
parsed a single file under `integration/` or `internal/testinfra`. The lanes that drive
QEMU, Postgres and RustFS — and now the binaries — were invisible to the gate that is
supposed to check them. Found while writing the e2e lane, when its own files turned out
not to be linted either.

Worth being precise about what this did *not* mean: the DEV-0013 fixture proves the
`simulable` analyzer flags a host-side package under `integration/`, and that analyzer
runs over the whole tree (`lint:simulable`, no tags needed for its own traversal). What
was missing was golangci-lint's layer — forbidigo, depguard, staticcheck — on tagged
files.

**Fixed by running with `--build-tags integration,e2e`**, which then surfaced 9 real
findings, all of one shape: **build-tagged test harnesses drive the real world, which is
why they exist.** There is no clock to inject into another *process*, and a harness that
waited on a simulated one would measure nothing. INV-01 governs production code, and none
of this is linked into a shipped binary — every file carries a build tag.

The exemption is by path and **narrow, with the narrowness checked rather than asserted**:
it matches `integration/**/*_test.go` and `internal/testinfra/`, so an ordinary
(non-`_test.go`) file under `integration/` is still flagged — verified by planting a
`time.Now()` in one and watching forbidigo reject it. Unit tests everywhere else stay
governed, because a `time.Now()` there is precisely how simulable code gets bypassed.

One staticcheck finding was real and is excluded with its reason: `manager.Uploader` is
deprecated in favour of a package this SDK version does not have, and §6.1 needs a
multipart upload to prove an ETag is not a checksum.

## ~~DEV-0015~~ — the descriptor had no integrity check *(resolved 2026-08-02)*

Found while writing the property test increment 6 owed (`descriptor_property_test.go`).
Everything else that leaves the host is self-verifying: WAL records carry a CRC32C of
the *plaintext* plus a GCM tag (§14.1), objects are verified after upload (INV-07), and
key material is an AEAD ciphertext whose version is bound as additional authenticated
data. `volumes/<vol>/descriptor.json` is plain JSON with nothing over it.

Truncation is caught — the test proves it at every byte, because JSON without its
closing brace does not decode. **A flipped bit inside a number is not.** Change a digit
in `size_bytes` and the object still decodes, into a different, perfectly valid
descriptor; §22.5's rebuild-metadata would then recreate the volume at the wrong size.

The blast radius is smaller than it first looks, and worth writing down precisely:

- `dek_wrapped` and `dek_key_id` are **self-detecting** — corrupting either makes the
  unwrap fail (`ErrUnwrap`), which the property test asserts on both fields.
- `current_epoch` is not authoritative here; the epoch object is (§12.4).
- What is left exposed is `size_bytes`, `block_size` and `chain_depth`, and only on the
  rebuild path — a live volume never reads its own descriptor for those.

**Closed by `DESCRIPTOR-DIGEST-SPEC.md`.** The stored object is now
`<64 hex chars>\n<json>`: a SHA-256 over the bytes as stored, verified before anything is
decoded. Bare JSON — the old shape — is refused with the same error as a corrupt object,
because nothing is deployed and a lenient branch would leave the hole open permanently for
a volume that does not exist.

**The first design was wrong and the property test broke it on its first run**, which is
the part worth keeping. The spec said: a `digest` field inside the JSON, recomputed from
the decoded struct. Byte 2 of the object is the `v` of `"volume_id"`; flip one bit and it
reads `"Volume_id"`, Go's decoder **matches field names case-insensitively**, the struct
decodes identically, and re-marshalling reproduces the original digest exactly. The same
hole swallows unknown fields, duplicate keys, whitespace and numeric spellings — a hash
over a *re-encoding* sees only what the decoder did not normalise away. The digest has to
be over the bytes, and outside them.

## ~~DEV-0014~~ — two Agents could share one `--data-dir` *(resolved 2026-08-02)*

Found while writing **ADR-0024** (a restarted writer re-attaches at the same epoch). The
ADR is safe for the case it covers — the previous process is *gone* — and rests on four
mechanisms that all concern what is in S3. None of them touches the case where the
previous process is still alive.

Start a second `volume-agent` against the same `--data-dir` (an operator, a supervisor
restarting one that never actually died) and both incarnations resume the **same segment
directory** at the same epoch, both appending through `disk.Open` (read + append), both
numbering from the same resumed point. That is local corruption of the WAL, upstream of
every invariant that watches the bucket.

**Nothing detects it, and one thing actively hides it.** `hostio.Listen` unlinks a stale
socket before binding — right for the crash case, and it means the second incarnation
**silently steals the socket** instead of failing with `EADDRINUSE`. The first Agent keeps
its open fds and its log; the guest follows the socket to the second.

**It is not a consequence of ADR-0024 and predates it**: bumping the epoch would only have
helped if the second incarnation went through the Control Plane, which is exactly what a
stale supervisor restart does not do. This is **mutual exclusion on the data directory**,
not an epoch policy, and the fix is an exclusive lock taken at start-up — the one thing
that fails closed regardless of how the second process got there. It needs a lock
primitive in `simio/disk` (INV-01: a lock is a syscall), which is why it is recorded
rather than fixed in passing.

**Closed by `DATA-DIR-LOCK-SPEC.md`.** `disk.Disk` gained `Lock(name) (io.Closer, error)`
with `ErrLocked` — `unix.Flock(LOCK_EX|LOCK_NB)` in `real`, a set on the Disk in `sim`,
one contract test over both — and `NewVolumeManager` claims `<data-dir>/agent.lock` for
its lifetime.

The lock lives in the manager and not in `main` deliberately: the manager owns `DataDir`,
and a step left to `main` is a step spin's runner will not inherit when ADR-0021 lifts the
manager across — which is precisely how `HostID` went missing until an e2e lane read the
log line about it.

Non-blocking, so the second Agent exits with a message naming the directory instead of
hanging silently. No pid file and no liveness check: the kernel already answers "is that
process alive?", and every hand-rolled version has the read-pid/reuse-pid race. And
because the lock belongs to the open file description, a `kill -9` releases it — so
ADR-0024's re-attach still works on the very next start, which a lock needing explicit
release would have broken.

Two things fell out of it. The contract test caught the two implementations disagreeing
about a second `Close` (`*os.File` returns `ErrClosed`, the sim returned nil) — now
idempotent in both, because a contract answered differently by the two Disks is one
nothing can rely on. And **DEV-0017**, below.

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

## Decisions waiting on a human

- **ADR-0013 (device pressure) is still `Proposed`.** It carries DEV-0011 and the
  `SetLimits` the segment code has no way to receive today.
- **DEV-0012**, above.
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
