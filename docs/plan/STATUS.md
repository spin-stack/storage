# STATUS — what is true right now

**The single answer to "what is done, what is partial, what is missing."** If another
file disagrees with this one, this one is wrong and should be fixed — nothing else
tracks state.

- **Date:** 2026-07-28 · **Branch:** everything is on `main`, pushed to `origin`
  (`/home/aledbf/spin-storage.git`, bare).
- **Gate:** `task ci` green again on 2026-07-28 — it had been red since `e8bbdab`
  (DEV-0013, resolved below), which nothing had noticed because nobody had run it.
  Green *on a developer machine, and nowhere else*: `task cover` 90.6%
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
  done**; the next one is the keystone.

## Pick up here

The next increment is **BUILD-INVENTORY increment 2, the keystone**: a per-volume
runtime in `internal/agent/volume.go` owning `{wal.Log, blockdev.Device, vhost.Server}`,
started and stopped by diffing the desired state in `loop.go`'s `readDesiredState`
(which today assigns `l.desired` and is read by nothing but a test accessor).

**Write it as a self-contained type the loop *uses*, never as a method on the loop.**
That is ADR-0021: spin's runner has to be able to take the same type without the loop,
its heartbeat, or its Control Plane client. Same work, different shape — free now, a
refactor later.

Three things are ready and waiting for it, all verified by execution today:

- **A volume exists.** `control-plane -seed-volume -kek-file … -seed-host <uuidv7>`
  writes the row, the wrapped DEK and the descriptor, so `GetDesiredState` finally
  answers with something. Run against Postgres 18 and a filesystem store.
- **The Agent can reach an object store** (`-s3-bucket`/`-object-store-dir`), and it
  says why when it cannot.
- **A real Linux guest boots and can issue FLUSH.** `task build:guest` produces the
  initramfs; `task fetch:kernel` puts the pinned kernel at `_output/guest/vmlinux` and
  `task guest:verify` checks both. What is missing is only attaching a `vhost-user-blk`
  device backed by `blockdev.Device` over `wal.Log` — which is what the keystone
  provides.

A useful first command tonight: `task guest:verify` (it fetches the kernel and says so
if the artefact no longer matches the pin), then read
`internal/agent/loop.go:readDesiredState` and
`integration/vhost/wal_test.go:74-111` — the latter is roughly the first half of the
keystone, already written out by hand in a test.

## Maturity, not "done"

| State | Meaning |
|---|---|
| **model** | Library logic with unit/property/DST coverage. No integrated caller, no real I/O. |
| **integrated** | Wired into a running binary through the real interfaces, exercised end to end. |
| **production-verified** | Real hardware/backends under fault injection, telemetry recorded, runbook times measured. |

**One path is integrated; nothing is production-verified.** The spine exists — `api/`
over Connect, an Agent that pulls, two `cmd/` binaries — and a real QEMU 11.0.2 guest
boots off a device whose bytes come from a `wal.Log`, writing records through the same
interfaces production would use. That is the **write** half of one volume on one host.
Everything downstream — FLUSH's ACK path, the uploader, checkpoints, truncation — is
still exercised only by tests, and there is no deployment.

## Where each phase actually is

| Phase | State | What is true, and what is not |
|---|---|---|
| 01 skeleton (simio + DST + obs) | **model** | Simulable interfaces, the DST harness and the metric catalog all exist and are enforced by lint. Metrics are recorded by the paths that own them; wiring continues with each new path. |
| 02 guest layout (3 devices + OverlayFS) | **not started** | Needs guest mounts / a VM. Nothing in the durability chain depends on it. Spec below. |
| 03 vhost-user | **3.1 integrated** | A real QEMU 11.0.2 guest completes the handshake and does READ/WRITE through our virtqueue (`task test:integration:qemu`). A **Linux** guest now boots the lane too (`task build:guest`) and is ready to issue FLUSH — it just has no device to issue it against until the keystone lands. 3.2 reconnection and 3.3 inflight-shmfd untouched; RISK-10 open. |
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
with its backoff; a volume can be provisioned; and a Linux guest boots the lane. See
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

**Consequence for the build order: the checkpoint/truncate increment must not merge
without the view-adoption increment** (`BUILD-INVENTORY.md`, increments 3 and 5).

## DEV-0007 — the spine's second half *(the only thing on the critical path)*

ADR-0018's definition of done is one volume, one host, a real QEMU guest running
**write → FLUSH → verified object → checkpoint → truncate**. The write is done. The rest
has never been driven by a guest, and two blockers stand in front of it — both separable,
neither deep:

1. ~~No guest in the lane can emit a FLUSH~~ **cleared 2026-07-27** (`e8bbdab`,
   `cef9881`). A real Linux guest boots the lane in ~1.1 s under TCG, runs a static Go
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
