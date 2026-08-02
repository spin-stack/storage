# BUILD INVENTORY — one volume, one host, real binaries

> Produced 2026-07-26 by an eleven-agent audit: five surveyors (Agent data path, API
> surface, durability chain, binaries/ops, verification lanes), each checked by an
> adversary that had to re-find every claimed gap in the code before confirming it, then
> one synthesis. It exists because `STATUS.md` could say "one path is integrated" and
> still leave the question "is anything actually finished?" unanswerable.
>
> **The target slice** is the definition of done: `cmd/volume-agent` receives a volume via
> `GetDesiredState`, fetches its DEK via `GetVolumeKeys`, serves a vhost-user-blk socket
> backed by `wal.Log`; a real guest writes; FLUSH uploads a verified object to a real
> object store; a checkpoint publishes and the local WAL truncates; kill the agent,
> restart, it recovers from S3, and the guest reads back the same bytes.
>
> Increments are checked off here as they land. `STATUS.md` stays the state of the
> repository as a whole; this is the state of one road through it.

## What is actually solid (do not rebuild)

- **wal**: `Log.Flush`/`durableStep` is the full §14.4 ACK path; two-mutex design, `mu` never held across a PUT; `Resume` handles the torn tail; `ErrDirtyLog` fails closed. `Uploader.Upload` is create-only + If-None-Match with 412 reconciled by byte comparison, and it is **proven against real RustFS** (`integration/backend/s3store_test.go:190`).
- **checkpoint / recovery / materialize / epoch / gc / snapshot**: complete and unit+DST tested. `Checkpointer.Create` publishes only what S3 can prove; `recovery.DurablePoint`/`Recover`/`VerifyPublisher` are done.
- **vhost + blockdev**: real handshake proven message-by-message against real QEMU 11.0.2; a real guest's WRITE already becomes a WAL record with **0 PUTs, counted not inferred** (`integration/vhost/wal_test.go`). `hostio.Listen`/`NewMapper`/`NewEventFD` are real.
- **agent.Loop**: heartbeat/backoff/lease-anchored-to-send-instant is correct and running in the binary. `Deps.Volumes` is already the right seam. `Loop.VolumeKeys` already fetches and caches the wrapped DEK.
- **cpserver + metadata (pg & sim) + schema**: term guards, uuidv7 CHECKs, FK-index rule, EXPLAIN assertions — all real against Postgres 18. Election through `controlplane.Elector` works end to end (verified: term 1 + term-claim object).
- **DST**: 35 scenarios, each checker proven against a planted bug.

The libraries are not the problem. **Every single gap below is assembly, configuration, or one missing seam.**

---

## THE KEYSTONE

**A per-volume runtime in `internal/agent`** (Increment 2). `internal/agent` contains only `agent.go` and `loop.go` and names *no* data-path package. `readDesiredState` assigns `l.desired` and nothing ever reads it. Until this type exists, every other item on this list is inert — including the ones already built. It unblocks: serving, FLUSH, checkpoint, truncate, restart, fencing, honest reporting.

---

## ~~Increment 0 — Make the binaries runnable and debuggable~~ **DONE 2026-07-26** (`5bf31d4`)

> All seven items landed. Two notes for whoever reads the table below: bucket creation
> is behind `-s3-create-bucket`, **off by default**, because silent creation on a
> typo'd name invents an empty deployment and reports success; and the versioning
> check still fails closed on every bucket it cannot verify — the audit called that
> classification wrong and it is not, `TestRequireVersioning` pins it on purpose. Only
> the error's wording changed.

| Piece | Where | Why the slice fails without it | Size | Review zone |
|---|---|---|---|---|
| S3 credentials | `cmd/control-plane/main.go` `openObjectStore` + a shared helper | `S3Config.AccessKey/SecretKey` are never set and `s3.New` does **not** resolve the AWS default chain — requests go out **unsigned** (verified by wire capture, with `AWS_*` exported). `-s3-bucket` is decorative. | S | no |
| Bucket create + versioning bootstrap | same helper / `internal/testinfra` | `real.NewS3Store` fails closed on an unversioned bucket; only test helpers ever call `CreateBucket`/`PutBucketVersioning`. First run cannot get past the constructor. | S | no |
| Fix `requireVersioning` error wrapping | `internal/simio/real/s3.go:77-83` | It reports *any* error (incl. the 403 you'll actually get) as "bucket versioning is not Enabled" — sends the operator to the wrong knob. | S | no |
| Object-store flags + `Deps.Store` on the Agent | `cmd/volume-agent/main.go`, `internal/agent/loop.go` | No store ⇒ no uploader, no checkpoint, no recovery. Note: `simio/objectstore` and `simio/real` are **already linked** into the binary — this is flags + one field, ~40 lines lifted from `cmd/control-plane`. | S | no |
| `-host-id` must be a UUIDv7 | `cmd/volume-agent/main.go` (use `internal/ids`) | `hosts.host_id` is the `uuidv7` domain + `pg.requireUUID`. `-host-id host-1` produces zero rows — and `Loop.Run` swallows the error, so it retries silently forever. | S | no |
| Log `Reconcile`'s error | `internal/agent/loop.go:95-99` | The error goes straight into `nextDelay` and is dropped. A permanently broken Agent logs one startup line and nothing else, ever. You cannot debug increments 1–8 without this. | S | no |
| `syscall.SIGTERM` in the CP's `NotifyContext` | `cmd/control-plane/main.go:76` | One line; `-shutdown-grace` is currently dead code under any supervisor. | S | no |

**Demo:** agent + CP + RustFS, credentials work, a misconfigured agent says why.

---

## ~~Increment 1 — A volume can exist~~ **DONE 2026-07-27** (`5953c73`)

> `controlplane.Provisioner` + `control-plane -seed-volume`. Verified against Postgres 18
> and a filesystem store: row, wrapped DEK that unwraps with its KEK, descriptor. Epoch
> starts at 1, and the epoch object is **not** written — exactly as the table below warns.

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| Volume provisioning: `GenerateDEK` + `WrapDEK` (KEK file flag on the CP), `descriptor.Write`, `metadata.CreateVolume` with `primary_host_id`, term-guarded — invoked by a `seed-volume` subcommand on `cmd/control-plane` | `cmd/control-plane`, new `internal/controlplane/provision.go` | Four RPCs exist, none creates anything. `crypto.NewDevKMS`, `GenerateDEK`, `WrapDEK`, `descriptor.Write` have **zero** production callers. Today `GetDesiredState` returns empty forever. `descriptor.json` is also the anchor for `RebuildMetadata` and `gc.Reachable`. | M | yes |
| Validate `size_bytes % 512 == 0` at creation | `provision.go` (+ ideally a CHECK in `schema.sql`) | `blockdev.New` refuses a non-sector-multiple capacity; the CP will happily create a volume no Agent can ever serve, failing on the host at attach. | S | no |

**Do NOT call `epoch.Store.Init` here.** `recovery.VerifyPublisher` returns nil when the epoch object is absent (deliberate); but an object Init'd at 0 while Postgres says `current_epoch = 1` makes **every checkpoint fail** with `ErrEpochChanged`. Explicitly out of scope. (Scoped-out, not needed for one host: admin API / `volctl`, `VolumeOperation` on `DesiredVolume`, detach + `ListVolumesByHost` state filter, snapshot/checkpoint completion RPCs, mTLS.)

**Demo:** `GetDesiredState` returns one volume; `GetVolumeKeys` returns a real wrapped DEK.

---

## Increment 2 — KEYSTONE: the per-volume runtime (2–3 days)

New file `internal/agent/volume.go`. One type owning `{root, epoch, *wal.Log, *blockdev.Device, vhost.Listener, *vhost.Server, serve goroutine, cancel}` with `Start`/`Stop`/`Status()`.

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| The runtime type + `Start`/`Stop` | `internal/agent/volume.go` | Nothing holds anything per volume. This is assembly — every callee exists and is tested; `integration/vhost/wal_test.go:74-111` is roughly the first half of it, written out. | L | no |
| Diff the desired state in `readDesiredState` → start/stop runtimes | `internal/agent/loop.go:181-193` | The slice's first step. `l.desired` is read by nothing but a test accessor. | M | no |
| `Deps.Disk` + WAL root convention `<data-dir>/wal/<volume-id>/<epoch>` | `loop.go`, `cmd/volume-agent/main.go` | The real `disk.Disk` exists in the binary but is buried inside `DiskUsage`; `wal.NewLog`'s first two args have no source. | S | no |
| `wal.LeaseChecker` adapter over `Loop.LeaseValid()` | `internal/agent/loop.go` | `LeaseChecker` needs `Valid() bool`; `Loop` has `LeaseValid()`. **Do not** pass `Loop`'s `*lease.Manager` directly — `applyLease` allocates a *new* manager whenever the CP's TTL changes, so the Log would be gated by a manager nobody renews. And `EnableRemote` accepts a nil lease silently; the failure only appears later as `ErrNoLease` in `durableStep`. | S | **yes (fencing)** |
| `-vhost-socket-dir` flag; `hostio.Listen(<dir>/<vol>.sock)` + `vhost.NewServer` + `go Serve` + a supervisor + unlink on detach | `volume.go`, `main.go` | Nothing outside `integration/vhost/qemu_test.go` ever listens. `Serve` loops back to Accept after a clean disconnect but **returns on any session error** — a supervisor is genuinely needed. One server per volume by construction. | M | no |
| `VolumeSource` over live runtimes (`Log.Watermarks`, `RemoteGapBytes`), replacing `agent.NewVolumeSet()` | `volume.go`, `main.go:88` | The binary reports an empty set forever: every heartbeat says `remote_backlog=0`, every `ReportVolumeState` carries zero reports. | S | no |
| `Fenced()` → tear the runtime down | `loop.go` + `volume.go` | `report()` computes the fenced list and its own comment says the SELF_FENCED transition "belongs to the data path". Resolves DEV-0012. | M | **yes (fencing)** |
| Decide `blockdev.Device.mu` held across the S3 PUT | `internal/blockdev/blockdev.go:33-37,114` | Its justifying comment ("the Log is not safe for concurrent use") is **stale**; `Flush` holds `d.mu` for the whole round trip, so every guest READ blocks on S3 — exactly what the two-mutex Log design removed. Needs a real resolution (a FLUSH slipping past a WRITE is the genuine hazard), not a mechanical removal. | M | **yes (durability)** |

**Demo:** point the existing QEMU harness at the *Agent's* socket. A real guest writes; a test-issued FLUSH lands a verified object in real RustFS; heartbeats carry real watermarks.

---

## Increment 3 — Checkpoint and truncate (1–2 days) — REVIEW ZONE (durability)

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| Per-volume durability scheduler goroutine: `checkpoint.Create` → (`AdvancePublished`) → `TruncateLocal(published)`, reclaimed bytes into `VolumeStatus` | `internal/agent/durability.go` | Nothing in the tree decides *when*. `Create` is the only caller of `AdvancePublished`, and `StrictOrder.AllowTruncate` refuses anything above `Published` — so with no scheduler, published stays 0 and not one byte is ever reclaimed. | M | yes |

Not required (adversary-rejected): `MaybeCloseForAge` (Flush already closes the batch), `WriteSummary` (only matters as a promotion floor — two writers, not this slice), a `wal.Limits` producer (zero value is legal and unbounded — real ADR-0013 gap, not a slice blocker), ioclass wiring, GC job.

**Demo:** segment files disappear; a checkpoint object appears in RustFS.

---

## Increment 4 — Warm restart, same host, segments intact (1 day) — REVIEW ZONE

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| Compose `recovery.DurablePoint(store, vol, epoch)` → `wal.Resume(..., durableInS3, ...)` → `EnableRemote` | `internal/agent/volume.go` | `Resume`'s `durableInS3` parameter **has no producer anywhere, tests included** — every call site passes 0 or an in-process watermark a restarted process does not have. Getting it wrong = re-issued spans (INV-21) or a dropped tail. | M | yes |
| A way to recover the published point: `checkpoint.Latest`/`List` (or watermarks on `DesiredVolume`) | `internal/checkpoint` | `NewLogAfter` leaves `published = 0`, so a resumed Log can never legally truncate again. `internal/checkpoint` exposes only `Key`/`Publish`/`Read` by explicit `(epoch, seq)` — nothing can find the newest checkpoint in the bucket. | M | yes |
| **ADR**: same-epoch re-attach vs. epoch bump on writer restart | `docs/plan/DECISIONS/` | Genuine disagreement in the audit. Two verifiers say same-epoch is safe here (`Resume` refuses a foreign volume/epoch and re-queues the tail). One argues a restarted writer reusing the epoch writes records indistinguishable from the dead incarnation's, and `BumpVolumeEpoch` is reachable from no binary. One host, one writer: same-epoch is defensible — but write it down. | S | **yes (fencing)** |

**Demo:** kill -9 the agent, restart, guest reads back the same bytes.

---

## Increment 5 — Cold restart: seed the read view from S3 (2–3 days) — REVIEW ZONE — **the real correctness hole**

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| A way to install a rebuilt `*cow.IntervalMap` into a serving Log — a `wal` adopt-constructor/option, or a read-through base in `blockdev` | `internal/wal` (or `internal/blockdev`) | **Verified**: `Log.view` is unexported and assigned in exactly one place (`NewLogAfter: cow.NewIntervalMap()`); there is no setter. `Resume` rebuilds the view *only* from local segments, and `TruncateLocal`→`segments.reclaim` unlinks them, while `scanSegments` explicitly tolerates a first segment that "may start anywhere". So **increment 3 + increment 4 combined = a restart that silently serves zeros for every truncated range, with no error anywhere.** `recovery.Recover` and `materialize.From*` produce exactly the right object and nothing can consume it. There is no test: `internal/wal/resume_test.go` has nine Resume/NewLog tests and not one truncates first. | L | **yes (durability + format)** |
| Property/DST test: write → flush → checkpoint → truncate → resume → read | `internal/wal`, `internal/dst` | The bug above must be catchable. | M | yes |

This also unblocks every drain destination and cross-host move.

**Demo:** truncate, restart, read back the truncated range correctly.

---

## Increment 6 — The DEK arm (1–2 days) — REVIEW ZONE (keys/format)

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| `dek_key_id` end to end: column on `volumes`, field on `GetVolumeKeysResponse`, `metadata.Volume`, `descriptor.Descriptor`, one line in `cpserver.GetVolumeKeys` | schema/proto/metadata/descriptor/cpserver | `DevKMS.UnwrapDEK` binds the version as **GCM AAD**, and `wal.NewEncryption` refuses KeyID 0 (`ErrUnversionedKey`, 0 = plaintext record). The Agent literally cannot build an encryptor from what the RPC returns today. | S–M | yes |
| KEK source on the Agent (`-kek-file`), `crypto.NewDevKMS`, `Deps.KMS`, `UnwrapDEK` → `Log.EnableEncryption` + `NewBatcher(keyID)` | `cmd/volume-agent`, `internal/agent/volume.go` | `internal/crypto` isn't even in the Agent's dependency set. Without it every object is plaintext — INV-15 unreachable. Note increments 2–5 work fine with `enc == nil`, which is why this is separable. | S | yes |

**Demo:** objects in RustFS are ciphertext; restart still reads back.

---

## ~~Increment 7 — A guest that can issue FLUSH~~ **MOSTLY DONE 2026-07-27** (`e8bbdab`, `cef9881`)

> Pulled forward, because spinbox already builds a kernel and already demonstrates a
> static Go init (`cmd/vminitd`) — the estimate below assumed both had to be built.
> `task build:guest` + `task guest:verify` exist and build the guest's two inputs.
>
> **Correction (2026-08-02, DEV-0018):** this note also claimed "a Linux guest boots the
> lane in ~1.1 s under TCG, reports a verdict and powers off". **No test did that** — no
> Go file in the tree referenced the kernel or the initramfs, and every test under
> `integration/vhost` boots a 512-byte boot sector under SeaBIOS. The lane now exists
> (`TestALinuxGuestIssuesFLUSH`) and passes — after fixing what writing it found: a guest
> re-initialises the device when the firmware hands off to the OS, with a new kick
> eventfd, and the queue loop stayed parked on the first one. See STATUS.md.
>
> **The firmware row below is wrong and was reverted.** `-kernel` never needed
> `linuxboot_dma.bin`: spinbox's kernel is an ELF with Xen PVH notes and QEMU enters it
> through `pvh.bin`, which the extract stage already copied. Proven by booting with the
> blob deleted. The blocker was only ever the missing kernel image.
>
> What is left: the timeout row (a TCG kernel boot is slow), and attaching the
> `vhost-user-blk` device — which is increment 2's job, not this one's.

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| Add `linuxboot_dma.bin` to the `extract` stage; assert it in `qemu:verify` | `Dockerfile.qemu:176-187`, `Taskfile.yml` | Verified by execution: `-kernel` on q35 dies with `rom: file linuxboot_dma.bin : error Failed to open file`. The blob **is already in the builder image** (`cp -a pc-bios/*`); only the 5-file cherry-pick omits it. One COPY line. (`linuxboot.bin`/`bios-microvm.bin` are not needed.) | S | no |
| Pinned kernel + initramfs: `Dockerfile.guest` (digest-pinned, like `RUSTFS_IMAGE`), `task build:guest` / `guest:verify` → `_output/guest/vmlinuz` | new `Dockerfile.guest`, `Taskfile.yml` | Nothing in the tree mentions a kernel. Without a Linux guest, no guest-issued `VIRTIO_BLK_T_FLUSH` — the backend side (`blkTypeFlush=4`) is already ready. | M | no |
| `/init` that writes, `fsync`s, re-reads after restart, and reports a verdict | `integration/vhost/testdata/` | `-kernel` with no rootfs panics. Verdict channel: keep isa-debug-exit 0xf4 (`/dev/port`, needs `CONFIG_DEVPORT`) or switch the lane to `-serial` + `-append console=ttyS0` (which also means dropping the current no-serial `-nodefaults` setup). | M | no |
| Raise `bootTimeout` (90s) and the lane's `-timeout 15m` | `integration/vhost/qemu_test.go:50`, `Taskfile.yml:356` | The lane runs `accel=kvm:tcg` and CI has no `/dev/kvm`. A kernel+initramfs boot under pure TCG is minutes, ×2 boots. Otherwise the new lane fails as a timeout and reads as a backend bug. | S | no |

---

## Increment 8 — The e2e lane and a gate that can notice regressions (2–3 days)

| Piece | Where | Why | Size | RZ |
|---|---|---|---|---|
| `testinfra`: bucket+versioning helper, credential injection into subprocess env (`AWS_ACCESS_KEY_ID`/`SECRET`/`REGION` — RustFS creds are `rustfsadmin`), process supervisor that starts/waits/kills/restarts `_output/bin/{control-plane,volume-agent}` and tees output to `t.Log` | `internal/testinfra/{bucket,procs}.go` | No test anywhere execs either binary (`exec.Command` appears twice, both QEMU). The "kill the agent" arm is literally a process kill. Postgres: reuse `task db:dev:up` rather than exporting the private container helper. | M | no |
| `integration/e2e` + `task test:e2e` (tag `e2e`, `-count=1`) in `ci:full` | `integration/e2e/`, `Taskfile.yml` | Nothing can currently notice the slice regressing. | M | no |
| CI must obtain `_output` before `test:integration` | `.github/workflows/ci.yml` | `test:integration:qemu` has `deps: [qemu:verify]`, which hard-fails without `_output`; nothing in the workflow builds or downloads QEMU and `_output` is gitignored. This has never been observed because `origin` is a local bare repo — **the workflows have never run.** "Gate: ci:full green" is true only on one machine. | S | no |
| **Decision**: reboot QEMU across the agent kill (do not keep the guest alive) | lane design | `vhost.Server.session` builds a fresh Device with fresh vring state per connection; reconnection (3.2) and inflight-shmfd (3.3) are unimplemented, RISK-10 open. Keeping the guest alive asserts behavior nobody has specified. | S | no |

---

## The first demonstrable end-to-end milestone

**End of Increment 2.** Real `control-plane` + real `volume-agent` + real RustFS + real Postgres: the Agent receives a volume via `GetDesiredState`, opens a WAL under `--data-dir`, publishes a vhost-user socket, a real QEMU guest writes through it with zero PUTs on the write path, a FLUSH produces a verified object in RustFS, and `ReportVolumeState` carries watermarks a real `wal.Log` produced.

**What it will NOT prove:**
- The FLUSH is issued **by the test, not by the guest** (no Linux guest until Inc 7).
- Nothing is encrypted — objects are plaintext (Inc 6).
- No checkpoint publishes, no byte is reclaimed; the WAL grows forever (Inc 3).
- Restart is untested; after truncation lands (Inc 3) restart is **actively wrong** until Inc 5.
- Fencing is unfenced on the S3 side: with no epoch object, `VerifyPublisher` passes through its "absent ⇒ permitted" escape. Only the Postgres term and the host lease are real.
- Nothing runs in CI (Inc 8), and `ci:full` has never executed on a runner.

The first milestone that matches the target slice's literal wording is **end of Increment 6**; the first one a merge gate can defend is **end of Increment 8**.