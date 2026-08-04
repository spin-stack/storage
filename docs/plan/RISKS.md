# RISKS

Open risks with an owner and a review trigger. Seeded from the doc's residual
weaknesses (§29) plus planning/execution risks introduced by this plan. Each risk is
re-reviewed when its trigger fires; closing a risk requires a note here.

## Format

```
### RISK-NN — <title>
- Source: §X / plan
- Owner: <role>
- Trigger: <event that forces a re-review>
- Mitigation: <planned mitigation and where it lives>
- Status: open | mitigated | accepted
```

## Design-residual risks (from §29)

### RISK-01 — FLUSH/FUA latency coupled to the object store
- Source: §29.1 (the most important residual)
- Owner: Implementer (Track B) + Adversary
- Trigger: —. It was `wal_put_latency` p99 > 500 ms, and **no such series exists**:
  `internal/obs.Catalog()` was trimmed with §26.2 (DEV-0022) and the WAL-remote block went
  with it. A trigger naming a metric nothing registers cannot fire.
- Mitigation: was a parallel PUT pipeline, a same-region backend, the `local` mode and
  WAL-object compaction — all of them V2 machinery.
- Status: **withdrawn 2026-08-04 with ADR-0026.** The object store is not in the FLUSH
  path at all: the ACK is a local `fdatasync` (§14.8, INV-18 — `wal.Log` structurally has
  no object store). The latency that remains is `image_publish_duration_seconds`, paid
  once when the volume stops, and it is not on any guest's I/O path. Comes back with the
  remote durability chain, which is what would put a PUT back in front of an ACK.

### RISK-02 — Durability availability coupled to PostgreSQL via leases
- Source: §29.2 (introduced by the fencing fix)
- Owner: Control Plane Implementer (Track C)
- Trigger: —. It was a `self_fenced_total` spike, and that series went with the rest of
  the WAL-remote block (DEV-0022); so did `ErrSelfFenced` on the ACK path.
- Mitigation: was the `remote`/`local` split, which no longer exists.
- Status: **withdrawn 2026-08-04 with ADR-0026.** No FLUSH ACK consults a lease, so a
  PostgreSQL outage cannot degrade durability — it stops attach, clone, snapshot and
  every other control operation, which is RISK-06 and a different failure. The lease
  survives as the Control Plane's liveness view and gates nothing on the data path.

### RISK-03 — Promotion depends on a clock-drift bound
- Source: §29.3
- Owner: Control Plane Implementer + Harness agent
- Trigger: `clock_offset_seconds > 0.5` alert — the one part of this risk with a live
  subject: the series is still in `internal/obs.Catalog()` and §26.3 still alerts on it.
- Mitigation: chrony monitored, alert at 500 ms.
- Status: **withdrawn 2026-08-04 with ADR-0026** — nothing promotes. `controlplane.Promoter`
  and `PromotionWaitChecker` are deleted and INV-11 is withdrawn, so there is no
  `FENCING_WAIT` to size against `max_clock_skew` and no promotion for a drifted host to
  be ineligible for. **What this risk said is worth having back when promotion returns**,
  which is why it is amended rather than deleted: old-writer safety must use the monotonic
  clock only, and the wall clock may only ever make the system *wait longer*. The old
  status is preserved by the same argument — all of it was modelled in DST against a
  simulated clock and never measured on a host.

### RISK-04 — Cold cross-host clone RTO
- Source: §29.4
- Owner: Implementer (Track A/Phase 11) + operators
- Trigger: cross-host clone test. (There is no drain drill: the drain went with ADR-0026.)
- Mitigation: **start the clone where the data already is.** `placement.Choose` puts
  `SourceHostID` first and the clone path calls it since 2026-08-03, so the common case
  downloads nothing (§20). What is left has no mitigation: the warm standby and lazy
  loading that used to be the answer are both V2, so a clone that cannot land on the
  source host pays a full download of the volume.
- Status: **open, and larger than it was.** ADR-0026 removed the two mitigations and made
  the remaining one time-bounded — the source host holds the data only while it still
  holds the volume. Nobody has measured the per-GiB cost, which is what §2's "descarga
  completa; medido por GiB, no prometido" row is waiting for.

### RISK-05 — Dependence on S3-compatible semantics (CAS, versioning, Object Lock)
- Source: §29.5
- Owner: Harness agent (conformance suite) + operators
- Trigger: enabling/upgrading any backend (MinIO/RustFS/S3) version.
- Mitigation: blocking backend conformance suite per version (§6.1); minimum on-prem
  durability requirement removes the single-node case (§6.1). **The "degrade gracefully to
  lease-only if CAS is absent" clause is withdrawn with ADR-0026 and must not come back by
  habit:** the manifest's compare-and-set *is* V1's whole fencing (§12), so a backend
  without preconditions has no fallback — it has a lost update. That is exactly what
  `scenarioTwoHostsCannotBothPublishAnImage`'s planted bug is (a backend that ignores
  preconditions), and it is why the §6.1 suite is blocking per backend.
- Status: mitigated for the certified dev backend (the §6.1 suite runs via `task backend:conformance`, ADR-0010); open for the production multi-node backends.

### RISK-06 — PostgreSQL as a single point of control
- Source: §29.6
- Owner: Control Plane Implementer
- Trigger: PG total-loss drill; `rebuild-metadata` rehearsal.
- Mitigation: VMs keep serving non-durable I/O; reconciliation makes catch-up trivial;
  PITR + `rebuild-metadata` cover blip-to-total-loss; verified CP term removes
  split-brain (INV-10, §7).
- Status: open. The cited reason is stale: DEV-0009 (`rebuild-metadata` rebuilt volumes only) is resolved at `2b09d1e`, and the rebuild now covers the snapshot catalog too. What remains uncovered is the rest of the row set a Control Plane needs to resume — hosts, leases and operations are not reconstructed from S3, so a total PG loss still leaves placement and fencing state to be re-established by hand. Closes when the rehearsal in the trigger has actually been run.

### RISK-07 — Added complexity (implementation surface) of v5
- Source: §29.7 (honest self-assessment)
- Owner: Planner + Adversary
- Trigger: any increment growing beyond its 3–10-day size; DST coverage gaps.
- Mitigation: the roadmap sequences pieces so each phase is DST-verifiable before the
  next; day-1 pieces are mostly interfaces + reserved fields (cheap now, impossible
  later); compaction/standby/flatten are flag-gated and optional for first deploy.
- Status: open (managed by the phase structure itself).

### RISK-08 — Objectization / truncation ordering
- Source: §29.8, §21.1, INV-13
- Owner: Implementer (Phase 10) + Harness agent
- Trigger: any WAL-truncation code; checkpoint publication path.
- Mitigation: strict ordering + never truncate above a verified `published_sequence`
  (`wal.TruncateLocal` still refuses, with `ErrTruncateAboveDurable`).
- Status: **withdrawn 2026-08-04 with ADR-0026**, and both halves of its subject are gone:
  nothing objectizes or truncates mid-session (INV-13 withdrawn, `TruncateLocal` has no
  production caller) and `internal/gc` is deleted (INV-14 back to pending). What survives
  is enforced at construction instead of by a sweep — `real.NewS3Store` refuses an
  unversioned bucket, so a delete marker stays reversible (`TestRequireVersioning`). One
  caveat outlives the risk and belongs to whoever rebuilds the sweep: **no real backend has
  ever been made to lose a listing or reorder a delete mid-sweep**, and §6.1's suite does
  not cover one.

## Planning / execution risks

> RISK-09 (hot-zone contention under parallel tracks) was removed on 2026-08-02 with
> ADR-0002. It was a risk of a *process* — a Planner role scheduling parallel increment
> tracks against a hot-zone list — and this repository does not run that process. A risk
> nobody owns is worse than no entry, because it reads as watched.

### RISK-10 — QEMU 11.0.2 inflight-shmfd behavior unverified
- Source: §16, §27 note, roadmap §30.3
- Owner: Implementer (Phase 03) + Harness agent
- Trigger: Phase 03 start (vhost-user reconnection + inflight tracking).
- Mitigation: pin QEMU 11.0.2 identically in CI and prod; verify
  `VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD` behavior against QEMU docs and by execution
  before relying on it; SPDK vhost as the reference model. Marked **Unverified:** until
  Phase 03 executes it.
- Status: **open** — the handshake half is verified (below); inflight-shmfd is not, and
  is not touched until Increment 3.3.

#### Verified by execution, 2026-07-26 (increment 3.1)

Executed: `task test:integration:qemu` — the real pinned binary
(`_output/bin/qemu-system-x86_64`, `QEMU emulator version 11.0.2`, `vhost-user-blk-pci`
present) connecting to a `vhost.Server` over a Unix socket, booting a guest from a
512-byte MBR (`integration/vhost/testdata/bootsector.S`) served by
`hostio.RawFile`. No KVM on this machine (`/dev/kvm` is not readable by the running
user), so `accel=kvm:tcg` fell back to TCG; the backend cannot tell the difference.
Guest memory is `memory-backend-memfd,share=on`, which vhost-user requires.

What it proved:

- **The full handshake completes and the device goes live.** The observed sequence was
  `GET_FEATURES, GET_PROTOCOL_FEATURES, SET_PROTOCOL_FEATURES, SET_OWNER, GET_FEATURES,
  SET_VRING_CALL, SET_VRING_ERR, GET_CONFIG, SET_FEATURES, SET_VRING_CALL, SET_FEATURES,
  SET_MEM_TABLE, SET_VRING_NUM, SET_VRING_BASE, SET_VRING_ADDR, SET_VRING_KICK,
  SET_VRING_ENABLE, SET_VRING_ENABLE, GET_VRING_BASE`.
- **READ and WRITE work end to end.** SeaBIOS read LBA 0 through the virtqueue to find
  the boot signature; the boot sector then read LBA 1 and wrote it to LBA 64 via INT 13h
  (AH=42h/43h), and the bytes are asserted on the backend side. A planted bug that makes
  `RawFile.ReadAt` return zeros turns the lane red.
- **FLUSH is not covered by *this* lane** — SeaBIOS's INT 13h has no flush verb, so the
  boot-sector lane never issues one. **A separate lane does cover it, since 2026-08-02**:
  `TestALinuxGuestIssuesFLUSH` boots the pinned Linux kernel and a real `fsync(2)` is
  answered by the backend, and `TestAGuestSurvivesAStopAndComesBackFromItsImage` carries
  the same volume through a stop and a second boot that has only the image to read from.

  **Corrected 2026-08-04.** This said the `fsync` "becomes a verified object" and named
  `TestAGuestSurvivesCheckpointAndTruncation`. Under ADR-0026 a FLUSH puts **zero** objects
  in the bucket — the ACK is a local `fdatasync` (§14.8, INV-18) — and that test was
  renamed with the checkpoint it no longer performs.

  **The reason recorded here for why that was impossible was wrong, and is corrected in
  place rather than deleted** — a wrong reason that is merely removed gets rediscovered.
  It said `-kernel` direct boot needed `linuxboot_dma.bin`, which `task build:qemu` does
  not extract. It does not extract it, and it does not matter: the kernel is an ELF with
  Xen PVH notes and QEMU enters it through `pvh.bin`, which was being extracted all
  along. Proven by booting with `linuxboot_dma.bin` deleted. The only real blocker was
  that no kernel image existed, which ADR-0022 fixed.

Measurements worth keeping, all of them things the specification did not say:

- QEMU 11.0.2 asks for **57 bytes** of configuration space in `GET_CONFIG` at offset 0 —
  not the 60 of `struct virtio_blk_config`. The backend fills 60 and zero-fills up to the
  protocol's 256-byte maximum, which is what makes this a non-event.
- `SET_VRING_NUM` is **128**, matching QEMU's `vhost-user-blk` `queue-size` default and
  the depth §4 fixes. The lane asserts this so a default change surfaces as a named
  failure rather than a connection that dies mid-handshake.
- QEMU sends **`SET_VRING_ERR`**, which the simulated front-end did not, and never sends
  `GET_QUEUE_NUM`, which the simulated front-end did.
- **QEMU 11.0.2's `vhost-user-blk` reconnects on its own** after a backend protocol
  error — `Reconnecting after error: vhost_backend_init failed: Protocol error` — with no
  `reconnect=` on the chardev. Increment 3.2 builds on this rather than adding it.

Still unverified, and the reason this risk stays open: `GET_INFLIGHT_FD` /
`SET_INFLIGHT_FD`. The backend does not advertise `INFLIGHT_SHMFD`, QEMU therefore never
sends them (asserted in the lane), and nothing here says anything about how QEMU behaves
when it does. That is Increment 3.3, and it is the durability-review zone.
