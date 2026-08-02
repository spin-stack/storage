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
- Trigger: `wal_put_latency` p99 > 500 ms in any DST/HW run; fsync-heavy workload test.
- Mitigation: parallel PUT pipeline on FLUSH (§14.4); same-region backend; `local`
  durability mode (§14.8); WAL-object compaction (§21.2); p99 measured from day 1 with
  injected latency. Documented write-back contract to the user (§2).
- Status: open (inherent to the no-host-to-host-replication model).

### RISK-02 — Durability availability coupled to PostgreSQL via leases
- Source: §29.2 (introduced by the fencing fix)
- Owner: Control Plane Implementer (Track C)
- Trigger: PG outage drill; any `self_fenced_total` spike tied to PG unavailability.
- Mitigation: accepted trade-off for correct fencing without self-hosted consensus; in
  v5.1 affects only `remote` volumes — `local` volumes (§14.8) do not depend on the
  lease for FLUSH ACK. PG standby + PITR; degraded mode and its timings in the runbook.
  Future option: S3-CAS-object lease renewal as a secondary path (§14, post-MVP §30.14).
- Status: accepted (documented degraded mode).

### RISK-03 — Promotion depends on a clock-drift bound
- Source: §29.3
- Owner: Control Plane Implementer + Harness agent
- Trigger: `clock_offset_seconds > 0.5` alert; DST drift-injection scenario.
- Mitigation: old-writer safety uses **monotonic** clock only (independent of the bound);
  chrony monitored, alert at 500 ms; hosts over `max_clock_skew` ineligible for
  promotion; DST injects drift beyond the bound and asserts the only effect is waiting
  longer, never losing writes (INV-11).
- Status: open. **Not for the reason this line used to give.** It cited DEV-0004 (fail-open fencing, non-atomic promotion), which `REFERENCE.md` records as resolved at `6d5655e`/`f9f5885` — fencing fails closed and promotion is resumable. What keeps it open is that all of it is *modelled*: the drift injection lives in DST against a simulated clock, and no host has ever been observed crossing `max_clock_skew` with chrony running. Closes on a measurement, not on code.

### RISK-04 — Cold cross-host materialization RTO
- Source: §29.4
- Owner: Implementer (Track A/Phase 11) + operators
- Trigger: cross-host clone test; drain drill.
- Mitigation: warm standby for volumes that matter (RTO in minutes, Phase 12); cold RTO
  published per-GiB in the runbook; lazy loading designed and format-compatible
  (post-MVP §22.4).
- Status: open.

### RISK-05 — Dependence on S3-compatible semantics (CAS, versioning, Object Lock)
- Source: §29.5
- Owner: Harness agent (conformance suite) + operators
- Trigger: enabling/upgrading any backend (MinIO/RustFS/S3) version.
- Mitigation: blocking backend conformance suite per version (§6.1); specified graceful
  degradation to lease-only if CAS is absent (§12.4); minimum on-prem durability
  requirement removes the single-node case (§6.1).
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
- Mitigation: strict ordering + DST/fault injection at each step + never truncate above
  a verified `published_sequence` + GC-cannot-permanently-delete as the final net
  (INV-13, INV-14).
- Status: open. DEV-0006 is resolved at `cd17e0b`: the GC marks reversibly and the store no longer exposes permanent deletion. The ordering property is proven in DST with a planted bug. It stays open because the final net has only ever been tested against *simulated* faults — no real backend has been made to lose a listing or reorder a delete mid-sweep, and §6.1's conformance suite does not cover a sweep.

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
  `TestALinuxGuestIssuesFLUSH` boots the pinned Linux kernel and a real `fsync(2)`
  becomes a verified object, and `TestAGuestSurvivesCheckpointAndTruncation` carries it
  through a checkpoint, a truncation and a reboot.

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
