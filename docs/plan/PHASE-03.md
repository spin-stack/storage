# PHASE 03 — vhost-user-blk raw backend + reconnection + inflight shmfd

> **Roadmap §30.3. Planning only** — implementation deferred to an environment with
> **QEMU 11.0.2** (pinned; same in CI and prod, §4/§16). Depends on Phase 01; integrates
> with Phase 02 (device features) and Phase 04 (the WAL/CoW engine behind the block dev).
>
> **⚠️ Touches durability/reconnection semantics.** Human-review-required for the
> inflight-recovery increment. RISK-10 tracks the QEMU inflight-shmfd behavior as
> **Unverified** until this phase executes it.

**Phase objective:** serve a block device to QEMU over a `vhost-user-blk` Unix socket
(single queue, depth 128) such that a crash or deploy of the Agent produces a pause of
seconds — not a VM restart — by recovering in-flight requests from shared memory
(`VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD`, the SPDK vhost model).

**Invariants touched:** feeds INV-06/INV-07 (the ACK path runs over this transport in
later phases); the reconnection guarantee underpins §31 criterion 11 (crash/deploy
without VM restart, zero I/O lost or duplicated).

**Metrics activated:** `vhost_reconnects_total`, `inflight_recovered_total`.

---

## Increment 3.1 — vhost-user-blk raw backend — **DONE 2026-07-26**

> Verified by execution against the pinned QEMU 11.0.2 (`task test:integration:qemu`),
> not against a simulator: full handshake, then SeaBIOS reading LBA 0 through our
> virtqueue and a boot sector reading and writing sectors that are asserted on the
> backend. TCG (no `/dev/kvm` for this user), guest RAM over memfd. What the spec did
> not say is recorded in RISKS.md under RISK-10. **FLUSH is not exercised by a guest**
> — SeaBIOS has no flush verb and the lane has no kernel — and reconnection /
> inflight-shmfd are untouched.

**Objective:** a minimal vhost-user-blk backend exposing a raw block device (backed by
the WAL/CoW engine or, initially, a raw file) to QEMU 11.0.2 over a Unix socket; single
queue, queue depth 128; READ/WRITE/FLUSH handled.
**[Verify]** QEMU 11.0.2 vhost-user-blk protocol handshake and feature bits against the
QEMU docs and by execution (RISK-10). Pin QEMU 11.0.2 in CI == prod.
**Tests first:** protocol handshake unit tests (with a simulated front-end); an
integration test issuing READ/WRITE/FLUSH from a real QEMU guest.
**Gate:** standard + QEMU integration smoke.

## Increment 3.2 — Reconnection (deploy without VM restart)
**Objective:** when the Agent restarts (deploy), QEMU automatically reconnects to the
socket; the device resumes. Rolling Agent restarts are transparent (§28.3).
**Tests first:** integration test that restarts the Agent mid-workload and asserts the
guest continues after a short pause (no VM restart); `vhost_reconnects_total` increments.
**Gate:** standard + reconnection integration test.

## Increment 3.3 — Inflight tracking via shmfd  ⚠️ review
**Objective:** in-flight requests live in the inflight shared-memory region so an
Agent crash/deploy recovers them without loss or duplication; the front-end hands the
inflight buffer back to the backend after restart (§16).
**Tests first:**
- DST arm: kill the Agent with requests in flight; assert every request is completed
  exactly once (no loss, no duplicate) on recovery.
- Integration: real QEMU + real kill; verify via checksums.
**Gate:** standard + the inflight-recovery DST arm + human review (durability zone).
Activates `inflight_recovered_total`. Verifies RISK-10.

## Phase 03 exit gate
- [ ] vhost-user-blk serves READ/WRITE/FLUSH to QEMU 11.0.2 (single queue, depth 128).
- [ ] Agent deploy = transparent rolling restart (QEMU reconnects, no VM restart).
- [ ] Inflight recovery: crash with in-flight I/O loses/duplicates nothing (DST + real).
- [ ] QEMU 11.0.2 pinned identically in CI and prod; RISK-10 closed.
