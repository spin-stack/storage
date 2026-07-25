# PHASE 02 — Guest layout: three devices + OverlayFS

> **Roadmap §30.2. Planning only** — implementation deferred to an environment with
> guest mounts / a VM (this sandbox lacks it). Depends on Phase 01.
>
> Implements doc §1, §9. No data-loss-zone formats here (guest-side mount wiring), so
> standard gate without a mandatory format review, but the fail-closed ephemeral
> behavior (§9) is safety-relevant and gets a DST/integration arm.

**Phase objective:** boot a guest with the three devices — `/dev/vda` EROFS read-only
(shared base image), `/dev/vdb` ext4 persistent (durable state), `/dev/vdc` ext4
ephemeral — assembled via OverlayFS, with Docker/containerd bound onto the ephemeral
device and a hard fail (no silent fallback to persistent) if the ephemeral device is
unavailable.

**Invariants touched:** INV (5.5) "the ephemeral may be lost" — nothing durable depends
on `/dev/vdc`; asserted by the fail-closed behavior. No new checkers in the harness
(guest-side), covered by integration tests.

---

## Increment 2.1 — Device model + backing provisioning
**Objective:** represent and provision the three devices (EROFS image, persistent ext4,
ephemeral ext4) as Agent-managed backing objects/files; sizes, block size (64 KiB
segment granularity recorded), and config.
**Scope in:** device descriptors; EROFS image reference (shared, read-only); persistent
+ ephemeral backing files; `deploy/` config surface.
**Tests first:** provisioning unit tests; descriptor round-trip. **Gate:** standard.

## Increment 2.2 — OverlayFS root + ephemeral binds (§9)
**Objective:** the guest boot assembly — `lowerdir=EROFS`, `upperdir=persistent/root-upper`,
`workdir=persistent/root-work`, merged root; ephemeral mounted at
`/var/lib/ephemeral` with binds for `/var/lib/docker` and `/var/lib/containerd`.
**Scope in:** initramfs/systemd mount units (or equivalent) per §9; the exact mount
sequence.
**Fail-closed:** if the ephemeral mount fails, Docker/containerd do **not** start and
the persistent device is **not** used as a silent fallback (§9, §5.5).
**Tests first:** integration test that boots the layout; a fault test that removes the
ephemeral device and asserts Docker/containerd fail to start (not fall back).
**Gate:** standard + the fail-closed fault test.

## Increment 2.3 — virtio-blk feature advertisement scaffolding (§9)
**Objective:** the guest-facing feature set on `/dev/vdb`: `VIRTIO_BLK_F_FLUSH` (write-
back explicit), `VIRTIO_BLK_F_DISCARD`, `VIRTIO_BLK_F_WRITE_ZEROES`, and resize via
config-space update + notification (guest `resize2fs`). The actual vhost transport that
serves these is Phase 03; here the feature negotiation and config space are defined.
**Tests first:** feature-negotiation unit tests; resize config-space update test.
**Gate:** standard.

## Phase 02 exit gate
- [ ] Three devices provisioned; OverlayFS root boots; ephemeral binds for Docker/containerd.
- [ ] Fail-closed on ephemeral loss verified (no silent persistent fallback).
- [ ] virtio-blk FLUSH/DISCARD/WRITE_ZEROES/resize features negotiated.
- [ ] Integration tests in `integration/` (real guest); runbook note on boot.
