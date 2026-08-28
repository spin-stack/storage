# ADR-0022 — the guest kernel is pinned by content and mirrored, not resolved by path

- **Status:** Accepted — 2026-07-28
- **Relates to:** ADR-0021 (storage consumes spinbox's artefacts, never its code),
  ADR-0010 (pin by digest, not by tag)

## Decision

storage does not build a kernel (ADR-0021). It names spinbox's artefact **by content**,
obtains it from whichever source has it, and verifies it before booting anything with it.

1. **One canonical path:** `_output/guest/vmlinux`. `task fetch:kernel` puts it there;
   every other task and test reads it there. `SPINBOX_KERNEL` survives only as one *source
   to copy from*, not as the place the lane looks.
2. **Pinned by sha256 in `Taskfile.yml`** (`GUEST_KERNEL_SHA256`), next to
   `GUEST_KERNEL_VERSION`. Any source offering a kernel with a different hash is rejected,
   loudly, with both hashes printed. Bumping the pin is a deliberate edit whose commit says
   why the kernel moved — the same contract as `RUSTFS_IMAGE` and the pinned QEMU version.
   The mirror therefore lags spinbox, deliberately: the lane wants the kernel it has
   evaluated, not the newest one.
3. **Sources are tried in order:** a sibling spinbox checkout (no network, and what exists
   today), then the mirrored image (what makes the lane runnable anywhere else). Failure
   names every source it tried and what each one lacked.
4. **Mirroring is not building.** `Dockerfile.guest-kernel` packages the kernel as a single
   file on `scratch`, so the image digest is a hash of the kernel and of nothing else. No
   kernel source, `.config`, cross-toolchain or build flags enter this repository; the
   mirror cannot produce a kernel spinbox did not produce first.
5. **What is verified is verified from the artefact itself.** `task guest:verify` reads the
   config the kernel carries (`CONFIG_IKCONFIG=y` embeds it gzipped after the `IKCFG_ST`
   marker) and asserts `CONFIG_VIRTIO_BLK`, `CONFIG_PVH`, `CONFIG_BLK_DEV_INITRD` and
   `CONFIG_SERIAL_8250_CONSOLE`. A `kernel-config` file sitting next to the binary would be
   easier to read and worthless: it does not travel with the artefact and can describe a
   different kernel. Each check turns a boot-time symptom into a named failure — without
   `VIRTIO_BLK` there is no `/dev/vda` and the guest's complaint reads exactly like a bug in
   our backend; without `PVH` QEMU fails opening a ROM; without `BLK_DEV_INITRD` the
   initramfs is ignored and PID 1 never runs; without the 8250 console the verdict the host
   greps for is never printed. All four were proven to fail by planting them.

**The external constraint that forces the mirror:** spinbox publishes no kernel. Its
workflows do not mention one; the artefact exists only as an untracked file under
`_output/` on whoever ran `task build:kernel` last, so there is nothing to point a URL at.
`GUEST_KERNEL_IMAGE` therefore defaults to empty — a machine with no checkout fails saying
the kernel is missing rather than pointing at a registry path that answers 404 — and
`task guest:kernel:push` still needs a decision about *where*: this repository's packages,
or spinbox's.

## Rejected

- **Build our own kernel.** ADR-0021 already rejected it: two kernels drift, and the one
  spin boots is the one that matters.
- **Wait for spinbox to publish it.** Correct long-term and another repository's decision;
  it would leave this lane undefendable meanwhile. If it lands, `GUEST_KERNEL_IMAGE` points
  at spinbox's package and the mirror becomes redundant — nothing here is undone, which is
  why the pin lives here rather than in the mirror.
- **Commit the kernel to this repository.** 37 MB of binary in git per bump, for an
  artefact that is not ours and that a digest names just as precisely.
- **Trust the path and check nothing.** The status quo this replaced: it let "a real guest
  issues FLUSH" become a claim about a file whose identity nobody had recorded.
