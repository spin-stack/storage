# ADR-0022 — the guest kernel is pinned by content and mirrored, not resolved by path

- **Status:** Accepted — 2026-07-28
- **Date:** 2026-07-28
- **Deciders:** human owner (approved), implementer agent (proposed)
- **Relates to:** ADR-0021 (storage consumes spinbox's artefacts, never its code),
  ADR-0010 (pin a backend by digest, not by tag), ADR-0018 (the spine)

## Context

The QEMU guest lane needs a Linux kernel. storage does not build one — ADR-0021 fixed
that: it consumes spinbox's artefact, because a kernel built here would be a second
kernel to keep in step with the one spin actually boots.

What ADR-0021 did not say is *how* the artefact arrives, and the answer it got by default
was a relative path into a sibling working copy:

```yaml
SPINBOX_KERNEL: '{{.SPINBOX_KERNEL | default "../spinbox/_output/spinbox-kernel-x86_64"}}'
```

Four things are wrong with that, and they compound:

1. **It only exists on a machine that has spinbox checked out and built.** CI has none.
   `task guest:verify` cannot pass on a runner, so the one lane that proves a real kernel
   issues FLUSH is structurally unable to defend anything.
2. **spinbox never publishes the kernel.** Its workflows (`ci.yml`, `release.yml`) do not
   mention it; the artefact exists only as an untracked file under `_output/` on whoever
   ran `task build:kernel` last. There is nothing to point a URL at.
3. **`cd ../spinbox && task build:kernel` exits non-zero on the owner's machine** (it
   fails writing a BuildKit cache directory) while still emitting the artefact. The
   instruction we printed on failure was one the reader could follow and still not
   believe the result.
4. **Nothing recorded which kernel the lane had certified.** "The guest boots in ~1.1 s
   and issues FLUSH" was true of *a* kernel, in `_output` on one laptop, on one day. A
   rebuilt spinbox would replace it silently, and the claim would transfer to a kernel
   nobody had evaluated. This is the same failure the project already rejected for the
   object store, where `RUSTFS_IMAGE` is pinned by digest precisely so a re-tagged image
   cannot change what the conformance suite certified.

## Decision

**The kernel is an artefact this repository names by content, obtains from whichever
source has it, and verifies before booting anything with it.**

1. **One canonical path.** `_output/guest/vmlinux`. `task fetch:kernel` puts it there;
   every other task and test reads it there. `SPINBOX_KERNEL` survives only as *a source
   to copy from*, one of several, and no longer as the place the lane looks.

2. **Pinned by sha256, in `Taskfile.yml`, next to the version it belongs to.** Any source
   that offers a kernel with a different hash is rejected, loudly, with both hashes
   printed. Bumping the pin is a deliberate edit whose commit says why the kernel moved —
   the same contract as `RUSTFS_IMAGE` and the pinned QEMU version.

3. **Sources are tried in order: a sibling spinbox checkout, then the mirrored image.**
   The checkout first because it needs no network and is what exists today; the image
   because it is what makes the lane runnable anywhere else. Failure names every source
   it tried and what each one lacked.

4. **storage may *mirror* the artefact into a registry.** `Dockerfile.guest-kernel`
   packages the kernel as a single file on `scratch`, so the image digest is a hash of
   the kernel and of nothing else, and pulls it back out with the same
   `--output type=local` mechanism `task build:qemu` already uses.

   **Mirroring is not building.** No kernel source, no `.config`, no cross-toolchain and
   no build flags enter this repository; the mirror cannot produce a kernel that spinbox
   did not produce first. ADR-0021 stands unchanged — this is the transport for the
   artefact it already blessed, not a second way to make one.

5. **What is verified is verified from the artefact itself.** `task guest:verify` reads
   the config the kernel carries (`CONFIG_IKCONFIG=y` embeds it gzipped after the
   `IKCFG_ST` marker) and asserts `CONFIG_VIRTIO_BLK`, `CONFIG_PVH`,
   `CONFIG_BLK_DEV_INITRD` and `CONFIG_SERIAL_8250_CONSOLE`. A `kernel-config` file
   sitting next to the binary would have been easier to read and worthless: it does not
   travel with the artefact, and it can describe a kernel other than the one in hand.

   Each check turns a boot-time symptom into a named failure. Without `VIRTIO_BLK` there
   is no `/dev/vda` and the guest reports a missing device — which reads exactly like a
   bug in our backend. Without `PVH`, QEMU fails opening a ROM. Without `BLK_DEV_INITRD`
   the initramfs is ignored and PID 1 never runs. Without the 8250 console the verdict
   the host greps for is never printed. All four were proven to fail by planting them.

## Consequences

- **The lane no longer requires a spinbox checkout.** Verified: with
  `SPINBOX_KERNEL=/nope`, `task fetch:kernel GUEST_KERNEL_IMAGE=…` pulls the kernel from
  an image and `task guest:verify` passes on it.
- **A kernel change costs a deliberate re-pin.** `task guest:kernel:pin` prints the new
  hash; the commit has to say why it moved. That friction is the point: it is the only
  moment anyone is asked whether the new kernel is one the lane's claims should transfer
  to.
- **The mirror will lag spinbox.** Intended. The lane wants a fixed kernel it has
  evaluated, not the newest one; when spin's runner needs a newer kernel, that is a
  re-pin with a reason, not a silent inheritance.
- **Nothing is published yet.** `GUEST_KERNEL_IMAGE` is deliberately empty by default, so
  a machine with no checkout fails saying the kernel is missing rather than pointing at a
  registry path that answers 404. Publishing it (`task guest:kernel:push`) is one
  command, and needs a decision about *where* — this repository's packages, or spinbox's.
- **This does not make CI run the lane on its own.** The kernel was one of two missing
  inputs; QEMU is the other, and it is a different problem — the binaries are dynamically
  linked against what the runtime image provides, so extracting them onto a bare runner
  and executing them is not enough. **Decided separately on 2026-08-02 by ADR-0025**:
  the lane runs inside the published runtime image as a container job.
- **If spinbox later publishes the kernel itself, nothing here has to be undone.**
  `GUEST_KERNEL_IMAGE` points at spinbox's package instead of ours and the mirror becomes
  redundant — which is the outcome to prefer, and the reason the pin lives here rather
  than in the mirror.

## Alternatives rejected

- **storage builds its own kernel.** ADR-0021 already rejected it, and the reason has not
  changed: two kernels drift, and the one spin boots is the one that matters.
- **Wait for spinbox to publish it.** Correct long-term and out of reach today — it is
  another repository, and it would leave this lane undefendable until that lands. The
  mirror does not compete with it; it is the same artefact under a different name, and
  point 5 above means we would notice if it were not.
- **Commit the kernel to this repository.** 37 MB of binary in git, re-added on every
  bump, for an artefact that is not ours and that a digest names just as precisely.
- **Trust the path and check nothing.** That is the status quo this ADR replaces. It is
  what let "a real guest issues FLUSH" become a claim about a file whose identity nobody
  had recorded.
