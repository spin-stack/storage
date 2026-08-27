# ADR-0025 — the guest lane runs inside the published QEMU runtime image

> **Amended 2026-08-27: the lane runs on an ordinary runner, and the principle survives
> the mechanism.** The decision below is "a container job inside
> `ghcr.io/<repo>/qemu:<version>`", and `.github/workflows/ci.yml`'s `guest` job is not
> one. The reason is a constraint this ADR did not know about: the demos start the
> development Postgres through Docker, and a container job gets no Docker daemon of its
> own. Both cannot be had.
>
> What the ADR was actually protecting is **one definition of the dependency set**, and
> that is intact. `task qemu:tools` computes the closure with `ldd` *inside the image* and
> copies the loader with it, so the list is still `Dockerfile.qemu`'s and still moves when
> the image does — the alternative this ADR refused, "install the runtime libraries on the
> runner", is a second hand-written list and is still refused. The wrapper is a third
> option the ADR did not consider, and it was not invented for this: it already carries
> `qemu-img` to every lane, written after the first CI run this repository ever had died
> on `liburing.so.2: cannot open shared object file`.
>
> **Amended 2026-08-05 (track B, `8c9d296`/`927f79d`): the decision stands, the skip is
> reversed.** Running the lane *inside* `ghcr.io/<repo>/qemu:<version>` is unchanged and is
> still right for the reason below — one definition of the dependency set, in
> `Dockerfile.qemu`. What this ADR also decided, twice, was that a missing artefact
> **skips** the lane rather than failing the gate: "the lane is skipped — visibly, with the
> reason", because "a red build that means *you did not build QEMU* trains people to ignore
> red builds". That is now the opposite of what the gate does.
>
> **What overturned it is that the skip was invisible where it mattered.** `task ci:full`
> exited 0 on every machine without QEMU while running none of the proofs a real Linux
> kernel carries, and the sentence people exchange is "ci:full was green" — a skip notice
> inside a run nobody reads does not survive that sentence, and a job that is skipped leaves
> its dependents free to run, so a workflow of green-and-grey boxes reports as a pass.
> Trained-to-ignore-red is a real cost; **believed-to-be-proven is a larger one**, and it is
> the one this repository has actually paid (three documents claimed a Linux-guest lane no
> test performed — DEV-0018).
>
> **Concretely, and each is checkable:** the preflight job is `guest-inputs`, not
> `guest-lane-image`, and it **exits 1** when either artefact is unpublished, writing the two
> commands that publish them into `$GITHUB_STEP_SUMMARY`. `guest-lane` and `guest-e2e-lane`
> run with `REQUIRE_PROOFS=1`, which the lanes export as `SPIN_REQUIRE_PROOFS` and
> `testinfra.missingInput` turns into a failure. The `ci` job no longer runs `ci:full` — it
> runs `ci:noguest`, the same lane list with the flag unset, which ends by printing what it
> did **not** prove. A `gate` job enumerates the lanes with `toJSON(needs)` and is the job to
> require on the branch.
>
> **What survives of the reasoning below is the developer path, deliberately.** Without
> `REQUIRE_PROOFS` the lanes still print SKIP and exit 0, so a laptop with no `_output` is
> not red; the difference is that no target which *claims* to be the merge gate can reach
> that state any more. The "Consequences" section's "`task test:integration:qemu` keeps
> skipping there" is therefore true of `task ci` and false of `task ci:full`.

- **Status:** Accepted — 2026-08-02, amended 2026-08-05
- **Date:** 2026-08-02
- **Deciders:** human owner (approved the increment plan), implementer agent (proposed)
- **Relates to:** ADR-0022 (the guest kernel, pinned and mirrored)
  increment 8, `Dockerfile.qemu`, `.github/workflows/qemu.yml`

## Context

The QEMU guest lane (`task test:integration:qemu`) is the only test that proves a **real
Linux kernel** issues `VIRTIO_BLK_T_FLUSH` and that our backend's answer satisfies
`fsync(2)`. It runs on a developer machine and has never run in CI.

Two of the three reasons were closed in July: the kernel is fetched and pinned rather than
read out of a sibling checkout (ADR-0022), and the lane skips loudly instead of hard-failing
when `_output` holds no QEMU. **The third is QEMU itself**, and it is not the same problem
the kernel had.

`qemu.yml` already publishes both a runtime image (`ghcr.io/<repo>/qemu:<version>`) and the
extracted binaries as a workflow artefact, so *obtaining* them is easy. The difficulty is
that the binaries are dynamically linked against what the runtime image provides —
`libglib2.0-0`, `libpixman-1-0`, `libcap-ng0`, `libseccomp2`, `libaio1`, `liburing2`,
`zlib1g`, the `runtime` stage of `Dockerfile.qemu`. Extracting them onto a bare runner and
executing them is therefore not enough.

## Decision

**A separate CI job that runs the guest lane inside `ghcr.io/<repo>/qemu:<version>`**, as a
GitHub Actions container job, gated on that image existing.

Not: install the runtime libraries on the runner and use `_output` as today.

## Why

**One definition of the dependency set.** The list above already exists, in
`Dockerfile.qemu`, and it is the same list that ships. Installing it a second time in a
workflow means two lists that drift silently — and the drift presents as *a QEMU that will
not start*, in a lane whose entire purpose is to tell us something else. Debugging a
missing `.so` while trying to learn whether a guest's `fsync` produced a FLUSH is the worst
possible time to be doing it.

**The image is already the artefact of record.** `qemu.yml` pins it by QEMU version — the
same version `qemu:verify` asserts — and tags it with the commit as well. Running the lane
*in* it means the lane runs against exactly what was built and verified, with no staging
step that could substitute something else.

**It is a separate job on purpose.** The main `ci` job needs Docker for TestContainers
(Postgres, RustFS) and for the e2e lane; a container job would need Docker-in-Docker to
keep those. The guest lane needs none of it — QEMU, a kernel image and an initramfs are
files — so splitting is cheaper than merging, and the two jobs run in parallel.

## What it costs, stated rather than discovered

- **No `/dev/kvm` on GitHub runners**, so the guest boots under TCG. The lane already
  accounts for this (`-timeout 15m`, and a TCG boot measured at ~1.1 s on a developer
  machine will be slower here).
- **The lane depends on `qemu.yml` having run at least once.** The image is built only when
  `Dockerfile.qemu`, the Taskfile or that workflow changes, because the build takes tens of
  minutes. A first push therefore has no image to run against.

  Rather than fail the gate for a reason that has nothing to do with the code, a preflight
  job checks whether the tag exists and the lane is skipped — visibly, with the reason —
  when it does not. That is the same call the Taskfile already makes locally, and for the
  same reason: **a red build that means "you did not build QEMU" trains people to ignore
  red builds.**

- **The layout differs.** The runtime image puts binaries in `/usr/local/bin` and firmware
  in `/usr/share/spin-stack/qemu`, while the lane expects `<root>/bin` and
  `<root>/share/spin-stack/qemu`. The test already reads `QEMU_OUTPUT_DIR`, so the job
  stages a directory of symlinks and points the lane at it — three lines, and no copy that
  could go stale.

## Consequences

- `.github/workflows/ci.yml` gains a `guest-lane` job. The `ci` job is unchanged, and
  `task test:integration:qemu` keeps skipping there, as it does on a laptop with no
  `_output`.
- If the guest lane is ever to be *required*, `qemu.yml` must run first on a fresh clone.
  That ordering is a property of the fleet, not of this repository, and it is why the
  preflight exists.
- **This is not verified.** The workflows in this repository have never executed:
  `origin` is a local bare repo, so there is no runner and no GitHub Packages registry
  behind any of it. Everything above is reasoned from the workflow files and the
  Dockerfile, and the first real run is the test of it. `STATUS.md` says so in the same
  words.
