# ADR-0025 — the guest lane's QEMU comes from the published runtime image, not from the runner's packages

Accepted 2026-08-02; the mechanism was replaced on 2026-08-27, the principle is unchanged.
Relates to ADR-0022 (the guest kernel), `Dockerfile.qemu`, `.github/workflows/qemu.yml`.

## The constraint

The guest lane is the only test that proves a **real Linux kernel** issues
`VIRTIO_BLK_T_FLUSH` and that our backend's answer satisfies `fsync(2)`. `qemu.yml`
publishes a runtime image and the extracted binaries, so obtaining them is easy; the
difficulty is that they are dynamically linked against the `runtime` stage of
`Dockerfile.qemu` — `libglib2.0-0`, `libpixman-1-0`, `libcap-ng0`, `libseccomp2`, `libaio1`,
`liburing2`, `zlib1g`. Copying them onto a bare runner is not enough: the first CI run this
repository ever had died on `liburing.so.2: cannot open shared object file`.

## Decision

**One definition of the dependency set, and it is `Dockerfile.qemu`'s.** The runtime image is
the artefact of record — pinned by the same QEMU version `task qemu:verify` asserts — and the
lane's QEMU comes out of that image together with the libraries it was built against. `task
qemu:tools` computes the closure with `ldd` *inside* the image and copies the loader with it,
so the list is still the Dockerfile's and still moves when the image does.

## Alternatives rejected

- **Install the runtime libraries on the runner.** A second, hand-written list; two lists
  drift silently, and the drift presents as *a QEMU that will not start*, inside the one lane
  whose purpose is to tell us something else. Still refused.
- **Skip the lane when the image is missing, rather than fail the gate.** Decided twice, on
  the grounds that "a red build that means *you did not build QEMU* trains people to ignore
  red builds". Reversed 2026-08-05: `ci:full` exited 0 on every machine without QEMU while
  running none of the proofs a real kernel carries, and the sentence people exchange is
  "ci:full was green" — a skip notice nobody reads does not survive that sentence, and a
  skipped job leaves its dependents free to run. Trained-to-ignore-red is a real cost;
  believed-to-be-proven is a larger one, and the one this repository has actually paid.
  **A missing lane input is a failure, never a skip.**
- **Run the lane as a container job inside the image** — this ADR's original mechanism,
  withdrawn 2026-08-27. It cannot be had: the demos start the development Postgres through
  Docker, and a container job gets no Docker daemon of its own. The `qemu:tools` wrapper
  answers the same problem on an ordinary runner and was not invented for it.

## Outside constraints

- **No `/dev/kvm` on GitHub runners**, so the guest boots under TCG — slower than the ~1.1 s
  measured on a developer machine, which is what the lane's `-timeout 15m` allows for.
- **The image exists only if `qemu.yml` has run.** That workflow is not part of the per-push
  gate: it fires on `Dockerfile.qemu`, `Taskfile.yml` and itself, because the build takes tens
  of minutes. A fresh clone has no image until it runs once.
