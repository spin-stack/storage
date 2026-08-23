# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## The pivot, and what this tree is now

**The custom block storage engine is withdrawn (2026-08-11).** QEMU manages the local
copy-on-write format via qcow2, and this system manages only immutable commits,
publication to object storage, and recovery (`arquitectura_mvp_volumenes_remotos_v6.md`,
which replaced v5.1). The data path is not ours any more.

What survives is the control half, unchanged — the catalog and its term guards, the RPC
surface, election/provision/place, the lease — plus the two pieces the new design is built
out of: `simio/real`'s object store, whose cross-process `flock` CAS is what a `HEAD`
compare-and-swap rests on, and `internal/framed` + `internal/descriptor`. What went, and
what it cost, is one commit in `git log`.

## What runs end to end

`task demo:stage1`, with the real binaries and a real Linux guest: a Control Plane is
elected, an Agent claims its data directory and registers, `-seed-volume` provisions a
volume, the Agent prepares its qcow2 chain, a guest boots off that file and writes, the
Agent is SIGKILLed and restarted **under the running guest**, the guest is told to stop
and powers off, and a second boot reads the bytes back. `integration/e2e` covers the same
seam without a guest, and `task test:e2e` now needs the pinned `qemu-img` (`task
qemu:tools`) because the Agent refuses to start without one.

**The Agent does not launch QEMU.** It prepares the chain, owns the volume's directory,
and speaks QMP to whatever is at the socket — spin's runner is what runs the VMs
(ADR-0021), and v6 §4/§7 give the Agent control over QEMU, not its lifetime. The contract
is two paths, `qcow.ActiveImage` and `qcow.QMPSocket`, and it is stated where they are
defined.

## Do this next

v6 §23's stages. The first has a thin path; everything below it is unbuilt.

1. **Stage 1's depth** (increment 2): a DST scenario for the reconciler, and the two
   sentences nothing yet proves — that a *detach* stops the volume for a running guest,
   and what happens to a chain whose directory is gone under it. `state.json` from v6 §5
   is deliberately not written: nothing reads it while a chain is one link long.
2. **Rotation by QMP** (v6 §23.2), whose exit criterion is measured — the snapshot pause
   and the upload throughput. `internal/qmp` is the transport it needs and is already
   driven by a real QEMU in the demo.
3. **The commit protocol** (v6 §9, §12): `HEAD` as the one mutable object under
   compare-and-swap, commit manifests immutable and create-only, each layer sealed with
   the volume's DEK on the way out (v6 §10 — which is why `crypto.NewEncryption` was
   carried across rather than deleted). A review zone from the first line. Its DST arm has
   the shape of the deleted `two-hosts-cannot-both-publish-an-image`, and the primitive
   under it is already proven by `task backend:conformance`.

## What the demolition left owed

- **`hack/deadcode-pending.txt` still has three entries**, and Stage 1 was never going to
  take them: v6 §10 leaves the local qcow2 in cleartext and seals a layer only on the way
  out, so nothing local needs a DEK. `agent.Loop.VolumeKeys` and its cache, and
  `crypto.NewEncryption`, are owed to the commit protocol or owed a deletion.
- **`internal/dst` has three scenarios and one checker.** The harness is intact and is the
  point: the commit protocol needs exactly this — seeded, crash-at-every-point,
  same-seed-same-trace — with a new subject. `mandatory_set_test.go` records what left and
  why.
- **`RebuildMetadata` restores volumes only.** Snapshots and published sequences were read
  out of the chunked image's manifests; a rebuilt volume now comes back with zeroed
  sequences and no parent link, and the commit protocol is what restores both.
- **`cpserver` records a published snapshot with no manifest key**, for the same reason.
- **There is one merge gate again** (`task ci:full`); `ci:noguest` and the REQUIRE_PROOFS
  mechanism went with the guest-backed lanes they arbitrated. `task build:qemu` and
  `task fetch:kernel` are still here and still pinned, because Stage 1 needs both.

## What only a pilot can answer

Named here so nobody mistakes them for things that were checked.

- **Nothing has run against real S3.** Every lane used the filesystem store or RustFS. The
  `If-Match` CAS is ours; S3 evaluates its own, and that path has never carried a publish.
- **An upgrade of a running fleet** has never been run; INV-19 becomes binding exactly there.

## Thin paths that shipped without being deepened

- **Stage 1 has no DST scenario and no checker**, by the gate's own rule for a first
  increment. What exists is unit tests over `internal/qcow`/`internal/qmp` with the
  process and the socket injected, plus the demo and one e2e assertion.
- **A volume is never deleted locally.** Releasing one leaves its image on disk, because
  nothing has decided who reclaims it; the device fills up and nothing sweeps.
- **Nothing measures the chain.** No metric was added — depth, size, attachment — so the
  only observation of a volume's state outside the Agent is its log and the catalog's
  refusal column.
- **No bucket lifecycle is configured by any code**, so a delete marker is reversible for as
  long as nobody sets one — which is the whole recovery window.
- **No alerting artifact exists** (no rules file, no threshold comparison in code) —
  including for `volumes.refusal`, the fleet's one machine-readable "this volume is down".
- **No distributed tracing.** Metrics reach a collector over OTLP; `request_id` correlates
  nothing.
- **No `/healthz` on the Control Plane**, and one that lost its term serves broken forever
  (the term is a closure over a constant; there is no renew loop).
- **The Connect API is unauthenticated and binds `:8080`**, with `GetVolumeKeys` on it.
- **Crypto-shred is partial**: deleting a volume drops both copies of the wrapped DEK, but a
  lineage shares one DEK and `crypto.KMS` has no destroy verb.
- **PITR has no artefact** — no tooling, no config, no rehearsal.

## Divergences (DEV entries)

The ratchet is `hack/dev-entries.sh` + `hack/dev-entries-open.txt`, and it turns one way: an
entry open here and unpinned there fails the gate, and a pin whose entry closed fails it too.
**0 open.** Resolved ones keep a struck heading so the check cannot pass vacuously; the
reasoning is in `git log`.

## ~~DEV-0007~~ — the spine's second half *(withdrawn 2026-08-03)*
## ~~DEV-0011~~ — a segment's space is charged as used, not reserved *(2026-08-08: accepted, not fixed)*
## ~~DEV-0012~~ — a self-fenced log still accepts WRITEs *(2026-08-02: not a divergence)*
## ~~DEV-0019~~ — a restarted encrypted volume served its guest ciphertext *(2026-08-02)*
## ~~DEV-0020~~ — a clone chain is flattened by copying, and nothing decided that *(2026-08-08)*
## ~~DEV-0021~~ — the recovery-point floor was fictional *(2026-08-02, by deletion)*
## ~~DEV-0022~~ — the §26.2 catalog described a withdrawn system *(2026-08-03)*
## ~~DEV-0023~~ — the design document still had resize *(2026-08-07: V2)*
## ~~DEV-0024~~ — the 64 KiB CoW granularity is retired *(2026-08-08)*
## ~~DEV-0025~~ — a manifest could not meet §25.2's bit-corruption half *(2026-08-08: framed)*
