# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## The pivot, and what this tree is now

**The custom block storage engine is withdrawn (2026-08-11).** QEMU manages the local
copy-on-write format via qcow2, and this system manages only immutable commits,
publication to object storage, and recovery (`arquitectura_mvp_volumenes_remotos_v6.md`,
which replaced v5.1). The data path is not ours any more.

Deleted whole, in one commit: `internal/vhost` (+`hostio`), `internal/blockdev`,
`internal/wal`, `internal/cow`, `internal/image`, `internal/lineage`, `internal/agent`'s
volume manager and device budget, `integration/vhost`, `integration/guestinit`, and every
DST scenario, e2e scenario and metric whose subject was one of them. Nothing was deployed,
so this cost only the code.

**What survives is the control half, unchanged**: the catalog and its term guards
(`internal/metadata`), the RPC surface (`internal/cpserver`), election/provision/place
(`internal/controlplane`, `internal/placement`), the state machines (`internal/lifecycle`),
the lease, `internal/obs`, `internal/ids`, `internal/storecfg`, `internal/simio` — in
particular `simio/real`'s object store, whose cross-process `flock` CAS is the fencing
primitive the new design's `HEAD` compare-and-swap rests on — and `internal/descriptor` +
`internal/framed`, which are what a mutable-under-CAS `HEAD` and an immutable commit
manifest are already built out of.

## What runs end to end

Two binaries, as processes, in `integration/e2e`: a Control Plane is elected, an Agent
claims its data directory with a real `flock`, reads its KEK and registers; both binaries
reach the same key id from the same file; `-seed-volume` provisions a volume;
`-fleet-status` prints the host and the volume; both stop cleanly on a signal.

**The Agent serves no volumes, and says so on the way up.** It heartbeats, holds a lease,
learns its desired state and reports an empty set. `agent.VolumeReconciler` is the seam
the qcow2 manager plugs into.

## Do this next

v6 §23's stages, and this tree is standing at the start of the first one.

1. **qcow2 local, nothing remote.** Create, attach, restart, detach, local persistence,
   driven by QMP. It implements `agent.VolumeReconciler` — the seam is already there — and
   it takes the data-directory lock back from `cmd/volume-agent/main.go`, which holds it
   only because nothing else does. Increment 1 is a command a human runs that boots a
   guest off a qcow2 this system created.
2. **Rotation by QMP**, then **the commit protocol** (v6 §9, §12): `HEAD` as the one
   mutable object under compare-and-swap, commit manifests immutable and create-only, each
   layer sealed with the volume's DEK on the way out (v6 §10 — which is why
   `crypto.NewEncryption` was carried across rather than deleted). Both are review zones
   from the first line. The commit protocol's DST arm has the shape of the deleted
   `two-hosts-cannot-both-publish-an-image`, and the primitive under it is already proven
   by `task backend:conformance`.

## What the demolition left owed

- **`hack/deadcode-pending.txt` has three entries**, all created by this commit and all
  waiting on the two stages above: `agent.Loop.VolumeKeys` and its cache (the client half
  of the surviving `GetVolumeKeys` RPC, whose caller was the volume manager), and
  `crypto.NewEncryption` (carried out of `internal/wal` deliberately — the commit protocol
  seals each layer with the volume's DEK). If Stage 1 and Stage 2 do not take them, they
  are deletions.
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
- **`CLAUDE.md`'s Taskfile list still names `task build:guest` and `task guest:verify`**,
  which went with the initramfs and the guest lane. Two lines, and nothing checks them —
  `task workflows:verify` reads the workflows, not this file.

## What only a pilot can answer

Named here so nobody mistakes them for things that were checked.

- **Nothing has run against real S3.** Every lane used the filesystem store or RustFS. The
  `If-Match` CAS is ours; S3 evaluates its own, and that path has never carried a publish.
- **An upgrade of a running fleet** has never been run; INV-19 becomes binding exactly there.

## Thin paths that shipped without being deepened

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
