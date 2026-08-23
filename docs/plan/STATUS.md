# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## The pivot, and what this tree is now

**The custom block storage engine is withdrawn (2026-08-11).** QEMU manages the local
copy-on-write format via qcow2, and this system manages only immutable commits,
publication to object storage, and recovery (`arquitectura_mvp_volumenes_remotos_v6.md`,
which replaced v5.1). The data path is not ours any more.

## What runs end to end

`task demo:stage1`, with the real binaries and a real Linux guest: a Control Plane is
elected, an Agent claims its data directory and registers, `-seed-volume` provisions a
volume, the Agent prepares its qcow2 chain, a guest boots off that file and writes, the
Agent is SIGKILLed and restarted **under the running guest**, the guest is told to stop
and powers off, and a second boot reads the bytes back.

`task demo:stage2` adds rotation: the guest writes without stopping while the Agent seals
the tip and starts a new layer under it, three times, and a second boot reads every byte
back through the four-layer chain.

`task demo:stage3` adds the commit protocol: each sealed layer is uploaded, given an
immutable manifest and named by a compare-and-set on `HEAD`. The bucket is then read back
with `cat` — every structural object is a digest line and JSON — walking the chain from
`HEAD` to the first commit and checking each layer is present, matches its recorded digest,
and carries none of the guest's bytes in the clear. `integration/e2e` covers the same seams
without a guest; `task test:e2e` needs the pinned `qemu-img` (`task qemu:tools`).

**The Agent does not launch QEMU.** It prepares the chain and speaks QMP to whatever is at
the socket; spin's runner runs the VMs (ADR-0021). The contract is `qcow.QMPSocket` and
`qcow.ActivePointer` — a file holding the path of the layer to launch against, because
rotation means the tip is a different file every time.

## Do this next

v6 §23's stages. The first has a thin path; everything below it is unbuilt.

1. **Recovery** (v6 §14, §23.4): rebuild a volume on a host that has never seen it, from
   PostgreSQL and the bucket alone — read `HEAD`, walk `parent_commit_id`, download and
   verify each layer, rebuild the chain, create a new tip. `commit.Fetch` is the half that
   exists and is allow-listed until this calls it. It is also what closes
   `RebuildMetadata` and `cpserver`'s empty `manifest_key` below.
2. **The age trigger and §11's defaults.** The size trigger and "nothing rotates while a
   sealed layer is unpublished" are in; `rpo_target` on `DesiredVolume` and a commit fired
   by age are not, and the upload-throughput half of the measurement is now possible.
3. **Depth for stages 1 and 2**: a DST scenario for the reconciler and for rotation, and
   the two sentences nothing yet proves — that a *detach* stops the volume for a running
   guest, and what happens to a chain whose directory is gone under it.

## What the demolition left owed

- **`hack/deadcode-pending.txt` is empty.** The three entries the demolition owed —
  `agent.Loop.VolumeKeys`, its cache, and `crypto.NewEncryption` — are all reached by the
  publish path.
- **`internal/dst` has four scenarios and two checkers**, one of them
  `effective-single-writer`, back with a behavioural planted bug: an object store whose
  conditional writes are advisory.
- **`RebuildMetadata` restores volumes only**, and **`cpserver` records a published
  snapshot with no manifest key** — both because snapshots were read out of the chunked
  image's manifests. Recovery is what restores them.

## What only a pilot can answer

Named here so nobody mistakes them for things that were checked.

- **No commit has been published to real S3.** The lanes use the filesystem store or
  RustFS; `task backend:conformance` is what stands between those and S3's own `If-Match`.
- **An upgrade of a running fleet** has never been run; INV-19 becomes binding exactly there.

## Thin paths that shipped without being deepened

- **Stages 1 and 2 have no DST scenario**, by the gate's own rule for a first increment.
  What exists is unit tests with the process and the socket injected, plus the demos.
- **Rotation and publishing have no production default.** `-rotate-at-bytes` is 0 and no
  object store is required, so an Agent started without both seals nothing and publishes
  nothing — loudly, in one WARN line. v6 §11 forbids choosing the threshold instead of
  measuring it, and the measurement is not finished.
- **A whole sealed layer is held in memory to publish it.** `objectstore.Store` takes a
  `[]byte`. At the sizes rotation produces (32 MiB measured) that is a buffer; an order of
  magnitude more and it is an OOM in a process holding somebody's disk. The fix is a
  streaming PUT on the store interface.
- **A restart between sealing and publishing duplicates a commit.** The commit id is minted
  in memory and reused across retries, so retries are exact; a restart mints a new one and
  publishes the same layer twice. It is a duplicate entry in a history, not a loss, and
  closing it needs v6 §5's `state.json`, which arrives with recovery.
- **Nothing deletes a published layer from local disk.** v6 §9's step 15 has no code.
- **A layer's size is a floor, not a bound** — measured at 8x the threshold with a 300 ms
  cycle. Nothing can hold a layer to a size while QEMU takes the guest's writes. Stated at
  `qcow.Config.RotateAtBytes`.
- **Nothing reclaims local disk.** A released volume keeps its layers; a published layer
  keeps its file (v6 §9 step 15); nothing sweeps orphaned overlays from a rotation that
  was interrupted. The device fills and nothing notices.
- **Nothing measures the chain or the commits.** No metric for depth, size, attachment,
  `unpublished_local_bytes` or `last_successful_commit_age` — which v6 §11 calls the
  product. The only observation outside the Agent is its log and the refusal column.
- **No alerting artifact, no bucket lifecycle, no distributed tracing, no PITR rehearsal**,
  and no `/healthz` on the Control Plane (whose term is a closure over a constant, with no
  renew loop).
- **The Connect API is unauthenticated and binds `:8080`**, with `GetVolumeKeys` on it.
- **Crypto-shred is partial**: deleting a volume drops both copies of the wrapped DEK, but a
  lineage shares one DEK and `crypto.KMS` has no destroy verb.

## Divergences (DEV entries)

The ratchet is `hack/dev-entries.sh` + `hack/dev-entries-open.txt`. **0 open.** Resolved
ones keep a struck heading so the check cannot pass vacuously; the reasoning is in `git log`.

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
