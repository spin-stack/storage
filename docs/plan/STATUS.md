# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## The pivot, and what this tree is now

**The custom block storage engine is withdrawn (2026-08-11).** QEMU manages the local
copy-on-write format via qcow2; this system manages immutable commits, publication and
recovery (`arquitectura_mvp_volumenes_remotos_v6.md`). The data path is not ours any more.

## What runs end to end

Three demonstrations, each with the real binaries and a real Linux guest. `demo:stage1`:
a volume is provisioned, a guest boots off its qcow2, the Agent is SIGKILLed and restarted
**under the running guest**, and a second boot reads the bytes back. `demo:stage2` adds
rotation — the guest writes without stopping while the Agent seals the tip and starts a
new layer under it, three times. `demo:stage3` adds the commit protocol, and reads the
bucket back with `cat`: every structural object is a digest line and JSON, so the chain
from `HEAD` is walked to the first commit, checking each layer is present, matches its
recorded digest, and carries none of the guest's bytes in the clear.

`demo:stage4` closes v6 §26's cycle: it destroys the host — the process killed and the
data directory deleted — and a rebuilt machine brings the volume back from the bucket
alone, with a guest reading bytes another guest wrote on a host that no longer exists.
`integration/e2e` covers the same seams without a guest; `task test:e2e` needs the pinned
`qemu-img` (`task qemu:tools`).

**The Agent does not launch QEMU.** It prepares the chain and speaks QMP to whatever is at
the socket; spin's runner runs the VMs (ADR-0021). The contract is `qcow.QMPSocket` and
`qcow.ActivePointer` — a file holding the path of the layer to launch against, because
rotation means the tip is a different file every time.

## The open question: what a lapsed lease should do

**Fencing stops the guest, and all three ways in are treated the same.** They are not the
same fact. A Control Plane that refused this host's report, and a compare-and-set that
lost, are somebody else having taken the volume. A lease that lapsed on this host's own
monotonic clock says only that the Control Plane is unreachable, and stopping a guest over
it costs a tenant their VM for a partition nobody else acted on.

Comparable systems enforce the fence *at the resource* rather than asking the writer to
stop — Ceph blocklists the client at the OSDs, SCSI-3 reservations are enforced by the
array — and that fence already exists here: the CAS on `HEAD` plus the epoch check mean
nothing a fenced host writes can enter the published history. vSphere HA is the closest
analogue and refuses to act on one signal: *isolated* when the network stops, *dead* only
when the datastore heartbeat agrees. The object store is that second path, and
`volumes/<id>/epoch` moves at the grant rather than at the first publish.

It was implemented once and reverted, and the reason is the useful part: §12.2's "a lapsed
lease gives the device up" has three tests protecting it, and each variant that satisfied
one broke another — they encode the rule being changed. It wants to be its own increment,
with those tests as its subject. Genuinely unresolved: a host cut off from the Control
Plane *and* the object store cannot tell isolation from supersession, and this design has
no equivalent of vSphere's datastore lock to stop the successor's guest from starting.

## Do this next

1. **What a lapsed lease should do** — the section above. It is first because it is the
   only open question that decides whether a guest is stopped.
2. **The age trigger and §11's defaults.** The size trigger and "nothing rotates while a
   sealed layer is unpublished" are in; `rpo_target` on `DesiredVolume` and a commit fired
   by age are not, and the upload-throughput half of the measurement is now possible.
3. **DST for recovery and rotation.** Four scenarios and two checkers exist; neither the
   reconciler nor the rebuild has one. Also unproven: that a *detach* stops a running
   guest's volume, and what a chain whose directory vanished under it does.

## What the demolition left owed

- **`hack/deadcode-pending.txt` is empty**, and `cpserver` still records a published
  snapshot with no manifest key — the one piece recovery did not bring back.

## What only a pilot can answer

- **No commit has been published to real S3.** The lanes use the filesystem store or
  RustFS; `task backend:conformance` is what stands between those and S3's own `If-Match`.
- **An upgrade of a running fleet** has never been run; INV-19 becomes binding there.

## Thin paths that shipped without being deepened

- **Rotation and publishing have no production default.** `-rotate-at-bytes` is 0 and no
  object store is required, so an Agent started without both seals nothing and publishes
  nothing — loudly, in one WARN line. §11 forbids choosing the threshold instead of
  measuring it, and the measurement is not finished. A layer's size is a *floor* anyway,
  not a bound: measured at 8x the threshold with a 300 ms cycle, and nothing can hold a
  layer to a size while QEMU takes the guest's writes.
- **A whole sealed layer is held in memory to publish it.** `objectstore.Store` takes a
  `[]byte`. At 32 MiB that is a buffer; an order of magnitude more and it is an OOM in a
  process holding somebody's disk. The fix is a streaming PUT on the store interface.
- **A restart in the microseconds between sealing and publishing duplicates a commit id.**
  The layer is derived from the chain and never lost; only the id is.
- **Nothing reclaims local disk.** A released volume keeps its layers, a published layer
  keeps its file (§9 step 15), and nothing sweeps the orphan overlay an interrupted
  rotation leaves. A sweep needs a `List` on `qcow.Paths`, which does not exist.
- **Nothing measures the chain or the commits.** No metric for depth, size, attachment,
  `unpublished_local_bytes` or `last_successful_commit_age` — which §11 calls the product.
  The only observation outside the Agent is its log and the refusal column.
- **Deleting a clone is a removal and not a shred**, by contract (§10: a lineage shares one
  DEK). `DeleteVolume` returns which it did; even a real shred rests on the bucket's
  lifecycle policy expiring the descriptor's non-current versions.
- **No alerting artifact, no bucket lifecycle, no tracing, no PITR rehearsal, no `/healthz`
  on the Control Plane** (whose term is a closure over a constant). The Connect API is
  unauthenticated on `:8080`, with `GetVolumeKeys` on it.

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
