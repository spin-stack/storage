# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

**The custom block engine is withdrawn (2026-08-11).** QEMU manages the local copy-on-write
format via qcow2; this system manages immutable commits, publication and recovery
(v6). The data path is not ours any more.

## What runs end to end

Six demonstrations, each with the real binaries and a real Linux guest. `demo:stage1`: a
volume is provisioned, a guest boots off its qcow2, the Agent is SIGKILLed and restarted
**under the running guest**, and a second boot reads the bytes back. `demo:stage2` adds
rotation — the guest writes without stopping while the Agent seals the tip and starts a new
layer under it, three times. `demo:stage3` adds the commit protocol and reads the bucket
back with `cat`: the chain from `HEAD` is walked to the first commit, checking each layer is
present, matches its digest, and carries none of the guest's bytes in the clear.

`demo:stage6` is §20: a snapshot is cloned and a second guest boots the clone and reads
back the bytes the parent's guest wrote, opened with the parent's key binding. `demo:stage5`
is §19 under v6: a snapshot is a *name for a commit*, asked for with the real binary while a
guest writes, and the catalog names a commit on the chain from HEAD.

`demo:stage4` closes v6 §26's cycle: it destroys the host — process killed, data directory
deleted — and a rebuilt machine brings the volume back from the bucket alone, a guest
reading bytes another guest wrote on a host that no longer exists. It also destroys the
volume's `HEAD` and shows the rebuilt machine refusing rather than serving a blank disk:
"no HEAD" means either a new volume or a lost one, and only the catalog knows which
(`volumes.head_commit_id`).

All six run in CI (`ci.yml`'s `guest` job), under TCG, from the mirrored kernel and the
published QEMU. `integration/e2e` covers the same seams without a guest; the root diagrams
are generated from the `.dot` beside each one and `task diagrams:check` fails on a stale one.

**The Agent does not launch QEMU.** It prepares the chain and speaks QMP to whatever is at
the socket; spin's runner runs the VMs (ADR-0021). The contract is `qcow.QMPSocket` and
`qcow.ActivePointer` — the path of the layer to launch against, because rotation makes the
tip a different file every time. Either launcher shape works: `-drive ...,if=virtio` names
the disk by a generated drive id, `-blockdev node-name=…` by its node, and `demo:stage2`
boots the second so both are proven by something that runs.

## What a lapsed lease does

**A guest is stopped only on confirmed supersession.** Three facts confirm it, and each is
somebody who knows saying so: the Control Plane refusing this host's report, the
compare-and-set on HEAD losing, and `volumes/<id>/epoch` recording a higher epoch than the
one this host holds. The last is read over the object store — a path that does not run
through the Control Plane, so it is still there when the Control Plane is not — and is the
second signal vSphere HA gets from its datastore heartbeat.

A lease that merely lapsed is *not* one of the three, and it used to be — v5's rule, right
there, where the Agent was the data path. v6 removed the premise: a FLUSH claims local
durability only, and nothing a superseded host writes enters the history without winning a
CAS it cannot win. What was left was the cost alone, a tenant's VM stopped for a partition
nobody else had acted on.

**Still unresolved, and now the whole of it:** a host that can reach neither the Control
Plane nor the object store cannot tell isolation from supersession. It keeps serving. The
data is safe either way, but nothing stops the successor's guest from starting, and this
system has no equivalent of vSphere's datastore lock.

## Do this next

Nothing on the §24 list. What is left is in the two sections below.

## What only a pilot can answer

- **No commit has been published to real S3.** The lanes use the filesystem store or
  RustFS; `task backend:conformance` stands between those and S3's own `If-Match`. And an
  upgrade of a running fleet has never been run; INV-19 becomes binding there.

## Thin paths that shipped without being deepened

- **No RPO target is set anywhere.** The age trigger is in and per-volume
  (`volumes.rpo_target_seconds` → `DesiredVolume`), and what a host measures against it now
  reaches the catalog; every volume carries a target of zero, because a target is a promise
  sold to a tenant and nothing here has sold one. `task measure:publish` says what the floor
  under any such promise is on a given backend. `-seed-rpo-seconds` sets one.
- **A busy volume that never detaches walks towards the chain ceiling.** Rotation stops at
  `qcow.MaxLayers` rather than building a chain nothing can rebuild, and the tip then grows
  instead — the RPO degrades, visibly, in the pair §11 asks be watched. Only a collapse
  undoes it, and a collapse needs the guest to let go of the files.
- **A collapse waits for the guest to detach.** The rebase onto the new root needs the
  files, and QEMU holds every one of a live chain, so the last step is owed until the
  volume reads as unattached. Until then the chain does not shrink and one line says so.
- **A volume detached and left detached keeps its layers.** A volume releases its claim
  when the object store confirms the fleet granted it elsewhere (`demo:stage4` step 10) and
  never on silence, so a volume nobody has moved is never released — and neither is one
  that was deleted, because absence is not a fact. What frees a file is one rule: no volume
  on this host names it — so an interrupted rotation, whose layer is recorded before the
  pointer names it, leaves ~190 KiB the sweep is not allowed to overrule.
- **§21's numbers reach a human, not an alert.** Eleven are recorded and scrapeable; the
  RPO and the unpublished backlog reach the catalog and `-fleet-status` per volume. §11's
  other half is built — a host holding a layer it failed to publish is cordoned — but
  nothing else reaches an alert, and nothing measures attachment.
- **Deleting a clone is a removal and not a shred**, by contract (§10: a lineage shares one
  DEK). `DeleteVolume` reports which it did; a real shred still rests on the bucket
  expiring the descriptor's non-current versions.
- **No alerting, bucket lifecycle, tracing, PITR rehearsal, or `/healthz` on the CP** (whose
  term is a closure over a constant). The Connect API is unauthenticated on `:8080`, with
  `GetVolumeKeys` on it.

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
