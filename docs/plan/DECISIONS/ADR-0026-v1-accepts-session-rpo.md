# ADR-0026 — V1 accepts an RPO of one session, and uploads at stop

- **Status:** **Accepted — 2026-08-02.** Nothing in it is implemented yet.
- **Date:** 2026-08-02
- **Deciders:** human owner (raised the question and must decide it), implementer agent
  (wrote it up)
- **Relates to:** §2 (use case and SLOs — this ADR **contradicts** its RPO row), §5.4,
  §5.5, §12 (fencing), §14.4, §14.8 (durability modes), §21.1, §22.3, INV-06, INV-07,
  INV-08, INV-13
- **Supersedes if accepted:** the `remote` durability mode as the default and, largely,
  as a mode at all

## Context

§2 declares the primary use case and the SLOs in the same section, and they pull in
opposite directions:

> **Caso de uso primario**: flotas de VMs de desarrollo, CI y entornos efímeros con
> clonado frecuente desde snapshots […]

> | RPO (writes cubiertos por FLUSH/FUA) | 0 bajo el modelo de fallas probado por DST |

Nobody asks a CI runner to survive the death of its host mid-job. But the RPO-0 row is
the assumption that generates the entire remote durability chain, and it has never been
checked against a stated requirement. §2 itself hedges elsewhere — fsync-heavy workloads
are "soportados pero **no el objetivo de optimización**" — which is the same tension
written down and left unresolved.

The proposal on the table is smaller than it sounds. It is **not** "remove the WAL". The
local WAL is what gives crash consistency, ordered replay and the read view a guest is
served from, and none of that involves an object store. It is:

> Make §14.8's `local` mode the only mode, and upload the volume once, at stop.

## The question, and its answer

**Has anyone ever asked for a VM to survive the loss of its host mid-session, or is that
an assumption of the design?**

**Answered by the human owner, 2026-08-02: no. "Es un wish, no un pedido."**

That is what makes this ADR accepted rather than proposed. §2's RPO-0 row was an
aspiration written next to a use case that does not need it, and it has been generating
half the system. It is now §2's *first paragraph* that governs — development, CI and
ephemeral fleets — and the SLO table that gets corrected.

It was asked rather than assumed because the last time this repository assumed an answer
instead of checking one, it produced three documents describing a lane no test performed
(DEV-0018).

## Decision (proposed)

1. **The FLUSH/FUA ACK contract for V1 is `fdatasync` only.** §14.8's `local` mode
   becomes the sole mode; `remote` is withdrawn rather than kept as an option, because a
   mode nobody selects is a second contract to keep correct for free.
2. **A volume is uploaded to the object store once, when it stops** — a clean shutdown, a
   detach, or an explicit snapshot. The object it produces is the volume's durable state
   and the anchor a later boot or clone starts from.
3. **The accepted RPO is one session.** A host that dies mid-session loses everything
   written since the volume attached. This is stated to the user, in the same place §2
   states the write-back contract.
4. **Fencing survives, at a fraction of the size.** See "What does not go away", below.
5. **The remote durability chain is deferred, not deleted from the design.** §14.4's six
   steps, checkpoints, mid-session recovery and cross-host materialization stay in the
   architecture document as the V2 path, and the code for them is removed from the tree —
   `git` is the archive, per `docs/plan/README.md`'s own rule.

## What this actually cuts

Measured, not estimated. Production lines, tests excluded.

**Survives untouched** — everything that makes a VM boot and run:

| | lines | why it is unaffected |
|---|---|---|
| `internal/vhost` | 2 822 | the transport to QEMU; knows nothing about durability |
| `internal/agent` (most) | 1 885 | the per-volume runtime, the socket, the reconciliation loop |
| `internal/blockdev` | 266 | the device the guest is served from |
| `internal/cow` | 230 | the read view |
| `internal/wal` (local half) | — | append, replay, the torn tail, segments, the view |

**Exists only because of the RPO-0 SLO:**

| | lines | what it is for |
|---|---|---|
| `internal/recovery` | 890 | reconstructing the durable point from S3 mid-session |
| `internal/gc` | 381 | collecting the orphans continuous upload creates |
| `internal/materialize` | 293 | moving a live volume between hosts |
| `internal/checkpoint` | 215 | truncating the local WAL mid-session |
| `internal/epoch` | 200 | two writers racing for one S3 prefix |
| `internal/lease` | 139 | making INV-06 true: no durable ACK without a valid lease |

Plus the remote half of `internal/wal` (uploader, `durableStep`'s S3 branch, the batcher's
role in it) and the fencing half of `internal/controlplane`. **On the order of half the
system, and the half that is expensive to reason about** — it is where every review-zone
increment, every fencing invariant and most of the DST harness live.

## What does not go away, and must not be assumed away

1. **Mutual exclusion at stop.** If two hosts boot the same volume and both upload when
   they stop, the second silently overwrites the first — a lost update with no error
   anywhere. This is the *only* part of fencing that survives, and it is much smaller
   than what exists: a compare-and-set on one object at stop time, rather than a lease
   renewed every three seconds gating every ACK. `internal/epoch` shrinks; it does not
   disappear.
2. **Snapshots — decided, 2026-08-02.** A snapshot of a *running* VM is an `fsync`
   followed by the upload of a copy. It does not stop the VM and it does not require the
   §14.4 chain: the guest's data is made durable locally, and one object is written that
   is complete enough for another VM to start from.

   **§19 survives this intact, and is what makes it work.** "A snapshot is a number, not
   an event": the source VM keeps writing immediately after the `fsync`, so the copy must
   be a frozen view rather than whatever the WAL holds when the upload finishes. A
   sequence number is exactly that frozen view, and it costs no pause — which is also how
   §2's "pausa de I/O por snapshot ~0" survives.

   What this removes from the old shape: a snapshot no longer has to be assembled from a
   checkpoint plus the WAL objects after it, because there are no checkpoints and no
   per-FLUSH objects. It is one copy of the volume as of one sequence number.
3. **The guest's `fsync` is not being lied to.** `fdatasync` is a real guarantee against
   an Agent crash, a QEMU crash and a guest crash. Only host loss is excluded. The promise
   narrows; it does not become false. This is worth stating plainly because it is the
   objection this ADR will attract first.

## Locality: the clone starts where the data already is

Raised with the acceptance, and it is not a new idea — **it is §20's placement rule 1**,
already written:

> ```
> 1. source_host con capacidad
> 2. host con snapshot cacheado (incluido el standby tibio)
> 3. cualquier host con capacidad
> ```
> Same-host: sin descarga; reutiliza EROFS, checkpoint y WAL cacheado.

Under this ADR that rule stops being an optimization and becomes most of the boot-time
story, because a cross-host clone now pays a full download with no warm standby and no
lazy loading to shorten it. A snapshot taken from a running VM leaves its data on that
host's local disk; starting the clone there is the difference between reading local NVMe
and pulling the volume from S3.

**`placement.Policy.Choose` already implements all three steps, `SourceHostID` first.**
What is missing is a caller: its only production caller is the drain
(`controlplane/drain.go`), and the clone path takes the host as a parameter instead of
asking the policy. That is the gap, and it is small.

Two things not to assume away:

- **It is a preference, not a constraint.** The source host can be full, cordoned or
  dead, and `Choose` already falls through to steps 2 and 3. Making same-host mandatory
  would couple scheduling to a host that has no obligation to be available.
- **The locality is time-bounded.** The source host holds the data only while it still
  holds the volume. Once the source VM stops and its local state is reclaimed, step 1 buys
  nothing and the clone pays the download — which is correct, and worth measuring before
  anyone promises a boot time.

Wiring the clone path to `placement.Choose` is its own increment, outside this ADR's
scope; recorded in `STATUS.md`.

## Alternatives considered

- **Keep both modes and default to `local`.** Rejected as the *primary* option: two ACK
  contracts is exactly the surface this ADR exists to remove, and the `remote` one would
  keep its lease, its epochs and its recovery path — which is the half the cut is for. It
  is, however, the obvious fallback if the question above is answered "yes for some
  volumes".
- **Keep the RPO-0 SLO and narrow the primary use case instead.** Coherent, and the
  correct answer if durable-across-host-loss is a product requirement. It means §2's first
  paragraph is wrong rather than its table.
- **Do nothing and decide later.** Rejected on cost: every increment built on the current
  premise makes the pivot more expensive, and this session added to that premise (§14.8's
  asynchronous drain). Deferring is not free and the price is paid in work that gets
  deleted.

## Consequences

- **Immediate:** the data-path cleanup increments are paused. Polishing code that may be
  deleted is the most expensive way to be wrong.
- The DST harness shrinks with the invariants it proves. INV-06, INV-08 and INV-13 are
  properties of the remote chain; INV-01, INV-02, INV-03, INV-04, INV-05, INV-15 and
  INV-18 are not and stay.
- The target slice is rewritten: "FLUSH uploads a verified object" is
  no longer the definition of done.
- **Reversal is not symmetric.** Removing the remote chain and re-adding it later is more
  than a revert: the fencing protocol is the part that is hard to get right, and it is
  currently correct and proven against planted bugs. This ADR trades a working
  implementation of a hard thing for a much smaller system, and that trade is only worth
  making if the hard thing is not needed. Which is the question at the top.

## What would reverse it

A customer requirement for a volume that survives host loss mid-session; or a workload
whose `fsync` frequency makes session-scoped RPO unacceptable in practice. Either turns
this into "V1 was the ephemeral tier" and the remote chain comes back — from git, and
against a real requirement rather than an assumed one.
