# ADR-0014 — A volume's quota charges what it wrote and what its snapshots retain, and space is reclaimed by squash

> **Withdrawn by ADR-0026 (2026-08-03).** Squash is compaction, compaction went with
> objectization, and the per-lineage accounting it describes has no producer: there are no
> segment objects and no GC. `internal/image` content-addresses chunks by their plaintext
> digest, which is the one piece of this ADR that shipped — a snapshot of an unchanged
> volume uploads nothing — but it does so without a quota, a lineage ledger or a squash.
>
> **The distinction it drew is the part to keep**: a quota is a soft allocation control
> and never fails a guest write, while the hard limit is ADR-0013's device budget, which
> is physical and returns ENOSPC. That separation is still correct and still unimplemented.

- **Status:** Accepted 2026-07-26 (the four open questions are answered in the second
  amendment; implementation waits on Phase 12 for squash and on DEV-0007 for anything
  guest-facing)
- **Date:** 2026-07-26
- **Deciders:** human (to decide), implementer agent (proposes)
- **Implements/Extends:** §3 (volume model, grow-only resize), §19 (snapshots), §20
  (clone / read chain), §21.1–21.3 (publication, GC marks only), §22.5
  (rebuild-metadata), Phase 12 (objectization/compaction), INV-14, INV-16.
- **Related:** ADR-0013 (the *device* budget on a host — a different question with a
  similar name), ADR-0012 (GC anchors).

## Amendment 2, 2026-07-26 — the four open questions, answered

**1. The quota is soft. The device budget is the hard one.**

A quota is an *allocation control*: it makes consumption predictable and it is what an
operator sets when creating a disk. It never fails a guest write. The condition that
must never happen — a node with no space — is physical, and its defence is ADR-0013's
device budget, which is hard and which returns ENOSPC to the guest. Two limits, two
jobs, and the guest only ever sees the physical one.

This simplifies the design considerably: quota accounting leaves the data path
entirely. It is computed at publish, compaction and snapshot deletion, so it may lag
without endangering anything, and no write ever waits on it.

Soft has to mean something, though, or it is not a limit. Over quota:

- snapshot creation, clone, resize and new volumes on the lineage are refused
  (`ErrQuotaExceeded`);
- the lineage is flagged for the operator, with the overage as a number rather than a
  boolean;
- the CP may **shrink the lineage's device share** (ADR-0013) rather than fail anything
  — backpressure, which the guest experiences as slower FLUSHes, not as an error.

What it deliberately does not do is stop a runaway writer. That is the device budget's
job, and pretending otherwise would put a billing control on the durability path.

**2. The base image is free, and stays free.**

The inherited set — the objects of the snapshot a lineage was cloned from, when that
snapshot belongs to a *different* lineage — is never charged to the clone. That is the
point of it: reuse of a golden image is what makes consumption predictable, so
predictability is what the rule protects. A base cloned from a snapshot inside the same
lineage is charged normally, because it is the lineage's own data.

Revisit only if a base image is ever charged to nobody — today it is charged to the
lineage that owns it, and that lineage cannot be deleted while a clone references it
(§5 below).

**3. Snapshot identity is content-addressed.** Confirmed; see amendment 1 below for the
mechanism and what it costs.

**4. The quota is per lineage, not per volume.**

A lineage is the transitive closure of clone-from-snapshot: the root volume, every clone
of it, and every clone of those. `volumes.lineage_id` is set at create — its own id for
a root, inherited on clone — and the quota and the charge live on the lineage, not on
the volume:

```
charged(L) = Σ unique physical bytes written by the members of L
           − the inherited set (a base image from another lineage)
```

Two things fall out of this, and both are why it is the right unit:

- **The "who pays when the parent is deleted" problem disappears.** §5 below spends
  three options on it; with a lineage charge the bytes belong to the lineage regardless
  of which member is deleted, and no re-charging is needed.
- **Content addressing makes the dedup exact.** Once identity is a content hash
  (amendment 1), "unique bytes across the lineage" is a set union over hashes rather
  than an estimate — the same change pays for itself twice.

The cost is that one runaway volume can exhaust its siblings' room. That is what a
tenant-level limit means, and it is visible: the overage names the lineage, and the
per-volume contribution is a sum the CP can already compute.

### Schema shape

A `lineages` table (`lineage_id`, `quota_bytes` NULL = unlimited, `charged_bytes`,
term-guarded like every CP mutation) and `volumes.lineage_id NOT NULL` with an index —
it is a FK-referencing column, so the schema's own rule requires one. `charged_bytes`
stays a cache that `rebuild-metadata` recomputes from S3, with the recompute-equals-
stored test that keeps every other cached number here honest.

## Amendment 1, 2026-07-26 — the snapshot digest resolves in favour of content, and INV-16 already said so

The crux flagged below ("a published snapshot's manifest is immutable and its digest
covers key strings, so squash cannot touch what a snapshot names") turns out to be a
gap between an invariant and its implementation, not a conflict between two
requirements. **INV-16 already permits this**, in its own words:

> Compaction may replace objects with byte-for-byte logically-equal ones, never alter
> referenced logical content.

The invariant constrains the *logical content* of a published snapshot. What contradicts
squash is `snapshot.Digest(target, objects)` — a hash over the object **key list** — so
replacing an object with a logically-equal one breaks `DigestMatches` even though INV-16
explicitly allows the replacement. The invariants table has said the compaction arm
lands in Phase 10/12 since it was written; this is that arm.

### Resolution: separate what a snapshot *is* from where its bytes *live*

- **Identity is content.** `root_digest` becomes a Merkle root over the snapshot's
  extent map — `(offset, length) → content hash` — not over key strings. Two byte-equal
  snapshots have one digest whatever objects hold them, and squash *proves* it preserved
  the snapshot: reconstruct from the new segments, recompute, and the digest must be
  unchanged. That is a verification, not a hope, and it is the thing a manifest full of
  key strings could never give.
- **Placement is a generation.** Where each extent currently lives moves to a location
  index published create-only per generation. Squash writes segments, writes the next
  generation, and advances the pointer; a crash leaves an unreferenced generation the GC
  collects. The current generation is found **by deterministic key** — the same
  per-volume index the ADR-0012 amendment puts into Phase 12's format, which is now
  carrying its third job and should be designed once.
- **INV-16 keeps its wording and gains its checker.** `ImmutableSnapshotChecker` must
  compare the *content digest* across a run, not the object list; today it would pass a
  squash that silently dropped an extent and fail a squash that was perfectly correct.
  Both halves of that are wrong.

### What it costs

- **The manifest format changes in place.** There is no v2 and no migration: nothing is
  deployed, no bucket holds a snapshot anyone will ever read, and inventing a
  compatibility story for zero users would buy a permanently unsquashable class of
  snapshot in exchange for nothing. The manifest gains the extent map and loses the
  object-key digest, and the tests change with it. (See "Formats before the first
  deployment" in CLAUDE.md; ADR-0005 set the precedent by correcting the WAL header to
  104 bytes rather than versioning around the error.)
- **The pointer is the one mutable object** in a create-only design. It advances by CAS
  on a generation that only increases (the same shape as the epoch object, §12.4), so a
  lost response is re-resolvable and two squashers cannot both win.
- `rebuild-metadata` must recompute the current generation from S3, like every other
  cached number here.

Everything below stands; open question 3 is answered by this amendment.

## Context

A volume today has one size: `volumes.size_bytes`, the provisioned capacity a guest
sees, grow-only. Nothing bounds what a volume *costs in the bucket*, and the two are
not the same number by a wide margin: a 100 GiB volume whose guest rewrites the same
10 GiB every hour produces unbounded WAL objects until compaction, and ten snapshots of
it retain ten versions of every block that changed between them.

The requirement is:

- a limit set **when the disk is created**;
- the **base image does not count** — a volume cloned from a 40 GiB golden image starts
  at zero, not at 40 GiB;
- **snapshots count against it** — a 100 GiB limit with ten 10 GiB snapshots is full;
- and when something is deleted, the space has to actually come back, which needs
  **squash**: consolidating the chain so superseded bytes stop being stored.

The last one is the part that does not exist at all today. The GC marks *unreachable
objects*; it cannot reclaim a superseded **block inside** a reachable object, and after
a snapshot is deleted most of the garbage is exactly that.

## Decision (proposed)

### 1. What is charged: unique bytes this volume's lineage added

Charge the **unique physical bytes attributable to the volume's own writes**, measured
after compaction, excluding every object the volume inherited.

```
charged(V) = Σ physical bytes of objects reachable from V's own chain
           − Σ physical bytes of objects reachable from V's clone origin manifest
```

Concretely, at clone time the parent manifest's object set is recorded as the volume's
**inherited set**, and it is never charged to V — it is charged to whoever owns the
snapshot it came from. Anything written afterwards is V's, whether it is live or
retained only by a snapshot.

Two numbers are kept, because they answer different questions:

| Column | Meaning | Who moves it |
|---|---|---|
| `quota_bytes` | the limit, set at create; NULL = unlimited | the operator, at create/resize |
| `charged_bytes` | unique physical bytes charged now | the CP, on publish/compaction/snapshot delete |

`charged_bytes` is an accounting cache, never the authority: like every other number in
this system it must be recomputable from S3 (§22.5), and `rebuild-metadata` recomputes
it by summing the object sizes reachable from the volume's chain minus its inherited
set. A test asserting recompute == stored is the one that keeps it honest.

### 2. Snapshots are charged to the volume that took them

A snapshot retains the blocks its manifest names that no later state still uses. Those
bytes are V's. This gives the requested semantics directly: ten 10 GiB snapshots of a
volume with a 100 GiB quota leave nothing, and deleting one returns its *exclusive*
bytes — the blocks only it retained — not its full size, because blocks shared with
another snapshot are still retained by that one.

**Shared blocks are charged once.** Anything else double-counts the common case (three
snapshots over a mostly-static volume) and makes the number useless.

### 3. Where the limit is enforced, and what the failure looks like

Three gates, each with a different audience:

- **Snapshot creation** — refused when the snapshot's *new* retained bytes would exceed
  the quota. This is the cheap, exact gate: an operator asking for a snapshot gets a
  clear `ErrQuotaExceeded`, and it is where the "10 × 10 GiB and you are done" rule
  actually lives.
- **Publication / compaction** — the CP refuses to publish a segment that would push a
  volume past its quota, and the volume enters a typed `QUOTA_EXCEEDED` degradation
  (the `wal.Degradation` vocabulary from wave 3 is the right shape).
- ~~**The guest write path**~~ — **withdrawn by amendment 2.** The quota is soft and
  never fails a guest write; the only ENOSPC a guest sees comes from the device budget
  (ADR-0013). Keeping a billing control on the durability path was the wrong trade.

Soft/hard: the guest write path is refused only at the hard limit; snapshot creation is
refused at a soft limit (proposal: 90%), so a volume cannot be pushed into a state where
the only way out — a snapshot to clone from, or a compaction — is itself refused.

### 4. Squash: reclaiming superseded bytes

Deleting a snapshot removes an anchor. That already makes objects it *solely* anchored
unreachable, and the existing GC marks them (reversibly, INV-14). That part works.

What does not: a WAL object or segment holding 100 extents of which 3 are still
referenced. It stays reachable, and 97 extents of dead data stay stored. **Squash** is
the compaction that fixes it:

1. compute, per segment, the fraction of extents still referenced by any live root
   (the live view + every published snapshot manifest);
2. for segments below a threshold (proposal: 50% live), read the live extents, write a
   **new segment** (create-only, new key) and a new manifest naming it;
3. publish the manifest, then let the ordinary GC mark the now-unreachable old
   segments.

Three properties make this safe, and they are not negotiable:

- **Squash never deletes.** It writes new objects and lets reachability retire the old
  ones, so INV-14 holds unchanged and a wrong squash is recoverable by restoring a
  version.
- **A published snapshot's manifest is immutable (INV-16).** Squash may not rewrite it.
  A snapshot whose objects were consolidated needs a *new* manifest that names the new
  segments and the same content — which means the snapshot's identity has to be its
  content digest, not its object list. **This is the crux of the design and the main
  thing to review**: today `root_digest` covers key strings, so consolidating objects
  changes the digest of an immutable snapshot. Either snapshots get a content-addressed
  digest (extent-level), or squash may not touch objects a published snapshot names —
  which would mean snapshot deletion is the only reclaim, and a volume with one old
  snapshot can never be consolidated.
- **Squash is background work** (§17 ioclass), yields to the foreground, and is
  interruptible: a crash mid-squash leaves an unreferenced new segment, which the GC
  collects. Never the other way round.

### 5. Clones and who pays when the parent is deleted

A clone pays only for what it writes; its inherited set is the origin's. If the origin
volume is deleted while clones exist, its retained bytes have to be charged to someone
or they become free storage forever. Proposal, in order of preference:

1. **Refuse to delete a volume whose snapshots are the origin of a live clone** (the
   simplest rule, and the one an operator can reason about);
2. re-charge the inherited set to the surviving clone when there is exactly one;
3. split it across clones (rejected: a number nobody can predict or explain).

## Alternatives considered

- **Charge logical bytes (extents in the live view).** Predictable and cheap, but it
  charges nothing for the ten snapshots the requirement is specifically about, and
  nothing for write amplification before compaction. Rejected on the requirement.
- **Charge raw bucket bytes including the base image.** Trivial to compute (one prefix
  sum) and wrong: every clone of a golden image would start already over quota.
- **Enforce only at snapshot time.** Cheap, and leaves the runaway-writer case — the one
  that actually fills a bucket — unbounded.
- **Reference-count every extent.** The exact answer, and the one that turns every write
  into a metadata update on a shared structure. Rejected for the same reason the rest of
  this system avoids shared mutable state on the data path; the segment-level liveness
  fraction is enough to drive squash.

## The tests that would enforce it

- A contract test that `CreateVolume` accepts and stores a quota, that `charged_bytes`
  never goes negative, and that the quota check is a **predicate of the write**
  (`... AND charged_bytes + $n <= quota_bytes`), like the capacity bound in wave 3 —
  two operators taking snapshots concurrently must not both pass.
- A test that a clone's `charged_bytes` starts at zero regardless of the origin's size,
  and rises only with its own writes.
- A test that deleting one of two snapshots sharing a block returns the *exclusive*
  bytes only.
- A squash test: a segment with 3 of 100 extents live is consolidated, the new manifest
  names the new segment, the old one becomes unreachable, and the volume's view is
  byte-identical before and after. Plus a crash between the new segment and the new
  manifest, asserting the orphan is collectable and the old state still readable.
- A DST scenario `squash-under-a-concurrent-snapshot`: consolidate while a snapshot is
  being published, asserting the published snapshot never loses an extent (the
  ImmutableSnapshotChecker is already there).
- `rebuild-metadata` recomputes `charged_bytes` from S3 and agrees with the stored
  value.

## Consequences

- New columns on `volumes` (`quota_bytes`, `charged_bytes`) and a per-snapshot
  `exclusive_bytes`, all term-guarded like every other CP mutation.
- Squash is a new background component with an on-S3 format contract. It belongs to
  Phase 12 (objectization), and this ADR is the reason to design that phase's segment
  format with liveness in mind rather than retrofitting it.
- The snapshot digest question (§4) may force a format decision *before* Phase 12: a
  content-addressed snapshot identity is a different object than today's.
- Until the Agent exists (DEV-0007) the guest-facing half — ENOSPC to the guest — cannot
  be implemented; the snapshot and publication gates can, and are useful on their own.

## Open questions — answered 2026-07-26

1. **Soft**, with the device budget as the hard limit (amendment 2).
2. **Free, and it stays free** (amendment 2).
3. **Content-addressed** (amendment 1).
4. **Per lineage** (amendment 2).
