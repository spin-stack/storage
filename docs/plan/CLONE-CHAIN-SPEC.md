# CLONE-CHAIN-SPEC — the clone's read chain (DEV-0007)

**Human-review zone: on-S3 format + the read path.** Reviewed before implementation.

## The hole

`controlplane.Clone` says what it is for, in its own doc comment:

> it is pure metadata — a new active child at epoch 1 that **reuses the parent
> snapshot's already-durable objects, with no data copy** … The clone inherits the
> parent's size, block size, durability, and DEK (so it can read the shared base)

It inherits the DEK. It does not inherit — or record anywhere — **which snapshot it is a
clone of**. `chain_depth` is incremented, which says a chain exists but not what is on the
other end of it.

The consequence is not subtle. A clone's Agent starts a WAL at epoch 1 under the *clone's*
volume id, and `fetchBase` calls `recovery.Recover(store, clone-id, 1)`, which finds
nothing, because every object the parent wrote is under the *parent's* id. The base is
empty and installs without complaint. **The clone reads zeros for everything its parent
ever wrote** — a volume advertised as a copy that is, in fact, blank.

That is the same failure shape increment 5 removed for truncation (an empty view where
data belongs is indistinguishable from a fresh volume), reached by a different route.

**What is *not* broken, checked rather than assumed:** the GC does not collect the
parent's objects. `gc.Reachable` walks snapshot manifests, treats structural metadata as
always reachable, and follows each manifest's own `parent` field — so the parent's WAL
objects stay anchored whether or not any clone exists. §20's data is safe; it is simply
unreachable *by the clone*.

## What gets built

**1 — the link, persisted three times, because three readers need it.**

- `volumes.parent_snapshot_id UUID REFERENCES snapshots(snapshot_id)`, nullable — most
  volumes have no parent — with an index, because it is an FK *referencing* column and
  `schema.sql`'s own rule says every one of those carries one.
- `metadata.Volume.ParentSnapshotID`, set by `Clone`.
- `descriptor.Descriptor.ParentSnapshotID`, so §22.5's `rebuild-metadata` reconstructs a
  clone as a clone. A rebuilt volume that lost its parent link would read zeros with
  nothing to say why — the same bug, arriving via a restore.

**2 — the wire.** `DesiredVolume.parent_snapshot_id`, so the Agent learns it the same way
it learns the epoch: from the desired state. ADR-0021 keeps the Agent from knowing what a
Control Plane is, so this cannot be a lookup the Agent performs.

**3 — the read path, which is the point.** `fetchBase` gains a first step: when the volume
names a parent snapshot, materialize that snapshot's view
(`materialize.FromSnapshot(parentVolume, parentSnapshot)`) and use it as the **base
layer**, with this volume's own recovery over it (`cow.NewIntervalMapOver`).

That layering is exactly what increment 5 built `cow.IntervalMap`'s base for, and the
ordering is the whole semantics: the clone's own writes shadow the parent's, and a
`DISCARD` in the clone must read as zeros rather than falling through to the parent —
which is what the tombstone half of `ClearedSpans` already does.

**Fail closed.** A clone whose parent snapshot cannot be materialized does not serve. It
is the identical rule to a base that cannot be recovered, and for the identical reason: an
empty view where data belongs is a wrong answer a guest cannot detect.

**The parent's volume id.** `materialize.FromSnapshot` needs it, and the clone's row does
not carry it — `snapshots.volume_id` does. Rather than have the Agent chase a second
lookup (it cannot; see ADR-0021), `DesiredVolume` carries the parent's **volume id**
alongside the snapshot id. Two fields on the wire, one lookup on the Control Plane, and
the Agent stays a thing that is told.

## What this does not do

- It does not implement **flattening** (§20.1). A chain stays a chain; `chain_depth` is
  still only advisory, and nothing collapses it.
- It does not make cross-host clones work. `materialize.FromSnapshot` reads from the
  object store, so it works wherever the store is reachable — but the *placement* half of
  Phase 11 is untouched.
- It does not persist a materialized view (the other DEV-0007 item). The clone rebuilds
  its base on every start, which is correct and slow; §22.4's lazy loading is the answer
  and it is designed, not implemented.

## Tests that land with it

- **The link survives every boundary**, the same shape as increment 6's DEK-version test:
  provision → snapshot → clone → catalog → descriptor → `GetDesiredState`, and it is the
  *parent's* ids that come out the far end.
- **Schema**: the FK rejects a parent that does not exist, on Postgres 18.
- **A DST arm**: a clone serves its parent's bytes, driven through `agent.VolumeManager`.
  Its planted bug is the link being dropped between the catalog and the Agent — the exact
  defect this closes — and the clone then reads zeros, which is what
  `DurableRangeChecker` already watches for.
