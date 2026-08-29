# ADR-0027 — layers live in one directory per host, and the record says whose they are

Accepted 2026-08-29. Changes v6 §5's local layout. Relates to §18 (clones), §19
(compaction), ADR-0013 (device pressure).

## The constraint

A clone must reuse its ancestors' layers rather than download them — §18 asks for it by
name. It did, by **copying** them into the clone's own directory, and the cost is not
marginal: a hundred clones of a volume with a 20 GiB published history occupy 2 TB of local
disk. Cloning exists to be done at that scale, so this is not an inefficiency, it is the
feature not working.

The duplication was never about the bytes. The object store has always held one object per
layer however many volumes descend from it (`layers/sha256/<digest>`); the local disk was
the only place in this system that did not deduplicate.

It was about the **path**. `qemu-img rebase -u` rewrites a layer's *header* to name its
parent's path, so under `volumes/<id>/layers/` every clone needs a different backing path
and therefore its own rebased file. Two ways out were rejected before this one:

- **hard links** — the clone's `rebase -u` would rewrite the *parent volume's* file through
  the shared inode, which is §18's one prohibition;
- **a backing file reaching into another volume's directory** — the clone's chain would
  depend on a volume nobody told it about, and a volume that is later released takes the
  clone's floor with it.

**Reflinks (`FICLONE`) were rejected on deployment cost**, not on merit: they solve it
exactly, and requiring XFS or Btrfs is a larger imposition than the problem.

## Decision

**One layers directory for the whole host: `<root>/layers/<layer-id>.qcow2`.** A layer is
named by its own id, which comes from the manifest, so the backing path a chain records is
the same on this host whoever reads it. The header is written once, when the layer is
materialized, and is already correct for every volume that will ever read through it.

Sharing is safe because a sealed layer is immutable (§6): it is a qcow2 backing file opened
read-only, which is what backing chains are for. The write lock is on the *tip*, and a tip
is read by one guest.

## What the path stopped being able to say

A per-volume directory answered "is this layer this volume's" by construction — §5 calls it
this Agent's one safety check on an attached VM. That answer is gone, and three places take
it over:

1. **A layer is written into the record before the pointer names it.** The pointer is one
   line of text with no identity of its own, so a target the record does not account for is
   refused outright — the shape a data directory restored from another volume's backup has.
   The ordering is what makes the check absolute rather than a window to tolerate.
2. **The image QEMU reports is checked against the record and the pointer together.**
   Neither alone covers a rotation: the pointer moves before QEMU is told to switch, so a
   restart in between finds the new tip in the pointer and the old one in the record.
3. **A restore trusts a local file only where some volume's record ties that commit to that
   layer**, unioned across the host — which is what makes a clone free. And it refuses to
   *download over* a path it cannot account for: replacing a shared file under a running
   guest is the same corruption as reading the wrong one, pointed the other way.

## One rule frees a layer file

**No volume on this host names it.** The keep-set is the union of every volume's record and
pointer. It replaces three separate rules — the per-volume sweep of orphan overlays, the
reclamation of a moved volume's disk, and a collapse's replaced prefix, which used to be
kept for ever so that compaction freed no local disk at all.

It **refuses to run rather than count short**: a volume directory whose record cannot be
read, or has none, is an unanswerable question and not an empty claim — `ReadState` answers
a missing file with an empty record, and sweeping against that deletes a running guest's
chain. That replaced a heuristic ("the layer id was minted after the tip's, so nothing can
back onto it") which existed to make a lost record survivable and which is a weaker thing
than declining to act on a keep-set you know is incomplete.

## Consequences

- An interrupted rotation now leaves a recorded layer nothing wrote to — about 190 KiB,
  kept for ever, because a record naming a file is exactly what the sweep may not overrule.
  The previous heuristic collected it. That is the price of the absolute pointer check.
- A restore can be refused for a cycle when a path it needs is occupied by a file nothing
  vouches for. It is self-healing: the sweep collects the file, the next cycle proceeds.
- `internal/recovery.copyLocal` is gone.

## Measured

`demo:stage6` step 13, real binaries and real guests: eight clones of one snapshot add
eight empty tips and 2 MB against 27 MB of shared history. The unit half asserts a hundred
clones add no layer files at all, and planting a path per volume turns it red.
