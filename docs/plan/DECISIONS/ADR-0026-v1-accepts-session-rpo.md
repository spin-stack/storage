# ADR-0026 — the object store leaves the ACK path; durability becomes a separate act

Accepted 2026-08-02, implemented. Contradicts §2's RPO row and supersedes the `remote`
durability mode.

> **Superseded in part by the v6 architecture document (2026-08-11), which is the source of
> truth.** v6 keeps the withdrawal below, but replaces "uploaded once, when it stops" with
> **periodic commits driven by a per-volume `rpo_target`** (v6 §3, §11) and defines the RPO as
> *the age of the newest commit that covers everything written* — measured as
> `last_successful_commit_age`, not asserted to be zero. The title's "one session" was the
> 2026-08-02 answer; the durable half of this ADR is the split, not the interval.

## The question, and its answer

**Has anyone ever asked for a VM to survive the loss of its host mid-session, or is that an
assumption of the design?**

**Answered by the human owner, 2026-08-02: no. "Es un wish, no un pedido."**

§2 declares the primary use case — development, CI and ephemeral fleets cloned from
snapshots — and an RPO-0 SLO in the same section, and they pull in opposite directions.
Nobody asks a CI runner to survive the death of its host mid-job, but that row is the
assumption that generates the entire remote durability chain, and it had never been checked
against a stated requirement. §2's first paragraph governs; the SLO table is what gets
corrected. It was asked rather than assumed because the last time this repository assumed an
answer instead of checking one, it produced three documents describing a lane no test
performed.

## Decision

1. **The FLUSH/FUA ACK contract is `fdatasync` only.** §14.8's `remote` mode is withdrawn
   rather than kept as an option: a mode nobody selects is a second contract to keep correct
   for free.
2. **Durability is a separate, explicit act, and the RPO is the age of the last one.** This
   ADR set that act at stop, detach or snapshot; v6 makes it periodic on `rpo_target`. Either
   way the object store is out of the ACK path and the RPO is a number that is measured.
3. **Fencing survives at a fraction of its size** — see below.
4. **The remote chain is deferred, not deleted from the design.** §14.4's six steps,
   checkpoints, mid-session recovery and cross-host materialization stayed in the
   architecture document as the V2 path; the code is removed and `git` is the archive.

This is not "remove the WAL". Crash consistency, ordered replay and the read view a guest is
served from involve no object store and were untouched.

## What it cut

Measured at the time, production lines, tests excluded: the durable-point reconstruction
(890), the orphan collector (381), live cross-host materialization (293), mid-session local
truncation (215), the S3-prefix race between two writers (200) and the ACK-gating lease
(139), plus the remote half of `internal/wal` and the fencing half of `internal/controlplane`.
**On the order of half the system, and the half that is expensive to reason about** — where
every review-zone increment, every fencing invariant and most of the DST harness lived.
(`internal/recovery` and `internal/lease` exist today under the same names as different code:
rebuilding a published chain on a host that has never seen the volume, and the Agent-side host
lease.)

## What does not go away, and must not be assumed away

1. **Mutual exclusion at publish.** If two hosts serve the same volume and both publish, the
   second silently overwrites the first — a lost update with no error anywhere. This is the
   only part of fencing that survives, and it is a compare-and-set on one object at publish
   rather than a lease renewed every three seconds gating every ACK.
2. **Snapshots of a running VM** are an `fsync` followed by the upload of a copy: no stop, no
   §14.4 chain. **§19 is what makes that work** — a snapshot is a number, not an event. The
   source keeps writing immediately, so the copy must be a frozen view rather than whatever
   the store holds when the upload finishes; that is also how §2's ~0 snapshot pause survives.
3. **The guest's `fsync` is not being lied to.** `fdatasync` is a real guarantee against an
   Agent crash, a QEMU crash and a guest crash. Only host loss is excluded — the promise
   narrows, it does not become false. Stated plainly because it is the first objection this
   attracts.
4. **Locality carries the boot-time story.** §20's placement rule 1 — source host, then a host
   with the snapshot cached, then anywhere with capacity — stops being an optimization,
   because a cross-host clone now pays a full download with no warm standby. It stays a
   *preference*: the source host can be full, cordoned or dead. And it is time-bounded: once
   the source stops and its local state is reclaimed, step 1 buys nothing.

## Alternatives rejected

- **Keep both modes and default to `local`.** Two ACK contracts is exactly the surface this
  removes, and `remote` would keep its lease, its epochs and its recovery path — the half the
  cut is for. The obvious fallback if the question above is ever answered "yes, for some
  volumes".
- **Keep the RPO-0 SLO and narrow the primary use case instead.** Coherent, and correct if
  durable-across-host-loss is a product requirement; it means §2's first paragraph is wrong
  rather than its table.
- **Do nothing and decide later.** Every increment built on the old premise makes the pivot
  more expensive; deferring is not free, and the price is paid in work that gets deleted.

## What would reverse it

A customer requirement for a volume that survives host loss mid-session, or a workload whose
`fsync` frequency makes a measured RPO unacceptable in practice. Either turns this into "V1
was the ephemeral tier" and the chain comes back — from git, against a real requirement.

**Reversal is not symmetric.** The fencing protocol was correct and proven against planted
bugs when it was deleted. This trades a working implementation of a hard thing for a much
smaller system, and that trade is only worth making if the hard thing is not needed — which
is the question at the top.
