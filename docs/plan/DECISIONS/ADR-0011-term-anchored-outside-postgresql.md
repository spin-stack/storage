# ADR-0011 — The Control Plane term is claimed in the object store before it is used

- **Status:** Accepted 2026-07-25 (human review of the fencing/durability spec done:
  the claim in the object store and the Elector shape were both approved)
- **Date:** 2026-07-25
- **Deciders:** human (to decide), implementer agent (proposes)
- **Implements/Extends:** §7 (single-active Control Plane, term-guarded mutations),
  §12.3–12.4 (promotion, epoch objects), §22.5 (rebuild-metadata), INV-10, INV-11.
- **Closes (if accepted):** TEST-GAPS finding "the CP term has no anchor outside
  PostgreSQL".

## Context

Every Control-Plane mutation is guarded by the term, in the SQL predicate:

```sql
... WHERE (SELECT term FROM control_plane_leader WHERE singleton) = $n
```

That makes a zombie CP affect 0 rows, which is exactly what §7 asks for — *as long
as the term only ever moves forward*. The term lives in one row of one table, and
`AcquireLeadership` derives the new term from that row (`term + 1`). So the whole
single-writer property rests on PostgreSQL never rewinding.

PostgreSQL does rewind, in the operational cases this project already plans for:

- a PITR restore of the CP database after an operator error (the same restore that
  motivated `ErrSourceLeaseUnknown` in promotion — a restored `host_leases` was the
  fail-open window closed in wave 1);
- a failover to a replica that was behind, promoted under pressure;
- a restore from a logical backup during a disaster-recovery drill.

After any of those, `control_plane_leader.term` reads, say, 41 while a live leader is
running with term 42. The next election hands out 42 again. Both processes now pass
`(SELECT term ...) = 42` on every mutation. They can both bump epochs, both grant
leases, both promote — and neither is a zombie by any check the system has. The epoch
compare-and-set added in this wave narrows the damage (two promoters cannot both
advance the same epoch), but it does not restore single-writer: the two leaders
simply take turns, each believing the other's writes are its own resumed work.

This is not a missing test. The system has nothing to test: there is no fact outside
the restorable row that says "term 42 has already been issued".

## Decision (proposed)

**A term must be claimed in the object store before it is used, and the claim is
create-only.**

`volumes/` already treats the object store as the authority that outlives PostgreSQL
(the epoch objects of §12.4, and `rebuild-metadata` in §22.5 rebuilding the catalog
*from* S3). The term joins them:

```
control-plane/terms/<zero-padded term>    # {"term":42,"holder_id":"cp-b","claimed_at":...}
```

Leadership acquisition becomes two steps, in this order:

1. `AcquireLeadership` in PostgreSQL as today, returning term N;
2. a create-only `PUT` (`If-None-Match: *`) of `control-plane/terms/N`. On
   `ErrPreconditionFailed` the term has been issued before — the database was
   rewound — so the CP does **not** use N. It loops back to step 1, which increments
   again, until a claim succeeds. A CP that cannot reach the object store does not
   become leader.

Two properties follow:

- **Terms are globally unique for the lifetime of the bucket**, not for the lifetime
  of the current database. A restored database re-issues 42, fails to claim it, and
  climbs to 43, 44, … until it passes the high-water mark. The live leader at 42 keeps
  its term; the new one is strictly above it, so the ordering the §7 guard depends on
  is intact.
- **The failure mode is a stall, not a split.** The worst case is a CP that cannot
  become leader because the object store is unreachable — the same dependency the data
  path already has, and the direction this project fails in everywhere else.

The claim object is small, written once per election, and never deleted by GC (it
joins the roots in §21.3 — a term claim that GC could collect would defeat the whole
mechanism, which is the one part of this that touches an area outside the term).

### Alternatives considered

- **Reconcile against the epoch objects at startup.** On becoming leader, list every
  volume's epoch object and refuse to serve if any of them is ahead of what
  PostgreSQL says. This detects a rewound *catalog* but not a rewound *term*: the
  epochs and the term rewind together, so both leaders would agree and neither would
  notice. It is a good startup check for a different finding (a restored catalog that
  is behind S3), not for this one.
- **Make the fencing token (term, epoch) instead of epoch.** Correct, and much more
  invasive: it changes the WAL object namespace, recovery, and every comparison in
  §12. The epoch object's holder (added in this wave) already carries the "who", which
  is what the pair would have bought at the fencing layer.
- **Store the term in a second database.** Moves the problem rather than solving it,
  and adds a dependency the project does not otherwise have.
- **Do nothing and document it as an operator hazard.** The runbook would have to say
  "never PITR the Control Plane database while a leader is running", which is exactly
  the situation in which somebody will.

## The test that would enforce it

An integration test in the pg lane (TestContainers, `-tags integration`), because the
whole point is that it survives a real database restore:

```
TestATermIsNeverIssuedTwiceAcrossADatabaseRestore
  1. leader A acquires -> term N, claims control-plane/terms/N
  2. snapshot the database (pg_dump, or a filesystem snapshot of the container volume)
  3. leader A does some term-guarded work so N is demonstrably in use
  4. leader B acquires -> term N+1, claims it
  5. restore the snapshot: control_plane_leader is back at N
  6. leader C acquires
     - assert C's term > N+1 (it climbed past every claimed term), and
     - assert C's term was claimed create-only: the object did not exist before
  7. assert a mutation under term N+1 (leader B, still live) is now ErrStaleTerm,
     and one under C's term succeeds — one writer, and it is the newest.
```

Plus two unit-lane cases against the simulated object store:

- `TestLeadershipRefusesATermAlreadyClaimed` — the claim PUT returns
  `ErrPreconditionFailed`; the elector must retry with a higher term, never return the
  claimed one to the caller;
- `TestLeadershipRefusesWhenTheClaimCannotBeWritten` — an object store that errors
  must leave the process a non-leader (fail closed), not a leader with an unclaimed
  term.

And a DST scenario, since this is the fencing zone: `scenarioRestoredControlPlane` —
partition, restore the simulated metadata store to an earlier term while the old
leader is still running, and let the invariant checker assert that no two live terms
are ever equal and that at most one process holds a lease-granting term at a time.

## Consequences

- `AcquireLeadership` stops being a pure `metadata.Store` operation. The cleanest
  shape is a new `controlplane.Elector` composing `metadata.Store` and
  `objectstore.Store`, leaving the Store interface (and every existing caller and DST
  fixture) untouched; `metadata.Store.AcquireLeadership` becomes the low-level step it
  drives rather than the public entry point.
- Elections cost one create-only PUT, and one extra round trip per already-claimed
  term after a restore. Elections are rare.
- The term claims accumulate (one small object per election). They are a GC root and
  are never collected; if that ever matters, the fix is a high-water-mark object, not
  deleting claims.
- Operators gain an audit trail of every term ever issued, with its holder — useful in
  exactly the incident this ADR is about.

## Implementation notes

Approved as specified, including moving leadership out of `metadata.Store`:
`metadata.Store.AcquireLeadership` stays as the low-level step, and
`controlplane.Elector` becomes the entry point every Control Plane uses. Existing
callers that only need *a* term in a test keep calling the Store directly; a process
that intends to act as leader must go through the Elector, which is where the claim
is made.
