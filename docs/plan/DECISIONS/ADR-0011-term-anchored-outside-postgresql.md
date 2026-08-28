# ADR-0011 — The Control Plane term is claimed in the object store before it is used

Accepted 2026-07-25, implemented. Extends §7 (single-active CP, term-guarded mutations),
§12.3–12.4, §22.5.

## The problem

Every CP mutation is guarded by `(SELECT term FROM control_plane_leader …) = $n`, so a
zombie CP affects 0 rows — *as long as the term only ever moves forward*. The term is one
row in one PostgreSQL database, and `AcquireLeadership` derives the next term from it. So
single-writer rests on PostgreSQL never rewinding, and PostgreSQL rewinds in cases this
project already plans for: a PITR restore after operator error, a failover to a replica
that was behind, a restore from a logical backup during a DR drill. After any of those the
row reads 41 while a live leader holds 42; the next election hands out 42 again and both
processes pass the guard. Neither is a zombie by any check the system has, and the epoch
CAS does not save it — the two leaders take turns, each reading the other's writes as its
own resumed work. Nothing outside the restorable row says "42 has already been issued".

## Decision

**A term is claimed in the object store before it is used, and the claim is create-only.**

```
control-plane/terms/<zero-padded term>   # {"term":42,"holder_id":"cp-b","claimed_at":…}
```

Leadership becomes two steps: `AcquireLeadership` in PostgreSQL returns term N, then a
create-only `PUT` (`If-None-Match: *`) of `control-plane/terms/N`. On
`ErrPreconditionFailed` the term has been issued before — the database was rewound — so N
is not used: loop back and increment until a claim succeeds. **A CP that cannot reach the
object store does not become leader.** `volumes/` already treats the object store as the
authority that outlives PostgreSQL (§12.4 epoch objects, §22.5 rebuilding the catalog from
S3); the term joins them.

Two properties follow:

- **Terms are globally unique for the lifetime of the bucket**, not of the current
  database. A restored database climbs past the high-water mark; the live leader keeps its
  term and the new one is strictly above it, so §7's ordering holds.
- **The failure mode is a stall, not a split** — the worst case is a CP that cannot become
  leader because the object store is unreachable, the dependency the data path already has.

**Constraint from outside this ADR:** the claims are a GC root (§21.3) and are never
collected. A term claim the GC could sweep would defeat the mechanism. If their
accumulation ever matters, the fix is a high-water-mark object, not deleting claims.

## Alternatives rejected

- **Reconcile against the epoch objects at startup.** The epochs and the term rewind
  together, so both leaders agree and neither notices. A good check for a *restored
  catalog that is behind S3* — a different finding.
- **Make the fencing token `(term, epoch)`.** Correct and far more invasive: it changes
  the object namespace, recovery and every comparison in §12. The epoch object's holder
  already carries the "who" that the pair would have bought.
- **Store the term in a second database.** Moves the problem and adds a dependency the
  project does not otherwise have.
- **Do nothing, document an operator hazard.** The runbook would read "never PITR the CP
  database while a leader is running" — exactly the situation somebody will be in.

## What enforces it

`controlplane.Elector` is the entry point a process that intends to lead must use;
`metadata.Store.AcquireLeadership` remains the low-level step it drives. The property is
pinned by a pg-lane test (the point is surviving a real database restore) and by a DST
scenario; both name this ADR at their declaration.
