# ADR-0015 — The fencing wait is a monotonic dwell, not a timestamp comparison

Accepted 2026-07-26. Extends §12.1 (clocks), §12.3 (promotion, FENCING_WAIT), §7.

> **No consumer today (ADR-0026).** V1 does not promote — `controlplane.Promoter` went with
> the fencing half in increment 4.6 — so nothing waits out `lease_ttl + max_clock_skew`, and
> INV-11 is withdrawn. **The durable half is kept deliberately:** `volumes.fencing_started_at`,
> `metadata.Volume.FencingStartedAt` and the FENCING_WAIT state still exist. They cost
> nothing and they are the part that is expensive to get right; what follows is what a future
> promotion must be rebuilt on.

## The problem

`Promote` derived the fencing deadline from `host_leases.last_renewal`. Wave 2 removed the
CP's own wall clock from that comparison, which closed the case of a clock that jumped. It
does not close the case where the **data** is old: a read served by a replica lagging more
than `lease_ttl + max_clock_skew` reports a `last_renewal` old enough that the wait already
looks over, and the epoch is granted while the old writer's monotonic lease is still valid.
Both clocks agree, so nothing in the design notices.

## Decision

**The wait is elapsed time on the promoter's own monotonic clock since the promoter observed
the lease**, not a comparison against the timestamp the lease carries. A stale read then
costs nothing: whatever `last_renewal` says, the promoter still sits through the full dwell
of its own time. The timestamp keeps its second job — refusing to promote when the lease was
renewed *after* the observation — and stops being what makes the wait long enough.

**The observation is durable**, written with the FENCING_WAIT state and term-guarded like
every other CP mutation. Without it a CP that restarts mid-fence has no memory of having
observed anything and must start the dwell again — which is why it is part of the decision
rather than an optimisation. A *missing* record starts a fresh full dwell: fail slow, never
short.

**The cost, stated:** a promotion issued by a CP that has just restarted waits a full dwell
from the moment *it* first looks. Failover is slower after a Control-Plane restart, and that
is the intended direction — the alternative is a fence whose length depends on how the
database happens to be deployed.

## Alternatives rejected

- **Route lease reads to the primary.** One line of pool configuration, and a guarantee that
  lives outside the code: the day somebody points the pool at a replica the fence degrades
  silently and no test can see it. Worth doing as defence in depth, not as the mechanism.
- **Bound replica lag and check it.** Needs a lag signal the CP can trust, which is the same
  problem one level down.
