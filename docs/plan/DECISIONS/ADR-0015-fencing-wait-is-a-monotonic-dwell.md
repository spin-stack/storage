# ADR-0015 — The fencing wait is a monotonic dwell, not a timestamp comparison

- **Status:** Accepted 2026-07-26
- **Date:** 2026-07-26
- **Deciders:** human (decided), implementer agent (proposed the options)
- **Implements/Extends:** §12.1 (clocks), §12.3 (promotion, FENCING_WAIT), §7 (volume
  states), INV-11.
- **Closes:** TEST-GAPS "a lagging replica read makes the fencing wait elapse early".

## Context

`Promote` derives the fencing deadline from `host_leases.last_renewal` and waits until
that instant plus `lease_ttl + max_clock_skew`. Wave 2 removed the CP's own wall clock
from the comparison (`metadata.Store.Now`, `ErrClockOffsetTooLarge`), which closed the
case where the container's clock jumped.

It does not close the case where the **data** is old. A read served by a replica lagging
by more than `lease_ttl + max_clock_skew` reports a `last_renewal` old enough that the
wait already looks over, and the epoch is granted while the old writer's monotonic lease
is still valid. Both clocks agree, so nothing in the current design notices: the offset
check compares clocks, and the clocks are fine.

## Decision

**The wait is measured as elapsed time on the promoter's monotonic clock since the
promoter itself observed the lease** — not as a comparison against the timestamp the
lease carries.

A stale read then costs nothing: whatever `last_renewal` says, the promoter still has to
sit through `lease_ttl + max_clock_skew` of its own monotonic time before granting. The
timestamp keeps its second job (refusing to promote when the lease was renewed *after*
the observation), but it is no longer what makes the wait long enough.

The observation is **durable**, recorded with the §7 `FENCING_WAIT` state the promoter
already writes: the state row gains the instant the fence started. Without it, a CP that
restarts mid-fence has no memory of having observed anything and must start the dwell
again.

### The cost, stated

A promotion issued by a CP that has just restarted waits a full
`lease_ttl + max_clock_skew` from the moment *it* first looks, even if the previous CP
had already waited most of it — unless the durable record above is present, which is why
it is part of the decision rather than an optimisation. **Failover is slower after a
Control-Plane restart, and that is the intended direction:** the alternative is a fence
whose length depends on how the database happens to be deployed.

## Alternatives considered

- **Route lease reads to the primary.** One line of pool configuration, and a guarantee
  that lives outside the code: the day somebody points the pool at a replica the fence
  degrades silently and no test can see it. Worth doing anyway as defence in depth, but
  not as the mechanism.
- **Bound replica lag and check it.** Requires a lag signal the CP can trust, which is
  the same problem one level down.

## The tests that would enforce it

- A promoter test with a `metadata.Store` whose `GetHostLease` returns a `last_renewal`
  from before the process started (a maximally stale read): the grant must still wait
  the full dwell on the injected clock, and `PromotionWaitChecker` must stay quiet.
- A test that a CP restart mid-fence resumes the dwell from the recorded instant rather
  than restarting it, and that a *missing* record starts a fresh full dwell (fail slow,
  never short).
- A DST scenario `stale-lease-read-does-not-shorten-the-fence`, with the existing INV-11
  checker and a planted bug that reverts the dwell to a timestamp comparison.

## Consequences

- `FENCING_WAIT` stops being a marker and becomes load-bearing state: the promoter must
  write it before the wait and read it on resume. That is the durable record §7 was
  described as providing and, until now, was not.
- A new column on `volumes` (the fence-start instant), term-guarded like every other CP
  mutation, and stamped by the store's clock — the same clock the deadline already
  trusts.
- The promoter needs a monotonic clock of its own (it has one: `simio/clock`), and the
  DST harness can now drive this path with time it controls, which it could not when the
  answer came from a timestamp in a row.
