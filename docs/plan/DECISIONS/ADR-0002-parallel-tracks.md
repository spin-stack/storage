# ADR-0002 — Execution model: parallel tracks with a hot-zone rule

- **Status:** Accepted (Phase 0)
- **Date:** 2026-07-24
- **Deciders:** human owner + tech-lead agent

## Context

The prompt allows a multi-agent workflow with "one increment, one Implementer" and
warns against "two in-flight increments with the same source file as a hot zone." The
human chose to allow **parallel tracks where there is no dependency**, trading strict
sequencing for speed while keeping the per-increment gate intact.

## Decision

1. **Phase 01 is a hard barrier.** Nothing runs in parallel until its exit gate is
   green (it is structural and every later track depends on the simulable interfaces,
   DST harness, and checker framework).
2. **After Phase 01, up to a few increments run in parallel** if and only if they touch
   **disjoint hot zones**. The tracks (PLAN §5):
   - Track A — guest + data path: 02 → 03 → 04 → 05.
   - Track B — durable/remote WAL: 06 (starts after 04).
   - Track C — control plane + fencing: 07 (starts after 06).
   - Track D — S3 client subsystem (§24): independent, starts after 01, feeds 06/08/11.
3. **Hot-zone ownership.** The Planner maintains the hot-zone list (PLAN §5) and is the
   single authority that assigns/serializes access. A source file that is a hot zone
   has **one** in-flight increment at a time. Current hot zones: WAL record/object
   format, lease/epoch state, object-store keyspace/layout.
4. **The per-increment gate is unchanged.** Parallelism never relaxes the standard gate
   (PLAN §2), the tests-first rule, or the human-review zones.
5. **Cross-track integration** (e.g. wiring the WAL data path through vhost-user, or the
   lease step into FLUSH ordering) is scheduled as its own increment with a single
   Implementer, not smuggled into either track's increment.

## Consequences

- Faster wall-clock via Track A / Track D concurrency after Phase 01.
- The Planner must actively police hot zones; a scheduling conflict is a **stop signal**
  (PLAN §3) and halts the conflicting increment.
- Merge conflicts on hot zones are treated as a planning failure, not a git chore.

## Alternatives considered

- **Strict sequential:** safest, simplest to reason about, but slower; rejected by the
  human for MVP velocity. Remains the fallback if hot-zone policing proves costly.
