# PHASE 10 — Objectization + checkpoints + GC (mark-and-sweep) + I/O classes

> **Roadmap §30.10.** Depends on Phase 06/08. **⚠️ Durability / GC — data-loss zone**,
> review before merge. Implements §21 (objectization, compaction, GC), §11 (I/O
> classes), §5.9, §5.11.

**Phase objective:** background objectization publishes checkpoints + manifests in a
strict order and only then marks local WAL truncatable — never above a verified
`published_sequence` (INV-13); the GC only **marks** (never permanent-delete; the
lifecycle sweeps), so it cannot cause the worst incident (INV-14); and background I/O
always yields to foreground/flush (INV-17).

**Invariants activated:** INV-13 (never truncate WAL above verified published),
INV-14 (GC cannot permanently delete), INV-17 (background always yields).

---

## Increment 10.1 — Checkpoint + objectization order + never-truncate rule (INV-13)
**Objective:** `checkpoint` object (durable prefix + state digest) published after its
objects are verified; `Log.TruncateLocal(upTo)` refuses `upTo > published_sequence`
(INV-13). Objectization order (§21.1): verify objects → publish checkpoint → publish
manifest → advance published → only then mark WAL `≤ published` eligible.
**Tests first / DST:** truncating above the verified published point is refused;
truncation at/below it succeeds and recovery from S3 is unaffected; a crash between
publish and truncate loses nothing.
**Gate:** standard + INV-13 in the DST set + human review. Activates INV-13.

## Increment 10.2 — GC mark-and-sweep + reversible delete (INV-14)
**Objective:** the object store models versioning + reversible delete markers; `gc`
computes the reachable set (descriptors + manifests + recovery-points + summaries +
epoch) and **marks** unreachable objects older than the grace period. It never issues a
permanent delete (no such capability); the lifecycle does the sweep.
**Tests first / DST:** a live (reachable) object is never marked; the GC performs zero
permanent deletes (reuses the Phase-01 `NoPermanentDeleteChecker`); a marked object is
reversible.
**Gate:** standard + INV-14 in the DST set + human review. Activates INV-14.

## Increment 10.3 — I/O classes + token buckets (INV-17)
**Objective:** every I/O op belongs to a class (foreground / flush / background) with a
per-resource token bucket; background yields when foreground/flush are pending (§5.9,
§11). `io_class_bytes_total`, `io_class_throttled_seconds`.
**Tests first / DST:** under contention, background is throttled while foreground/flush
proceed; background never exceeds its budget while higher classes wait.
**Gate:** standard + INV-17 in the DST set. Activates INV-17.

## Phase 10 exit gate
- [ ] WAL never truncated above the verified published point (INV-13).
- [ ] GC only marks; zero permanent deletes; live objects never marked (INV-14).
- [ ] Background I/O yields to foreground/flush (INV-17).
- [ ] Objectization order strict; checkpoints published + verified before truncation.
