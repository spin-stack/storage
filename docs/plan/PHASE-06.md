# PHASE 06 — Remote WAL: on-demand batching + PUT idempotency + summary objects

> **Roadmap §30.6.** Depends on Phase 04 (record/object format) and Phase 05 (the
> bytes are already ciphertext). **⚠️ Human-review zone** (durability, on-S3 format).
> Implements §14.2, §14.3, §14.4 (remote-PUT + durable watermark; the lease-verify
> step is Phase 07), §14.5, §22.1.

**Phase objective:** closed WAL objects are assembled (§14.2) and uploaded on demand
(§14.3 — no timer, only FLUSH/FUA, size, or age) to the object store with idempotent,
create-only PUTs (§14.5); FLUSH/FUA advances `durable_sequence` only after the covering
objects are verified in S3 (§14.4); a periodic summary object (§22.1) makes later
recovery a couple of GETs plus a short LIST.

**Invariants activated:** INV-07 (FLUSH/FUA ordering — the remote-PUT half; the lease
step lands Phase 07), INV-21 (idempotent PUT), INV-04 (full: remote-coupled backpressure).

**Metrics activated:** `wal_batch_size_bytes`, `wal_batch_age_seconds`,
`wal_put_latency_seconds`, `wal_put_retries_total`, `wal_small_batch_ratio`,
`wal_objects_total`, `wal_durable_sequence`.

---

## Increment 6.1 — WAL object assembly + on-demand batcher
**Objective:** a `Batcher` that accumulates encoded records and closes a batch per the
§14.3 rules (FLUSH/FUA, ≥ 8 MiB target, ≥ 16 MiB hard max, ≥ 20 s age, snapshot,
unflushed limit, shutdown — **never a short timer**); a closed batch assembles into a
WAL object (§14.2 `ObjectHeader` + concatenated records + `PayloadSHA256`) with the
deterministic key (§14.2).
**Tests first:** each close rule fires exactly when it should and not otherwise; object
assembly round-trips (parse header, extract records, verify SHA-256 + key); a
fsync-heavy pattern yields many small batches (the `wal_small_batch_ratio` case).
**Gate:** standard + object golden/round-trip. No invariant flips.

## Increment 6.2 — Idempotent PUT + FLUSH ordering + durable watermark
**Objective:** upload closed batches to the object store with `If-None-Match:*`; on a
lost response, retry → 412 → HEAD → checksum compare → idempotent success; same key +
different hash ⇒ hard fail (§14.5). FLUSH/FUA closes the batch, uploads all pending ≤
target in parallel, verifies, then advances `durable_sequence` (§14.4 steps 1–4,6,7;
step 5 lease-verify is Phase 07). Remote-coupled backpressure completes INV-04.
**Tests first / DST:** idempotent-PUT under injected lost response and throttle (INV-21,
extends the Phase-01 arm with real batches); FLUSH ordering — no `durable_sequence`
advance before the covering objects are verified (INV-07), with crashes injected between
steps; hash-mismatch ⇒ hard fail.
**Gate:** standard + INV-07 + INV-21 in the DST set + human review (durability zone).
Activates INV-07, INV-21, full INV-04.

## Increment 6.3 — Summary objects (§22.1)
**Objective:** periodically (every N seconds of activity or per checkpoint) write a small
`wal/<vol>/<epoch>/summary.json` with the last durable sequence and the object/range
list, so recovery is 2 GETs + 1 short LIST rather than a massive LIST.
**Tests first:** summary reflects uploaded objects; a recovery-style read (LIST from the
summary point, verify a contiguous prefix) reconstructs the durable point. (Full
recovery is Phase 08; this lays the object down and unit-tests its shape.)
**Gate:** standard.

## Phase 06 exit gate
- [ ] Batches close only on the §14.3 rules; objects assemble with correct SHA-256/key.
- [ ] PUT is create-only + idempotent under lost response/throttle; hash mismatch = hard fail.
- [ ] FLUSH/FUA advances `durable_sequence` only after S3 verification (INV-07).
- [ ] Full unflushed/backpressure coupled to remote progress (INV-04).
- [ ] Summary objects written; metrics flowing.
- [ ] Human review of the durability path + on-S3 object shape.
