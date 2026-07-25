# PHASE 09 — Pause-free snapshots + same-host clones + online resize (grow)

> **Roadmap §30.9.** Depends on Phase 06 (WAL objects) and Phase 07/08 (epoch +
> recovery). **⚠️ Touches formats (manifest) / durability** — review before merge.
> Implements §19, §20, §9 (resize), §5.2.

**Phase objective:** a snapshot is a **number**, not an event that drains queues —
capture the current sequence atomically (µs, guest pause ≈ 0), seal a manifest in S3
in the background, and it is immutable forever (§5.2). Same-host clones reuse the
snapshot with no data copy; the persistent volume grows online.

**Invariants activated:** INV-16 (immutable snapshots).
**Metrics activated:** `snapshot_publish_duration_seconds`,
`snapshot_pause_duration_seconds` (~0).

---

## Increment 9.1 — Manifest + pause-free snapshot (§19, §5.2)
**Objective:** `snapshot.Manifest` (snapshot id, volume, epoch, target_sequence,
parent, root digest, referenced WAL object keys ≤ N) published create-only to
`snapshots/<vol>/<snap>/manifest.json` (immutable). `Snapshotter.Create` captures
`N = local_sequence` (the only "pause"), flushes to N, builds the manifest from the
durable objects ≤ N, and publishes it. Writes with sequence > N are not in the
snapshot.
**Tests first / DST:** the manifest's `target_sequence` = N and excludes later writes;
re-publishing the same manifest key fails (immutable, INV-16); referenced objects are
unchanged after subsequent writes; `snapshot_pause_duration ≈ 0`.
**Gate:** standard + INV-16 in the DST set + human review. Activates INV-16.

## Increment 9.2 — Snapshot catalog + same-host clone + online resize
**Objective:** snapshot rows in PG (sqlc `CreateSnapshot`/`GetSnapshot`); a same-host
`Clone` creates a new volume that references the parent snapshot with **no data copy**
(reads fall through to the snapshot's objects); `Resize` grows `size_bytes` (guest-side
`resize2fs` is the guest layer, Phase 02).
**Tests first / DST:** clone references the parent and copies no objects; a write to the
clone does not affect the parent (independent active child, §5.2); resize grows the
recorded size; snapshot catalog round-trips (sim + TestContainers).
**Gate:** standard + human review.

## Phase 09 exit gate
- [ ] Snapshot = captured sequence; guest pause ≈ 0; manifest immutable (INV-16).
- [ ] Manifest excludes writes after the captured sequence.
- [ ] Same-host clone: new active child, no data copy, independent of the parent.
- [ ] Online resize (grow) recorded; snapshot catalog in PG.
