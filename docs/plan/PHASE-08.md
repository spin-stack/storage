# PHASE 08 — Recovery with S3 as authority + recovery-point + rebuild-metadata

> **Roadmap §30.8.** Depends on Phase 06 (WAL objects + summary) and Phase 07 (epoch
> object + recovery-point, already seeded by `internal/recovery`). **⚠️ Durability /
> data-loss zone** — human review of the diffs before merge. Implements §22.1, §22.2,
> §22.5, §12.5, §5.8.

**Phase objective:** a promoted or restarted Agent reconstructs volume state purely
from S3 — the durable point is the end of the longest contiguous WAL prefix for the
highest fenced epoch (§5.8), accelerated by the summary object (§22.1) — and PostgreSQL
can be rebuilt from the self-describing S3 layout (`rebuild-metadata`, §22.5).

**Invariants activated:** INV-08 (S3 is the recovery authority), INV-12 (recovery-point
is the epoch boundary), INV-20 (PG reconstructible from S3).

**Metrics activated:** `recovery_duration_seconds`, `bytes_downloaded_before_boot`.

---

## Increment 8.1 — Recover volume state from S3 (summary-accelerated) + replay
**Objective:** `recovery.Recover` determines the durable point (GET epoch → GET
summary → short LIST → verify contiguous prefix, §22.1) and replays the WAL objects up
to it, decrypting each record, to reconstruct the read view (interval map). The durable
point is computed from S3 alone, never from PG watermarks (INV-08). W2 writes its
`recovery-point.json` (§12.5) which bounds the fenced predecessor's late PUTs (INV-12).
**Tests first / DST:** write a volume via a `Log`, recover from S3, assert reads match;
summary present vs absent both work; a late/out-of-prefix object is excluded (INV-12);
durable point unaffected by wrong PG watermarks (INV-08).
**Gate:** standard + INV-08 + INV-12 in the DST set + human review. Activates INV-08, INV-12.

## Increment 8.2 — descriptor.json + basic rebuild-metadata (§22.5)
**Objective:** `volumes/<vol>/descriptor.json` (size, block_size, kek_id, dek_wrapped,
current_epoch, durability) written on create and read by `rebuild-metadata`, which
scans the buckets (descriptors + epoch objects + recovery-points/summaries) and
repopulates a `metadata.Store` from scratch.
**Tests first / DST:** wipe the sim metadata, run `rebuild-metadata` from S3, assert the
reconstructed volume/epoch match ground truth (INV-20). TestContainers variant against
real PG optional.
**Gate:** standard + INV-20 + human review. Activates INV-20.

## Phase 08 exit gate
- [ ] Recover reconstructs state from S3; durable point from S3 only (INV-08).
- [ ] Recovery-point bounds late PUTs (INV-12).
- [ ] descriptor.json written/read; rebuild-metadata repopulates PG (INV-20).
- [ ] Summary-accelerated recovery (2 GETs + short LIST) with a full-LIST fallback.
- [ ] Human review of the recovery/durability diffs.
