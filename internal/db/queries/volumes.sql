-- name: CreateVolume :execrows
-- Term-guarded create (§7). current_epoch is normally 0 for new volumes but is set
-- by rebuild-metadata (§22.5) from the authoritative S3 epoch object.
--
-- The conflict path is what makes rebuild-metadata safe to run twice, or from two
-- operators at once: both see ErrNotFound for the same volume and both INSERT, and
-- the loser must converge instead of aborting with 23505 after having written an
-- arbitrary prefix of the catalog. It converges *without regressing*: the epoch
-- (the fencing token), the size (§3 is grow-only), the watermarks and the ownership
-- columns keep the higher/existing value, and `state` is not touched at all — the
-- §7 lifecycle moves only through SetVolumeState.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $14
)
INSERT INTO volumes (volume_id, size_bytes, block_size, rpo_target_seconds, current_epoch, state,
                     dek_wrapped, kek_id, dek_key_id, primary_host_id, standby_host_id,
                     chain_depth, parent_snapshot_id,
                     local_sequence, durable_sequence, published_sequence)
SELECT $1, $2, $3, sqlc.arg(rpo_target_seconds)::int, $4, $5, $6, $7, sqlc.arg(dek_key_id)::bigint, $8, $9, $10,
       sqlc.narg(parent_snapshot_id)::uuid, $11, $12, $13
WHERE EXISTS (SELECT 1 FROM valid)
  -- The capacity bound, as a predicate of the write that places the volume (ADR-0017): a
  -- clone admitted by a pure placement.Choose against a fleet read another operation shared
  -- lands here, against the derived value, and affects 0 rows. A write with no bound is not
  -- a placement decision — rebuild-metadata records volumes that already occupy the host.
  -- Two ceilings, because they answer different questions (placement.Policy.Admits carries
  -- the reasoning): §28.2 bounds what the host was *promised*, ADR-0013 what it reported
  -- *using*. The measured arm charges bound_add_bytes nothing on purpose, and reads the
  -- column rather than a caller's copy so the freshest heartbeat wins. A bound naming an
  -- unregistered host admits nothing: no view row, a NULL comparison, no write.
  AND (sqlc.narg(bound_host)::uuid IS NULL
       OR (EXISTS (SELECT 1 FROM hosts
                    WHERE host_id = sqlc.narg(bound_host)::uuid
                      AND nvme_used_bytes <= sqlc.arg(bound_used_limit)::bigint)
           AND (SELECT c.committed_bytes FROM host_committed_bytes c
                 WHERE c.host_id = sqlc.narg(bound_host)::uuid)
               + sqlc.arg(bound_add_bytes)::bigint <= sqlc.arg(bound_limit)::bigint))
ON CONFLICT (volume_id) DO UPDATE
  SET size_bytes = GREATEST(volumes.size_bytes, EXCLUDED.size_bytes),
      block_size = EXCLUDED.block_size,
      -- Overwritten like block_size and not GREATEST-ed like the watermarks: an RPO is
      -- a promise somebody set, and a lowered one is a promise being tightened. Keeping
      -- the higher value would make a target impossible to reduce.
      rpo_target_seconds = EXCLUDED.rpo_target_seconds,
      current_epoch = GREATEST(volumes.current_epoch, EXCLUDED.current_epoch),
      dek_wrapped = EXCLUDED.dek_wrapped,
      kek_id = EXCLUDED.kek_id,
      -- The three key columns move together or not at all: a wrapped DEK paired with
      -- another DEK's version is a volume nothing can open.
      dek_key_id = EXCLUDED.dek_key_id,
      primary_host_id = COALESCE(volumes.primary_host_id, EXCLUDED.primary_host_id),
      standby_host_id = COALESCE(volumes.standby_host_id, EXCLUDED.standby_host_id),
      chain_depth = EXCLUDED.chain_depth,
      -- Never cleared by a converging write: a clone that lost its parent link reads
      -- zeros, and rebuild-metadata's re-INSERT must not be able to cause that.
      parent_snapshot_id = COALESCE(volumes.parent_snapshot_id, EXCLUDED.parent_snapshot_id),
      local_sequence = GREATEST(volumes.local_sequence, EXCLUDED.local_sequence),
      durable_sequence = GREATEST(volumes.durable_sequence, EXCLUDED.durable_sequence),
      published_sequence = GREATEST(volumes.published_sequence, EXCLUDED.published_sequence),
      updated_at = now();

-- name: GetVolume :one
SELECT * FROM volumes WHERE volume_id = $1;

-- name: ListVolumesByHost :many
-- The volumes a drain must evacuate (§28.1), in a deterministic order.
SELECT * FROM volumes WHERE primary_host_id = $1 ORDER BY volume_id;

-- name: ListVolumes :many
-- Every volume, placed or not, for a human reading the catalog: a NULL primary matches no
-- host id, so the per-host listings union to "everything already being served", and after
-- rebuild-metadata that union is empty. Ordered by the primary key — index scan, no sort.
SELECT * FROM volumes ORDER BY volume_id;

-- name: BumpVolumeEpoch :one
-- Grant the next epoch to a host, term-guarded and guarded by the epoch the promoter read
-- (§12.3): a compare-and-set, not an increment. Under a blind increment every promoter gets
-- an epoch, each CASes the S3 epoch object to the number *it* computed, one wins, and all of
-- them have already written their own host into primary_host_id — leaving the row naming a
-- host that holds neither the object nor a lease. 0 rows: stale term, missing volume, or an
-- epoch that moved on.
UPDATE volumes
   SET current_epoch = current_epoch + 1,
       primary_host_id = $2,
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND current_epoch = sqlc.arg(expected_epoch)
RETURNING current_epoch;

-- name: SetVolumePrimaryHost :execrows
-- Place a volume on a host, or clear its placement (a NULL primary_host_id),
-- term-guarded. It is the only write of primary_host_id that is not a promotion:
-- CreateVolume's conflict path protects the column with COALESCE, so before this query an
-- attach was permanent. The state moves with the ownership in this statement because they
-- are one fact; two writes have a window, and a volume left ACTIVE with no host is one a
-- snapshot request accepts and no Agent can ever take.
--
-- Three predicates, each refusing something different: the CP term (§7); `state = ANY(...)`,
-- the §7 transition table as a predicate, in SQL so a read-modify-write cannot be split by
-- a second Control Plane; and the placement guard, which refuses only the straight
-- hand-over A -> B, since a host discovers it has lost a volume on its next poll.
--
-- fencing_started_at and the refusal are cleared for the same reason: neither is true of a
-- volume that has just changed hands, and a dwell or an explanation left behind would be
-- inherited by the next promotion or printed against a host that is serving fine.
UPDATE volumes
   SET primary_host_id = sqlc.narg(primary_host_id)::uuid,
       state = CASE WHEN sqlc.narg(primary_host_id)::uuid IS NULL
                    THEN 'DETACHED' ELSE 'ACTIVE' END,
       fencing_started_at = NULL,
       refusal = '',
       refusal_detail = '',
       updated_at = now()
 WHERE volume_id = sqlc.arg(volume_id)
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND state = ANY(sqlc.arg(allowed_states)::text[])
   AND (primary_host_id IS NULL
        OR sqlc.narg(primary_host_id)::uuid IS NULL
        OR primary_host_id = sqlc.narg(primary_host_id)::uuid);

-- There is no ResizeVolume query. It was here, it was grow-only and term-guarded, and
-- nothing but a contract test ever ran it: V1 has no resize, because the row is the only
-- half of it that existed (see metadata.Store, where the method was). No statement in
-- this file writes size_bytes after CreateVolume, and that is what makes the geometry the
-- Agent and descriptor.json were handed at create still true when they are read back.

-- name: UpdateVolumeWatermarks :execrows
-- Lazy, informative watermark update (§5.8, §12.6), term-guarded and monotonic.
-- GREATEST is the fencing part: promotion does not change the CP term, so an
-- epoch-N primary's report that was queued behind a retry still passes the term
-- guard after epoch N+1 has published its own. Component-wise max preserves
-- published ≤ durable ≤ local (INV-03), which the caller already validated.
UPDATE volumes
   SET local_sequence = GREATEST(local_sequence, $2),
       durable_sequence = GREATEST(durable_sequence, $3),
       published_sequence = GREATEST(published_sequence, $4),
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $5;

-- name: SetVolumeRefusal :execrows
-- Record why the host holding this volume is not serving it, or clear the record when it
-- is. Term-guarded like every other mutation and — unlike every other one — qualified by
-- the *reporting* host and epoch. That predicate is exactly what UpdateVolumeWatermarks
-- must not have: a watermark is monotonic and GREATEST makes a late report harmless, while
-- a refusal is a state about right now, and an unqualified last-report-wins would let a
-- host the fleet moved past mark a volume NOT SERVED while its successor serves it (the
-- term guard passes: promotion does not change the CP term). 0 rows is therefore normal.
UPDATE volumes
   SET refusal = sqlc.arg(refusal)::text,
       -- One statement, so the two can never disagree: an explanation with nothing to
       -- explain is what volumes_refusal_detail_needs_a_refusal refuses outright.
       refusal_detail = CASE WHEN sqlc.arg(refusal)::text = ''
                            THEN '' ELSE sqlc.arg(refusal_detail)::text END,
       updated_at = now()
 WHERE volume_id = sqlc.arg(volume_id)
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND primary_host_id = sqlc.arg(host_id)::uuid
   AND current_epoch = sqlc.arg(epoch);

-- name: SetVolumeState :execrows
-- The §7 ownership machine, term-guarded and transition-guarded: $4 is the set of states
-- that may legally become $2, taken from the lifecycle table. In the predicate rather than
-- in Go so two Control Planes reacting to the same suspicion cannot both win a
-- read-modify-write.
--
-- It also stamps the fence-start instant (ADR-0015), in the same write. Entering
-- FENCING_WAIT with nothing recorded starts the dwell by *this* database's clock — the one
-- last_renewal is stamped by (§12.1); entering it again does not move the instant, or a
-- resumable promotion would push its own deadline forward on every pass; leaving it clears
-- the record, so the next promotion waits its own dwell.
UPDATE volumes
   SET state = $2,
       fencing_started_at = CASE
           WHEN $2 <> 'FENCING_WAIT'            THEN NULL
           WHEN fencing_started_at IS NOT NULL   THEN fencing_started_at
           ELSE now()
       END,
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND state = ANY(sqlc.arg(allowed_states)::text[]);

-- name: ClearVolumeParent :execrows
-- Record that a volume descends from nothing any more, term-guarded: the one write
-- CreateVolume's COALESCE deliberately forbids to a converging create, and the one a
-- FLATTEN cannot do without (metadata.Store carries the reasoning). chain_depth goes with
-- it in the same statement: §20.1's ceiling refuses on the depth, and a volume left at N
-- would be as unclonable as before the copy it just paid for. No state predicate and no
-- descendant check — the link it clears is one the descriptor has already stopped
-- carrying, and clearing twice affects one row both times.
UPDATE volumes
   SET parent_snapshot_id = NULL,
       chain_depth = 0,
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $2;

-- name: DeleteVolume :execrows
-- Remove a volume and its snapshots, term-guarded. There is no DELETING state and no
-- expiry column: the recovery window is the bucket's own versioning plus its lifecycle
-- policy, and a second window here would drift from the one that controls the bytes.
--
-- One statement, three parts, ordered by the two foreign keys that make volumes and
-- snapshots mutually referencing:
--
--   * `doomed` is the guard — the term, and nothing descending from this volume's
--     snapshots in *either* direction (volumes.parent_snapshot_id and
--     snapshots.parent_snapshot_id, both indexed for exactly this). It selects rather
--     than deletes, so an unmet guard writes nothing at all.
--   * the snapshots go: a snapshot row whose volume is gone is unreadable, its manifest
--     living under the volume's prefix.
--   * the volume goes last.
--
-- Data-modifying CTEs run to completion whether or not the primary query reads them, and
-- referential integrity is checked at the end of the statement, so the two deletes are one
-- atomic act. Rejected: ON DELETE CASCADE on snapshots.volume_id — a cascade is a rule
-- nobody reads until it has fired, and this is the only statement that may remove a volume.
WITH doomed AS (
    SELECT v.volume_id
      FROM volumes v
     WHERE v.volume_id = sqlc.arg(volume_id)
       AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
       AND NOT EXISTS (SELECT 1 FROM volumes d
                        WHERE d.volume_id <> sqlc.arg(volume_id)
                          AND d.parent_snapshot_id IN (SELECT s.snapshot_id FROM snapshots s
                                                        WHERE s.volume_id = sqlc.arg(volume_id)))
       AND NOT EXISTS (SELECT 1 FROM snapshots d
                        WHERE d.volume_id <> sqlc.arg(volume_id)
                          AND d.parent_snapshot_id IN (SELECT s.snapshot_id FROM snapshots s
                                                        WHERE s.volume_id = sqlc.arg(volume_id)))
), gone AS (
    DELETE FROM snapshots WHERE volume_id IN (SELECT volume_id FROM doomed)
)
DELETE FROM volumes WHERE volume_id IN (SELECT volume_id FROM doomed);
