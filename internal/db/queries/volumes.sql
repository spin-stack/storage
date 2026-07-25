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
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $15
)
INSERT INTO volumes (volume_id, size_bytes, durability, block_size, current_epoch, state,
                     dek_wrapped, kek_id, primary_host_id, standby_host_id, chain_depth,
                     local_sequence, durable_sequence, published_sequence)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
WHERE EXISTS (SELECT 1 FROM valid)
ON CONFLICT (volume_id) DO UPDATE
  SET size_bytes = GREATEST(volumes.size_bytes, EXCLUDED.size_bytes),
      durability = EXCLUDED.durability,
      block_size = EXCLUDED.block_size,
      current_epoch = GREATEST(volumes.current_epoch, EXCLUDED.current_epoch),
      dek_wrapped = EXCLUDED.dek_wrapped,
      kek_id = EXCLUDED.kek_id,
      primary_host_id = COALESCE(volumes.primary_host_id, EXCLUDED.primary_host_id),
      standby_host_id = COALESCE(volumes.standby_host_id, EXCLUDED.standby_host_id),
      chain_depth = EXCLUDED.chain_depth,
      local_sequence = GREATEST(volumes.local_sequence, EXCLUDED.local_sequence),
      durable_sequence = GREATEST(volumes.durable_sequence, EXCLUDED.durable_sequence),
      published_sequence = GREATEST(volumes.published_sequence, EXCLUDED.published_sequence),
      updated_at = now();

-- name: GetVolume :one
SELECT * FROM volumes WHERE volume_id = $1;

-- name: ListVolumesByHost :many
-- The volumes a drain must evacuate (§28.1), in a deterministic order.
SELECT * FROM volumes WHERE primary_host_id = $1 ORDER BY volume_id;

-- name: BumpVolumeEpoch :one
-- Increment the epoch and set the primary host, term-guarded. Returns 0 rows if
-- the term is stale or the volume is missing (§12.3).
UPDATE volumes
   SET current_epoch = current_epoch + 1,
       primary_host_id = $2,
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
RETURNING current_epoch;

-- name: ResizeVolume :execrows
-- Grow-only (§3: shrink is a non-goal). The size guard rejects a shrink at the DB.
UPDATE volumes
   SET size_bytes = $2, updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND $2 >= size_bytes;

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

-- name: SetVolumeState :execrows
-- The §7 ownership machine, term-guarded and transition-guarded: $4 is the set of
-- states that may legally become $2, taken from the lifecycle table. In the
-- predicate rather than in Go so two Control Planes reacting to the same suspicion
-- cannot both win a read-modify-write.
UPDATE volumes
   SET state = $2, updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND state = ANY(sqlc.arg(allowed_states)::text[]);
