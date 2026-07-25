-- name: CreateVolume :execrows
-- Term-guarded create (§7).
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $8
)
INSERT INTO volumes (volume_id, size_bytes, durability, block_size, state, dek_wrapped, kek_id)
SELECT $1, $2, $3, $4, $5, $6, $7
WHERE EXISTS (SELECT 1 FROM valid);

-- name: GetVolume :one
SELECT * FROM volumes WHERE volume_id = $1;

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

-- name: UpdateVolumeWatermarks :execrows
-- Lazy, informative watermark update (§5.8, §12.6), term-guarded.
UPDATE volumes
   SET local_sequence = $2,
       durable_sequence = $3,
       published_sequence = $4,
       updated_at = now()
 WHERE volume_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $5;
