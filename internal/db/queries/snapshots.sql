-- name: CreateSnapshot :execrows
-- Term-guarded snapshot record (§19).
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $11
)
INSERT INTO snapshots (
    snapshot_id, volume_id, parent_snapshot_id, epoch, target_sequence,
    root_digest, source_host_id, state, manifest_key, request_id
)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
WHERE EXISTS (SELECT 1 FROM valid);

-- name: GetSnapshot :one
SELECT * FROM snapshots WHERE snapshot_id = $1;
