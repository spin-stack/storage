-- name: UpsertHost :execrows
-- Term-guarded (§7): the INSERT ... SELECT produces no row when the term is stale,
-- so a zombie CP affects 0 rows.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $8
)
INSERT INTO hosts (
    host_id, state, agent_version, max_format_version,
    nvme_total_bytes, nvme_used_bytes, nvme_committed_bytes, last_heartbeat
)
SELECT $1, $2, $3, $4, $5, $6, $7, now()
WHERE EXISTS (SELECT 1 FROM valid)
ON CONFLICT (host_id) DO UPDATE
  SET state = EXCLUDED.state,
      agent_version = EXCLUDED.agent_version,
      max_format_version = EXCLUDED.max_format_version,
      nvme_total_bytes = EXCLUDED.nvme_total_bytes,
      nvme_used_bytes = EXCLUDED.nvme_used_bytes,
      nvme_committed_bytes = EXCLUDED.nvme_committed_bytes,
      last_heartbeat = now();

-- name: GetHost :one
SELECT * FROM hosts WHERE host_id = $1;

-- name: RenewHostLease :execrows
-- Grouped per-host lease renewal (§12.6), term-guarded.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $3
)
INSERT INTO host_leases (host_id, granted_at, last_renewal, ttl_seconds)
SELECT $1, now(), now(), $2
WHERE EXISTS (SELECT 1 FROM valid)
ON CONFLICT (host_id) DO UPDATE
  SET last_renewal = now(),
      ttl_seconds = EXCLUDED.ttl_seconds;

-- name: GetHostLease :one
SELECT * FROM host_leases WHERE host_id = $1;
