-- name: UpsertHost :execrows
-- Term-guarded (§7): the INSERT ... SELECT produces no row when the term is stale,
-- so a zombie CP affects 0 rows.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $7
)
INSERT INTO hosts (
    host_id, state, agent_version, max_format_version,
    nvme_total_bytes, nvme_used_bytes, last_heartbeat
)
SELECT $1, $2, $3, $4, $5, $6, now()
WHERE EXISTS (SELECT 1 FROM valid)
-- The conflict path is a heartbeat: it refreshes only what the host knows about
-- itself. `state` is the Control Plane's (SetHostState) and is deliberately absent —
-- a routine heartbeat carrying State=ACTIVE would un-cordon a host that is being
-- drained. Committed capacity is not here either, and no longer could be: it is
-- derived from the volumes and plans that name the host (ADR-0017), so there is
-- nothing for a heartbeat to overwrite.
ON CONFLICT (host_id) DO UPDATE
  SET agent_version = EXCLUDED.agent_version,
      max_format_version = EXCLUDED.max_format_version,
      nvme_total_bytes = EXCLUDED.nvme_total_bytes,
      nvme_used_bytes = EXCLUDED.nvme_used_bytes,
      last_heartbeat = now();

-- name: GetHost :one
-- Committed capacity comes with the host because every reader of a host is a
-- placement decision waiting to happen, and a §28.2 decision taken against a stale
-- copy of that number is exactly what ADR-0017 removed the copy to prevent.
--
-- This is the canonical derivation; three other queries repeat it (ListHosts below,
-- and the bound predicates in volumes.sql and operations.sql). schema.sql says why
-- it is not a view: the pinned Atlas Community refuses to diff a schema containing
-- one, and a licensed toolchain — or a hand-written migration — is too high a price
-- for sugar over a sum.
--
--   committed(host) = Σ size_bytes of the volumes whose primary_host_id is the host
--                   + Σ size_bytes reserved by in-flight operation plans targeting it
--
-- The second term is what makes it correct rather than merely simple: a volume being
-- moved must be charged to its destination *before* it becomes the primary there, or
-- two placements would both see room. It is read out of the operation's own recorded
-- progress, which carries a per-volume stage, so "reserved but not yet primary" is
-- readable rather than inferred. An entry stops reserving the moment the volume it
-- names is actually primary on the host — otherwise the volume is charged twice,
-- once as a plan and once as a placement — and a settled entry (DONE, FOREIGN)
-- reserves nothing at all.
--
-- The join is on volume_id::text rather than a cast of the JSON value to uuid: a
-- malformed plan must make the row disappear from the sum, not make every capacity
-- read raise. The consequence, stated in ADR-0017, is that an operation whose plan is
-- lost makes its destination look emptier than it is — which is why the plan is
-- written term-guarded, before the work it describes.
--
-- It runs at placement time, not on the data path, over a fleet of hundreds of rows.
SELECT sqlc.embed(h), (
       COALESCE((SELECT SUM(v.size_bytes) FROM volumes v
       WHERE v.primary_host_id = h.host_id), 0)
       + COALESCE((SELECT SUM(rv.size_bytes)
       FROM operations o
       CROSS JOIN LATERAL jsonb_array_elements(
       CASE WHEN jsonb_typeof(o.current_state -> 'volumes') = 'array'
       THEN o.current_state -> 'volumes'
       ELSE '[]'::jsonb END) AS e
       JOIN volumes rv ON rv.volume_id::text = e ->> 'volume_id'
       WHERE o.phase NOT IN ('SUCCEEDED', 'CANCELED')
       AND e ->> 'to_host' = (h.host_id)::text
       AND COALESCE(e ->> 'stage', '') NOT IN ('DONE', 'FOREIGN')
       AND rv.primary_host_id IS DISTINCT FROM h.host_id), 0))::BIGINT AS committed_bytes
  FROM hosts h
 WHERE h.host_id = $1;

-- name: ListHosts :many
-- Deterministic order: placement decisions must not depend on row order (INV-02).
-- The committed-bytes expression is GetHost's; see the comment there.
SELECT sqlc.embed(h), (
       COALESCE((SELECT SUM(v.size_bytes) FROM volumes v
       WHERE v.primary_host_id = h.host_id), 0)
       + COALESCE((SELECT SUM(rv.size_bytes)
       FROM operations o
       CROSS JOIN LATERAL jsonb_array_elements(
       CASE WHEN jsonb_typeof(o.current_state -> 'volumes') = 'array'
       THEN o.current_state -> 'volumes'
       ELSE '[]'::jsonb END) AS e
       JOIN volumes rv ON rv.volume_id::text = e ->> 'volume_id'
       WHERE o.phase NOT IN ('SUCCEEDED', 'CANCELED')
       AND e ->> 'to_host' = (h.host_id)::text
       AND COALESCE(e ->> 'stage', '') NOT IN ('DONE', 'FOREIGN')
       AND rv.primary_host_id IS DISTINCT FROM h.host_id), 0))::BIGINT AS committed_bytes
  FROM hosts h
 ORDER BY h.host_id;

-- name: SetHostState :execrows
-- cordon / drain / mark dead (§28.1), term-guarded and transition-guarded: $4 is the
-- set of states that may legally become $2, taken from the lifecycle table. Doing it
-- in the predicate keeps the check atomic (no read-modify-write race) and means the
-- rule holds even for a client that skipped the Go layer.
UPDATE hosts
   SET state = $2
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND state = ANY(sqlc.arg(allowed_states)::text[]);

-- name: RenewHostLease :execrows
-- Grouped per-host lease renewal (§12.6), term-guarded. The host-exists predicate
-- turns "lease for an id nobody registered" into 0 rows (ErrNotFound) instead of a
-- foreign-key error: a lease is a fencing token, and granting one to an unknown
-- host invents authority over a volume nobody can find.
--
-- The state predicate is the second half of that rule. Marking a host DEAD is the
-- Control Plane asserting its writer is gone — the assertion promotion accepts as a
-- reason to skip the fencing wait — so a routine heartbeat must not be able to
-- re-arm the lease of a host that has just been fenced. CORDONED and DRAINING are
-- deliberately still allowed: both are still serving the volumes they hold, and
-- refusing their renewals would stop their ACKs in the middle of an evacuation.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $3
)
INSERT INTO host_leases (host_id, granted_at, last_renewal, ttl_seconds)
SELECT $1, now(), now(), $2
WHERE EXISTS (SELECT 1 FROM valid)
  AND EXISTS (SELECT 1 FROM hosts
               WHERE host_id = $1
                 AND state = ANY(sqlc.arg(serving_states)::text[]))
ON CONFLICT (host_id) DO UPDATE
  SET last_renewal = now(),
      ttl_seconds = EXCLUDED.ttl_seconds;

-- name: RevokeHostLease :execrows
-- Take a host's lease away (§12.6), term-guarded. Deleting the row is what stops
-- the Control Plane's own view of the lease from being renewed behind a fence; the
-- Agent counts its copy down on a monotonic clock and never learns the row is gone,
-- which is why a promotion still waits out the full lease_ttl + max_clock_skew.
--
-- The host-exists predicate keeps "no such host" (0 rows, ErrNotFound) apart from
-- "that host holds no lease", which is the state the caller asked for.
DELETE FROM host_leases
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $2;

-- name: HostExists :one
SELECT EXISTS (SELECT 1 FROM hosts WHERE host_id = $1);

-- name: GetHostLease :one
SELECT * FROM host_leases WHERE host_id = $1;
