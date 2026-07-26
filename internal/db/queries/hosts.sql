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
-- The conflict path is a heartbeat: it refreshes only what the host knows about
-- itself. `state` and `nvme_committed_bytes` are the Control Plane's (SetHostState,
-- CommitHostCapacity) and are deliberately absent — a routine heartbeat carrying
-- State=ACTIVE would un-cordon a host that is being drained, and one carrying the
-- agent's idea of committed bytes would zero the ledger the drain releases against.
ON CONFLICT (host_id) DO UPDATE
  SET agent_version = EXCLUDED.agent_version,
      max_format_version = EXCLUDED.max_format_version,
      nvme_total_bytes = EXCLUDED.nvme_total_bytes,
      nvme_used_bytes = EXCLUDED.nvme_used_bytes,
      last_heartbeat = now();

-- name: GetHost :one
SELECT * FROM hosts WHERE host_id = $1;

-- name: ListHosts :many
-- Deterministic order: placement decisions must not depend on row order (INV-02).
SELECT * FROM hosts ORDER BY host_id;

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

-- name: CommitHostCapacity :execrows
-- Reserve (positive) or release (negative) committed NVMe bytes (§28.2), term-
-- guarded. The non-negative guard makes an over-release affect 0 rows instead of
-- corrupting the accounting.
UPDATE hosts
   SET nvme_committed_bytes = nvme_committed_bytes + $2
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND nvme_committed_bytes + $2 >= 0;

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
