-- name: UpsertHost :execrows
-- Term-guarded (§7): the INSERT ... SELECT produces no row when the term is stale,
-- so a zombie CP affects 0 rows.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = sqlc.arg(term)
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
-- Committed capacity comes with the host because every reader of a host is a placement
-- decision waiting to happen, and a §28.2 decision taken against a stale copy is what
-- ADR-0017 removed the copy to prevent. The derivation is the host_committed_bytes view
-- (schema.sql). Joined rather than read as a scalar subquery: both plans push the host
-- filter down, but the join lets the planner visit `hosts` once for the listing below.
SELECT sqlc.embed(h), COALESCE(c.committed_bytes, 0)::BIGINT AS committed_bytes
  FROM hosts h
  JOIN host_committed_bytes c ON c.host_id = h.host_id
 WHERE h.host_id = $1;

-- name: ListHosts :many
-- Deterministic order: placement decisions must not depend on row order (INV-02).
SELECT sqlc.embed(h), COALESCE(c.committed_bytes, 0)::BIGINT AS committed_bytes
  FROM hosts h
  JOIN host_committed_bytes c ON c.host_id = h.host_id
 ORDER BY h.host_id;

-- name: SetHostState :execrows
-- cordon / drain / mark dead (§28.1), term-guarded and transition-guarded: the
-- allowed_states array is the set of states that may legally become $2, taken from
-- the lifecycle table. Doing it in the predicate keeps the check atomic (no
-- read-modify-write race) and means the rule holds even for a client that skipped
-- the Go layer.
--
-- The third predicate is the same move for cordon authority (ADR-0013 §3, §5):
-- overwritable_reasons is what a write made for this reason may replace, so the pressure
-- loop's write does not match a host an operator cordoned — in Go it would be a read, a
-- comparison and a write, with the operator's cordon landing between the read and the write
-- and being cleared anyway. cordon_reason comes from a caller-derived argument rather than a
-- CASE here, because the table constraint and the Go type already state that rule.
UPDATE hosts
   SET state = $2, cordon_reason = sqlc.arg(cordon_reason)
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND state = ANY(sqlc.arg(allowed_states)::text[])
   AND cordon_reason = ANY(sqlc.arg(overwritable_reasons)::text[]);

-- name: RenewHostLease :execrows
-- Grouped per-host lease renewal (§12.6), term-guarded. The host-exists predicate turns
-- "lease for an id nobody registered" into 0 rows (ErrNotFound) instead of a foreign-key
-- error: granting a fencing token to an unknown host invents authority. The state predicate
-- is the other half — DEAD is the Control Plane asserting the writer is gone, so a routine
-- heartbeat must not re-arm it, while CORDONED and DRAINING still renew because both are
-- still serving the volumes they hold.
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

-- name: GetHostLease :one
SELECT * FROM host_leases WHERE host_id = $1;

-- name: LockHostPlacement :exec
-- Serialize the placements aimed at one host, for the duration of one transaction.
--
-- The capacity bound is a predicate of the write that places the bytes (ADR-0017), which
-- is necessary and — in PostgreSQL — not sufficient. READ COMMITTED fixes a statement's
-- snapshot before it runs, and the derived committed value is an aggregate over rows the
-- statement does not lock, so two overlapping placements each evaluate the bound against
-- a fleet without the other. Measured, not feared: two psql sessions, one bound of 100
-- bytes, two 100-byte volumes, 200 committed afterwards.
--
-- It has to be its own statement in the same transaction — a lock taken inside the
-- placing statement changes nothing, its snapshot already taken, while READ COMMITTED
-- gives the next statement a fresh one, so the loser blocks here and then refuses itself.
-- Chosen over SERIALIZABLE (a retry loop in every caller for a two-row hot spot) and over
-- `SELECT ... FOR UPDATE` on the host row (every heartbeat writes it, and the rows being
-- counted are in volumes). The key is a hash of the host id, so two hosts never wait.
--
-- (Wording note, not a style rule: TestEveryMutatingQueryIsTermGuarded matches
-- INSERT/UPDATE/DELETE over the whole chunk, comments included.)
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(host_id)::uuid::text, 0));
