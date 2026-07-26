-- name: AcquireLeadership :one
-- Take (or renew) Control Plane leadership, incrementing the term (§7).
INSERT INTO control_plane_leader (singleton, term, holder_id, renewed_at)
VALUES (true, 1, $1, now())
ON CONFLICT (singleton) DO UPDATE
  SET term = control_plane_leader.term + 1,
      holder_id = $1,
      renewed_at = now()
RETURNING term;

-- name: GetLeader :one
SELECT term, holder_id, renewed_at
FROM control_plane_leader
WHERE singleton;

-- name: DatabaseNow :one
-- The database's own clock. Every fencing deadline is derived from a timestamp this
-- clock stamped (host_leases.last_renewal), so the Control Plane compares against
-- this rather than against its own wall clock: a container clock that jumps forward
-- would otherwise shorten the fencing wait by exactly that offset (§12.1).
SELECT now()::timestamptz;
