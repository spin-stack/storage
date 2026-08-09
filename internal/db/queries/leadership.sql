-- name: AcquireLeadership :one
-- Take (or renew) Control Plane leadership, incrementing the term (§7).
INSERT INTO control_plane_leader (singleton, term, holder_id, renewed_at)
VALUES (true, 1, $1, now())
ON CONFLICT (singleton) DO UPDATE
  SET term = control_plane_leader.term + 1,
      holder_id = $1,
      renewed_at = now()
RETURNING term;

-- name: RenewLeadership :execrows
-- Say "still here" without becoming a new leader (§7). The term and the holder are
-- both predicates and neither is written: this is the one leadership write that must
-- not move the term, because every admin one-shot in cmd/control-plane reads GetLeader
-- and then writes under the term it read.
--
-- 0 rows is the whole point of the guard. It means this process is no longer the
-- leader — superseded by an election, or running against a database that was restored
-- underneath it — and the caller's answer to that is to exit, not to retry.
UPDATE control_plane_leader
   SET renewed_at = now()
 WHERE singleton
   AND term = $1
   AND holder_id = $2;

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
