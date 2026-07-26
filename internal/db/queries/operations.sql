-- name: RecordOperation :execrows
-- Admin idempotency by request_id (§18) + term guard (§7): the INSERT ... SELECT
-- produces no row for a stale term, so a zombie CP cannot record work. Returns 1 if
-- newly recorded, 0 if the operation_id already existed or the term is stale.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $8
)
INSERT INTO operations (operation_id, kind, volume_id, host_id, desired_state, current_state, phase)
SELECT $1, $2, $3, $4, $5, $6, $7
WHERE EXISTS (SELECT 1 FROM valid)
ON CONFLICT (operation_id) DO NOTHING;

-- name: GetOperation :one
SELECT * FROM operations WHERE operation_id = $1;

-- name: ListOperationsByHost :many
-- Every operation recorded against a host, so a reconciler can ask what is already
-- happening to it before starting something else (§7, §28.1). Deterministic order:
-- the answer must not depend on row order (INV-02), and the composite index
-- operations (host_id, operation_id) satisfies both the filter and the sort.
SELECT * FROM operations WHERE host_id = $1 ORDER BY operation_id;

-- name: UpdateOperationPhase :execrows
-- Transition-guarded (§7): $5 is the set of phases that may legally become $3, so a
-- terminal operation cannot be resurrected even by a buggy caller.
UPDATE operations
   SET current_state = $2, phase = $3, error = $4, updated_at = now()
 WHERE operation_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND phase = ANY(sqlc.arg(allowed_phases)::text[]);
