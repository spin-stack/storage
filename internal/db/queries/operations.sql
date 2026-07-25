-- name: RecordOperation :execrows
-- Admin idempotency by request_id (§18). Returns 1 if newly recorded, 0 if the
-- operation_id already existed (a duplicate request).
INSERT INTO operations (operation_id, kind, volume_id, host_id, desired_state, current_state, phase)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (operation_id) DO NOTHING;

-- name: GetOperation :one
SELECT * FROM operations WHERE operation_id = $1;

-- name: UpdateOperationPhase :execrows
-- Transition-guarded (§7): $5 is the set of phases that may legally become $3, so a
-- terminal operation cannot be resurrected even by a buggy caller.
UPDATE operations
   SET current_state = $2, phase = $3, error = $4, updated_at = now()
 WHERE operation_id = $1
   AND phase = ANY(sqlc.arg(allowed_phases)::text[]);
