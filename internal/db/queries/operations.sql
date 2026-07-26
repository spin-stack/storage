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
--
-- It also carries the §28.2 oversubscription bound (ADR-0017), because an operation's
-- recorded progress *is* its reservation: the plan entry a drain writes here is what
-- charges the destination for a volume that is not primary there yet. The bound is
-- evaluated against the derived value as it stands before this write, plus the bytes
-- this write is about to reserve, so two operations that chose the same destination
-- against the same fleet read produce one reservation and one ErrCapacityExceeded.
-- A write with no bound reserves nothing new (a progress save, a phase change).
-- The outer column references are qualified because the capacity predicate below
-- reads `operations` again (every in-flight plan, this one included): unqualified
-- `operation_id` would then be ambiguous.
UPDATE operations
   SET current_state = $2, phase = $3, error = $4, updated_at = now()
 WHERE operations.operation_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND operations.phase = ANY(sqlc.arg(allowed_phases)::text[])
   -- The derived value is the host_committed_bytes view; schema.sql says why it is
   -- a view and what it sums. A bound naming a host nobody registered admits
   -- nothing: no row in the view, a NULL comparison, no write.
   AND (sqlc.narg(bound_host)::uuid IS NULL
       OR (EXISTS (SELECT 1 FROM hosts WHERE host_id = sqlc.narg(bound_host)::uuid)
           AND (SELECT c.committed_bytes FROM host_committed_bytes c
                 WHERE c.host_id = sqlc.narg(bound_host)::uuid)
               + sqlc.arg(bound_add_bytes)::bigint <= sqlc.arg(bound_limit)::bigint));
