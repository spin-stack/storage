-- name: UpsertHost :execrows
-- Term-guarded (§7): the INSERT ... SELECT produces no row when the term is stale,
-- so a zombie CP affects 0 rows.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $8
)
INSERT INTO hosts (
    host_id, state, agent_version, max_format_version,
    nvme_total_bytes, nvme_used_bytes, nvme_remote_backlog_bytes, last_heartbeat
)
SELECT $1, $2, $3, $4, $5, $6, $7, now()
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
      nvme_remote_backlog_bytes = EXCLUDED.nvme_remote_backlog_bytes,
      last_heartbeat = now();

-- name: GetHost :one
-- Committed capacity comes with the host because every reader of a host is a
-- placement decision waiting to happen, and a §28.2 decision taken against a stale
-- copy of that number is exactly what ADR-0017 removed the copy to prevent.
--
-- The derivation itself is the host_committed_bytes view (schema.sql), which is
-- where its reasoning lives; three other queries read the same view (ListHosts
-- below, and the bound predicates in volumes.sql and operations.sql). It used to be
-- copied into all four, for a tooling reason ADR-0019 removed.
--
-- It runs at placement time, not on the data path, over a fleet of hundreds of rows.
-- Joined rather than read as a scalar subquery: both plans push the host filter
-- into the derivation, but the join lets the planner visit `hosts` once for the
-- whole listing below instead of once per row.
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
-- overwritable_reasons is what a write made for this reason may replace, so the
-- pressure loop's write simply does not match a host an operator cordoned. In Go it
-- would be a read, a comparison and then a write, and an operator's cordon landing
-- between the read and the write would be cleared anyway — which is the single
-- outcome the reason column exists to prevent.
--
-- cordon_reason is set from an argument the caller derives rather than from a CASE on
-- $2 here, because "the reason is empty unless the state is CORDONED" is the same
-- rule the table constraint states and the Go type states; a third copy in SQL is a
-- third place it can drift.
UPDATE hosts
   SET state = $2, cordon_reason = sqlc.arg(cordon_reason)
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3
   AND state = ANY(sqlc.arg(allowed_states)::text[])
   AND cordon_reason = ANY(sqlc.arg(overwritable_reasons)::text[]);

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
-- The third predicate is the ADR-0016 revocation window: while it is open the
-- Control Plane has revoked this host's lease to fence one of its volumes, and a
-- renewal would put back exactly what the fence took away. It is bounded by its own
-- deadline, so a Control Plane that dies mid-promotion cannot leave a host unable to
-- renew for ever.
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $3
)
INSERT INTO host_leases (host_id, granted_at, last_renewal, ttl_seconds)
SELECT $1, now(), now(), $2
WHERE EXISTS (SELECT 1 FROM valid)
  AND EXISTS (SELECT 1 FROM hosts
               WHERE host_id = $1
                 AND state = ANY(sqlc.arg(serving_states)::text[])
                 AND (renewals_blocked_until IS NULL OR renewals_blocked_until <= now()))
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

-- name: BlockHostRenewals :execrows
-- Open (or re-arm) the ADR-0016 revocation window on a host for the next $2 seconds,
-- term-guarded. Re-arming is what a resumed pass does: the promotion it belongs to is
-- still running, so the window follows it rather than expiring under it.
UPDATE hosts
   SET renewals_blocked_until = now() + make_interval(secs => $2)
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $3;

-- name: UnblockHostRenewals :execrows
-- Close the window (term-guarded). Idempotent: a host with no window is the state the
-- caller asked for, which matters because this runs on every exit path of a promotion
-- including the ones that never opened one.
UPDATE hosts
   SET renewals_blocked_until = NULL
 WHERE host_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = $2;

-- name: LockHostPlacement :exec
-- Serialize the placements aimed at one host, for the duration of one transaction.
--
-- The capacity bound is a predicate of the write that places the bytes (ADR-0017),
-- which is necessary and — in PostgreSQL — not sufficient. READ COMMITTED fixes a
-- statement's snapshot before the statement runs, and the derived committed value is
-- an aggregate over rows the statement does not lock, so two INSERTs that overlap in
-- time each evaluate the bound against a fleet that does not contain the other. Both
-- affect one row and the destination lands at twice its ceiling. Measured, not
-- feared: two psql sessions, one bound of 100 bytes, two 100-byte volumes, 200
-- committed afterwards.
--
-- An advisory lock taken *inside* that statement would change nothing — the snapshot
-- is already taken. It has to be its own statement in the same transaction, because
-- READ COMMITTED gives the next statement a fresh snapshot: the loser blocks here,
-- and the INSERT it then runs sees the winner's row and refuses itself.
--
-- Chosen over SERIALIZABLE, which would push a retry loop into every caller of the
-- Store for a conflict that is a two-row hot spot, and over `SELECT ... FOR UPDATE`
-- on the host row, which locks the wrong thing: every heartbeat writes that row, and
-- the rows being counted are in volumes and operations.
--
-- (Wording note, not a style rule: TestEveryMutatingQueryIsTermGuarded classifies a
-- query by matching INSERT/UPDATE/DELETE over the whole chunk, comments included, so
-- prose here that names one of those verbs makes this read look like a write.)
--
-- The key is a hash of the host id, so two hosts never wait for each other and a
-- collision costs one placement a wait and nothing else.
SELECT pg_advisory_xact_lock(hashtextextended(sqlc.arg(host_id)::uuid::text, 0));
