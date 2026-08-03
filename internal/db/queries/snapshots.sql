-- name: CreateSnapshot :execrows
-- Term-guarded snapshot record (§19).
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $11
)
INSERT INTO snapshots (
    snapshot_id, volume_id, parent_snapshot_id, epoch, target_sequence,
    root_digest, source_host_id, state, manifest_key, request_id
)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
WHERE EXISTS (SELECT 1 FROM valid)
-- INV-16: a snapshot is immutable once it is in the catalog, so a duplicate record
-- — a retried request, or two operators rebuilding at once — is a no-op rather than
-- an overwrite or a 23505 that aborts the rebuild half-way. Untargeted, so the
-- request_id uniqueness (§18 idempotency) is covered too. 0 rows is ambiguous
-- between "already there" and "stale term"; the adapter disambiguates.
ON CONFLICT DO NOTHING;

-- name: GetSnapshot :one
SELECT * FROM snapshots WHERE snapshot_id = $1;

-- name: SetSnapshotState :execrows
-- The §19 lifecycle (CREATING → PUBLISHED | FAILED → DELETING), term-guarded and
-- transition-guarded in the predicate: $3 is the set of states that may legally
-- become $2. Without this a snapshot whose publication crashed stays CREATING
-- forever and the catalog side of GC never sees it.
UPDATE snapshots
   SET state = $2
 WHERE snapshot_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND state = ANY(sqlc.arg(allowed_states)::text[]);

-- name: ListPendingSnapshots :many
-- The snapshots a host has been asked to take (§19), for the desired state it is
-- handed. Joined through volumes rather than filtered on snapshots.source_host_id,
-- because the request names a *volume*: whichever host serves it when the request is
-- picked up is the one that can freeze it, and source_host_id is stamped on
-- completion by the host that actually did — which is what §20's placement rule 1
-- later reads.
--
-- Ordered by snapshot_id so a host with several outstanding takes them oldest first
-- (UUIDv7 is time-ordered, INV-22) and two Control Planes answer identically (INV-02).
SELECT sqlc.embed(s) FROM snapshots s
  JOIN volumes v ON v.volume_id = s.volume_id
 WHERE v.primary_host_id = $1
   AND s.state = 'CREATING'
 ORDER BY s.snapshot_id;

-- name: PublishSnapshot :execrows
-- CREATING → PUBLISHED, stamping the three facts only the host that took it knows:
-- the sequence the copy was frozen at, the manifest it wrote, and which host did it.
-- Term-guarded, and guarded on CREATING so a report replayed after the snapshot has
-- moved on cannot resurrect it (INV-16: PUBLISHED never changes).
UPDATE snapshots
   SET state = 'PUBLISHED',
       target_sequence = $2,
       source_host_id = $3,
       manifest_key = $4
 WHERE snapshot_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND state = 'CREATING';
