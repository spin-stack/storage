-- name: CreateSnapshot :execrows
-- Term-guarded snapshot record (§19).
WITH valid AS (
    SELECT 1 FROM control_plane_leader WHERE singleton AND term = $8
)
INSERT INTO snapshots (
    snapshot_id, volume_id, parent_snapshot_id, epoch,
    commit_id, source_host_id, state, request_id
)
SELECT $1, $2, $3, $4, sqlc.narg(commit_id)::uuid, $5, $6, $7
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
-- The snapshots a host has been asked to take (§19). Joined through volumes rather than
-- filtered on snapshots.source_host_id, because the request names a *volume* and whichever
-- host serves it can freeze it; source_host_id is stamped on completion, which is what
-- §20's placement rule 1 later reads. Ordered by snapshot_id: oldest first (UUIDv7 is
-- time-ordered, INV-22) and identical between two Control Planes (INV-02).
SELECT sqlc.embed(s) FROM snapshots s
  JOIN volumes v ON v.volume_id = s.volume_id
 WHERE v.primary_host_id = $1
   AND s.state = 'CREATING'
 ORDER BY s.snapshot_id;

-- name: ListUnfinishedSnapshots :many
-- The snapshots nothing has closed out, fleet-wide, for a human reading the catalog.
--
-- The states come in as an array rather than being written here, the same move
-- SetSnapshotState makes: the §19 vocabulary's authority is internal/lifecycle
-- (SnapshotState.Unfinished), and a literal IN-list would stop matching the day a state is
-- added. No index, deliberately: this is unfiltered by host on purpose — the stuck snapshot
-- is the one whose volume has no primary — and an index on `state` would be maintained by
-- every snapshot write for a query an operator runs by hand during an incident.
SELECT * FROM snapshots
 WHERE state = ANY(sqlc.arg(states)::text[])
 ORDER BY snapshot_id;

-- name: PublishSnapshot :execrows
-- CREATING → PUBLISHED, stamping the two facts only the host that took it knows: the
-- commit its history is named by, and which host reported it. The manifest key is not
-- among them and must not be: it is derived from the commit id (commit.ManifestKey), so
-- believing a string the Agent sent is how a catalog and a bucket come to disagree about
-- where a snapshot lives.
-- Term-guarded, and guarded on CREATING so a report replayed after the snapshot has
-- moved on cannot resurrect it (INV-16: PUBLISHED never changes).
UPDATE snapshots
   SET state = 'PUBLISHED',
       commit_id = $2,
       source_host_id = $3
 WHERE snapshot_id = $1
   AND (SELECT term FROM control_plane_leader WHERE singleton) = sqlc.arg(term)
   AND state = 'CREATING';
