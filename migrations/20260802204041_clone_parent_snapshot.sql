ALTER TABLE volumes
ADD COLUMN parent_snapshot_id uuid CONSTRAINT volumes_parent_snapshot_id_fkey REFERENCES snapshots (snapshot_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS volumes_parent_snapshot_id_idx ON volumes (parent_snapshot_id);

-- pgschema:wait
SELECT 
    COALESCE(i.indisvalid, false) as done,
    CASE 
        WHEN p.blocks_total > 0 THEN p.blocks_done * 100 / p.blocks_total
        ELSE 0
    END as progress
FROM pg_class c
LEFT JOIN pg_index i ON c.oid = i.indexrelid
LEFT JOIN pg_stat_progress_create_index p ON c.oid = p.index_relid
WHERE c.relname = 'volumes_parent_snapshot_id_idx';
