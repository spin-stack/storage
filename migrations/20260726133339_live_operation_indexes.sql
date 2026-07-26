CREATE INDEX CONCURRENTLY IF NOT EXISTS operations_live_by_host_idx ON operations (host_id, operation_id) WHERE (phase <> ALL (ARRAY['SUCCEEDED'::text, 'CANCELED'::text]));

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
WHERE c.relname = 'operations_live_by_host_idx';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS operations_one_live_drain_per_host_idx ON operations (host_id) WHERE (kind = 'drain'::text) AND (phase <> ALL (ARRAY['SUCCEEDED'::text, 'CANCELED'::text]));

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
WHERE c.relname = 'operations_one_live_drain_per_host_idx';
