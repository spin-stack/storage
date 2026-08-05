DROP TABLE IF EXISTS operations CASCADE;

CREATE OR REPLACE VIEW host_committed_bytes AS
 SELECT host_id,
    COALESCE(( SELECT sum(v.size_bytes) AS sum
           FROM volumes v
          WHERE v.primary_host_id = h.host_id::uuid), 0::numeric)::bigint AS committed_bytes
   FROM hosts h;
