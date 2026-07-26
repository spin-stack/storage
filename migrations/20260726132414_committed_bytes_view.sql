CREATE OR REPLACE VIEW host_committed_bytes AS
 SELECT host_id,
    (COALESCE(( SELECT sum(v.size_bytes) AS sum
           FROM volumes v
          WHERE v.primary_host_id = h.host_id::uuid), 0::numeric) + COALESCE(( SELECT sum(rv.size_bytes) AS sum
           FROM operations o
             CROSS JOIN LATERAL jsonb_array_elements(
                CASE
                    WHEN jsonb_typeof(o.current_state -> 'volumes'::text) = 'array'::text THEN o.current_state -> 'volumes'::text
                    ELSE '[]'::jsonb
                END) e(value)
             JOIN volumes rv ON rv.volume_id::text = (e.value ->> 'volume_id'::text)
          WHERE (o.phase <> ALL (ARRAY['SUCCEEDED'::text, 'CANCELED'::text])) AND (e.value ->> 'to_host'::text) = h.host_id::text AND (COALESCE(e.value ->> 'stage'::text, ''::text) <> ALL (ARRAY['DONE'::text, 'FOREIGN'::text])) AND rv.primary_host_id IS DISTINCT FROM h.host_id::uuid), 0::numeric))::bigint AS committed_bytes
   FROM hosts h;
