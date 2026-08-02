ALTER TABLE volumes
ADD COLUMN dek_key_id bigint NOT NULL CONSTRAINT volumes_dek_key_id_check CHECK (dek_key_id > 0 AND dek_key_id <= '4294967295'::bigint);
