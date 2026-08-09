ALTER TABLE volumes
ADD COLUMN refusal text DEFAULT '' NOT NULL CONSTRAINT volumes_refusal_check CHECK (refusal IN (''::text, 'IMAGE_MISSING'::text, 'DURABILITY_LOST'::text, 'NO_READ_VIEW'::text, 'NO_KEY'::text, 'LEASE_LOST'::text, 'ATTACH_FAILED'::text));

ALTER TABLE volumes ADD COLUMN refusal_detail text DEFAULT '' NOT NULL;

ALTER TABLE volumes
ADD CONSTRAINT volumes_refusal_detail_needs_a_refusal CHECK (refusal <> ''::text OR refusal_detail = ''::text) NOT VALID;

ALTER TABLE volumes VALIDATE CONSTRAINT volumes_refusal_detail_needs_a_refusal;
