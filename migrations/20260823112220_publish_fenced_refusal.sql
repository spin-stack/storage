ALTER TABLE volumes DROP CONSTRAINT volumes_refusal_check;

ALTER TABLE volumes
ADD CONSTRAINT volumes_refusal_check CHECK (refusal IN (''::text, 'IMAGE_MISSING'::text, 'DURABILITY_LOST'::text, 'NO_READ_VIEW'::text, 'NO_KEY'::text, 'LEASE_LOST'::text, 'ATTACH_FAILED'::text, 'PUBLISH_FENCED'::text)) NOT VALID;

ALTER TABLE volumes VALIDATE CONSTRAINT volumes_refusal_check;
