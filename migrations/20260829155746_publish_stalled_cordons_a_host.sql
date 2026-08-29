ALTER TABLE hosts DROP CONSTRAINT hosts_cordon_reason_check;

ALTER TABLE hosts
ADD CONSTRAINT hosts_cordon_reason_check CHECK (cordon_reason IN (''::text, 'OPERATOR'::text, 'DEVICE_PRESSURE'::text, 'STALLED_PUBLISH'::text)) NOT VALID;

ALTER TABLE hosts VALIDATE CONSTRAINT hosts_cordon_reason_check;

ALTER TABLE volumes ADD COLUMN publish_stalled boolean DEFAULT false NOT NULL;
