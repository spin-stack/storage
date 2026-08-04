ALTER TABLE hosts
ADD COLUMN cordon_reason text DEFAULT '' NOT NULL CONSTRAINT hosts_cordon_reason_check CHECK (cordon_reason IN (''::text, 'OPERATOR'::text, 'DEVICE_PRESSURE'::text));

ALTER TABLE hosts
ADD CONSTRAINT hosts_cordon_reason_belongs_to_a_cordon CHECK (state = 'CORDONED'::text OR cordon_reason = ''::text) NOT VALID;

ALTER TABLE hosts VALIDATE CONSTRAINT hosts_cordon_reason_belongs_to_a_cordon;
