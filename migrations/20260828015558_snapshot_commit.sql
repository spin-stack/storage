ALTER TABLE snapshots DROP COLUMN target_sequence;

ALTER TABLE snapshots DROP COLUMN root_digest;

ALTER TABLE snapshots DROP COLUMN manifest_key;

ALTER TABLE snapshots ADD COLUMN commit_id uuidv7;

ALTER TABLE snapshots
ADD CONSTRAINT snapshots_published_names_a_commit CHECK (state <> 'PUBLISHED'::text OR commit_id IS NOT NULL) NOT VALID;

ALTER TABLE snapshots VALIDATE CONSTRAINT snapshots_published_names_a_commit;
