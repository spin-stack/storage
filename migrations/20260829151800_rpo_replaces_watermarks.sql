ALTER TABLE volumes DROP COLUMN active_root_id;

ALTER TABLE volumes DROP COLUMN published_root_id;

ALTER TABLE volumes DROP COLUMN local_sequence;

ALTER TABLE volumes DROP COLUMN durable_sequence;

ALTER TABLE volumes DROP COLUMN published_sequence;

ALTER TABLE volumes
ADD COLUMN commit_age_seconds integer CONSTRAINT volumes_commit_age_seconds_check CHECK (commit_age_seconds >= 0);

ALTER TABLE volumes
ADD COLUMN unpublished_local_bytes bigint DEFAULT 0 NOT NULL CONSTRAINT volumes_unpublished_local_bytes_check CHECK (unpublished_local_bytes >= 0);

ALTER TABLE volumes ADD COLUMN reported_at timestamptz;

ALTER TABLE volumes
ADD CONSTRAINT volumes_progress_is_reported_together CHECK ((commit_age_seconds IS NULL) = (reported_at IS NULL)) NOT VALID;

ALTER TABLE volumes VALIDATE CONSTRAINT volumes_progress_is_reported_together;
