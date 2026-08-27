ALTER TABLE volumes
ADD COLUMN rpo_target_seconds integer DEFAULT 0 NOT NULL CONSTRAINT volumes_rpo_target_seconds_check CHECK (rpo_target_seconds >= 0);
