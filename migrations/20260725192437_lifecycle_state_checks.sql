-- Modify "hosts" table
ALTER TABLE "hosts" ADD CONSTRAINT "hosts_state_check" CHECK (state = ANY (ARRAY['ACTIVE'::text, 'CORDONED'::text, 'DRAINING'::text, 'DEAD'::text]));
-- Modify "operations" table
ALTER TABLE "operations" ADD CONSTRAINT "operations_kind_check" CHECK (kind = ANY (ARRAY['attach'::text, 'detach'::text, 'clone'::text, 'resize'::text, 'drain'::text, 'recovery'::text, 'flatten'::text, 'gc'::text])), ADD CONSTRAINT "operations_phase_check" CHECK (phase = ANY (ARRAY['PENDING'::text, 'RUNNING'::text, 'CANCELING'::text, 'CANCELED'::text, 'SUCCEEDED'::text, 'FAILED'::text]));
-- Modify "snapshots" table
ALTER TABLE "snapshots" ADD CONSTRAINT "snapshots_state_check" CHECK (state = ANY (ARRAY['CREATING'::text, 'PUBLISHED'::text, 'FAILED'::text, 'DELETING'::text]));
-- Modify "volumes" table
ALTER TABLE "volumes" ADD CONSTRAINT "volumes_durability_check" CHECK (durability = ANY (ARRAY['remote'::text, 'local'::text])), ADD CONSTRAINT "volumes_state_check" CHECK (state = ANY (ARRAY['ACTIVE'::text, 'PRIMARY_SUSPECTED'::text, 'FENCING_WAIT'::text, 'RECOVERY_REQUIRED'::text, 'RECOVERING'::text, 'DETACHED'::text]));
