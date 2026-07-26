-- Modify "volumes" table
ALTER TABLE "volumes" ADD COLUMN "fencing_started_at" timestamptz NULL;
