-- Modify "hosts" table
ALTER TABLE "hosts" ADD COLUMN "renewals_blocked_until" timestamptz NULL;
