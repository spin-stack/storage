-- Create index "operations_host_id_idx" to table: "operations"
CREATE INDEX "operations_host_id_idx" ON "operations" ("host_id");
-- Create index "operations_volume_id_idx" to table: "operations"
CREATE INDEX "operations_volume_id_idx" ON "operations" ("volume_id");
-- Create index "snapshots_parent_snapshot_id_idx" to table: "snapshots"
CREATE INDEX "snapshots_parent_snapshot_id_idx" ON "snapshots" ("parent_snapshot_id");
-- Create index "snapshots_source_host_id_idx" to table: "snapshots"
CREATE INDEX "snapshots_source_host_id_idx" ON "snapshots" ("source_host_id");
-- Create index "snapshots_volume_id_idx" to table: "snapshots"
CREATE INDEX "snapshots_volume_id_idx" ON "snapshots" ("volume_id");
-- Create index "volumes_primary_host_id_volume_id_idx" to table: "volumes"
CREATE INDEX "volumes_primary_host_id_volume_id_idx" ON "volumes" ("primary_host_id", "volume_id");
-- Create index "volumes_standby_host_id_idx" to table: "volumes"
CREATE INDEX "volumes_standby_host_id_idx" ON "volumes" ("standby_host_id");
