-- Drop index "operations_host_id_idx" from table: "operations"
DROP INDEX "operations_host_id_idx";
-- Create index "operations_host_id_operation_id_idx" to table: "operations"
CREATE INDEX "operations_host_id_operation_id_idx" ON "operations" ("host_id", "operation_id");
