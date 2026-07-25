-- Modify "hosts" table
ALTER TABLE "hosts" ADD CONSTRAINT "hosts_host_id_check" CHECK ((get_byte(uuid_send(host_id), 6) >> 4) = 7);
-- Modify "operations" table
ALTER TABLE "operations" ADD CONSTRAINT "operations_operation_id_check" CHECK ((get_byte(uuid_send(operation_id), 6) >> 4) = 7);
-- Modify "snapshots" table
ALTER TABLE "snapshots" ADD CONSTRAINT "snapshots_request_id_check" CHECK ((get_byte(uuid_send(request_id), 6) >> 4) = 7), ADD CONSTRAINT "snapshots_snapshot_id_check" CHECK ((get_byte(uuid_send(snapshot_id), 6) >> 4) = 7);
-- Modify "volumes" table
ALTER TABLE "volumes" ADD CONSTRAINT "volumes_volume_id_check" CHECK ((get_byte(uuid_send(volume_id), 6) >> 4) = 7);
