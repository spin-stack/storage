CREATE DOMAIN uuidv7 AS uuid
  CONSTRAINT uuidv7_check CHECK ((get_byte(uuid_send(VALUE), 6) >> 4) = 7);

ALTER TABLE hosts DROP CONSTRAINT hosts_host_id_check;

ALTER TABLE hosts ALTER COLUMN host_id TYPE uuidv7 USING host_id::uuidv7;

ALTER TABLE operations DROP CONSTRAINT operations_operation_id_check;

ALTER TABLE operations ALTER COLUMN operation_id TYPE uuidv7 USING operation_id::uuidv7;

ALTER TABLE snapshots DROP CONSTRAINT snapshots_request_id_check;

ALTER TABLE snapshots DROP CONSTRAINT snapshots_snapshot_id_check;

ALTER TABLE snapshots ALTER COLUMN snapshot_id TYPE uuidv7 USING snapshot_id::uuidv7;

ALTER TABLE snapshots ALTER COLUMN request_id TYPE uuidv7 USING request_id::uuidv7;

ALTER TABLE volumes DROP CONSTRAINT volumes_volume_id_check;

ALTER TABLE volumes ALTER COLUMN volume_id TYPE uuidv7 USING volume_id::uuidv7;

ALTER TABLE volumes ALTER COLUMN active_root_id TYPE uuidv7 USING active_root_id::uuidv7;

ALTER TABLE volumes ALTER COLUMN published_root_id TYPE uuidv7 USING published_root_id::uuidv7;
