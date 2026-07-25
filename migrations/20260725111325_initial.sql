-- Create "control_plane_leader" table
CREATE TABLE "control_plane_leader" (
  "singleton" boolean NOT NULL DEFAULT true,
  "term" bigint NOT NULL,
  "holder_id" text NOT NULL,
  "renewed_at" timestamptz NOT NULL,
  PRIMARY KEY ("singleton"),
  CONSTRAINT "control_plane_leader_singleton_check" CHECK (singleton)
);
-- Create "hosts" table
CREATE TABLE "hosts" (
  "host_id" uuid NOT NULL,
  "state" text NOT NULL,
  "agent_version" text NOT NULL DEFAULT '',
  "max_format_version" integer NOT NULL DEFAULT 2,
  "nvme_total_bytes" bigint NOT NULL DEFAULT 0,
  "nvme_used_bytes" bigint NOT NULL DEFAULT 0,
  "nvme_committed_bytes" bigint NOT NULL DEFAULT 0,
  "last_heartbeat" timestamptz NOT NULL,
  PRIMARY KEY ("host_id")
);
-- Create "host_leases" table
CREATE TABLE "host_leases" (
  "host_id" uuid NOT NULL,
  "granted_at" timestamptz NOT NULL,
  "last_renewal" timestamptz NOT NULL,
  "ttl_seconds" integer NOT NULL DEFAULT 10,
  PRIMARY KEY ("host_id"),
  CONSTRAINT "host_leases_host_id_fkey" FOREIGN KEY ("host_id") REFERENCES "hosts" ("host_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create "volumes" table
CREATE TABLE "volumes" (
  "volume_id" uuid NOT NULL,
  "size_bytes" bigint NOT NULL,
  "durability" text NOT NULL DEFAULT 'remote',
  "block_size" integer NOT NULL,
  "current_epoch" bigint NOT NULL DEFAULT 0,
  "state" text NOT NULL,
  "primary_host_id" uuid NULL,
  "standby_host_id" uuid NULL,
  "active_root_id" uuid NULL,
  "published_root_id" uuid NULL,
  "chain_depth" integer NOT NULL DEFAULT 0,
  "dek_wrapped" bytea NOT NULL,
  "kek_id" text NOT NULL,
  "local_sequence" bigint NOT NULL DEFAULT 0,
  "durable_sequence" bigint NOT NULL DEFAULT 0,
  "published_sequence" bigint NOT NULL DEFAULT 0,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("volume_id"),
  CONSTRAINT "volumes_primary_host_id_fkey" FOREIGN KEY ("primary_host_id") REFERENCES "hosts" ("host_id") ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "volumes_standby_host_id_fkey" FOREIGN KEY ("standby_host_id") REFERENCES "hosts" ("host_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create "operations" table
CREATE TABLE "operations" (
  "operation_id" uuid NOT NULL,
  "kind" text NOT NULL,
  "volume_id" uuid NULL,
  "host_id" uuid NULL,
  "desired_state" jsonb NOT NULL,
  "current_state" jsonb NOT NULL,
  "phase" text NOT NULL,
  "error" text NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("operation_id"),
  CONSTRAINT "operations_host_id_fkey" FOREIGN KEY ("host_id") REFERENCES "hosts" ("host_id") ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "operations_volume_id_fkey" FOREIGN KEY ("volume_id") REFERENCES "volumes" ("volume_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
-- Create "snapshots" table
CREATE TABLE "snapshots" (
  "snapshot_id" uuid NOT NULL,
  "volume_id" uuid NOT NULL,
  "parent_snapshot_id" uuid NULL,
  "epoch" bigint NOT NULL,
  "target_sequence" bigint NOT NULL,
  "root_digest" text NOT NULL,
  "source_host_id" uuid NULL,
  "state" text NOT NULL,
  "portable" boolean NOT NULL DEFAULT false,
  "manifest_key" text NULL,
  "request_id" uuid NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("snapshot_id"),
  CONSTRAINT "snapshots_request_id_key" UNIQUE ("request_id"),
  CONSTRAINT "snapshots_parent_snapshot_id_fkey" FOREIGN KEY ("parent_snapshot_id") REFERENCES "snapshots" ("snapshot_id") ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "snapshots_source_host_id_fkey" FOREIGN KEY ("source_host_id") REFERENCES "hosts" ("host_id") ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "snapshots_volume_id_fkey" FOREIGN KEY ("volume_id") REFERENCES "volumes" ("volume_id") ON UPDATE NO ACTION ON DELETE NO ACTION
);
