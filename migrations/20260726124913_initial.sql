CREATE TABLE IF NOT EXISTS control_plane_leader (
    singleton boolean DEFAULT true,
    term bigint NOT NULL,
    holder_id text NOT NULL,
    renewed_at timestamptz NOT NULL,
    CONSTRAINT control_plane_leader_pkey PRIMARY KEY (singleton),
    CONSTRAINT control_plane_leader_singleton_check CHECK (singleton)
);

CREATE TABLE IF NOT EXISTS hosts (
    host_id uuid,
    state text NOT NULL,
    agent_version text DEFAULT '' NOT NULL,
    max_format_version integer DEFAULT 2 NOT NULL,
    nvme_total_bytes bigint DEFAULT 0 NOT NULL,
    nvme_used_bytes bigint DEFAULT 0 NOT NULL,
    last_heartbeat timestamptz NOT NULL,
    renewals_blocked_until timestamptz,
    CONSTRAINT hosts_pkey PRIMARY KEY (host_id),
    CONSTRAINT hosts_host_id_check CHECK ((get_byte(uuid_send(host_id), 6) >> 4) = 7),
    CONSTRAINT hosts_state_check CHECK (state IN ('ACTIVE'::text, 'CORDONED'::text, 'DRAINING'::text, 'DEAD'::text))
);

CREATE TABLE IF NOT EXISTS host_leases (
    host_id uuid,
    granted_at timestamptz NOT NULL,
    last_renewal timestamptz NOT NULL,
    ttl_seconds integer DEFAULT 10 NOT NULL,
    CONSTRAINT host_leases_pkey PRIMARY KEY (host_id),
    CONSTRAINT host_leases_host_id_fkey FOREIGN KEY (host_id) REFERENCES hosts (host_id)
);

CREATE TABLE IF NOT EXISTS volumes (
    volume_id uuid,
    size_bytes bigint NOT NULL,
    durability text DEFAULT 'remote' NOT NULL,
    block_size integer NOT NULL,
    current_epoch bigint DEFAULT 0 NOT NULL,
    state text NOT NULL,
    primary_host_id uuid,
    standby_host_id uuid,
    active_root_id uuid,
    published_root_id uuid,
    chain_depth integer DEFAULT 0 NOT NULL,
    dek_wrapped bytea NOT NULL,
    kek_id text NOT NULL,
    local_sequence bigint DEFAULT 0 NOT NULL,
    durable_sequence bigint DEFAULT 0 NOT NULL,
    published_sequence bigint DEFAULT 0 NOT NULL,
    fencing_started_at timestamptz,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT volumes_pkey PRIMARY KEY (volume_id),
    CONSTRAINT volumes_primary_host_id_fkey FOREIGN KEY (primary_host_id) REFERENCES hosts (host_id),
    CONSTRAINT volumes_standby_host_id_fkey FOREIGN KEY (standby_host_id) REFERENCES hosts (host_id),
    CONSTRAINT volumes_durability_check CHECK (durability IN ('remote'::text, 'local'::text)),
    CONSTRAINT volumes_state_check CHECK (state IN ('ACTIVE'::text, 'PRIMARY_SUSPECTED'::text, 'FENCING_WAIT'::text, 'RECOVERY_REQUIRED'::text, 'RECOVERING'::text, 'DETACHED'::text)),
    CONSTRAINT volumes_volume_id_check CHECK ((get_byte(uuid_send(volume_id), 6) >> 4) = 7)
);

CREATE INDEX IF NOT EXISTS volumes_primary_host_id_volume_id_idx ON volumes (primary_host_id, volume_id);

CREATE INDEX IF NOT EXISTS volumes_standby_host_id_idx ON volumes (standby_host_id);

CREATE TABLE IF NOT EXISTS operations (
    operation_id uuid,
    kind text NOT NULL,
    volume_id uuid,
    host_id uuid,
    desired_state jsonb NOT NULL,
    current_state jsonb NOT NULL,
    phase text NOT NULL,
    error text,
    created_at timestamptz DEFAULT now() NOT NULL,
    updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT operations_pkey PRIMARY KEY (operation_id),
    CONSTRAINT operations_host_id_fkey FOREIGN KEY (host_id) REFERENCES hosts (host_id),
    CONSTRAINT operations_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES volumes (volume_id),
    CONSTRAINT operations_kind_check CHECK (kind IN ('attach'::text, 'detach'::text, 'clone'::text, 'resize'::text, 'drain'::text, 'recovery'::text, 'flatten'::text, 'gc'::text)),
    CONSTRAINT operations_operation_id_check CHECK ((get_byte(uuid_send(operation_id), 6) >> 4) = 7),
    CONSTRAINT operations_phase_check CHECK (phase IN ('PENDING'::text, 'RUNNING'::text, 'CANCELING'::text, 'CANCELED'::text, 'SUCCEEDED'::text, 'FAILED'::text))
);

CREATE INDEX IF NOT EXISTS operations_host_id_operation_id_idx ON operations (host_id, operation_id);

CREATE INDEX IF NOT EXISTS operations_volume_id_idx ON operations (volume_id);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id uuid,
    volume_id uuid NOT NULL,
    parent_snapshot_id uuid,
    epoch bigint NOT NULL,
    target_sequence bigint NOT NULL,
    root_digest text NOT NULL,
    source_host_id uuid,
    state text NOT NULL,
    portable boolean DEFAULT false NOT NULL,
    manifest_key text,
    request_id uuid NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT snapshots_pkey PRIMARY KEY (snapshot_id),
    CONSTRAINT snapshots_request_id_key UNIQUE (request_id),
    CONSTRAINT snapshots_parent_snapshot_id_fkey FOREIGN KEY (parent_snapshot_id) REFERENCES snapshots (snapshot_id),
    CONSTRAINT snapshots_source_host_id_fkey FOREIGN KEY (source_host_id) REFERENCES hosts (host_id),
    CONSTRAINT snapshots_volume_id_fkey FOREIGN KEY (volume_id) REFERENCES volumes (volume_id),
    CONSTRAINT snapshots_request_id_check CHECK ((get_byte(uuid_send(request_id), 6) >> 4) = 7),
    CONSTRAINT snapshots_snapshot_id_check CHECK ((get_byte(uuid_send(snapshot_id), 6) >> 4) = 7),
    CONSTRAINT snapshots_state_check CHECK (state IN ('CREATING'::text, 'PUBLISHED'::text, 'FAILED'::text, 'DELETING'::text))
);

CREATE INDEX IF NOT EXISTS snapshots_parent_snapshot_id_idx ON snapshots (parent_snapshot_id);

CREATE INDEX IF NOT EXISTS snapshots_source_host_id_idx ON snapshots (source_host_id);

CREATE INDEX IF NOT EXISTS snapshots_volume_id_idx ON snapshots (volume_id);
