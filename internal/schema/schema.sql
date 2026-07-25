-- Control Plane metadata schema (§8). Authority for leases, epochs, ownership,
-- attachments, snapshot/clone catalog, hosts/capacity, reconciliation operations,
-- and Control Plane terms. NOT the authority for the durable point of data (that is
-- S3, §5.8). Reconstructible from S3 via rebuild-metadata (§22.5).

-- Single-active Control Plane leadership with a verified term (§7). Every CP write
-- transaction validates term = the holder's term; a zombie CP affects 0 rows.
CREATE TABLE control_plane_leader (
    singleton  BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    term       BIGINT NOT NULL,
    holder_id  TEXT NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE hosts (
    host_id              TEXT PRIMARY KEY,
    state                TEXT NOT NULL,   -- ACTIVE | CORDONED | DRAINING | DEAD
    agent_version        TEXT NOT NULL DEFAULT '',
    max_format_version   INTEGER NOT NULL DEFAULT 2,  -- fleet-mixed gating (§27)
    nvme_total_bytes     BIGINT NOT NULL DEFAULT 0,
    nvme_used_bytes      BIGINT NOT NULL DEFAULT 0,
    nvme_committed_bytes BIGINT NOT NULL DEFAULT 0,   -- thin provisioning
    last_heartbeat       TIMESTAMPTZ NOT NULL
);

-- Lease POR HOST (§12.6): one grouped renewal per host, not per volume. Each volume
-- binds to its host's lease via (primary_host_id, current_epoch).
CREATE TABLE host_leases (
    host_id      TEXT PRIMARY KEY REFERENCES hosts(host_id),
    granted_at   TIMESTAMPTZ NOT NULL,
    last_renewal TIMESTAMPTZ NOT NULL,
    ttl_seconds  INTEGER NOT NULL DEFAULT 10
);

CREATE TABLE volumes (
    volume_id          TEXT PRIMARY KEY,
    size_bytes         BIGINT NOT NULL,                 -- mutable: resize grow
    durability         TEXT NOT NULL DEFAULT 'remote',  -- 'remote' | 'local' (§14.8)
    block_size         INTEGER NOT NULL,                -- CoW segment granularity (64 KiB)
    current_epoch      BIGINT NOT NULL DEFAULT 0,
    state              TEXT NOT NULL,
    primary_host_id    TEXT,
    standby_host_id    TEXT,
    active_root_id     TEXT,
    published_root_id  TEXT,
    chain_depth        INTEGER NOT NULL DEFAULT 0,
    dek_wrapped        BYTEA NOT NULL,                  -- DEK wrapped with the KEK
    kek_id             TEXT NOT NULL,
    -- Watermarks are INFORMATIVE (lazy); authority is S3 (§5.8).
    local_sequence     BIGINT NOT NULL DEFAULT 0,
    durable_sequence   BIGINT NOT NULL DEFAULT 0,
    published_sequence BIGINT NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE snapshots (
    snapshot_id        TEXT PRIMARY KEY,
    volume_id          TEXT NOT NULL REFERENCES volumes(volume_id),
    parent_snapshot_id TEXT,
    epoch              BIGINT NOT NULL,
    target_sequence    BIGINT NOT NULL,
    root_digest        TEXT NOT NULL,
    source_host_id     TEXT,
    state              TEXT NOT NULL,
    portable           BOOLEAN NOT NULL DEFAULT false,
    manifest_key       TEXT,
    request_id         UUID UNIQUE NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconciliation operations (§7): desired/current state converge idempotently.
CREATE TABLE operations (
    operation_id  UUID PRIMARY KEY,   -- = client request_id
    kind          TEXT NOT NULL,      -- attach|detach|clone|resize|drain|recovery|flatten|gc
    volume_id     TEXT,
    host_id       TEXT,
    desired_state JSONB NOT NULL,
    current_state JSONB NOT NULL,
    phase         TEXT NOT NULL,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
