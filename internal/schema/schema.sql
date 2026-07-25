-- Control Plane metadata schema (§8). Authority for leases, epochs, ownership,
-- attachments, snapshot/clone catalog, hosts/capacity, reconciliation operations,
-- and Control Plane terms. NOT the authority for the durable point of data (that is
-- S3, §5.8). Reconstructible from S3 via rebuild-metadata (§22.5).
--
-- Identity columns are `uuid` (not text): volume_id in particular is the same 16-byte
-- UUID the on-disk WAL format carries (RecordHeader.VolumeID [16]byte). IDs are
-- generated as UUIDv7 (time-ordered, better index locality) — app-side via
-- google/uuid.NewV7 for values that must match the durable format, and the DB runs
-- Postgres 18 (native uuidv7()). Schema is the Atlas source of truth (atlas.hcl);
-- migrations live in migrations/ (ADR-0006, ADR-0007).

-- Single-active Control Plane leadership with a verified term (§7). Every CP write
-- transaction validates term = the holder's term; a zombie CP affects 0 rows.
CREATE TABLE control_plane_leader (
    singleton  BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    term       BIGINT NOT NULL,
    holder_id  TEXT NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL
);

-- UUIDv7 enforcement (INV-22, ADR-0007): the version nibble is the high 4 bits of
-- the 7th byte of the UUID; requiring it to equal 7 rejects any non-v7 id at insert,
-- regardless of the client. Foreign-key columns are covered transitively (they must
-- reference a v7-checked primary key).
CREATE TABLE hosts (
    host_id              UUID PRIMARY KEY CHECK ((get_byte(uuid_send(host_id), 6) >> 4) = 7),
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
    host_id      UUID PRIMARY KEY REFERENCES hosts(host_id),
    granted_at   TIMESTAMPTZ NOT NULL,
    last_renewal TIMESTAMPTZ NOT NULL,
    ttl_seconds  INTEGER NOT NULL DEFAULT 10
);

CREATE TABLE volumes (
    volume_id          UUID PRIMARY KEY                 -- = on-disk VolumeID [16]byte
                         CHECK ((get_byte(uuid_send(volume_id), 6) >> 4) = 7),
    size_bytes         BIGINT NOT NULL,                 -- mutable: resize grow
    durability         TEXT NOT NULL DEFAULT 'remote',  -- 'remote' | 'local' (§14.8)
    block_size         INTEGER NOT NULL,                -- CoW segment granularity (64 KiB)
    current_epoch      BIGINT NOT NULL DEFAULT 0,
    state              TEXT NOT NULL,
    primary_host_id    UUID REFERENCES hosts(host_id),
    standby_host_id    UUID REFERENCES hosts(host_id),
    active_root_id     UUID,
    published_root_id  UUID,
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
    snapshot_id        UUID PRIMARY KEY CHECK ((get_byte(uuid_send(snapshot_id), 6) >> 4) = 7),
    volume_id          UUID NOT NULL REFERENCES volumes(volume_id),
    parent_snapshot_id UUID REFERENCES snapshots(snapshot_id),
    epoch              BIGINT NOT NULL,
    target_sequence    BIGINT NOT NULL,
    root_digest        TEXT NOT NULL,
    source_host_id     UUID REFERENCES hosts(host_id),
    state              TEXT NOT NULL,
    portable           BOOLEAN NOT NULL DEFAULT false,
    manifest_key       TEXT,
    request_id         UUID UNIQUE NOT NULL CHECK ((get_byte(uuid_send(request_id), 6) >> 4) = 7),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconciliation operations (§7): desired/current state converge idempotently. The
-- operation_id is the client request_id (a UUIDv7).
CREATE TABLE operations (
    operation_id  UUID PRIMARY KEY CHECK ((get_byte(uuid_send(operation_id), 6) >> 4) = 7),
    kind          TEXT NOT NULL,      -- attach|detach|clone|resize|drain|recovery|flatten|gc
    volume_id     UUID REFERENCES volumes(volume_id),
    host_id       UUID REFERENCES hosts(host_id),
    desired_state JSONB NOT NULL,
    current_state JSONB NOT NULL,
    phase         TEXT NOT NULL,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
