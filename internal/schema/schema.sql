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
-- Lifecycle vocabularies are CHECK-constrained (the third enforcement layer next to
-- the Go types in internal/lifecycle and the transition-guarded UPDATEs): no client,
-- script, or manual psql can persist a state that does not exist. Adding a state
-- means editing internal/lifecycle *and* a migration — deliberately, not by accident.
CREATE TABLE hosts (
    host_id              UUID PRIMARY KEY CHECK ((get_byte(uuid_send(host_id), 6) >> 4) = 7),
    state                TEXT NOT NULL CHECK (state IN ('ACTIVE', 'CORDONED', 'DRAINING', 'DEAD')),
    agent_version        TEXT NOT NULL DEFAULT '',
    max_format_version   INTEGER NOT NULL DEFAULT 2,  -- fleet-mixed gating (§27)
    nvme_total_bytes     BIGINT NOT NULL DEFAULT 0,
    nvme_used_bytes      BIGINT NOT NULL DEFAULT 0,
    -- There is deliberately no nvme_committed_bytes column (ADR-0017). Committed
    -- capacity is derived from the rows that already say who holds what; see the
    -- host_committed_bytes view at the bottom of this file.
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
    durability         TEXT NOT NULL DEFAULT 'remote'
                         CHECK (durability IN ('remote', 'local')),   -- §14.8
    block_size         INTEGER NOT NULL,                -- CoW segment granularity (64 KiB)
    current_epoch      BIGINT NOT NULL DEFAULT 0,
    state              TEXT NOT NULL                                  -- §7 failover states
                         CHECK (state IN ('ACTIVE', 'PRIMARY_SUSPECTED', 'FENCING_WAIT',
                                          'RECOVERY_REQUIRED', 'RECOVERING', 'DETACHED')),
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
    state              TEXT NOT NULL                                  -- §19
                         CHECK (state IN ('CREATING', 'PUBLISHED', 'FAILED', 'DELETING')),
    portable           BOOLEAN NOT NULL DEFAULT false,
    manifest_key       TEXT,
    request_id         UUID UNIQUE NOT NULL CHECK ((get_byte(uuid_send(request_id), 6) >> 4) = 7),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconciliation operations (§7): desired/current state converge idempotently. The
-- operation_id is the client request_id (a UUIDv7).
CREATE TABLE operations (
    operation_id  UUID PRIMARY KEY CHECK ((get_byte(uuid_send(operation_id), 6) >> 4) = 7),
    kind          TEXT NOT NULL CHECK (kind IN ('attach', 'detach', 'clone', 'resize',
                                                'drain', 'recovery', 'flatten', 'gc')),
    volume_id     UUID REFERENCES volumes(volume_id),
    host_id       UUID REFERENCES hosts(host_id),
    desired_state JSONB NOT NULL,
    current_state JSONB NOT NULL,
    phase         TEXT NOT NULL CHECK (phase IN ('PENDING', 'RUNNING', 'CANCELING',
                                                 'CANCELED', 'SUCCEEDED', 'FAILED')),
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes.
--
-- Two rules, both checked by tests:
--
-- 1. Every foreign-key *referencing* column is indexed. Postgres creates an index
--    for the referenced side (the PK) but never for the referencing side, so without
--    these every DELETE/UPDATE on a parent row (a host being decommissioned, a
--    volume being removed) sequentially scans the child table while holding locks —
--    and each child lookup does too. `TestPGEveryForeignKeyHasAnIndex` fails if a
--    future FK arrives without one.
-- 2. A query with a filter + ORDER BY gets a composite index in that order, so the
--    planner can skip the sort. Today that is ListVolumesByHost, the loop a drain
--    iterates, and ListOperationsByHost, the lookup that stops a second drain of a
--    host that already has one (§28.1).
--
-- Deliberately NOT added yet (no query uses them; each has a named trigger so the
-- index lands with its query rather than on speculation):
--   * operations (phase) WHERE phase NOT IN ('SUCCEEDED','CANCELED') — a partial
--     index for "find work to reconcile". Needed when the reconciler loop lands
--     (§7); without it that scan grows with completed-operation history.
--   * hosts (last_heartbeat) / host_leases (last_renewal) — expiry sweeps (§12.3).
--     The fleet is hundreds of rows; a sequential scan is cheaper than the index
--     until it is not.
--   * snapshots (volume_id, created_at DESC) — newest-first catalog listing (§19).
--     The FK index below covers the lookup; add the sort key when the listing query
--     exists.

-- ListVolumesByHost: WHERE primary_host_id = $1 ORDER BY volume_id (§28.1 drain).
-- Composite so the index satisfies both the filter and the ordering; it also serves
-- as the FK index for primary_host_id.
CREATE INDEX volumes_primary_host_id_volume_id_idx ON volumes (primary_host_id, volume_id);

-- ListOperationsByHost: WHERE host_id = $1 ORDER BY operation_id (§28.1, the drain's
-- exclusion check). Composite for the same reason as the volumes one above, and it
-- doubles as the FK index for host_id.
CREATE INDEX operations_host_id_operation_id_idx ON operations (host_id, operation_id);

-- FK indexes (rule 1).
CREATE INDEX volumes_standby_host_id_idx ON volumes (standby_host_id);
CREATE INDEX snapshots_volume_id_idx ON snapshots (volume_id);
CREATE INDEX snapshots_parent_snapshot_id_idx ON snapshots (parent_snapshot_id);
CREATE INDEX snapshots_source_host_id_idx ON snapshots (source_host_id);
CREATE INDEX operations_volume_id_idx ON operations (volume_id);

-- Committed NVMe capacity (§28.2) is DERIVED, not stored (ADR-0017). There is no
-- column for it and no view either:
--
--   committed(host) = Σ size_bytes of the volumes whose primary_host_id is the host
--                   + Σ size_bytes reserved by in-flight operation plans targeting it
--
-- Both terms are queries over rows that already exist and are already term-guarded,
-- so there is no delta to apply and nothing to apply twice: a resumed pass computes
-- the same answer as the pass that crashed. The column this replaces was an
-- incremental ledger, and every safeguard the last two waves added to it — the
-- non-negative guard, the expected-value predicate, the per-volume release stage —
-- existed only because a delta is not an idempotency key.
--
-- **Why the expression is repeated in internal/db/queries instead of living in a
-- view.** A view is the obvious home for it, and it is not available: Atlas
-- Community (the pinned toolchain, ATLAS_VERSION in Taskfile.yml) refuses to diff a
-- schema containing one — "views are available to logged-in users only". Pinning a
-- licensed Atlas, or hand-writing the migration outside `task db:migrate:diff`,
-- would buy syntactic sugar over a sum four queries can each do for themselves, at
-- the price of the one property that makes the schema trustworthy: that
-- internal/schema/schema.sql is the declared state and the migrations are derived
-- from it mechanically. So the four copies are deliberate. They live in
-- internal/db/queries/hosts.sql (GetHost, ListHosts), volumes.sql (CreateVolume's
-- bound) and operations.sql (UpdateOperationPhase's bound), and hosts.sql carries
-- the full reasoning; if you change one, change all four.
