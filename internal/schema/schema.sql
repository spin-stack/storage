-- Control Plane metadata schema (§8). Authority for leases, epochs, ownership,
-- attachments, snapshot/clone catalog, hosts/capacity, reconciliation operations,
-- and Control Plane terms. NOT the authority for the durable point of data (that is
-- S3, §5.8). Reconstructible from S3 via rebuild-metadata (§22.5).
--
-- Identity columns are `uuidv7` (a domain over uuid, not text): volume_id in
-- particular is the same 16-byte
-- UUID the on-disk WAL format carries (RecordHeader.VolumeID [16]byte). IDs are
-- generated as UUIDv7 (time-ordered, better index locality) — app-side via
-- google/uuid.NewV7 for values that must match the durable format, and the DB runs
-- Postgres 18 (native uuidv7()). This file is the declared state and the single
-- source of truth: pgschema plans against it (ADR-0019), sqlc generates from it
-- (ADR-0006), and the integration lane builds its database from it. migrations/
-- holds the reviewed plans, not the apply path.

-- UUIDv7 enforcement (INV-22, ADR-0007) as a type. The version nibble is the high
-- 4 bits of the 7th byte of the UUID; requiring it to equal 7 rejects any non-v7 id
-- at insert, whatever the client. It is a domain rather than a predicate copied onto
-- every identity column because a copied rule holds where somebody remembered to
-- copy it: active_root_id and published_root_id went without one from the start, and
-- nothing said so. A new table gets the rule by declaring the type — the same move
-- internal/lifecycle made in Go for the state vocabularies (ADR-0009).
--
-- Foreign-key referencing columns stay plain `uuid`: they can only hold a value that
-- is already in a v7-checked primary key, so the rule reaches them transitively, and
-- TestPGIdentityColumnsUseTheUUIDv7Domain exempts exactly those.
CREATE DOMAIN uuidv7 AS uuid CHECK ((get_byte(uuid_send(VALUE), 6) >> 4) = 7);

-- Single-active Control Plane leadership with a verified term (§7). Every CP write
-- transaction validates term = the holder's term; a zombie CP affects 0 rows.
CREATE TABLE control_plane_leader (
    singleton  BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    term       BIGINT NOT NULL,
    holder_id  TEXT NOT NULL,
    renewed_at TIMESTAMPTZ NOT NULL
);

-- Lifecycle vocabularies are CHECK-constrained (the third enforcement layer next to
-- the Go types in internal/lifecycle and the transition-guarded UPDATEs): no client,
-- script, or manual psql can persist a state that does not exist. Adding a state
-- means editing internal/lifecycle *and* the schema — deliberately, not by accident.
CREATE TABLE hosts (
    host_id              UUIDV7 PRIMARY KEY,
    state                TEXT NOT NULL CHECK (state IN ('ACTIVE', 'CORDONED', 'DRAINING', 'DEAD')),
    agent_version        TEXT NOT NULL DEFAULT '',
    max_format_version   INTEGER NOT NULL DEFAULT 2,  -- fleet-mixed gating (§27)
    nvme_total_bytes     BIGINT NOT NULL DEFAULT 0,
    nvme_used_bytes      BIGINT NOT NULL DEFAULT 0,
    -- There is deliberately no nvme_committed_bytes column (ADR-0017). Committed
    -- capacity is derived from the rows that already say who holds what; see the
    -- note at the bottom of this file.
    last_heartbeat       TIMESTAMPTZ NOT NULL,
    -- The end of the bounded revocation window (§12.6, ADR-0016 stage 1): until this
    -- instant, by this database's clock, the host's lease renewals are refused.
    --
    -- The drain revokes the source's lease to fence it, and the source's next
    -- heartbeat would otherwise re-arm it — moving the instant the fencing wait is
    -- measured from, so a healthy host could never be evacuated. Refusing renewals
    -- for any DRAINING host fixes that and costs too much: the lease is per host and
    -- the evacuation is per volume, so it stops the durable ACKs of every volume the
    -- host still holds, including the ones nobody is moving. This column is the same
    -- mechanism with the blast radius cut to one promotion.
    --
    -- It is a deadline rather than a flag on purpose. The Control Plane closes the
    -- window on every exit path of the promotion, but a Control Plane that dies
    -- mid-promotion runs no closing write at all, and a host that can never renew
    -- again is worse than the bug this fixes. The deadline is the backstop: at most
    -- one lease_ttl + max_clock_skew per volume moved, whatever happens to the CP.
    renewals_blocked_until TIMESTAMPTZ
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
    volume_id          UUIDV7 PRIMARY KEY,              -- = on-disk VolumeID [16]byte
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
    active_root_id     UUIDV7,
    published_root_id  UUIDV7,
    chain_depth        INTEGER NOT NULL DEFAULT 0,
    dek_wrapped        BYTEA NOT NULL,                  -- DEK wrapped with the KEK
    kek_id             TEXT NOT NULL,
    -- Watermarks are INFORMATIVE (lazy); authority is S3 (§5.8). Informative is not
    -- unconstrained: INV-03 (§5.6) says published <= durable <= local at every
    -- observation point, and this is that rule at the table rather than in the
    -- queries that happen to write it. The triple is what an operator reads during
    -- an incident to decide whether to accept data loss; a disordered one is not a
    -- wrong number but three numbers that cannot all be true.
    --
    -- Nothing on the reporting path can trip it: UpdateVolumeWatermarks and
    -- CreateVolume's conflict path both advance the three with GREATEST, and
    -- component-wise max preserves the ordering of ordered inputs. What it does
    -- catch is a row written out of order at birth — which no later report could
    -- repair, since each watermark only ever moves forward.
    local_sequence     BIGINT NOT NULL DEFAULT 0,
    durable_sequence   BIGINT NOT NULL DEFAULT 0,
    published_sequence BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT volumes_watermarks_ordered
        CHECK (published_sequence <= durable_sequence AND durable_sequence <= local_sequence),
    -- When the Control Plane observed the lease of the writer it is fencing
    -- (ADR-0015). Stamped by this database's clock — the same clock that stamps
    -- host_leases.last_renewal, and so the one every fencing deadline lives on —
    -- when the volume enters FENCING_WAIT, and cleared when it leaves.
    --
    -- The promotion dwell is measured from here rather than from
    -- host_leases.last_renewal, because last_renewal answers a question about the
    -- *writer* and this answers a question about the *promoter*: a read served by a
    -- lagging replica reports a last_renewal old enough that the wait already looks
    -- over, and the epoch is granted while the old writer's monotonic lease is still
    -- valid. This column is written by the promoter and read back by it, so a stale
    -- read of it returns NULL — which starts a full dwell. Fail slow, never short.
    --
    -- It is what makes FENCING_WAIT load-bearing state rather than a marker: a
    -- Control Plane that restarts mid-fence resumes the wait its predecessor started
    -- instead of beginning a new one.
    fencing_started_at TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE snapshots (
    snapshot_id        UUIDV7 PRIMARY KEY,
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
    request_id         UUIDV7 UNIQUE NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reconciliation operations (§7): desired/current state converge idempotently. The
-- operation_id is the client request_id (a UUIDv7).
CREATE TABLE operations (
    operation_id  UUIDV7 PRIMARY KEY,
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
-- **Why the expression is still repeated in internal/db/queries instead of living
-- in a view.** A view is the obvious home for it, and until ADR-0019 it was not
-- available at all: Atlas Community refused to diff a schema containing one, so the
-- choice was between a licensed toolchain, a hand-written migration outside the
-- tool, or four copies — and the four copies were the only one of the three that
-- kept this file mechanically authoritative. pgschema diffs views fine, so that
-- constraint is gone and the copies are now a debt rather than a necessity.
--
-- Paying it is a deliberate follow-up, not a rider on the tool change: ADR-0019
-- lands the swap and stays boring, and where a view actually helps (this sum, the
-- lineage charge of ADR-0014, the volumes-with-in-flight-plans join) gets decided on
-- its own. Until then the four copies stand. They live in
-- internal/db/queries/hosts.sql (GetHost, ListHosts), volumes.sql (CreateVolume's
-- bound) and operations.sql (UpdateOperationPhase's bound), and hosts.sql carries
-- the full reasoning; if you change one, change all four.
