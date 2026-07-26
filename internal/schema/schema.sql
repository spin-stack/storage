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
--    iterates, and ListLiveOperationsByHost, the lookup that stops a second drain of
--    a host that already has one (§28.1). Where the query also has a fixed
--    predicate, the index carries it: see the live-operation indexes below.
--
-- Deliberately NOT added yet (no query uses them; each has a named trigger so the
-- index lands with its query rather than on speculation):
--   * operations (phase) WHERE phase NOT IN ('SUCCEEDED','CANCELED') — a partial
--     index for "find work to reconcile", fleet-wide rather than per host. Needed
--     when the reconciler loop lands (§7); the per-host one below does not serve it,
--     since a scan for work everywhere has no host to lead with.
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

-- The FK index for operations.host_id (rule 1). It is not the index the drain reads
-- by: a parent DELETE has to find every child row, including the finished ones, so
-- this one cannot be partial — and precisely because it cannot, it is the wrong
-- index for a query that only ever wants the live ones.
CREATE INDEX operations_host_id_operation_id_idx ON operations (host_id, operation_id);

-- The live-operation predicate, twice.
--
-- `phase NOT IN ('SUCCEEDED', 'CANCELED')` is lifecycle.OperationPhase.Terminal()
-- written in SQL, and that duplication is the real cost of these two indexes: the
-- authority for the vocabulary is internal/lifecycle, and this is a second copy of
-- one of its rules. It is stated as the *complement* of the terminal set rather than
-- as a list of live phases because that set is the one the lifecycle defines and the
-- one that does not grow when a phase is added — a new live phase is covered by
-- these indexes on the day it is declared, without a schema change.
--
-- The duplication is only acceptable because a test refuses to let it drift:
-- TestPGLivePhaseSetsAgreeWithTheLifecycle evaluates these predicates in PostgreSQL
-- once per value of the vocabulary and compares each answer with lifecycle's own. If
-- that test is ever deleted, delete these indexes with it.

-- ListLiveOperationsByHost: WHERE host_id = $1 AND phase NOT IN (…) ORDER BY
-- operation_id (§28.1, the drain's exclusion check). Partial because `operations` is
-- append-only history — nothing deletes a finished operation — so an index over all
-- of them makes the check that runs before every drain pass slower for the rest of
-- the cluster's life. Composite so the index satisfies the filter and the sort.
CREATE INDEX operations_live_by_host_idx ON operations (host_id, operation_id)
    WHERE phase NOT IN ('SUCCEEDED', 'CANCELED');

-- One live drain per host (§28.1). Two evacuations of one host each capture their
-- own plan and promote the same volumes; whichever loses a race is left holding a
-- destination reservation nobody will release, because releasing it is the losing
-- operation's own next step and that step now fails for ever (§28.2). Wave 3 closed
-- the harm in Go with a read followed by a write, which is not exclusion: two
-- goroutines inside one leader can both pass the read. This closes it.
--
-- A unique partial index rather than EXCLUDE USING gist (host_id WITH =): the
-- constraint form needs the btree_gist extension and buys nothing here, since
-- equality is all this excludes on. host_id is nullable and NULLs are distinct, so
-- an operation attached to no host is unaffected — which is right, since nothing can
-- be draining a host nobody named.
--
-- Only an INSERT can violate it. A row enters the live set at creation or by leaving
-- the terminal set, and no phase transition leaves it (SUCCEEDED and CANCELED have
-- no successors), so the phase update path cannot create a second live drain.
CREATE UNIQUE INDEX operations_one_live_drain_per_host_idx ON operations (host_id)
    WHERE kind = 'drain' AND phase NOT IN ('SUCCEEDED', 'CANCELED');

-- FK indexes (rule 1).
CREATE INDEX volumes_standby_host_id_idx ON volumes (standby_host_id);
CREATE INDEX snapshots_volume_id_idx ON snapshots (volume_id);
CREATE INDEX snapshots_parent_snapshot_id_idx ON snapshots (parent_snapshot_id);
CREATE INDEX snapshots_source_host_id_idx ON snapshots (source_host_id);
CREATE INDEX operations_volume_id_idx ON operations (volume_id);

-- Committed NVMe capacity (§28.2) is DERIVED, not stored (ADR-0017):
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
-- It is a view because it is a rule, and a rule lives once. It was inlined in four
-- queries until now for a tooling reason that no longer exists (Atlas Community
-- refused to diff a schema containing a view; ADR-0019 replaced it), and four copies
-- of an accounting rule is four places for a placement decision to be taken against
-- a different definition of "full".
--
-- The second term is what makes it correct rather than merely simple: a volume being
-- moved must be charged to its destination *before* it becomes the primary there, or
-- two placements would both see room. It is read out of the operation's own recorded
-- progress, which carries a per-volume stage, so "reserved but not yet primary" is
-- readable rather than inferred. An entry stops reserving the moment the volume it
-- names is actually primary on the host — otherwise the volume is charged twice,
-- once as a plan and once as a placement — and a settled entry (DONE, FOREIGN)
-- reserves nothing at all.
--
-- The join is on volume_id::text rather than a cast of the JSON value to uuid: a
-- malformed plan must make the row disappear from the sum, not make every capacity
-- read raise. The consequence, stated in ADR-0017, is that an operation whose plan
-- is lost makes its destination look emptier than it is — which is why the plan is
-- written term-guarded, before the work it describes.
--
-- Both sums are correlated subqueries in the select list rather than an aggregate
-- over a join, so a reader asking about one host is charged for one host: the
-- host_id filter is applied to the scan of `hosts` and the subqueries run only for
-- the rows that survive it. That is the property a view puts at risk and the one
-- TestPGCommittedBytesViewDoesNotDeriveTheWholeFleet measures — the drain reads this
-- on every pass, per host it considers.
--
-- It is a plain view and not a materialized one on purpose: staleness in an
-- accounting path is the exact failure ADR-0017 removed when it deleted the ledger,
-- and a materialized view is a ledger with a refresh job.
CREATE VIEW host_committed_bytes AS
SELECT h.host_id,
       (COALESCE((SELECT SUM(v.size_bytes) FROM volumes v
                   WHERE v.primary_host_id = h.host_id), 0)
        + COALESCE((SELECT SUM(rv.size_bytes)
                      FROM operations o
                      CROSS JOIN LATERAL jsonb_array_elements(
                          CASE WHEN jsonb_typeof(o.current_state -> 'volumes') = 'array'
                               THEN o.current_state -> 'volumes'
                               ELSE '[]'::jsonb END) AS e
                      JOIN volumes rv ON rv.volume_id::text = e ->> 'volume_id'
                     WHERE o.phase NOT IN ('SUCCEEDED', 'CANCELED')
                       AND e ->> 'to_host' = (h.host_id)::text
                       AND COALESCE(e ->> 'stage', '') NOT IN ('DONE', 'FOREIGN')
                       AND rv.primary_host_id IS DISTINCT FROM h.host_id), 0))::BIGINT
           AS committed_bytes
  FROM hosts h;
