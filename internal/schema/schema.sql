-- Control Plane metadata schema (§8). Authority for leases, epochs, ownership,
-- attachments, snapshot/clone catalog, hosts/capacity, and Control Plane terms.
-- NOT the authority for the durable point of data (that is S3, §5.8).
-- Reconstructible from S3 via rebuild-metadata (§22.5).
--
-- There is no `operations` table. §7's reconciliation operations — the
-- desired_state/current_state rows a drain, a promotion or a recovery converged
-- through — described the machinery ADR-0026 withdrew, and after it nothing wrote a
-- row: every write path (RecordOperation, UpdateOperationPhase) had a test for its
-- only caller. It is dropped rather than left empty because an empty table with
-- three indexes, a kind vocabulary and a term-guarded writer reads as a mechanism
-- somebody is about to use, and the next reader has no way to tell.
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
    -- Why the host is CORDONED, empty for every other state (ADR-0013 §3).
    --
    -- Cordon stopped being something only a human does: the Control Plane cordons a
    -- host whose device passes 70% used, so an operator reading state = 'CORDONED'
    -- can no longer assume somebody meant it. The column is what tells the two
    -- apart, and it is also what stops the automatic loop from clearing a cordon a
    -- human set for a cause the fleet cannot see — the pressure writer may only
    -- replace '' or 'DEVICE_PRESSURE' (lifecycle.CordonReason.OverwritableNames,
    -- applied in SetHostState's predicate).
    --
    -- It is emptied rather than left behind when the host leaves CORDONED, because a
    -- reason that outlives its cordon is a reason the next reader will believe.
    cordon_reason        TEXT NOT NULL DEFAULT ''
                           CHECK (cordon_reason IN ('', 'OPERATOR', 'DEVICE_PRESSURE')),
    agent_version        TEXT NOT NULL DEFAULT '',
    max_format_version   INTEGER NOT NULL DEFAULT 2,  -- fleet-mixed gating (§27)
    nvme_total_bytes     BIGINT NOT NULL DEFAULT 0,
    nvme_used_bytes      BIGINT NOT NULL DEFAULT 0,
    -- The part of nvme_used_bytes that no verified object covers yet, summed over
    -- every volume this host holds (ADR-0013 §1). Reported by the Agent in each
    -- heartbeat, like the two columns above it.
    --
    -- It is stored rather than derived, unlike committed capacity, because nothing
    -- in this database can compute it: it is the distance between what the host has
    -- written locally and what S3 has acknowledged, and the volumes table carries
    -- watermarks in sequence numbers, not bytes. It is also the number that
    -- distinguishes the two ways a device fills — a busy host, and a host whose
    -- object store stopped answering, which is the one that will not stop growing
    -- because no local truncation may reclaim those records (INV-13).
    nvme_remote_backlog_bytes BIGINT NOT NULL DEFAULT 0,
    -- There is deliberately no nvme_committed_bytes column (ADR-0017). Committed
    -- capacity is derived from the rows that already say who holds what; see the
    -- note at the bottom of this file.
    last_heartbeat       TIMESTAMPTZ NOT NULL,
    -- There is deliberately no renewals_blocked_until column either, and it is a
    -- different deletion from the one above: this one held a mechanism that worked.
    -- ADR-0016 stage 1 refused a host's lease renewals for the length of one
    -- promotion, so that the lease the Control Plane revoked to fence a source could
    -- not be re-armed by the source's next heartbeat. ADR-0026 then withdrew the
    -- promotion, and the ADR's own amendment states the consequence: the window "is
    -- currently empty, because a revocation stops nothing on the data path". Its
    -- three writers (BlockHostRenewals, UnblockHostRenewals, RevokeHostLease) had no
    -- caller, so the column could only ever be NULL and the renewal predicate that
    -- read it could only ever be true.
    --
    -- Kept for stage 2 was the alternative, and it is worse than it looks: stage 2 is
    -- a fence that follows the *volume*, so what it needs is not this column with a
    -- caller added — it is a different granularity. A column no write ever sets is
    -- indistinguishable, to the next reader, from one whose writer is broken.
    -- A durability tier that gates an ACK on the lease brings back the requirement,
    -- and it will bring back the schema with the code that exercises it.
    -- A reason belongs to a cordon and dies with it. Stated as a table constraint
    -- because it spans two columns: a column-level CHECK reading another column is
    -- accepted by PostgreSQL and silently promoted to one anyway, which hides from
    -- the reader that dropping either column takes this rule with it.
    CONSTRAINT hosts_cordon_reason_belongs_to_a_cordon
        CHECK (state = 'CORDONED' OR cordon_reason = '')
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
    -- size_bytes is written once, at create, and no statement in queries/ updates it.
    -- It said "mutable: resize grow" until 2026-08-06; the grow-only UPDATE that made
    -- that true had no caller, and the rest of a resize (an Agent that acts on a new
    -- size, a device whose capacity can change, a guest that can be told) does not
    -- exist. Leaving the column documented as mutable would have been the expensive
    -- half: descriptor.json carries this same number and is written only at create, so
    -- a size that moved here and not there is a catalog and a bucket that disagree —
    -- and -rebuild-metadata restores from the bucket.
    size_bytes         BIGINT NOT NULL,
    -- There is no durability column. §14.8 once stored a per-volume FLUSH ACK
    -- contract here ('remote' | 'local'); ADR-0026 withdrew the remote half, leaving
    -- the local ACK as the only contract and the column as a value every write set,
    -- every read parsed, and nothing ever branched on. Dropping it rather than
    -- leaving it defaulted is deliberate: a column that still says 'remote' is a
    -- catalog claiming a durability the data path no longer provides, and the next
    -- reader has no way to tell it is decoration.
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
    -- The snapshot this volume was cloned from (§20), or NULL for a volume that was
    -- created rather than cloned. chain_depth says a chain exists; this says what is
    -- on the other end of it, which is what a clone's Agent needs to find the objects
    -- it reads through. Without it a clone starts an empty WAL under its own id, finds
    -- nothing under that id in the object store, and serves zeros for everything its
    -- parent ever wrote (DEV-0007).
    --
    -- The constraint itself is added below, after snapshots exists: snapshots already
    -- references volumes, so the pair is circular and one of the two directions has to
    -- be an ALTER. This one, because volumes is the table that has to exist first for
    -- anything else to reference it.
    parent_snapshot_id UUID,
    dek_wrapped        BYTEA NOT NULL,                  -- DEK wrapped with the KEK
    kek_id             TEXT NOT NULL,
    -- The DEK's own version, RecordHeader.KeyID (§15.1). Rotation re-keys new data
    -- without re-encrypting history, which only works if every record says which key
    -- sealed it — and the Agent can only say that if the catalog remembers it.
    --
    -- BIGINT because the format field is uint32 and Postgres INTEGER is signed 32-bit.
    -- The lower bound is not a sanity check: KeyID 0 means "plaintext record" on the
    -- WAL path, so a row carrying 0 would hand the Agent a version it must refuse
    -- (wal.ErrUnversionedKey) at attach, with the DEK already unwrapped. Refusing it
    -- at the write is refusing it where it can still be corrected.
    dek_key_id         BIGINT NOT NULL
                         CHECK (dek_key_id > 0 AND dek_key_id <= 4294967295),
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
--    planner can skip the sort. Today that is ListVolumesByHost, the listing a
--    volume's placement is read back from (§28.1).
--
-- Deliberately NOT added yet (no query uses them; each has a named trigger so the
-- index lands with its query rather than on speculation):
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

-- FK indexes (rule 1).
CREATE INDEX volumes_standby_host_id_idx ON volumes (standby_host_id);
CREATE INDEX snapshots_volume_id_idx ON snapshots (volume_id);
CREATE INDEX snapshots_parent_snapshot_id_idx ON snapshots (parent_snapshot_id);

-- The other half of the volumes <-> snapshots cycle (see volumes.parent_snapshot_id).
ALTER TABLE volumes
    ADD CONSTRAINT volumes_parent_snapshot_id_fkey
    FOREIGN KEY (parent_snapshot_id) REFERENCES snapshots(snapshot_id);

-- Every FK *referencing* column carries an index: Postgres indexes only the referenced
-- side, so a clone lookup by parent would otherwise be a sequential scan, and deleting
-- a snapshot would take a full scan of volumes to check the constraint.
CREATE INDEX volumes_parent_snapshot_id_idx ON volumes (parent_snapshot_id);
CREATE INDEX snapshots_source_host_id_idx ON snapshots (source_host_id);

-- Committed NVMe capacity (§28.2) is DERIVED, not stored (ADR-0017):
--
--   committed(host) = Σ size_bytes of the volumes whose primary_host_id is the host
--
-- It is a query over rows that already exist and are already term-guarded, so there
-- is no delta to apply and nothing to apply twice: a resumed pass computes the same
-- answer as the pass that crashed. The column this replaces was an incremental
-- ledger, and every safeguard the last two waves added to it — the non-negative
-- guard, the expected-value predicate, the per-volume release stage — existed only
-- because a delta is not an idempotency key.
--
-- It is a view because it is a rule, and a rule lives once. It was inlined in four
-- queries until now for a tooling reason that no longer exists (Atlas Community
-- refused to diff a schema containing a view; ADR-0019 replaced it), and four copies
-- of an accounting rule is four places for a placement decision to be taken against
-- a different definition of "full".
--
-- **ADR-0017's second term is gone with the operations table.** It summed the
-- size_bytes an in-flight operation plan had reserved on a destination — "charged to
-- its destination *before* it becomes the primary there", which is what stops two
-- placements from both seeing room for one volume in flight. Removing it does not
-- change a single number this view has ever produced: nothing outside a test ever
-- wrote an operations row, so the lateral join ran over an empty relation for every
-- host in every state a V1 catalog can reach. What it removes is the *headroom* for
-- a move that spans two hosts — and V1 performs none, because ADR-0026 withdrew the
-- drain and the promotion that made one. ADR-0017's own "the tests that enforce it"
-- section already says so: the behavioural cases for that term went with the drain,
-- "V1 performs no moves, so the interleaving they quantified over is empty".
--
-- What this means for the one move a V1 catalog *can* make: detach-then-attach
-- (SetVolumePrimaryHost) makes the volume primary on the destination in the same
-- statement that places it, so there is no interval between "reserved" and "primary"
-- for a second term to cover. The gap that write does have is a different one and it
-- is still open — it carries no CapacityBound at all, so nothing evaluates a ceiling
-- inside it (recorded against D6 in docs/plan/tracks/TRACK-D.md). The reservation
-- term would not have closed it: a reservation is written by the operation that
-- plans a move, and an attach plans nothing.
--
-- The sum is a correlated subquery in the select list rather than an aggregate over
-- a join, so a reader asking about one host is charged for one host: the host_id
-- filter is applied to the scan of `hosts` and the subquery runs only for the rows
-- that survive it. That is the property a view puts at risk and the one
-- TestPGCommittedBytesViewDoesNotDeriveTheWholeFleet measures.
--
-- It is a plain view and not a materialized one on purpose: staleness in an
-- accounting path is the exact failure ADR-0017 removed when it deleted the ledger,
-- and a materialized view is a ledger with a refresh job.
CREATE VIEW host_committed_bytes AS
SELECT h.host_id,
       COALESCE((SELECT SUM(v.size_bytes) FROM volumes v
                  WHERE v.primary_host_id = h.host_id), 0)::BIGINT
           AS committed_bytes
  FROM hosts h;
