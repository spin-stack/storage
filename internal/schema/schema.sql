-- Control Plane metadata schema (§8). Authority for leases, epochs, ownership,
-- attachments, snapshot/clone catalog, hosts/capacity, and Control Plane terms.
-- NOT the authority for the durable point of data (that is S3, §5.8).
-- Reconstructible from S3 via rebuild-metadata (§22.5).
--
-- There is no `operations` table: ADR-0026 withdrew the reconciliation machinery it served
-- and nothing outside a test ever wrote a row. Dropped rather than left empty, so it does
-- not read as a mechanism somebody is about to use.
--
-- Identity columns are the `uuidv7` domain (not text): volume_id is the same 16-byte id the
-- on-disk format carries. This file is the declared state and the single source of truth —
-- pgschema plans against it (ADR-0019), sqlc generates from it, and the
-- integration lane builds its database from it; migrations/ holds the reviewed plans, not
-- the apply path.

-- UUIDv7 enforcement (INV-22, ADR-0007) as a type: the version nibble is the high 4 bits
-- of the UUID's 7th byte, so requiring it to equal 7 rejects a non-v7 id at insert whatever
-- the client. A domain rather than a predicate copied onto every identity column, because a
-- copied rule holds only where somebody remembered to copy it, and two columns went
-- without one for a year before anyone noticed. FK-referencing columns stay plain `uuid`
-- (the rule reaches them transitively); TestPGIdentityColumnsUseTheUUIDv7Domain exempts
-- exactly those.
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
    -- Why the host is CORDONED, empty for every other state (ADR-0013 §3). The Control
    -- Plane cordons a host whose device passes 70% used, so the state alone no longer means
    -- a human meant it; this column tells the two apart, and it is what stops the automatic
    -- loop from clearing a human's cordon (the pressure writer may only replace '' or
    -- 'DEVICE_PRESSURE' — lifecycle.CordonReason.OverwritableNames, in SetHostState's
    -- predicate). Emptied when the host leaves CORDONED: a reason that outlives its cordon
    -- is one the next reader will believe.
    cordon_reason        TEXT NOT NULL DEFAULT ''
                           CHECK (cordon_reason IN ('', 'OPERATOR', 'DEVICE_PRESSURE')),
    agent_version        TEXT NOT NULL DEFAULT '',
    max_format_version   INTEGER NOT NULL DEFAULT 2,  -- fleet-mixed gating (§27)
    nvme_total_bytes     BIGINT NOT NULL DEFAULT 0,
    nvme_used_bytes      BIGINT NOT NULL DEFAULT 0,
    -- The part of nvme_used_bytes that no verified object covers yet, summed over every
    -- volume this host holds (ADR-0013 §1), reported by the Agent in each heartbeat. Stored
    -- rather than derived because nothing in this database can compute it: the volumes table
    -- carries watermarks in sequence numbers, not bytes. It is also what distinguishes a busy
    -- host from one whose object store stopped answering, which will not stop growing because
    -- no local truncation may reclaim those records (INV-13).
    nvme_remote_backlog_bytes BIGINT NOT NULL DEFAULT 0,
    -- There is deliberately no nvme_committed_bytes column (ADR-0017). Committed
    -- capacity is derived from the rows that already say who holds what; see the
    -- note at the bottom of this file.
    last_heartbeat       TIMESTAMPTZ NOT NULL,
    -- There is deliberately no renewals_blocked_until column. It refused a host's lease
    -- renewals for the length of one promotion; ADR-0026 withdrew the promotion, leaving it
    -- with no writer and no reader. What stage 2 needs is a fence that follows the
    -- *volume*, not this column with a caller added.
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
    -- Written once, at create; no statement in queries/ updates it. descriptor.json carries
    -- the same number and is written only at create, so a size that moved here and not there
    -- is a catalog and a bucket that disagree — and -rebuild-metadata restores from the
    -- bucket.
    size_bytes         BIGINT NOT NULL,
    -- There is no durability column. §14.8's per-volume FLUSH ACK contract
    -- ('remote' | 'local') lost its remote half to ADR-0026, and nothing ever branched on
    -- what was left; a column still saying 'remote' would claim a durability the data path
    -- no longer provides.
    -- The logical block size reported to the guest. It is carried to the Agent in the
    -- desired state and lands in the virtio-blk config as blk_size
    -- (vhost.Config.BlockSize, set by VolumeManager.supervise);
    -- controlplane.VolumeSpec.validate refuses one that is not a multiple of the
    -- 512-byte sector, and control-plane -seed-block-size defaults it to 4096.
    --
    -- It is not a CoW segment granularity, whatever the older comment said (DEV-0024):
    -- nothing reads block_size as an objectization unit, and the 64 KiB segment §13.1
    -- named exists nowhere — cow.IntervalMap works on the guest's real extents with no
    -- grid. Whether that granularity is V2 or simply dead is still open, and is not this
    -- column either way.
    block_size         INTEGER NOT NULL,
    -- rpo_target_seconds is how far behind the object store this volume may fall: the age
    -- at which its host seals the tip and commits it even under the size threshold (v6 §11).
    -- Zero is no age trigger, and the volume commits on size alone.
    --
    -- Per volume and not per Agent because it is a promise made to one tenant, while the
    -- size threshold beside it is the host's own affair. DEFAULT 0 rather than a chosen
    -- number: §11 forbids picking a target instead of measuring one, and no measurement of
    -- upload throughput against a real object store has been made.
    rpo_target_seconds INTEGER NOT NULL DEFAULT 0 CHECK (rpo_target_seconds >= 0),
    current_epoch      BIGINT NOT NULL DEFAULT 0,
    state              TEXT NOT NULL                                  -- §7 failover states
                         CHECK (state IN ('ACTIVE', 'PRIMARY_SUSPECTED', 'FENCING_WAIT',
                                          'RECOVERY_REQUIRED', 'RECOVERING', 'DETACHED')),
    primary_host_id    UUID REFERENCES hosts(host_id),
    standby_host_id    UUID REFERENCES hosts(host_id),
    chain_depth        INTEGER NOT NULL DEFAULT 0,
    -- The snapshot this volume was cloned from (§20), NULL for a volume that was created
    -- rather than cloned. chain_depth says a chain exists; this says what is on the other end
    -- of it, which is what a clone's Agent needs to find the objects it reads through —
    -- without it a clone serves zeros for everything its parent ever wrote (DEV-0007).
    --
    -- The constraint is added below, after snapshots exists: the pair is circular and one
    -- direction has to be an ALTER.
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
    -- What §11 calls the product: how far behind the object store this volume was when
    -- its host last said. `commit_age_seconds` is the age of the newest published commit
    -- that covers everything the guest has written — so a volume nobody writes to reads
    -- as 0 and is *inside* its RPO rather than behind it — and `unpublished_local_bytes`
    -- is what would be lost with the host at that moment. The pair is what §11 asks be
    -- alarmed on, and until it was stored the fleet's headline number lived only as a
    -- gauge on each Agent, where nothing can ask a question about a volume by name.
    --
    -- They replace local_sequence, durable_sequence and published_sequence, three columns
    -- of the withdrawn write-ahead log that every v6 Agent reported as zero.
    --
    -- NULL means no host has reported yet, which is not the same as zero and must not
    -- render as an RPO of nothing. Stated as an age rather than a timestamp because the
    -- Agent has no wall clock (INV-01): it measures an interval, and reported_at is this
    -- database's clock — the one every other deadline here lives on — so a reader adds
    -- `now() - reported_at` and gets an age that keeps growing while a host is silent.
    -- The newest commit a host has told the catalog it published for this volume, NULL
    -- for one that has never published.
    --
    -- It answers the question a host cannot answer for itself: the object store has no
    -- HEAD for this volume — is it new, or is its HEAD gone? Guess "new" and a guest boots
    -- a blank qcow2 with no I/O error anywhere, and the first commit off that chain makes
    -- the blank disk the volume's history. The host that published can consult its own
    -- state.json, and that is the case that does not matter: it still has the layers. The
    -- case that loses a tenant's disk is a volume placed on a machine that has never seen
    -- it — §14's recovery path — where only the catalog knows.
    --
    -- Not a foreign key and not an authority on what the commit holds: commits live in
    -- the object store (§5.8), and a catalog row that could only exist alongside a bucket
    -- object would make -rebuild-metadata impossible by construction. NULL under-claims,
    -- which is the safe direction: the volume is then served exactly as it was before
    -- this column existed.
    head_commit_id         UUIDV7,
    commit_age_seconds     INTEGER CHECK (commit_age_seconds >= 0),
    unpublished_local_bytes BIGINT NOT NULL DEFAULT 0
                             CHECK (unpublished_local_bytes >= 0),
    reported_at            TIMESTAMPTZ,
    CONSTRAINT volumes_progress_is_reported_together
        CHECK ((commit_age_seconds IS NULL) = (reported_at IS NULL)),
    -- Why the host that holds this volume is not serving it, empty when it is
    -- (internal/lifecycle.Refusal). The Agent fails closed in five places — a missing image,
    -- a durability floor, a read view that never resolved, a KEK it does not hold, a lost
    -- lease — and without this column every one of them was invisible to the fleet:
    -- `-fleet-status` rendered the volume as normal off its last healthy watermarks.
    --
    -- Last-report-wins, qualified by primary_host_id and current_epoch so a host the fleet
    -- has moved past cannot resurrect a refusal its successor has cleared. The three
    -- columns above are written by the same statement and want the same predicate for the
    -- same reason, which is why one report is one write.
    refusal            TEXT NOT NULL DEFAULT ''
                         CHECK (refusal IN ('', 'IMAGE_MISSING', 'DURABILITY_LOST',
                                            'NO_READ_VIEW', 'NO_KEY', 'LEASE_LOST',
                                            'ATTACH_FAILED', 'PUBLISH_FENCED')),
    -- The sentence the Agent sent with the refusal, printed verbatim by `-fleet-status` and
    -- branched on by nothing: the token above is what a column, a grep and an alert key on.
    -- Emptied with the refusal, for the reason hosts.cordon_reason is.
    refusal_detail     TEXT NOT NULL DEFAULT '',
    CONSTRAINT volumes_refusal_detail_needs_a_refusal
        CHECK (refusal <> '' OR refusal_detail = ''),
    -- When the Control Plane observed the lease of the writer it is fencing (ADR-0015),
    -- stamped by this database's clock — the one that also stamps host_leases.last_renewal,
    -- and so the one every fencing deadline lives on — on entry to FENCING_WAIT, cleared on
    -- exit.
    --
    -- The dwell is measured from here and not from last_renewal because that answers a
    -- question about the *writer*: a read served by a lagging replica reports one old enough
    -- that the wait already looks over. This column is written and read back by the promoter,
    -- so a stale read returns NULL and starts a full dwell. Fail slow, never short — and it
    -- is what lets a Control Plane restarting mid-fence resume its predecessor's wait.
    fencing_started_at TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A snapshot is a *name for a commit* (§19 under v6), not a copy: the published history is
-- already a chain of immutable commits. Taking one costs a rotation and a publish when the
-- tip holds unpublished bytes, and nothing at all when it does not.
--
-- `commit_id` is all that identifies the point in history: a commit id names a manifest
-- whose key is derived (commit.ManifestKey), so a catalog and a bucket cannot disagree
-- about where a snapshot lives — which is what `manifest_key`, a string the Agent reported,
-- could not hold. (`target_sequence` and `root_digest` went with the v5 engine.)
CREATE TABLE snapshots (
    snapshot_id        UUIDV7 PRIMARY KEY,
    volume_id          UUID NOT NULL REFERENCES volumes(volume_id),
    parent_snapshot_id UUID REFERENCES snapshots(snapshot_id),
    epoch              BIGINT NOT NULL,
    -- The commit this snapshot names, empty until the host reports one. It is not a
    -- foreign key to anything: commits live in the object store, which is the authority
    -- on them, and a catalog row that could only exist alongside a bucket object would
    -- make -rebuild-metadata impossible by construction.
    commit_id          UUIDV7,
    source_host_id     UUID REFERENCES hosts(host_id),
    state              TEXT NOT NULL                                  -- §19
                         CHECK (state IN ('CREATING', 'PUBLISHED', 'FAILED', 'DELETING')),
    portable           BOOLEAN NOT NULL DEFAULT false,
    request_id         UUIDV7 UNIQUE NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A PUBLISHED snapshot names a commit — that direction only: a DELETING snapshot keeps
    -- the commit it was published at, and a biconditional would refuse the transition. What
    -- it protects is the row a clone follows: PUBLISHED is the only state that says a
    -- snapshot is usable, and one that says so while naming nothing sends a reader to an
    -- object that is not there.
    CONSTRAINT snapshots_published_names_a_commit
        CHECK (state <> 'PUBLISHED' OR commit_id IS NOT NULL)
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

CREATE INDEX volumes_parent_snapshot_id_idx ON volumes (parent_snapshot_id);
CREATE INDEX snapshots_source_host_id_idx ON snapshots (source_host_id);

-- Committed NVMe capacity (§28.2) is DERIVED, not stored (ADR-0017):
--
--   committed(host) = Σ size_bytes of the volumes whose primary_host_id is the host
--
-- It is a query over rows that already exist and are already term-guarded, so a resumed
-- pass computes the same answer as the pass that crashed. The column it replaces was an
-- incremental ledger, and every safeguard added to it — the non-negative guard, the
-- expected-value predicate, the per-volume release stage — existed only because a delta
-- is not an idempotency key.
--
-- A view because a rule lives once: it was inlined in four queries for a tooling reason
-- ADR-0019 removed (Atlas Community refused to diff a schema containing a view), and four
-- copies of an accounting rule is four places to decide "full" differently. Plain and not
-- materialized: a materialized view is a ledger with a refresh job.
--
-- ADR-0017's second term — bytes an in-flight operation plan had reserved on a
-- destination — went with the operations table, and changes no number this view ever
-- produced (nothing outside a test wrote an operations row). What it removes is headroom
-- for a move spanning two hosts, and V1 performs none. The one move a V1 catalog can
-- make, detach-then-attach, carries no CapacityBound at all — an open gap recorded
-- against D6 in docs/plan/tracks/TRACK-D.md, which a reservation term would not have
-- closed. The sum is a correlated subquery rather than an aggregate over a join, so a
-- reader asking about one host is charged for one host (TestPGCommittedBytesViewDoesNotDeriveTheWholeFleet).
CREATE VIEW host_committed_bytes AS
SELECT h.host_id,
       COALESCE((SELECT SUM(v.size_bytes) FROM volumes v
                  WHERE v.primary_host_id = h.host_id), 0)::BIGINT
           AS committed_bytes
  FROM hosts h;
