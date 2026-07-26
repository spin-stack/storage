# PostgreSQL features worth using now that the tool allows them

pgschema (ADR-0019) round-trips views, domains, partial indexes, exclusion constraints,
functions, triggers and partitioning. That removes the *tooling* reason not to use them.
It does not remove the *design* reasons, and half of this document is about which ones
still stand.

The rule this project already follows: **a rule lives once, and where a wrong caller
cannot skip it.** A feature earns its place when it moves a rule from "every query
remembers to" to "the database will not allow otherwise". A feature that instead hides
behaviour from the query — a trigger doing bookkeeping — moves in the wrong direction,
however elegant.

## Recommended

### 1. A `uuidv7` domain — INV-22 expressed once

`CHECK ((get_byte(uuid_send(x), 6) >> 4) = 7)` appears **5 times** today, once per id
column, and every new table copies it. A domain names it:

```sql
CREATE DOMAIN uuidv7 AS uuid CHECK ((get_byte(uuid_send(VALUE), 6) >> 4) = 7);
```

Then a column is `host_id uuidv7 PRIMARY KEY`. The invariant stops being a copied
predicate and becomes a type — the same move `internal/lifecycle` made in Go, and the
same one ADR-0009 argued for. New tables get it by using the type, which is what makes
this worth doing before the schema grows.

*Check first:* sqlc must map `uuidv7` to `pgtype.UUID` as it maps `uuid` — an override
in `sqlc.yaml` if not. That is the only risk, and it is compile-time.

### 2. A `CHECK` for watermark ordering — INV-03 at the table

`volumes` carries `local_sequence`, `durable_sequence`, `published_sequence` and
**nothing in the database stops them being out of order**. INV-03 (§5.6, published ≤
durable ≤ local) is enforced by the query (`GREATEST`) and by the Go caller. One
constraint makes it structural:

```sql
CHECK (published_sequence <= durable_sequence AND durable_sequence <= local_sequence)
```

This is the invariant this project describes as load-bearing during an incident — the
number an operator reads to decide whether to accept data loss — and it is the cheapest
of everything here. It is not a pgschema unlock; Atlas would have taken it too. It was
simply never added.

### 3. A view for the derived committed bytes — ADR-0017's rule, once

The derivation is currently **inlined in four queries** because the old tool refused a
view. As a view it is written once and every caller — placement, drain, clone, the
capacity bound — reads the same definition:

```sql
CREATE VIEW host_committed_bytes AS
SELECT h.host_id,
       COALESCE(SUM(v.size_bytes), 0) + COALESCE(SUM(r.reserved_bytes), 0) AS committed_bytes
FROM hosts h ...
```

sqlc generates from a view like any relation, so the Go side does not change shape.
**Verify the plan**: a view that turns an index scan into a sequential one is a
regression the `EXPLAIN` test should catch, and the drain reads this on every pass.

### 4. Partial indexes for the "live operation" queries

`ListOperationsByHost` (wave 3) filters to non-terminal phases and the index is on
`(host_id, operation_id)` — it reads every operation the host ever had. A partial index
is exactly this query:

```sql
CREATE INDEX operations_live_by_host_idx ON operations (host_id)
  WHERE phase IN ('PENDING', 'RUNNING', 'CANCELING');
```

The cost is real and should be stated: **the phase set is now written in two places** —
the transition table in `internal/lifecycle` and this predicate. That is what made wave 3
choose a Go-side filter over an index. The difference now is that it lives in
`schema.sql` next to the `CHECK` that already enumerates the same values, not buried in
a migration file, and a test can assert the two agree.

### 5. An exclusion constraint for "one live drain per host"

Wave 3 closed the *harm* of two concurrent drains but left the race: two goroutines
inside one leader can both pass the Go-side check. This closes it in the database:

```sql
ALTER TABLE operations ADD CONSTRAINT one_live_drain_per_host
  EXCLUDE USING gist (host_id WITH =)
  WHERE (kind = 'drain' AND phase IN ('PENDING', 'RUNNING', 'CANCELING'));
```

Needs the `btree_gist` extension. A unique partial index on `(host_id) WHERE …` is the
simpler equivalent and needs no extension — **prefer that** unless a range condition
appears later. Same duplicated-phase-set cost as (4).

## Deliberately not

- **Triggers for anything.** The term guard, the lifecycle transitions and the capacity
  bound are all *query predicates* on purpose: they are visible in the query a reviewer
  reads, they compose with `RETURNING`/rows-affected, and a caller that skips them
  affects 0 rows rather than being silently corrected. A trigger moves that behaviour
  where no query shows it.
- **Materialized views.** Staleness in an accounting path is the exact failure ADR-0017
  removed when it deleted the ledger. A materialized view is a ledger with a refresh
  job.
- **Native enums instead of TEXT + CHECK.** Already decided (ADR-0009) and the reason
  was never the tool: adding a value is a schema change either way, `TEXT` compares and
  sorts without casts in every client, and the vocabulary's authority is
  `internal/lifecycle` — the CHECK is the third enforcement layer, not the definition.
- **RLS.** One tenant, one Control Plane. Nothing to separate yet.
- **Partitioning `operations` by time.** Right shape eventually, no volume to justify it
  now, and it would freeze a retention decision nobody has made.

## Suggested order

(2) and (1) first — both are pure invariant work with no query changes. Then (3), with
the `EXPLAIN` assertion extended to it. Then (4)/(5) together, since they share the
phase-set duplication and should be reviewed as one decision.
