# migrations/ — the record of reviewed plans

This directory is **not the apply path** any more, and nothing replays it in order.

`internal/schema/schema.sql` is the declared state and the single source of truth.
pgschema (ADR-0019) computes the DDL that takes a live database there; sqlc generates
from the same file; the integration lane builds its Postgres from it directly. Nothing
here is read by any test, and `task db:verify` proves the database `schema.sql` builds
is exactly the schema it declares.

What lives here is the **ordered record of the plans that were reviewed and applied**.
pgschema keeps no history table, so this is where "what did we actually run against
that database, and who looked at it?" is answered.

Each change leaves a pair, timestamp-prefixed so the directory reads chronologically:

| file | what it is |
|---|---|
| `<timestamp>_<name>.sql` | the DDL, for humans — this is the artefact to review |
| `<timestamp>_<name>.json` | the plan pgschema executes, fingerprinted against the database it was computed from |

## The loop

```
$ task db:dev:up                      # pinned Postgres 18, no local install assumed
$ $EDITOR internal/schema/schema.sql   # change the desired state
$ task db:plan -- <name>              # writes the pair above; read the .sql
$ task db:apply PLAN=migrations/<timestamp>_<name>.json
```

`db:apply` runs the **saved** plan, never one recomputed at apply time — reviewing plan
A and applying plan B is the failure mode a state-based tool invites. pgschema records
a fingerprint of the database the plan was made against and refuses the plan if the
database has moved since, so a stale plan fails loudly instead of applying something
nobody read.

## Why the Atlas migration chain is gone

There was one, up to `20260726122007_revocation_window.sql`, with an `atlas.sum`
checksum and a Go package embedding it for the tests. It was folded into `schema.sql`
rather than carried: nothing is deployed, no database anywhere has those files applied,
and CLAUDE.md's "formats change in place until the spine ships" covers the schema as
much as the on-disk ones. Keeping a chain whose only reader was a test that could have
built the same schema from the declared state was history for its own sake.

The integrity property `atlas.sum` provided is replaced by `task db:verify`, which
compares the real result of applying `schema.sql` against the state it declares rather
than the bytes of a file against a hash.
