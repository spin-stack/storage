# SNAPSHOT-LIFECYCLE-SPEC — §19's background half (DEV-0007)

**Human-review zone: on-S3 format (the manifest is the last data object published) and
the snapshot lifecycle.** Reviewed before implementation.

## What §19 asks for, and what exists

The design splits a snapshot into a pause and a background job:

```text
1. Capturar atómicamente N = local_sequence (bajo el lock del volumen; µs).
2. Seguir aceptando writes normalmente (sequences > N).
3. En background (clase flush para los PUTs que le den durabilidad a N):
   a. Cerrar/subir batches hasta N …  e. Publicar manifest en S3.
4. Registrar PUBLISHED en PostgreSQL (request_id).
```

`Snapshotter.Create` does 1 and 3 in **one blocking call**. Step 1 is already right — it
captures `Watermarks().Local` with no I/O, and the measured pause is what §19 says it
should be (~0). What is wrong is that the caller then blocks through the flush, the object
listing and the manifest PUT.

The consequence is not a paused guest — §19's headline property already holds, because
the guest keeps writing at sequences > N. It is that **the requester** holds an RPC for
the length of an upload, and that there is no `CREATING` state anywhere: a process that
dies mid-seal leaves no row and no manifest, so nothing knows a snapshot was ever
attempted.

**And there is no owner.** `Snapshotter.Create` is called by DST scenarios and nothing
else — no RPC, no CLI, no worker. That is worth stating plainly, because it bounds what
this increment can honestly claim: it makes the *library* match §19's shape, so that
whatever drives it later cannot be forced into the blocking one. It does not make
snapshots reachable from outside the process; that is spine work (ADR-0018) and it needs
an RPC that does not exist.

## What gets built

**The split.** `Capture` returns immediately with the sequence and the pause it cost;
`Seal` does the durable work and publishes. `Create` stays as the composition of the two,
because every current caller wants exactly that and a test that wants the whole snapshot
should not have to drive two steps.

```go
snap := s.Capture(log)        // µs: the sequence, and nothing else
m, err := s.Seal(ctx, log, snap, ...)  // flush, list, manifest
```

**The states become observable.** `Capture` returns a value carrying `CREATING`; `Seal`
returns a manifest (`PUBLISHED`) or an error, and the caller records `FAILED`. The
lifecycle vocabulary already exists (`lifecycle.SnapshotCreating/Published/Failed`, with
the transition table and the schema CHECK) — nothing produced those states, and this is
what produces them.

**The two mandatory metrics get recorded.** §19 names them and `internal/obs` already
registers them: `snapshot_pause_duration_seconds` (must stay ~0) and
`snapshot_publish_duration_seconds`. Neither has ever been observed by anything. `Capture`
records the first, `Seal` the second.

**Idempotency is unchanged and load-bearing.** The manifest is published create-only
(`If-None-Match`), so a `Seal` retried after a crash either wins or finds its own manifest
already there. That is what makes a background job safe to re-run, and it is why the split
does not need a new mechanism.

## What this deliberately does not do

- **No goroutine, no worker pool, no queue.** "In background" is a property of the
  *caller*, and this package must not decide it: the Agent has a scheduler with io-class
  budgets (INV-17) and spin's runner may own the lifecycle instead (ADR-0021). A
  `go seal()` here would be a policy decision taken in a library.
- **No RPC.** Snapshots stay unreachable from outside the process until the spine grows a
  call for them.
- **No `checkpoint_on_snapshot`.** §21.1's step (d) objectizes segments during a snapshot;
  segment objects do not exist at all (see `OBJECTIZATION-SPEC.md`).

## Tests that land with it

- **The pause is the capture, and nothing else.** A `Capture` against a log with megabytes
  of unflushed WAL must cost no I/O — asserted on the simulated clock, which does not
  advance unless something does work.
- **Writes continue at sequences > N**, and the sealed manifest covers exactly the prefix
  ≤ N: a snapshot that swept in a later write is a snapshot of a state that never existed.
- **A crashed seal is retryable.** Seal, kill, seal again: one manifest, same digest.
- **Both metrics are recorded**, with the pause bounded — the one number §19 says must
  stay ~0 and that nothing has ever checked.
