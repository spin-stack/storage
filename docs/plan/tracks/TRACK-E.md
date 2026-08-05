# Track E — observability

**This file is track E's alone.** It was carved out of `STATUS.md` on 2026-08-04
because five lanes appending to one file is a collision every wave: in wave 3 one lane
committed a stale copy and deleted 121 lines of another's, ten seconds after they
landed, and only that lane looking again restored them. Ownership by *file* is a
control; ownership by *section of a file* is a convention, and a convention is what
`PARALLEL-PLAN.md` says is not a control.

`STATUS.md` remains the single answer to "what is true right now" — its head, its
tables and its DEV entries. This is the running log of one track's increments.

---

## Track E — observability (open work, appended per increment)

*Only track E appends here* — it owns `internal/obs`, `internal/vhost`,
`internal/blockdev` and `internal/cow`. The head tables are recounted once, at integration,
by track A.

### E1 — `internal/obs`'s logging and tracing half is deleted (2026-08-03)

`obs` had two halves and only one of them was connected. The metrics half has real
callers — the WAL's watermarks and `wal_out_of_space`, the Agent's lease counter and
gauge, §19's two snapshot histograms — all reaching an instrument through `obs.Recorder`.
The other half had none. `Tracer`, `NewTracer`, `InjectContext`/`ExtractContext`, the five
correlation context keys with `WithRequestID` and friends, `NewLogger`, `LoggerFrom` and
`Provider.RecordedSpans` were referenced **only inside `internal/obs` and its own tests**:
no RPC injected a header, no handler extracted one, no binary constructed a `Tracer`, and
not one line in the tree was logged through `LoggerFrom`. §26.1's CP → Agent → S3 → KMS
trace propagation was a package that propagated between two halves of its own test.

Deleted rather than wired. Wiring is not one line and it is not this package's to make:
the injection point is an interceptor in `api/`/`internal/cpserver` that does not exist,
and the extraction point is the Agent's loop. Keeping the machinery until they do means
"registered and unused" — the same finding as DEV-0010, which was about this very package's
metric catalog. Git holds it; the increment that grows the interceptor brings back the
functions it calls.

`Provider.Meter` went with them, for a related reason worth separating: it existed "for
ad-hoc instrument creation in tests" and was used by one test that made a counter and
added 1 to it. It handed out the ability to create a series outside `Catalog()`, which is
the one property `Recorder` exists to hold (§26.2). Three tests went with the code:
`TestTracePropagationAcrossBoundary`, `TestStructuredLogHasCorrelationFields`,
`TestNestedSpansShareTrace`, plus `TestMeterRecords`.

Net: −144 lines of production code, −64 of test. `go mod tidy` moved
`go.opentelemetry.io/otel/sdk` and `go.opentelemetry.io/otel/trace` from direct to
indirect requirements — nothing imports them any more.

**For track A, not edited here:** §26.1 of `arquitectura_mvp_volumenes_remotos_v5.md`
describes a mechanism the tree no longer contains — it needs the ADR-0026 treatment the
other withdrawn sections got, not a deletion. `REFERENCE.md:123` (`§26.1 | Distributed
tracing.`) resolves the *document* section and stays accurate as written; it is listed
only so track A decides deliberately rather than by omission.

### E3+E4 — the fake leaves the production surface, and four doc comments stop lying (2026-08-03)

Two commits, no behaviour change, `task ci` green on each.

**E3.** `vhost.RawDevice` — an in-memory Backend whose own comment said it exists "so the
unit tests can prove the transport" — lived in `internal/vhost/backend.go`, a production
file. It now lives in `internal/vhost/rawdevice_test.go`. The proof there was never a
production caller is that `go build ./...` still succeeds with it gone from the production
surface; `backend.go` is down to the `Backend` interface and `ErrOutOfRange`, and lost
three imports. An `internal/vhost/vhosttest` package was rejected: it buys cross-package
reuse nobody has asked for and puts the fake back on the production surface under another
name.

The item's premise that `hostio.RawFile` "is gone, surviving only in two doc comments" is
**wrong** — `internal/vhost/hostio/rawfile.go` is 180 lines and
`integration/vhost/qemu_test.go:226` calls `CreateRawFile` to serve a real kernel a
Backend with no WAL underneath it. It is the same shape of scaffolding as `RawDevice` and
it cannot make the same move: `integration/vhost` is a different package and Go has no way
to import another package's tests. Left where it is; the two doc comments were corrected
to describe it accurately instead of deleted. Nothing else matched
`fake|stub|noop|dummy` outside a `_test.go` file in the four packages.

**E4.** Every `pkg.Symbol`, `Err*` and `Test*` identifier in the doc comments of
`internal/{obs,vhost,vhost/hostio,blockdev,cow}`'s production files was grepped out and
looked up. Four did not resolve, and the interesting part is that three of them were one
thing: the deleted remote durability chain, still describing what a guest is promised.

- `wal.ErrSelfFenced` in `blockdev/doc.go`'s error list — gone with the lease-gated ACK
  (ADR-0026 4.5). Replaced by `wal.ErrLogBroken`, which `refuse` has branched on since.
- The same file's FLUSH paragraph promised "the guarantee against losing the host arrives
  only with a FLUSH" and a `remote` mode that "returns only after every covering object is
  verified in S3 and the lease is confirmed valid on the monotonic clock". `durableStep`
  is one `fdatasync`. The doc now says a FLUSH survives the process, the Agent and QEMU,
  and **not** the host. Narrowing a doc to what the code does is not an ACK-rule change and
  no code moved — but this comment *is* where the guest-facing promise is written down, so
  it is flagged for the human who reviews that zone.
- `TestAGuestWriteCompletesWhileAFlushIsUploading`, cited by `Device`'s comment as the
  proof a concurrent WRITE cannot be ACKed by a FLUSH, exists nowhere in the tree. Now
  cites `TestConcurrentRequestsDoNotRaceTheLog`, which does exist here, and says the
  sequence argument is wal's to prove rather than borrowing a name for it.
- `cow.ActiveMap`, promised by `internal/cow`'s package doc ("and, in Increment 4.4, the
  64 KiB segment active map"), was deleted 2026-08-02.

Each correction quotes the text it replaces, in the file and in the commit message.

**For other tracks, seen and not edited:**

- **Track C — `wal.Log.Flush`'s own doc comment (`internal/wal/log.go:575`) is stale in
  exactly the way `blockdev/doc.go` was**: it still lists "remote (default): upload +
  verify every covering object, VERIFY the lease… (§12.2, INV-06)" and a `local` mode,
  twenty lines above `durableStep`, which says both were deleted. Same for `Log`'s
  concurrency comment ("mu … is **never held across an object-store PUT**", "flushMu
  serializes durable steps (Flush, WriteFUA)" — `WriteFUA` is gone) and
  `ErrFUAOnWrite`'s "fdatasync, verified PUT, valid lease".
- **Track C — `wal.Log.AdvancePublished` has no production caller** (only
  `internal/dst/scenarios_wal.go:313`), so `published` never advances for a live volume
  and `TruncateLocal` can reclaim nothing. That is why `blockdev.go:163`'s operator-facing
  string still offers "restore the object store" as a remedy for a full device: it is
  wrong, but what replaces it depends on the truncation story. Left alone deliberately —
  the same phrase is in `internal/wal/degraded.go:15` and `internal/simio/disk/disk.go:21`.
- **Track A — `REFERENCE.md` still resolves `§14.4` as "**The order of operations in
  FLUSH/FUA.** Six steps" and `INV-07` as "ACK only after the six §14.4 steps | active",
  and `§14.8` as "Per-volume durability modes (`remote` / `local`)". `INVARIANTS.md`'s
  INV-07 row is already correct ("Six steps became two"); it is `REFERENCE.md`'s one-line
  resolution that still sends a reader to the deleted chain.

### E2 — a metric leaves the process (2026-08-04)

`obs.NewProvider(name, exporter)` is the production Provider, and
`real.NewOTLPMetricExporter(ctx, endpoint)` is the OTLP/HTTP exporter behind it. Nine
metrics were being recorded and none of them left either binary: `obs` had only
`NewTestProvider`, so both mains passed `Recorder: nil` — honestly, with a comment saying
that passing the test provider "would export the metrics to memory and look like
observability from the outside".

The socket-opening half is in `internal/simio/real/otlp.go` and nowhere else. That is
INV-01, not tidiness: building the exporter inside `internal/obs` would have needed an
exemption in **both** `.golangci.yml` and the `simulable` analyzer, which is the widening
those two exist to prevent. `obs` takes an `sdkmetric.Exporter` and knows nothing about
transports.

**A nil exporter is a working Provider that exports nothing**, and an unset endpoint
returns exactly that nil — so a binary needs no conditional and an Agent with no collector
starts and runs as it does today. Two smaller decisions are written at the code: a
malformed endpoint is *refused* at startup (the exporter's own behaviour on a bad URL is
to keep its defaults and quietly export to localhost), and exporter retries are **off**,
because the export that matters is the one `Shutdown` flushes while a volume is stopping —
the default one-minute retry would add a minute to every Agent's shutdown when a collector
is down, delaying the publish that carries V1's whole RPO. Nothing is lost by dropping it:
OTLP metrics are cumulative, so the next successful export restates the totals.

**Verified against a real receiver, not against a constructor returning non-nil.**
`TestOTLPExporterDeliversARecordedMetricToACollector` stands up an OTLP/HTTP server,
records `lease_renewal_failures_total{host=host-a} += 3` through the Recorder production
code holds, and asserts the collector decoded the name, the value 3, the label, the
resource's `service.name=volume-agent`, and the POST path `/v1/metrics`. **Three planted
bugs, each watched go red:** dropping the reader in `NewProvider` so the exporter is
ignored (`the collector received no lease_renewal_failures_total; it saw map[]`), dropping
the `Shutdown` flush (same line), and pointing the exporter at `http://127.0.0.1:1`
(`Shutdown: failed to upload metrics: … connect: connection refused`, returned in under a
second — which is also the retries-off decision proving itself).

**The handoff — track C owns `cmd/volume-agent/main.go`, so this lane did not wire it.**
Five lines, and they are exactly these:

```go
otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
    "OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
// …after signal.NotifyContext:
exporter, err := real.NewOTLPMetricExporter(ctx, *otlpEndpoint) // (nil, nil) when unset
if err != nil {
    return err
}
metrics, err := obs.NewProvider("volume-agent", exporter)
if err != nil {
    return err
}
defer func() {
    // context.WithoutCancel: the flush must outlive the SIGTERM that started the shutdown.
    // Logged, never fatal — a collector that is down must not change the Agent's exit code.
    if err := metrics.Shutdown(context.WithoutCancel(ctx)); err != nil {
        slog.Error("flushing metrics", "error", err)
    }
}()
```

then `Recorder: metrics.Recorder(),` in `agent.Deps` in place of `Recorder: nil` and its
three-line comment. `cmd/control-plane` is the same change with `"control-plane"` as the
name. What this lane could *not* make one line is the `defer`: a periodic reader holds up
to a minute of samples, so a process that exits without flushing exports nothing at all —
which is the same defect this item removed, in a different place. It is deliberately
`metrics.Recorder()` and not `obs.NewRecorder(metrics.Metrics)` so the wiring is one
expression.

`go.mod` grew `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp` (v1.39.0,
matching the pinned core) and, through it, `go.opentelemetry.io/proto/otlp` v1.9.0; MVS
pulled `golang.org/x/crypto` 0.43.0 → 0.49.0 and added `golang.org/x/net`. `task ci` green;
`task cover` 90.1% against the 90% floor.

**Still not proven end to end**, and it is the seam this repository loses defects at: no
lane starts the real Agent binary with `-otlp-endpoint` pointed at a receiver and asserts a
series arrives. That belongs in `integration/e2e`, which is track C's file set this wave.
Until it exists, "a metric leaves the process" is proven for the exporter and the provider,
not for the binary.

### E5 — a number on what the resident read view costs, and what the number found (2026-08-05)

`cow.IntervalMap` is the one per-volume structure on an Agent whose size the guest decides
rather than the operator: a base from the published image, a layer per resumed session, and
one more layer for every snapshot ever taken. Nothing measured it. `IntervalMap.Bytes`
existed and its own comment called it "metric `active_map_bytes` proxy" — a proxy for a
metric that was never wired, called by nothing but its own test.

**What is measured, and what was rejected.** `cow.Cost` reports `Bytes` (payload of every
live extent in the chain), `Extents` (records, which are both the other half of memory —
about forty bytes of Go each — and the shape of every read, since `Read` scans a layer's
extents linearly) and `Layers` (chain depth). Three and not one, because a snapshotted
volume moves them apart: `Freeze` adds a layer and not one byte, so a single `bytes` gauge
would report "fine" for a volume whose reads had become a sixty-five-layer walk. The
tombstone count is in `Cost` for tests and gets no series: `cover` merges adjacent spans, so
it is bounded by construction (`TestClearsAreMerged`), and a fourth per-volume series that
can only ever be small is cardinality bought for nothing. A "reachable bytes" number — what
`Ranges` still covers, which is what would separate live data from stale copies — was
rejected for the metric because it is O(extents); the measurement tests below compute it,
where the cost does not matter.

**When it is computed.** Each layer maintains its own byte count as it is mutated (one add
in `insert`, one subtract per overlap in `removeRange`), so `Cost` is O(layers) rather than
O(extents). The number is read at the WAL's flush cadence — every guest `fsync` — and a fold
over the extents at that cadence would make the measurement scale with the thing it
measures: the fuller the volume, the more the metric costs.
`TestTheMaintainedByteCountEqualsAFoldOverTheExtents` is what keeps the incremental counter
honest — arbitrary `rapid`-drawn write/clear sequences, compared against the fold it
replaced after every single operation.

**The finding, and it is a defect rather than a reassurance.** Nothing ever collapses the
chain: `Freeze` seals a layer and installs a new one over it, no layer is dropped when every
extent in it has been superseded, and no base is released once its image is published. So a
volume that rewrites one hot block between snapshots holds one copy of that block per
snapshot, for ever — `TestASnapshottedVolumeHoldsOneCopyPerSnapshot` prints the
amplification for the number of snapshots it models, and the multiplier is exactly the
number of snapshots plus one. Depth is also paid on every guest read, because `Read` paints
the base and lets each layer overwrite it: `BenchmarkReadAtDepth` (the repository's first
benchmark, and the only executable form of the claim) shows a 4 KiB read of a block every
layer has rewritten going from tens of nanoseconds at depth 1 to tens of microseconds at
depth 256 — the same order as the NVMe the read view exists to avoid. The slope is the
code's; the constants are the machine's.

The other half is genuinely cheap, and it matters: a volume that never rewrites pays for its
data and nothing else (`TestALayeredVolumeThatDoesNotRewriteCostsItsDataAndNoMore`), and a
sparse image costs its written set and not its address space
(`TestASparseImageCostsItsWrittenSetAndNotItsSize`). The gauge is therefore not watching a
structure that is doomed either way; it separates the volume whose view is its working set
from the volume whose view is its history.

**For track A — `internal/obs.Catalog()` is §26.2**, so the architecture document's §26.2
needs the same three lines: `read_view_bytes`, `read_view_extents`, `read_view_layers`, all
gauges labelled by volume.

**The handoff — track C owns `internal/wal`, so this lane did not wire the call site.** The
only place a read view can be read without racing its writer is under the lock `wal.Log`
holds over `l.view`; `ViewAtRest` hands the pointer out and its one caller uses it on a
stopped volume, so no package outside `wal` can sample a live one. Three lines, in
`recordWatermarks`, which already runs under `l.mu` at the flush cadence:

```go
cost := l.view.Cost()
l.rec.Gauge(ctx, "read_view_bytes", float64(cost.Bytes), vol)
l.rec.Gauge(ctx, "read_view_extents", float64(cost.Extents), vol)
l.rec.Gauge(ctx, "read_view_layers", float64(cost.Layers), vol)
```

Until that lands, **`cow.Cost` has no production caller** — said here because `task deadcode`
cannot say it: no `cow` symbol appears in its output at all (the reflection blind spot
`hack/deadcode.sh` documents, the same one that hides `wal.TruncateLocal`). What this lane
could prove is everything except the call site:
`TestTheReadViewCostReachesTheCatalogSeries` builds a real layered view, records exactly the
three lines above through the `Recorder` production code holds, and asserts the *collected*
series carry the chain's numbers rather than the top layer's. **Three planted bugs, each
watched go red:** deleting `read_view_layers` from the catalog (`nothing collected for
read_view_layers; the catalog carries map[read_view_bytes:8192 read_view_extents:2]` — an
unregistered name is dropped silently, which is why the catalog entry *is* the wiring at
this layer), dropping the subtraction in `removeRange` (`maintained 1 live bytes, a fold
over the extents says 0`), and stopping `Cost` from following the base chain (`Cost() =
{Bytes:0 Extents:0 Layers:1 Cleared:1}, want {Bytes:6 Extents:2 Layers:3 Cleared:1}`, and
the snapshot measurement collapsing to `Bytes=4096 Extents=1 Layers=1 (amplification 1x)`).

`task test` and `task dst` green; `task cover` 90.0% against the 90% floor. `task ci` fails
before reaching them on two other lanes' uncommitted files (`fmt:check` on
`integration/e2e/desired_test.go`, `lint` on `internal/metadata/pg`), neither touched here;
`golangci-lint run` and `fmt --diff` over `internal/cow` and `internal/obs` are clean.
