# PHASE 11 — Cross-host por materialización completa + cordon/drain + capacidad

> **DONE ✓** (11.1–11.3). One deviation recorded and resolved: **DEV-0002 / ADR-0008**
> (drain evacuates from the durable prefix, not from a source-taken snapshot).

> **Roadmap §30.11.** Depends on Phase 08 (recovery from S3), 09 (snapshots/clone),
> 10 (checkpoints, I/O classes). Implements §20 (placement order, cross-host),
> §22.3 (cold materialization), §28.1 (cordon/drain), §28.2 (capacity & placement),
> §29.4 (residual weakness: cold cross-host RTO).
> **⚠️ Increments 11.2 and 11.3 touch fencing + durability** (a volume changes host):
> human review of this spec *before* implementation and of the diff before merge.

**Phase objective:** a volume can move to another host **without any host-to-host data
path** — the destination materializes it from S3 alone (the recovery authority, §5.8) —
and the fleet gains the operational primitives that make maintenance possible: capacity
accounting with a declared oversubscription policy, `cordon` (place nothing new here)
and `drain` (a long-running, resumable, cancelable reconciled operation that moves every
volume off a host).

**Invariants:** Phase 11 activates **no new invariant ID** (the only pending one is
INV-19, Phase 13). It *extends the coverage* of existing ones to the host-move path:
INV-08 (materialization reads S3 only, never the source host), INV-09 (no ACKed-durable
write lost when the volume changes host), INV-10 (never two writers during a move),
INV-11 (the move promotes only after FENCING_WAIT), INV-16 (the materialization source
snapshot/checkpoint is immutable), INV-17 (materialization is background class and
yields). Each gets a new DST scenario driving the *existing* checker.

---

## Increment 11.1 — Capacity accounting + placement policy + cordon (§28.1, §28.2)

**Scope-in.** `metadata.Store` grows the fleet surface it lacks: `ListHosts`,
`SetHostState` (ACTIVE | CORDONED | DRAINING | DEAD, term-guarded), `ListVolumesByHost`,
and `CommitHostCapacity(hostID, deltaBytes)` (term-guarded reservation/release of
`nvme_committed_bytes`, refusing to go negative). Both implementations (`metadata/sim`
and `metadata/pg` + sqlc queries + a `schema.sql` change if the schema needs it) and the
integration test.

New package `internal/placement`: a pure, deterministic policy — no I/O, no clock.

```
Policy{ MaxOversubscription float64 }   // committed/total <= ratio (e.g. 2.0), §28.2
Choose(hosts, Request{SizeBytes, SourceHostID, CachedHostIDs}) (hostID, error)
```

Order exactly as §20: (1) `source_host` if it has capacity, (2) a host with the snapshot
cached / the warm standby, (3) any host with capacity. Hosts in CORDONED / DRAINING /
DEAD are never chosen — including as the source. Ties break deterministically (lowest
committed ratio, then host_id) so DST replays identically. `ErrNoCapacity` when nothing
fits. Metric `host_nvme_committed_ratio` (§28.2 alert).

**Tests first.** Table-driven `Choose` cases: source-first; cordoned source falls
through to a cached host; draining host never selected; the oversubscription bound is
respected at the boundary (== ratio allowed, > refused); no candidate ⇒ `ErrNoCapacity`;
determinism (same input ⇒ same host across runs). Store: `SetHostState`/
`CommitHostCapacity` are term-guarded (stale term ⇒ `ErrStaleTerm`, 0 rows) and
capacity never goes negative; `ListVolumesByHost` returns exactly the host's volumes.

**Gate.** Standard. No new invariant. Not a data-loss zone.

## Increment 11.2 — Cross-host materialization from S3 (§20, §22.3 cold) ⚠️

**Scope-in.** New package `internal/materialize`. `Materializer.Run` rebuilds a volume's
state on a destination host **from the object store only**:

```
1. resolve the source: a published snapshot manifest (clone/drain) or the latest
   verified checkpoint + the WAL objects after it (recovery of a live volume).
2. fetch each referenced object under ioclass.Background (yields, INV-17).
3. replay + decrypt into the read view (reuses recovery.Recover semantics).
4. verify: the referenced set is the contiguous prefix and the root digest matches;
   a missing object or a gap ⇒ hard error, never a partially materialized volume.
5. report progress: objects/bytes done vs total (feeds the drain operation, §28.1)
   and the measured cold RTO per GiB (§29.4 runbook input).
```

Structurally there is **no host-to-host transport**: the Materializer's only I/O
dependency is `objectstore.Store` (INV-08, and it keeps §5.4 honest — locality is not
durability). `controlplane.Clone` gains the cross-host arm: metadata clone + materialize
on the destination, capacity committed on the destination *before* the fetch starts.

**Tests first / DST.** `scenarioCrossHostMaterialization`: write + flush on host A,
snapshot, materialize on host B, assert the rebuilt view is byte-identical to A's
recovered view; assert zero reads against any host-to-host channel (there is none — the
sim network sees no traffic); a deleted/lost referenced object ⇒ error, never a silent
partial; a WAL gap ⇒ error; digest mismatch ⇒ error. Under contention with foreground
I/O, materialization is refused/throttled (existing `BackgroundYieldsChecker`).

**Gate.** Standard + the scenario in the mandatory `task dst` set + **human review**.
Extends INV-08/16/17.

## Increment 11.3 — Drain: reconciled, resumable, cancelable host evacuation (§28.1) ⚠️

**Scope-in.** `controlplane.Drainer`. `Drain(hostID, operationID)` is a long-running
reconciled operation, idempotent by `operation_id` (§18), with visible progress in
`operations` (`phase`, `current_state` JSON: `{moved, total, current_volume}`):

```
1. SetHostState(host, CORDONED)  → nothing new lands here (11.1).
2. SetHostState(host, DRAINING).
3. for each volume on the host (deterministic order):
     a. placement.Choose destination (prefers cached/standby, §20) + commit capacity
     b. materialize the destination from the epoch's durable prefix in S3 (11.2).
        *(Implemented per ADR-0008: from the durable prefix, not from a source-taken
        snapshot — the doc's "snapshot + restore" needs a live, cooperating source and
        the CP↔Agent RPC of Phases 02/03. Same guarantee, works on a dead host.)*
     c. FENCING_WAIT → Promoter.Promote: epoch N+1 to the destination + epoch-object
        CAS + recovery-point (§12.3–12.5). The source is fenced before the destination
        writes: never two writers (INV-10).
     d. detach the source, release its committed capacity, update progress.
4. host DRAINED (state stays DRAINING until an operator returns it to ACTIVE).
```

Cancellation is honored only **at a volume boundary** — never between promote and
detach — so a canceled drain leaves every volume with exactly one writer. Resume after
a CP failover re-reads the operation and skips volumes already moved (idempotent, no
volume moves twice).

**Tests first / DST.** `scenarioDrainMovesVolumesFenced`: two volumes on host A, drain →
both end on B with a bumped epoch; `NoLostAckedWriteChecker` — the destination's
recovered prefix ≥ what A ACKed as durable; `SingleWriterChecker` — the source's post-
move publish attempt fails (`ErrEpochChanged` / `ErrCASConflict`); `PromotionWaitChecker`
— no early grant. Plus: interrupt the drain mid-way and resume ⇒ completes without
re-moving; cancel mid-way ⇒ moved volumes are consistent and the rest stay on the source;
drain refuses a destination without capacity (`ErrNoCapacity`) instead of over-committing.

**Gate.** Standard + the scenario in the mandatory `task dst` set + **human review**.
Extends INV-09/10/11.

---

## Phase 11 exit gate

- [x] Placement honors the §20 order and the declared oversubscription policy; cordoned/
      draining/dead hosts are never chosen; `host_nvme_committed_ratio` exported.
- [x] Capacity commit/release is term-guarded and cannot go negative.
- [x] Cross-host materialization uses S3 only (no host-to-host path) and refuses to
      produce a partial volume (missing object / gap / digest mismatch ⇒ hard error).
- [x] Materialization runs in the background I/O class and yields (INV-17).
- [x] Drain moves every volume with the full fencing protocol: no double writer, no lost
      ACKed write, no early grant; it is idempotent, resumable, and cancelable only at a
      volume boundary.
- [x] Both new DST scenarios in the mandatory set; `task ci` + `task cover` (≥ 90%) +
      `task test:integration` green; `DEVIATIONS.md` clean.
