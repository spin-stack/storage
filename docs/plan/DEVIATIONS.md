# DEVIATIONS

Doc↔code divergences detected by the Doc-sync agent (or anyone). Each entry has a
**mandatory resolution**: either the code is corrected, or an ADR is written (and, if
warranted, a patch to the architecture document is proposed). **Open entries block the
gate** (PLAN §2).

## Format

```
### DEV-NNNN — <short title>
- Detected: <date> by <role/agent> in <increment>
- Doc section(s): §X.Y
- Divergence: <what the code does vs what the doc says>
- Severity: low | medium | high (data-loss zones = high)
- Resolution: <fix | ADR-XXXX> — <status: open | resolved>
```

## Open

### DEV-0007 — Several phases marked done are partial models
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §19, §20, §21.1, §22.3
- Divergence: snapshot sealing is synchronous, not a background lifecycle, and `local`
  mode does not upload asynchronously; clone persists no parent/read-chain link;
  objectization publishes no segment objects and no manifest→epoch→PG sequence, and a
  checkpoint digests only a sequence plus key strings; cross-host materialization
  returns an in-memory view that the caller discards — nothing is persisted on the
  destination host.
- Severity: medium (feature completeness; the invariants they claim are narrower than
  documented)
- Resolution: reopen the affected phases as integration work once the Agent/API spine
  exists (REBASELINE.md, step 5–6). **Status: open.**

## Resolved

### DEV-0010 — Observability is registered but never recorded
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §26.1, §26.2
- Divergence: the §26.2 catalog is registered at startup and no production code path
  records a counter, histogram, or gauge; only an in-memory test provider exists. Phase
  documents state that metrics are flowing.
- Severity: medium (no operational visibility; every RTO/RPO claim is unmeasured)
- Resolution: record the metrics from the code paths that own them and ship an
  exporter, before any measured-time claim. **Status: open.**
- **Resolved 2026-07-25** by `fc02579`: `obs.Recorder` records the §26.2 metrics from the paths that own them (WAL watermarks/RPO and self-fencing, GC marking, materialized bytes), with tests that collect from the SDK to prove a series carries data rather than merely existing. Wiring the remaining metrics follows the code that produces them.


### DEV-0009 — rebuild-metadata rebuilds volumes only
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §22.5
- Divergence: `RebuildMetadata` recreates volume rows; descriptors carry no snapshot
  lineage, hosts, or operations, so a total PG loss is not recoverable to the documented
  state. INV-20 is recorded as "active (basic)" and must not be read as the §22.5
  guarantee.
- Severity: medium
- Resolution: extend the descriptor + rebuild to the full catalog, or narrow the
  documented claim. **Status: open.**
- **Resolved 2026-07-25** by `2b09d1e`: the rebuild also reconstructs the snapshot catalog from the manifests (refusing any that fails its own digest) and returns what it could *not* rebuild — hosts, leases, operations — instead of a count that reads as complete.


### DEV-0008 — Drain is not idempotent across every crash boundary
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §28.1, §7
- Divergence: after a successful promotion, a failure while writing the recovery point
  or releasing the source's committed capacity leaves the volume assigned to the
  destination, so a resumed drain no longer lists it: the recovery point is never
  written and the source's capacity is leaked.
- Severity: high (fencing/durability zone)
- Resolution: fix — stage the move explicitly and make each step resumable, keyed on
  the operation rather than on the source's volume list. **Status: open.**
- **Resolved 2026-07-25** by `f9f5885`: the drain walks the plan recorded in the operation's desired_state, and progress carries the ids that are fully done, so a volume promoted before a crash is finished (boundary written, capacity released) instead of disappearing from the source's listing, and capacity is released exactly once.


### DEV-0006 — The object store exposes permanent deletion; GC does not mark
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §21.3, §5.11
- Divergence: `sim` and `real` (filesystem) implement `Delete` as an irreversible
  removal, while the interface documents that permanent deletion is not exposed
  (INV-14). The filesystem store's conditional PUT is check-then-write, not an atomic
  CAS. `gc.Collect` only computes candidate keys: no marking, no grace period, no
  versioned deletion.
- Severity: high (GC is a data-loss zone)
- Resolution: fix — reversible delete (delete markers) in the interface and both
  implementations, atomic conditional PUT, and a GC that marks with a grace period.
  **Status: open.**
- **Resolved 2026-07-25** by `cd17e0b`: `Delete` is a reversible delete marker in every implementation (sim keeps the bytes, the filesystem store writes a sidecar marker, S3 uses the backend's versioning), all expose `Restore`, and `gc.Mark` marks with a grace period instead of returning a list nobody acts on.


### DEV-0004 — Fencing is fail-open, and promotion is not atomic or resumable
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §12.2, §12.3–12.4, §14.8
- Divergence: `Log.Flush` self-fences only when a lease checker was installed
  (`l.lease != nil`), so a `remote`-mode log built without one ACKs FLUSH with no lease.
  `Promoter.Promote` performs PG epoch bump, S3 epoch CAS, and lease grant as three
  independently-failing steps with no idempotent resume.
- Severity: high (fencing; INV-06 claimed structural but is opt-in)
- Resolution: fix — make the lease mandatory for `remote` mode at construction time,
  and turn promotion into a resumable staged operation.
  **Status: partially resolved 2026-07-25** by `6d5655e`: a remote FLUSH with no lease
  checker now returns `wal.ErrNoLease` instead of ACKing, and `EnableRemote` takes the
  lease as a parameter so every call site must answer what fences that writer. The
  test that specified the vulnerability (`TestNoLeaseConfiguredStillAcks`: "flush
  without a lease gate should ACK") was removed with its reason recorded in place.
  **Still open:** promotion is three independently-failing steps with no idempotent
  resume.
- **Resolved 2026-07-25** by `6d5655e` (fail-closed lease) + `f9f5885` (staged promotion): a remote FLUSH without a lease checker returns `wal.ErrNoLease`, `EnableRemote` takes the lease explicitly, and `Promote` derives its target epoch from the state already out there (PG/S3/lease), so each step is idempotent and two runs grant one epoch. A multi-epoch gap is refused rather than resumed.


### DEV-0005 — Not every Control-Plane mutation is term-guarded
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §7
- Divergence: `RecordOperation` and `UpdateOperationPhase` have no
  `control_plane_leader` term predicate, so a zombie CP can write operation rows.
- Severity: medium (control plane consistency)
- Resolution: fix — add the predicate to both queries plus a structural test that
  enumerates mutating queries and fails on any without one. **Status: open.**
- **Resolved 2026-07-25** by `93b70aa`+`15cb1e2`: both operations queries carry the leader-term predicate, and a structural test enumerates every mutating query and fails on any without one (exemptions explicit).


### DEV-0003 — Recovery accepts an unvalidated WAL object as durable
- Detected: 2026-07-25 by human review (rebaseline)
- Doc section(s): §22.1, §14.2, §5.8
- Divergence: `recovery.listObjects` parses an object header and trusts it — no check
  of `PayloadSHA256`, `PayloadLength`, `RecordCount`, or that the object belongs to the
  volume/epoch under recovery. `DurablePrefix` derives the durable point from header
  `LastSequence` values without reading any record, so a truncated or corrupt object
  still raises the durable point. The prefix starts at the lowest object present rather
  than requiring sequence 1 or the previous epoch's recovery point as a floor.
- Severity: high (durability; undermines INV-08 and INV-09)
- Resolution: fix — validate every object against its header before it may contribute
  to the durable point; require an explicit prefix floor; DST scenarios for a truncated
  object at the prefix edge and a header that lies. **Status: open.**
- **Resolved 2026-07-25** by `6d5655e`: every stored object is validated against its own header (volume/epoch, payload length, SHA-256, replayed record count and first/last sequence, contiguity) before it may contribute to the durable point, and the contiguous run now starts at an explicit floor — sequence 1, or one past the epoch's recovery point (§12.5). Failing objects end the run instead of failing recovery. Tests: `internal/recovery/integrity_test.go`.


### DEV-0002 — Drain moves from the durable prefix, not from a snapshot
- Detected: 2026-07-25 by implementer agent in Increment 11.3
- Doc section(s): §28.1 (and §20, §22.1)
- Divergence: §28.1 describes drain as "snapshot + restore cross-host en el MVP". A
  snapshot is taken by the *source Agent* (§19) and so requires the evacuated host to be
  alive and cooperating, plus a CP↔Agent RPC that does not exist yet. The implementation
  evacuates from the volume's **durable prefix in S3** (§5.8/§22.1) instead.
- Severity: high (fencing / durability zone)
- Resolution: **ADR-0008** — the move is bulk-materialize → fence → final materialize →
  recovery point; the same durability guarantee, and it also works when the source host
  is dead or partitioned. The snapshot path remains for cross-host clones.
  **Status: resolved (pending human review of the fencing diff).**

### DEV-0001 — WAL header size: doc says 96 bytes, fields sum to 104
- Detected: 2026-07-25 by tech-lead agent in Increment 4.1
- Doc section(s): §14.1, §14.2
- Divergence: both header structs are documented as "96 bytes" but their enumerated
  fields total 104 bytes each. Every field is load-bearing (incl. crypto `AuthTag[16]`,
  `PayloadSHA256[32]`).
- Severity: high (on-disk format, data-loss zone)
- Resolution: **ADR-0005** — headers are 104 bytes, `HeaderLen`/`HeaderLength`=104, all
  fields kept, little-endian, CRC32C over the pre-CRC bytes. Proposed patch to the arch
  doc to correct the "96" erratum. **Status: resolved (pending human format review of
  the branch).**
