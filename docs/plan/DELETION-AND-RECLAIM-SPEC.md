# DELETION-AND-RECLAIM-SPEC — the first thing in this repository that removes an object

## DECIDED — 2026-08-07, human owner

**"Deleting a volume means you cannot create or start a VM from it: the data is not there any
more. A soft delete for a couple of days to allow recovery, and after that it does not exist."**

And, from the same sitting, the answer §9 said had to come first: **`chain_depth` is a
structure** (CHUNK-ADDRESSING-SPEC, "DECIDED"). That answer changes this spec's §9, so read
the two together.

### 1. Soft delete is the bucket's job, not ours — and that is the whole design

`real.NewS3Store` refuses a bucket without versioning. So a delete is already a **reversible
marker**: the object stops being readable, its previous version is still there, and what
removes it permanently is the bucket's own lifecycle policy after N days. "A couple of days of
recovery, then gone" is therefore *a lifecycle rule on the bucket*, not code in this
repository — and this is the one place where the right implementation is to write no code and
say so loudly.

That is exactly INV-14's shape — *"GC only marks; permanent removal is the bucket lifecycle,
and GC credentials lack permanent delete"* — which is `pending` only because its subject went
with `internal/gc`. This decision brings the invariant back with the sweeper, and the checker
it needs is the one that already existed: **nothing this repository runs may issue a permanent
delete.** Structural, not aspirational: the `objectstore.Store` interface has no permanent
delete to call.

**What the deployment therefore owes**, and it belongs in the runbook rather than in code: the
lifecycle rule, its retention window, and the fact that shortening it shortens the only
recovery path a deleted volume has.

### 2. What "the data is not there" means for the catalog, precisely

The volume's row does not linger in a DELETED state waiting for the lifecycle to fire. The
delete marks the objects and removes the rows; a recovery within the window is a
**`-rebuild-metadata` from the bucket's live versions**, which is a path that already exists
and is already tested (INV-20). That is why this decision is cheap: the undo is not a new
mechanism, it is the one built for losing the database.

*Rejected: a DELETED lifecycle state with a timer.* It puts the retention window in two places
— the bucket's policy and a column — and they drift; the bucket's is the one that actually
controls the bytes.

### 3. §9's question is answered by the chain-depth decision, and the answer is B

§9 offered **A** (refuse to delete a parent that has descendants) and **B** (flatten the
descendants first), and said A was nearly free *if* chunks stayed addressed per volume,
because publishing already flattened. **They do not stay per volume.** Under the lineage
prefix a clone never becomes independent by publishing, so A stops being a temporary refusal
and becomes a permanent one — "you can never delete this volume" — which contradicts the
decision above.

**So: B. A delete of a volume with descendants flattens them first**, and FLATTEN is the
operator-run one-shot the chunk decision already obliges. The hole §9 found in A —
`parent_snapshot_id` is write-once by construction, so "stop the clone and retry" never clears
the link — becomes FLATTEN's job to clear, deliberately, as the last step of making a clone
self-contained. That is a write nothing performs today and it is part of the increment.

### 4. What this does not decide

Whether a *snapshot* can be deleted independently of its volume, and what that means for a
clone that descends from it. §3's reachability facts are what settle it and they are already
in this document; nobody has been asked. It is smaller than the above and it can wait for the
increment that needs it.


**★ HUMAN-REVIEW ZONE.** Nothing here has an implementation yet, and that is the point:
five review-zone increments in this repository landed their spec in the same commit as
their code, and `PARALLEL-PLAN.md`'s "Review zones needing a human before implementation"
says do not make it eight. Read this, decide the question at the end, then implement.

**The one thing it makes true:** a volume an operator deletes stops costing storage and
stops answering reads, nothing that still depends on its bytes can be broken by it, and
every step is reversible until the bucket's lifecycle expires the version.

**On citations.** Every claim below names the file and the symbol, and every line number was
re-checked against `26a9525` after four other lanes landed during the writing of this spec.
**Two claims in an earlier draft did not survive that re-check** — both about machinery
`c81d83d` ("the operations table is retired, and ADR-0017's second capacity term with it")
removed, and both are corrected in place with a note saying so (§1, §4). That is the whole
argument for re-verifying rather than carrying a draft forward: the two facts that changed
were the two this spec was leaning on hardest. Where the symbol is enough, the symbol is what
to trust; every probe output quoted below was re-run against the tree, not copied.

---

## 1. What is broken: nothing deletes, and the vocabulary half-pretends otherwise

Verified, not assumed:

- **No production caller issues a delete.**
  `grep -rln '\.Delete(ctx\|\.Restore(ctx' --include=*.go internal/ cmd/ integration/ |
  grep -v '^internal/simio/'` — the exclusion is the implementations' own tests, which of
  course exercise their own verbs — reaches exactly two files, and both are tests:
  `internal/wal/concurrency_test.go` (a wrapper that forwards to an inner store) and
  `integration/backend/s3store_test.go` (the §6.1 conformance lane). The interface has both
  verbs —
  `objectstore.Store.Delete` / `.Restore`, `internal/simio/objectstore/objectstore.go:74,84`
  — and no code outside a test calls either.
- **`metadata.Store` has no delete verb at all.** `grep -c Delete internal/metadata/metadata.go`
  returns `0`. A volume row, once created, is permanent.
- **`lifecycle.VolumeState` has no DELETING.** Six states —
  `VolumeActive`, `VolumePrimarySuspected`, `VolumeFencingWait`, `VolumeRecoveryRequired`,
  `VolumeRecovering`, `VolumeDetached` — and `schema.sql`'s `volumes.state` CHECK lists the
  same six (`schema.sql:133-135`).
  *(`internal/lifecycle/lifecycle.go` is cited by symbol only, deliberately: a lane removing
  `AgentVolumeState` moved every line below it by 61 while this spec was being written, so a
  number here would have been wrong before it was read.)*
- **`lifecycle.SnapshotState` *does* have DELETING, and it is terminal and unreachable.**
  In `snapshotMachine`: `SnapshotPublished → SnapshotDeleting`, `SnapshotFailed →
  SnapshotDeleting`, and `SnapshotDeleting: {}`. The only production writers of snapshot
  state are `controlplane.Snapshot` (→ CREATING, `snapshot.go:47`), `RebuildMetadata`
  (→ PUBLISHED, `rebuild.go:203`) and `metadata.PublishSnapshot`; none of them can write
  DELETING. Outside `lifecycle.go` itself, `SnapshotDeleting` is written in exactly one
  place — `internal/metadata/metadatatest/contract.go` (`:806`, `:972`), the shared
  contract suite, which is a test in every sense but its filename. What *reads* it is real:
  `lifecycle.Unfinished` counts DELETING, `lifecycle.UnfinishedSnapshotStateNames` turns
  that set into the SQL guard, `pg.ListUnfinishedSnapshots` passes it
  (`pg.go:778`), and `cmd/control-plane -fleet-status` prints the result
  (`cmd/control-plane/fleet.go:135`). So the fleet report has a column for a state no
  production path can produce.
- **There is no longer a vocabulary to hang this on, and an earlier draft of this spec was
  wrong about it.** `lifecycle.OperationKind` did declare `OpGC` and `OpFlatten`; both went
  with the `operations` table in `c81d83d`, and what stands in their place is
  a comment saying so — *"There are no reconciliation operations here any more. §7's
  OperationKind (attach|detach|clone|resize|drain|recovery|flatten|gc) and OperationPhase
  … both went with it"* (`lifecycle.go`, the block below `SnapshotState.PredecessorNames`), matched by `schema.sql`'s head: *"There is
  no `operations` table"* (`schema.sql:6-12`). **This makes the design below cheaper and
  §9's option B dearer**, and both places say so: a delete has no half-built machinery to
  adopt (decision 3), and a background FLATTEN is now new machinery rather than a declared
  name waiting for an implementation (§9).
- **INV-14 is `pending`** (`INVARIANTS.md`), and no DST checker mentions a delete:
  `internal/dst/checkers.go` declares five — `MonotonicClockChecker`,
  `WatermarkOrderChecker`, `NoPlaintextLeavesHostChecker`, `SingleWriterChecker`,
  `TruncateBelowPublishedChecker`. `NoPermanentDeleteChecker`, which ADR-0012's "test that
  enforces it" section names, is not among them; it went with `internal/gc`.
- **`cmd/control-plane` has no delete flag.** Its verbs are `-seed-volume`,
  `-snapshot-volume`, `-clone-snapshot`, `-rebuild-metadata`, `-fleet-status`,
  `-detach-volume`, `-attach-volume`.

So the state is: a half-declared vocabulary, a fully-built reversible-delete surface on the
object store with no caller, and a bucket that only ever grows.

## 2. What is in the bucket, per volume

Every object any production path writes. There are five in the tree
(`grep -rn '\.Put(ctx' --include=*.go internal/ cmd/ | grep -v simio/ | grep -v _test.go`),
and four of them are per-volume:

| key | writer | write mode | what it is |
|---|---|---|---|
| `volumes/<vol>/descriptor.json` | `descriptor.Write` (`descriptor.go:66`) | plain PUT, overwrites | geometry, epoch, wrapped DEK + its version, chain depth, parent link. Digest-framed (`frame`/`unframe`). |
| `image/<vol>/manifest.json` | `image.Publish` (`image.go:193`) | CAS on ETag (`IfMatch`), create-only when the writer holds none | the volume's live image: offset/length/digest per chunk, plus the frozen sequence |
| `image/<vol>/snapshots/<snap>.json` | `image.PublishSnapshot` (`image.go:151`) | create-only (`IfNoneMatch`) | a frozen point, same shape |
| `image/<vol>/chunks/<sha256-of-plaintext>` | `image.uploadChunks` (`image.go:239`) | create-only | the bytes, sealed `<nonce:12><ct><tag:16>` |

The fifth is `controlplane.TermClaimKey` (`election.go:104`, `cp` term claims), which is
fleet state, not a volume's, and is out of scope here.

**And on the host:** `<data-dir>/wal/<volume-id>/<epoch>/` (`wal.SegmentDir`), which
`Volume.release` deliberately does not remove — "Nothing here deletes a segment: the
records stay on disk exactly as they were" (`internal/agent/volume.go:171`, above
`release` at `:174`). That is
what makes ADR-0024's "restart and it republishes" true, and it means a deleted volume
leaves local NVMe occupied. See decision 6.

## 3. Reachability — three facts, each reproduced, and they decide the shape

I ran these against the real packages through a `go test -overlay` probe (the probe file
lives in `/tmp`; `git status internal/image/` is clean after the run), so no file entered
the working tree. The outputs are quoted verbatim because a claim about sharing is exactly
the kind this project has been wrong about from reading alone. **A reviewer can re-run
them:** the probe is four tests over `internal/image` + `internal/cow` with a
`sim.NewObjectStore`, and each one is three calls long.

### 3.1 A snapshot shares the volume's own chunks — the same objects, not copies

`SnapshotKey` sits under the volume's prefix (`image.SnapshotKey`, `image.go:114-116`), and
`uploadChunks` is shared by `Publish` and `PublishSnapshot` (`image.go:143,181`), so a
snapshot's manifest names chunk keys the volume's manifest already named. Publishing a 1 MiB
volume and then snapshotting the same view:

```
chunks before snapshot=1 after=1; every object under the volume prefix:
  image/00000000-0000-7000-8000-000000000000/chunks/4e29ad18ab9f42d7…cea1eb56
  image/00000000-0000-7000-8000-000000000000/manifest.json
  image/00000000-0000-7000-8000-000000000000/snapshots/snap-a.json
```

The snapshot added **one object, the manifest**, and no chunk: `uploadChunks` HEADs
`chunkKey(volumeID, digest)` and skips what is already there (`image.go:225-226`), and the
key it computes is under the same volume prefix either way.

**Consequence:** "delete the volume's image" can never mean "delete the chunks the volume's
manifest names" while a snapshot of that volume survives. Reclaim is a set difference over
that volume's manifests, never a walk of one manifest.

### 3.2 A chunk is *not* shared between a parent and its clones — but the clone reads the parent's objects anyway

`chunkKey` embeds the volume id (`image.go:106-108`), and `chunkAAD` binds it into the seal
(`image.go:369-371`), so the same plaintext under two volumes is two objects under two
prefixes with two ciphertexts. Copying volume A's chunk objects *and its manifest* verbatim
under volume B's prefix and loading B — the strongest form of "share the bytes" available
without a code change — fails on the seal, not on the key:

```
CONTROL loading volume A (same bytes, own identity):
  err = <nil>
loading volume B over a chunk sealed for volume A:
  err = image: chunk 6896d9ea3f73a443…c8a6f735daa41b1 at offset 0: crypto: authentication failed
```

**The control line is the point, not decoration.** The probe hand-builds B's manifest, so
"the load failed" is a result a merely bad manifest would also produce; the control shows the
same objects opening under A's own identity, which leaves the changed volume id as the only
difference. That is the same discipline CLAUDE.md demands of an assertion — a check that
cannot distinguish the case it names has proved nothing.

So there is no cross-volume chunk sharing to protect. **What there is instead is worse to
reason about: a cross-prefix *reference*.** A clone reads its parent's objects directly —
`agent.parentView` calls `image.LoadSnapshot` under the *parent's* volume id
(`internal/agent/volume.go:923-960`, the load at `:950`) with the DEK re-bound to the parent
(`parentEncryption`, `:965`), and `loadManifest` then GETs
`chunkKey(parentVolume, digest)` for every chunk (`image.go:308`).

That reference lasts until the clone publishes its own image, because publishing flattens
(CHUNK-ADDRESSING-SPEC §1, reproduced there). **Nothing in the bucket and nothing in the
catalog records whether a given clone has reached that point.** The catalog records the
links — `volumes.parent_snapshot_id` and `snapshots.parent_snapshot_id` (§4) — and nothing
about whether either is still load-bearing.

**Consequence, and it is the one that shapes decision 1:** the safe question a delete can
ask is not "is anyone still reading this?" (unanswerable) but "does anything descend from
this one?" (two indexed lookups, §4). The delete must refuse on the second question.

### 3.3 A clone shares its parent's DEK, so crypto-shred is not available

`controlplane.Clone` copies `DEKWrapped`, `KEKID` and `DEKKeyID` from the parent
(`internal/controlplane/clone.go:92-98`), and `wal.NewEncryption` keeps that DEK and only
swaps the volume id it binds (`internal/wal/crypto.go:41-46`). So a whole lineage shares one
key. The design document's invariant 12 — *"borrado de volumen = crypto-shred"*
(`arquitectura_mvp_volumenes_remotos_v5.md:1348`) — cannot be honoured for a volume that has
clones without destroying the clones' own chunks too.

It is not available for a volume with *no* clones either, today: `crypto.KMS` declares
`WrapDEK`, `UnwrapDEK` and `KEKID` and no destroy verb of any kind
(`internal/crypto/kms.go:16-23`), and the wrapped DEK exists in at least three places — the descriptor in the bucket, the `volumes` row, and any
backup of either. See decision 7.

## 4. What the catalog knows, and the two things it already gets right

- **Dependency is indexed — but it is *two* columns, not one, and missing the second is how
  a delete would orphan a snapshot chain.** `volumes.parent_snapshot_id REFERENCES
  snapshots(snapshot_id)` (added by ALTER at `schema.sql:254-256`, because the pair is
  circular) with `volumes_parent_snapshot_id_idx` on the referencing column — **and
  `snapshots.parent_snapshot_id UUID REFERENCES snapshots(snapshot_id)`
  (`schema.sql:207`) with `snapshots_parent_snapshot_id_idx` (`:251`)**. A snapshot can
  descend from a snapshot, so "does anything descend from this?" is two indexed lookups.
  The schema comment says why the index is there in exactly these words: *"deleting a
  snapshot would take a full scan of volumes to check the constraint"*. Somebody already
  thought about this; they thought about it for one direction.
- **The catalog cannot answer it today.** `metadata.Store` has no "list what descends from
  this snapshot" verb — `ListVolumes` and `ListVolumesByHost` are the whole listing surface
  (`metadata.go:435-453`) — so decision 1 is *a query to add*, not a query to call. It is
  one `sqlc` query per direction against an index that already exists, which is the cheapest
  new surface in this spec and worth naming rather than assuming.
- **Capacity releases itself, and it now does so more simply than an earlier draft of this
  spec claimed.** `host_committed_bytes` is a view, not a ledger (ADR-0017), and it is
  exactly `Σ size_bytes of volumes whose primary_host_id is the host` (`schema.sql:311-316`)
  — the second term for in-flight plans **went with the `operations` table in `c81d83d`**, and the schema
  says why in a way this spec should not paraphrase: *"Removing it does not change a single
  number this view has ever produced"* (`schema.sql:281-291`). A volume with no primary host
  charges nobody. `-detach-volume` is exactly `SetVolumePrimaryHost(term, id, "")`
  (`cmd/control-plane/main.go:265`), so **detach already does the capacity half of a delete**
  and a delete must not re-invent it.
- **`rebuild-metadata` is the inverse map.** `RebuildMetadata` (`rebuild.go:60`) lists
  `volumes/` for descriptors (`listDescriptors`, `:126`) and `image/<vol>/snapshots/` per
  volume (`rebuildSnapshots`, `:166`), and treats a snapshot manifest's existence *as* its PUBLISHED state,
  because it is create-only and never changes. So: **the descriptor is what makes a volume
  exist to a restore, and a snapshot manifest is what makes a snapshot exist.** That gives
  the delete order for free (decision 2).

## 5. Which of ADR-0012 survives ADR-0026

ADR-0012 is withdrawn, and its own banner already says what it thinks survives: the
construction-time check. Having read both, more of it survives than that, and one large part
of it has no subject at all.

**Survives, and should be carried into this design:**

- **The structural claim, enforced at construction.** `real.NewS3Store` (`s3.go:117`) calls
  `requireVersioning` (`:78`, called at `:141`) and fails closed on a bucket it cannot
  verify (`TestRequireVersioning`, which `INVARIANTS.md`'s INV-14 row already names as the
  surviving half). So every `Delete` is a delete marker and `Restore` reverses it — which the
  interface states as a requirement on every implementation rather than a property of one:
  *"Every implementation keeps the bytes: permanent removal belongs to the bucket lifecycle,
  and this interface deliberately cannot reach it"*
  (`internal/simio/objectstore/objectstore.go:69-74`). This is the whole safety argument of
  decision 5.
- **The asymmetry, in its own words:** *"an object collected one cycle late costs storage, an
  object collected one cycle early costs data."* Every tie in this spec is broken that way.
- **The refusal to create a second source of truth for "is this published".** ADR-0012
  rejected a snapshot index because the manifest *is* the evidence and `rebuild-metadata`
  depends on it. That argument is stronger now, not weaker: `RebuildMetadata`'s comment says
  the same thing in the same words. A delete must therefore not maintain a side index of
  what it has removed.
- **The revisit trigger.** *"An anchor outside every durable prefix — which is what
  compaction/objectization (Phase 12) will create"*. Still the right trigger, still not now.

**Has no subject, and saying so is most of the value of re-reading it:**

- **The epoch-ceiling rule** (decision 2 of ADR-0012) — `addDurablePrefixes`,
  `recovery.EpochCeiling`, `recovery.DurablePoint`. Those packages went with the fencing
  chain. There is no durable prefix and no epoch ceiling to be a licence for anything.
- **The whole "reachability is computed from a LIST" premise**, and with it reproduction (3),
  the strongly-consistent-LIST precondition *for the sweep*, and the deferred per-sweep
  freshness probe. Under ADR-0026 a volume's reachable set is named by manifests read at
  deterministic keys. **Deletion here is not mark-and-sweep. There is no sweep.** Strongly
  consistent LIST is still a precondition of `rebuild-metadata` and `listDescriptors` — that
  is untouched — but nothing destructive depends on a listing, which is the single biggest
  thing ADR-0026 changed about this subject.

## 6. The decisions

### 1. A delete refuses while anything descends from the volume, and requires it detached

Two preconditions, both answered by the catalog:

- **Detached** — `primary_host_id IS NULL`. Not "ask the host": a host learns it has lost a
  volume on its next poll (`controlplane.Place`'s comment on `ErrAlreadyPlaced` says exactly
  this), so the only fact available at delete time is the one the catalog holds. It also
  means capacity is already released before the first object is touched.
- **No descendants** — no volume whose `parent_snapshot_id` is a snapshot of this volume,
  **and no snapshot whose `parent_snapshot_id` is one either** (§4: two indexed lookups, and
  the second is the one an implementation will forget, because `volumes.parent_snapshot_id`
  is the link everything else in the tree talks about).

*Rejected: reference-counting chunks.* It would be a second source of truth for reachability,
which is the thing ADR-0012 argued hardest against, and under per-volume chunk keys (§3.2) it
would count to one everywhere.

*Rejected: "delete anyway; the clone fails closed."* It does fail closed —
`fetchBase` sets `baseFailed` and calls `log.FailBase`, so reads error rather than answering
zeros (`internal/agent/volume.go:841-846`, the `parentView` error path). But a guest whose disk stops answering is an
outage, and producing one from an admin verb is not made acceptable by it being loud.

### 2. The order is the exact inverse of publish, and that is what makes a half-done delete safe

Publish is *chunks, then manifest*, so that a manifest that exists always resolves
(`image.Publish`'s doc comment). Delete is therefore:

```
descriptor.json  →  snapshots/*.json  →  manifest.json  →  chunks/*
```

- **Descriptor first**, because it is what makes the volume exist to `rebuild-metadata`
  (`listDescriptors`). A rebuild that runs mid-delete must not resurrect a volume whose
  manifest is already gone; with the descriptor gone first, the rebuild simply does not see
  it. *Rejected: descriptor last* — a rebuild in the window brings the volume back ACTIVE,
  unplaced, and unreadable, which is a catalog row an operator has no way to interpret.
- **Chunks last**, because the reverse leaves a live manifest naming missing bytes — which
  `loadManifest` correctly refuses (`image.go:307-311`), turning a delete-in-progress into a
  volume that reports corruption. Unreferenced chunks cost storage; a manifest with holes
  costs an incident.

Every step treats `ErrNotFound` as success, which is what makes the whole thing re-runnable
(the interface says `Delete` on a key that is already marked or was never there is
`ErrNotFound`, `objectstore.go:69-73`; `real.S3Store.Delete` HEADs first — *"S3 answers 204
for a missing key; the interface contract (and the sim) report ErrNotFound, so keep the two
implementations identical"*, `s3.go:275-280` — so the real backend and the sim agree on it,
which is what lets the idempotence observable run in either lane).

### 3. The catalog row is not removed; it becomes the resume record

Add `DELETING` to `lifecycle.VolumeState` (and to the `volumes.state` CHECK, which is the
schema's third enforcement layer), reachable from `DETACHED` only, terminal.

The row is the only thing that can name a partially deleted volume's remaining keys, because
its descriptor is gone by then and there is no sweeper to find them by listing. Re-running the
delete walks the same deterministic key set and finds most of it already marked.

*Rejected: DELETE the row.* A crash between the descriptor and the chunks then leaves objects
nothing names — re-creating, by hand, exactly the orphan class ADR-0012's sweep existed to
collect.

*Rejected: an `operations` row of kind `gc`.* This was the obvious home while the table
existed, and it is **not available any more**: the table and `OpGC` are gone (§1), and
`schema.sql`'s head gives the reason a delete must not bring them back — *"an empty table
with three indexes, a kind vocabulary and a term-guarded writer reads as a mechanism
somebody is about to use, and the next reader has no way to tell"*. It should stay rejected
on its own merits even if someone restores the table for another reason: this work has no
host, is driven by the Control Plane against the bucket, and its only progress state is
"which keys are gone", which is re-derivable from the bucket itself. An operation row would
be progress state that can disagree with the truth.

### 4. Deleting a snapshot reclaims what no other manifest of that volume names

Read `manifest.json` and every remaining `snapshots/*.json` of the volume, union their
digests, and delete the chunks the doomed snapshot names that are not in that union. Then
delete its manifest. Bounded by one volume's manifests; no listing of the chunk space; and
idempotent, because a re-run computes the same union.

Refuse if the snapshot is any volume's `parent_snapshot_id` **or any snapshot's** — the same
pair of queries as decision 1 (§4), and either FK would in any case make the row
un-removable. Which is worth stating the other way round, because it is the safety net: the
database refuses the catalog half regardless. What the database cannot refuse is the
*object* half — nothing stops a delete issuing `Delete` on a chunk a surviving manifest
names — so the query is what protects the bucket, and the FK only protects the row.

*Rejected: delete the manifest and leave the chunks.* Cheap, safe, and it reclaims nothing —
which makes snapshot deletion pointless for the workload §2 is built around (frequent
snapshots of a churning volume).

### 5. The delete marker is the safety argument, and the restore order is the delete order reversed

Because the bucket is versioned by construction (§5), the runbook for "we deleted the wrong
volume" is `Restore` over the same deterministic key set — and it must run in the **inverse
of the delete order**: `chunks → manifest.json → snapshots/*.json → descriptor.json`. That
way the volume becomes visible to a reader and to `rebuild-metadata` only once its bytes are
already back.

Two honest limits, both worth writing into the runbook:

- **It is reversible only until the bucket lifecycle expires the non-current version.** That
  window is a bucket policy, not a constant in this repository (§5.11 of the design document:
  *"El borrado real lo ejecuta el lifecycle del bucket sobre versiones no-actuales tras el
  grace period"*). A runbook that does not state the deployment's number is not a runbook.
- **`ErrRestoreSuperseded` on a chunk is success, not failure.** `real.S3Store.Restore`
  refuses when the key was written again after being marked, because for a mutable object the
  marked version is no longer what a restore would surface. A chunk is content-addressed: a
  re-published chunk is a *different ciphertext of the same plaintext* and opens fine. So the
  restore path must treat that error as satisfied for `chunks/*` and as fatal for
  `manifest.json`. Getting this backwards is how a restore reports success and hands back a
  volume built from something else.

### 6. Local NVMe reclaim is the Agent's, and it is explicitly not in this increment

A deleted volume leaves `<data-dir>/wal/<vol>/<epoch>/` on whatever host last served it
(`wal.SegmentDir`, `internal/wal/segment.go:66`; `Volume.release` removes nothing,
`volume.go:174`).

It must stay separate, and the reason is sharp: from the Agent's side, "this volume is no
longer in my desired state" is *also* what a detach looks like, and a detached volume's
segments are exactly what ADR-0024 re-attaches to. An Agent that deletes a directory on
absence-from-desired-state destroys the unpublished session of every volume that was merely
moved. Local reclaim needs the delete to be *committed and observable to the Agent* first,
which is a different increment with a different failure mode.

### 7. Crypto-shred is not V1, and it is a divergence to record rather than to close

Design invariant 12 says volume deletion is crypto-shred. §3.3 shows why it cannot be
implemented here: a shared lineage DEK, no destroy verb in `crypto.KMS`, and the wrapped DEK
living in the bucket, the catalog and any backup of either. Deleting the descriptor removes
one copy of the wrapped key and no copy of the KEK.

**This needs a DEV entry in `STATUS.md`** naming the divergence and its three causes. This
spec cannot write one — `STATUS.md` belongs to the integration owner and track A — so it is
listed here as owed.

## 7. The observables

Outside-observable, with the plant that proves each can fail.

1. **The bucket empties, and a rebuild does not bring it back** (`integration/e2e`). A guest
   writes, the Agent stops, the image is in the bucket. `control-plane -delete-volume` runs.
   Every key under `image/<vol>/` and `volumes/<vol>/descriptor.json` answers `ErrNotFound` to
   a fresh `Get`, and `-rebuild-metadata` against that bucket produces a summary with that
   volume absent.
   *Plant:* skip the descriptor delete → the rebuild resurrects the volume, red.
2. **The refusal costs nothing.** A snapshot exists and a clone of it exists. `-delete-volume`
   on the parent exits non-zero, names the clone, and **not one object is gone** — assert the
   parent's `manifest.json` still `Get`s.
   *Plant:* drop the descendant query → the clone's Agent logs *"the clone's parent snapshot
   could not be materialized; its reads will fail"* and its device stops answering. That plant
   is the outage the check exists to prevent, so it belongs in the test.
3. **Twice is once.** Run the delete twice. The second exits 0, and a `List` of the bucket is
   identical to after the first.
   *Plant:* treat `ErrNotFound` as a failure → the second run exits non-zero.
4. **Reversibility, against a real versioned bucket** (`integration/backend`, the only lane
   with one). Delete, then restore the recorded key set, then `image.Load` returns the same
   bytes the guest wrote.
   *Plant:* restore in the delete order rather than its inverse, and watch a window in which
   `Load` fails on a missing chunk — which proves the order is load-bearing rather than
   decorative.
5. **Snapshot deletion reclaims only the exclusive chunks.** A volume with two snapshots
   sharing a chunk; delete one; the shared chunk still `Get`s, the exclusive one does not.
   *Plant:* delete every digest the doomed manifest names → the surviving snapshot fails to
   load, red.
6. **Capacity** (`internal/metadata/pg` integration lane). After the delete,
   `host_committed_bytes` for that host no longer counts the volume — which it already will
   not, because detach is a precondition; the test exists to pin that the delete did not
   *re-place* it.

## 8. What this deliberately does not do

- **No sweeper, and no listing-derived reachability.** Two orphan classes survive: the chunks
  of a publish that ran out of grace (SHUTDOWN-PUBLISH-SPEC records this in its own "does not
  do" section) and the chunks of a delete that crashed after the descriptor. Both cost storage
  and neither costs correctness. INV-14 stays `pending`: a checker for "the sweep did not
  destroy live data" has no sweep to check. What *can* be activated instead is narrower and
  honest — **a delete only ever issues keys named by a manifest it read in this run** — and
  that is a property a DST checker can hold, with a planted bug that deletes a key derived
  from a prefix instead.
- **No permanent delete.** The interface has none on purpose (`objectstore.go:69-73`), and
  this spec does not add one. Expiry is the bucket's lifecycle policy.
- **No Object Lock.** The design document puts governance-mode Object Lock as a gate before
  the first real production data (`arquitectura…v5.md:33,190`). Nothing here needs it, and
  adding it now makes every test bucket a special case.
- **No format change.** Not one byte of the manifest, the descriptor or the chunk sealing
  moves. A delete that needed a format change would be two reviews, not one.
- **No cascade.** Deleting a volume never deletes its clones and never rewrites another
  volume's objects.

## 9. ~~The question for review~~ — answered 2026-08-07: **B, flatten first**

See "DECIDED" at the top, and CHUNK-ADDRESSING-SPEC's decision, which is what forces B. The
original framing follows because it is what the answer was chosen against — in particular the
hole it found in A, which is now FLATTEN's responsibility.

**Must a clone be independent of its parent before the parent can be deleted, or should the
delete make it independent?**

- **A — refuse.** Decision 1 as written: two indexed lookups, no data movement, and the
  operator's escape is to stop the clone and retry, because a clone's first publish already
  copies everything it inherited (verified in CHUNK-ADDRESSING-SPEC §1). The cost is an admin
  verb whose success depends on somebody else's VM being restartable: *"you cannot delete this
  volume until that guest reboots"*.

  **A has a hole, and the reviewer should decide it rather than discover it.** The refusal is
  on `parent_snapshot_id`, and **nothing can clear that column — the catalog is built so that
  nothing can.** The upsert is
  `parent_snapshot_id = COALESCE(volumes.parent_snapshot_id, EXCLUDED.parent_snapshot_id)`
  (`internal/db/queries/volumes.sql:65`), which is write-once by construction: once set, no
  existing write path changes it or nulls it. Nor would a rebuild launder it —
  `volumeFromDescriptor` reads the link back out of the descriptor
  (`rebuild.go:115`, `descriptor.ParentSnapshotID` at `descriptor.go:39`) and writes it in
  again, which is correct, because the bucket is the authority. So "stop the
  clone and retry" does *not* work as written: the clone becomes independent in the bucket
  and stays a descendant in the catalog, forever. A is only a temporary refusal if something
  clears the link when a clone publishes a self-contained image — which is a write nothing
  performs, on a fact §3.2 shows nothing records. Two ways out, both small, both decisions:
  clear `parent_snapshot_id` (and the descriptor's) as part of the clone's first successful
  publish, which means giving up the `COALESCE` write-once guard and saying why; or leave the
  link alone and make the delete's precondition "descends from this **and** has no image of
  its own", which is one `Head` on `image/<clone>/manifest.json` and no schema change at all.
  **The second is cheaper and the first is more honest** — the first makes the catalog stop
  claiming a dependency that is over, the second leaves the catalog wrong and reads around
  it. This is exactly the kind of thing that is free now and a migration later.
  *(Related, and it is why nobody has hit this: a clone that has published cannot be
  restarted at all today — CHUNK-ADDRESSING-SPEC §2, reproduced there. The state in which A's
  escape hatch would be used is a state the system currently cannot reach.)*
- **B — flatten first.** The delete schedules a FLATTEN for each dependent clone and
  completes when they are independent. Correct under every addressing scheme. **It got more
  expensive while this spec was being written:** `lifecycle.OpFlatten` and the `operations`
  table it was a kind of are both gone in `c81d83d` (§1), so B is no longer "fill in an implementation
  behind a name that already exists" — it is a background operation kind, its row, its term
  guard and its reconciliation, reintroduced for one admin verb. On top of which a delete
  becomes unbounded background data movement on hosts serving other people's guests, and the
  io-class scheduler that would have bounded it was deleted by ADR-0026 increment 4.5.

**These two specs meet here, and the order of decisions matters.** If chunks stay addressed
per volume, A is nearly free, because publishing already flattens. If chunk addressing moves
to a lineage prefix (CHUNK-ADDRESSING-SPEC, option 2), a clone never becomes independent by
publishing and A becomes a permanent refusal — at which point B is the only answer and the
FLATTEN operation stops being optional. **Answer CHUNK-ADDRESSING-SPEC's question first, or
answer both in the same sitting.**
