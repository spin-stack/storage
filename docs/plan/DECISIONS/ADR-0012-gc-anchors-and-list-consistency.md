# ADR-0012 — The GC gets no snapshot index; a listing-derived ceiling stops being a licence to destroy

- **Status:** Accepted 2026-07-25 for the half that narrows the sweep (implemented in
  this increment); the half that would add a new on-S3 object — a per-sweep
  LIST-freshness probe — is written up under "Deferred" and is **not** implemented,
  because it widens the GC's credentials and adds a key prefix, which is a review the
  operator side of this project has to make.
- **Date:** 2026-07-25
- **Deciders:** human (to decide), implementer agent (proposes)
- **Implements/Extends:** §21.1 (publication order), §21.3 (GC marks, never deletes),
  §12.5 (epoch boundaries), §5.8/§22.1 (S3 is the authority for recovery), §6.1
  (backend conformance), INV-08, INV-14, INV-16.
- **Addresses:** TEST-GAPS finding "A GC sweep cannot see an anchor its listing has
  not caught up to".

## Context

Reachability is computed from a LIST of the bucket. §21.1 publishes a snapshot's WAL
objects *before* the manifest that anchors them, so with an eventually consistent LIST
there is a window in which the manifest is GET-visible and LIST-invisible while its
objects are already listed and already past grace. The sweep then marks live data, and
no amount of re-listing inside `internal/gc` fixes it: both listings miss the same
anchor. The package doc has said so since wave 1; the finding asks for a root the
sweep can read **by deterministic key** instead — a snapshot catalog, or a snapshot
index in the volume descriptor.

Two facts bound the damage today and have to be kept in view so this is not
over-designed. The mark is reversible (INV-14: a delete marker on a versioned bucket,
with `Restore`), and `TestListSeesAFreshPut` in `integration/backend` certifies
strongly consistent LIST per backend and is blocking (§6.1). There is no incident
waiting to happen in production; there is a property that rests on the backend rather
than on the design.

Three things about the current sweep matter for what follows:

- **Only WAL objects can ever be marked.** `Reachable` marks every key that is not
  `wal/**/*.wal` reachable outright, so the entire exposure is WAL objects — whose key
  names their volume, epoch and sequence range.
- **The durable prefix is already a root.** `addDurablePrefixes` protects every WAL
  object up to each volume/epoch's durable point, computed from S3 alone. A manifest
  is therefore *not* normally the only thing holding its objects up: for a healthy
  volume the prefix covers everything the manifest names, and the two roots cover for
  each other.
- **The durable point the GC uses is the *adopted* one** (`recovery.DurablePoint`),
  i.e. the epoch's contiguous prefix clamped to the ceiling that the successor
  epoch's recovery point recorded (§12.5). That clamp is what makes the case below
  real.

### What was actually reproduced

Against the simulated store, with the listing lagging exactly as `SetListLag`/
`SetEventualList` model it:

1. **Manifest LIST-invisible, its objects inside the durable prefix → nothing is
   marked.** The prefix root already covers the anchor's objects. This is the shape
   the finding describes, and in a healthy volume it is harmless.
2. **Manifest LIST-invisible, its objects above the epoch's *ceiling* → the
   snapshot's object is marked.** A promotion recorded a boundary of 1 for an epoch
   whose published snapshot targets 2, so the clamped durable point no longer covers
   the snapshot's second object; the manifest was its only anchor, and the manifest
   was not in the listing. Real, reproducible data loss with correct code everywhere.
3. **A hole in the listing below the durable point → an *ACKed* object is marked, with
   no manifest involved at all.** When LIST does not return one object of a contiguous
   run, the run ends before it, the durable point drops (to 0 in the reproduction),
   and the objects above the hole become orphans by every rule the sweep has. No
   snapshot index would have changed this outcome by one key.

(3) is the decisive one. It says the GC's dependency on a strongly consistent LIST is
not confined to anchor discovery: it is inherited from the durable prefix, which §5.8
makes the authority for what a volume contains. And it is *irreducible*: "an object
above a gap in the open epoch" is simultaneously the orphan §22.1 asks the GC to
collect and the ACKed object that a lagging listing made look like an orphan. Nothing
in the bucket distinguishes them — not an index, not a catalog, not a second listing.
The only thing that does is the backend having the property.

## Decision

### 1. No snapshot index, in the descriptor or anywhere else

The GC does not get a by-key snapshot root. The reasons, in order of weight:

- **It closes one of the two doors.** Reproduction (3) is at least as dangerous as the
  one the index would close, lives in the same package, and is untouched by it. The
  strongly-consistent-LIST precondition survives the index in full, so the index buys
  a smaller precondition, not the removal of one.
- **It is a second source of truth for the one question the sweep must never get
  wrong.** Today the manifest *is* the evidence: it is written create-only, and its
  presence at a deterministic key means the snapshot is published (this is exactly what
  `rebuild-metadata` relies on, §22.5). An index makes "published" a claim held in two
  places, and every skew between them has to be resolved by a component that may only
  ever stop, never widen.
- **Neither shape of index survives the operational worst case** — see the
  alternatives below: mutable loses a concurrent publication, create-only cannot be
  read by key without becoming a listing again.

### 2. An epoch ceiling is not a licence to destroy

`addDurablePrefixes` keeps computing the durable point as today, and additionally:
**for an epoch that a successor has closed, every WAL object of that epoch which the
listing shows is reachable.**

"Closed" is established by `recovery.EpochCeiling`, which reads the successor's
recovery point with a strongly consistent GET at a deterministic key — not by a
listing. The rule adds to `reachable` and never removes from it, so it can only
shrink the mark set.

The argument is the difference between two questions. The ceiling answers *"what is
the volume's state?"* — and there the clamp is right: what a fenced writer PUT after
the boundary was never part of the volume (§12.5). The GC asks a different question,
*"what may I destroy?"*, and for that question an object above the ceiling is not
garbage: it is either a snapshot's object that a pre-promotion manifest still names
(reproduction 2), or the physical evidence of a boundary that was itself computed from
a listing and may be wrong. The boundary object is create-only and permanent; letting
a permanent number derived from a listing authorize a destructive act is precisely the
coupling this project refuses everywhere else.

What it costs: the in-flight objects a fenced writer landed after a promotion are
never collected. That is a handful of objects per promotion, bounded and one-off,
against an unbounded loss — the trade INV-14 states in its own words ("an object
collected one cycle late costs storage, an object collected one cycle early costs
data"). Genuine orphans in the *open* epoch — the §22.1 case, the late PUT past a gap
— are still collected, unchanged.

### 3. The precondition stays, and stays stated

Strongly consistent LIST remains a precondition of running a sweep, and of `recovery`
(the durable prefix, INV-08/INV-09 — its package doc already says so and its planted
bug is literally `SetEventualList(true)`), of `descriptor.ListVolumeIDs`, and of
`rebuild-metadata`. It is certified per backend by `TestListSeesAFreshPut` (§6.1,
blocking). No design inside these packages can retire it; the honest options are to
certify it (today) or to check it at the point of use (deferred, below).

## Alternatives considered

- **A snapshot index in the volume descriptor (mutable).** `volumes/<vol>/descriptor.json`
  is already read by deterministic key, so the sweep could derive a candidate's volume
  from its own key, GET the descriptor, and GET each manifest it names. What it makes
  worse: the descriptor is overwritten, so two snapshots of one volume published
  concurrently (§21.1 allows it) race, and the loser's id is dropped by a
  last-write-wins PUT. That turns a *transient* invisibility into a *permanent* hole
  in the only root the sweep would then trust. `If-Match` CAS plus retry closes the
  lost update, at the price of making every snapshot publication contend on one
  object, and of a descriptor write that must be retried by a component holding no
  lease.
- **A create-only index (per-snapshot entries, or per-generation objects).** Immutable
  entries cannot be lost-updated, but they cannot be read by deterministic key either:
  finding "all entries" is a LIST of a prefix, which is the original problem with more
  objects in it. The only create-only shape that *is* key-addressable is a chain of
  generations (`volumes/<vol>/snapshots/<n>.json`, each carrying the full set, probed
  upward until `ErrNotFound`), which trades the lost update for unbounded probing, a
  rewrite of the whole set per publication, and a fresh way for two publishers to race
  at the same generation.
- **Order of the two writes.** Whatever the index, publishing is then two writes and
  the orders are not symmetric. *Index first, manifest second*: a crash in between
  leaves an index entry whose manifest does not exist — the sweep reads it as an
  unreadable anchor and **stops**, which is the safe direction but wedges the GC for
  that volume until somebody cleans up, i.e. one crashed snapshot publication disables
  garbage collection indefinitely. *Manifest first, index second*: the window is the
  original bug made durable — a published snapshot the index does not name, and the
  sweep widens. Only one of the two orders is safe, and it is the one whose failure
  mode is a permanent operator task.
- **A Control-Plane query as the anchor source.** The catalog is in PostgreSQL and is
  queryable without any listing. It is also restorable: ADR-0011 exists precisely
  because a PITR restore of the CP database is a planned operational event. A restored
  catalog missing the last hour of snapshots would hand the sweep a licence to destroy
  their data — and `addDurablePrefixes` already carries the comment explaining why the
  GC does not ask PostgreSQL about epochs. Strictly worse than the listing.
- **Having `Mark` consume `Reachable`'s listing** (or re-listing inside `Reachable`).
  Both listings miss the same anchor; this is stated in the finding and is confirmed by
  the reproductions.
- **Widening the grace period.** Grace protects objects that are *young*. The failing
  case is old objects newly anchored, which no grace value reaches.
- **Doing nothing.** Leaves reproduction (2) — an ordinary promotion plus a lagging
  listing destroying a published snapshot's data — closed only by the backend
  property, when it can be closed by the sweep's own rules at no risk.

## The test that enforces it

Unit lane, `internal/gc` (the case that fails today, in its own commit before the fix):

```
TestASupersededEpochsObjectsSurviveASweep
  volume with epoch-1 objects covering sequences 1..2, both past grace
  a promotion records the epoch-2 boundary at recovered_up_to = 1 (ceiling below the
    published snapshot's target)
  a snapshot manifest targeting 2 is published — with the listing lagging, so the
    manifest is GET-visible and LIST-invisible
  sweep -> the object covering sequence 2 must not be marked, and must still read back
```

with a table over the visible/invisible manifest and over "epoch closed / still open",
so the arm that must keep collecting (open epoch, object past a gap) is asserted in the
same place as the arm that must stop.

DST, `internal/dst/scenarios_gc.go` — `gc-keeps-a-superseded-epochs-snapshot`: the same
sequence driven through a real `wal.Log`, a real promotion boundary and a real
`snapshot.Publish`, with the fault injected as the backend defect it is
(`SetEventualList(true)` before the manifest is published), asserting that after the
sweep the manifest's objects still read back and the snapshot is still materializable.
It needs no new checker: the property is an assertion the scenario itself makes, and
the existing `NoPermanentDeleteChecker` still rides along on the marks it does write.

## Consequences

- The sweep marks strictly less than before. Nothing that was collected in an open
  epoch stops being collected.
- A fenced writer's post-boundary objects become permanent. If that ever shows up as
  cost, the fix is an explicit operator-driven expiry of a *named, closed* epoch — not
  a rule that lets the sweep infer the licence itself.
- The GC's rules now read the successor's recovery point for every epoch it judges
  (one GET per volume/epoch, at a deterministic key). `EpochCeiling` already fails
  closed on an unreadable boundary, so an unreachable object store aborts the sweep,
  which is the existing discipline.
- **No on-S3 format changes.** No new object, no new field, no new key prefix; buckets
  that already hold published snapshots need no migration and no backfill, because
  nothing new has to exist for the rule to apply.
- `rebuild-metadata` is untouched: the manifest stays the sole evidence of a published
  snapshot, so §22.5 keeps its single source of truth and gains no second one that
  could drift.
- The precondition is unchanged and remains stated in the `gc` and `recovery` package
  docs. The finding is narrowed, not closed: an anchor outside every durable prefix —
  which is what compaction/objectization (Phase 12) will create, when a manifest
  becomes the *sole* anchor of WAL objects a segment has replaced — puts the exposure
  back. That is the trigger to revisit this ADR, and it is the right time to do it:
  the index would then have a format to index.

## Deferred: enforcing the precondition at the point of use

The precondition is certified against a backend *version*; a sweep runs against a
bucket, an endpoint and a configuration. A replica bucket that lags, a caching gateway,
an endpoint pointed at an implementation that never ran §6.1 — in each of those the
certified property is silently false and reproduction (3) is live. A per-sweep probe
would catch that class: PUT a small object at a key unique to this sweep, LIST its
prefix, and abort the sweep unless the listing returns it (then mark the probe, so it
does not accumulate).

It is not implemented, for three honest reasons:

1. it is a smoke test, not a proof — read-your-own-write on a listing does not
   establish that *another writer's* fresh PUT is visible;
2. it needs `PutObject` in the GC's credentials, which today are deliberately narrow
   (INV-14's structural claim rests on what the GC cannot do), and it introduces a
   `gc/` key prefix — an on-S3 format and a permissions decision;
3. a cheaper variant covers most of the same ground with no code at all: run the §6.1
   conformance suite against the *deployment's own* endpoint and bucket as a deploy
   gate, not only against a backend version in CI.

If the probe is wanted, it should be its own increment with its own review, and (3)
should be evaluated first.
