# OBJECTIZATION-SPEC — segment objects (§21.1, DEV-0007's last item)

**Human-review zone: on-S3 format.** This is a spec for review, **not** an increment that
was implemented alongside it. The sizing below is the reason.

## What exists, and what §21.1 asks for

§21.1's strict order is seven steps:

```text
1. Crear segmentos (aplicando DISCARDs).
2. Subir segmentos (clase background).
3. Verificarlos (HEAD + checksum).
4. Publicar checkpoint (validando epoch por CAS).
5. Publicar manifest.
6. Actualizar PostgreSQL.
7. Marcar como elegible el WAL local con sequences <= published_sequence.
```

Steps **4 through 7 are implemented and proven**: `checkpoint.Create` publishes
create-only after verifying the epoch holder, `AdvancePublished` follows, `TruncateLocal`
refuses anything above it (INV-13), and the durability scheduler drives the whole thing on
its §21.1 triggers.

Steps **1 through 3 do not exist at all.** There is no `segments/` prefix in any bucket,
no `segment_target_size`, no producer and no consumer. `checkpoint.Checkpoint` says so in
its own doc comment: *"Objects are the WAL object keys it covers (segments are added by
full objectization later)."*

So this item is not "implemented wrongly" — it is unstarted, and it is a feature rather
than a defect.

## What it is worth, stated honestly

Without segment objects the system is **correct**: a checkpoint references WAL objects,
recovery replays them, and every durability invariant holds — which is why a real guest
now writes, flushes, checkpoints, truncates and reads back across a restart
(`TestAGuestSurvivesCheckpointAndTruncation`).

What segments buy is **bounded replay**. Today rebuilding a read view costs one replay of
every WAL object since the epoch began, so recovery time grows with write volume rather
than with volume *size*. §21.2's compaction attacks the same number from the other side.
That is RISK-04's cold-RTO problem and it is a performance property, not a correctness
one.

## Why it is not being implemented here

It is a phase, not the tail of an increment, and the review zone is the reason to say so
rather than start:

- **A new on-S3 object kind.** Key layout, header, checksum, encryption (§15 applies —
  segments carry guest data), and a serialize/replay property test with truncations and
  bit corruptions (§25.2, CLAUDE.md's gate).
- **A `Checkpoint` format change**, so a checkpoint can reference segments as well as WAL
  objects — with the "read-old / write-new" rule (INV-19) still not binding but about to
  matter more.
- **`recovery` and `materialize` must read both kinds**, in the right order, with the
  DISCARD semantics applied at segment build time rather than at replay. That is the
  highest-risk code in the tree: it is what INV-08 and INV-09 rest on, and it is what
  `DurableRangeChecker` and the promotion arm watch.
- **The GC's reachability changes.** `gc.Reachable` anchors WAL objects from checkpoints
  and manifests; segments become a third anchored kind, and a mistake there is INV-14
  (the GC causing the worst incident).

Any one of those is a reviewed increment. Doing all four in one pass, in the same session
that closed five other DEV items, is how a plausible-looking format change gets merged
without anyone having read it.

## The shape it should take when it is built

Roughly, and to be reviewed rather than assumed:

1. **A `segment` package** beside `wal`, owning the format: 128 MiB target
   (`segment_target_size`), built from a `cow.IntervalMap` view at a sequence, DISCARDs
   applied so a segment holds live extents only, encrypted with the volume's DEK.
2. **`Checkpoint.Segments []string`** alongside `Objects`, with the root digest covering
   both. A checkpoint that names segments means "everything up to `DurableSequence` is in
   these segments plus these WAL objects", and the reader must not need to know which
   came first.
3. **Built by the durability scheduler**, background class (INV-17), on the same triggers
   that already fire — which is why `checkpoint_on_snapshot` (§21.1 step d) falls out
   rather than needing its own path.
4. **Recovery prefers segments**, falling back to WAL replay for the tail above the newest
   segment. The property that must be tested first, before any of this is written: *a view
   rebuilt from segments + tail is byte-identical to the same view rebuilt from WAL alone.*
   That is the assertion that makes the whole feature safe, and it can be written against
   the existing code the day the segment format exists.

## Recommendation

Land it as its own increment, after the spine has an owner for snapshots and checkpoints
that is not a test. Until then the honest state is what `STATUS.md` now says: §21.1's
publish-and-truncate half is done and proven end to end; its objectize half is designed
and unstarted, and the system is correct without it.
