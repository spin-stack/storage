# WAL segmentation — format spec for review

**Status:** draft for human review. This is the *format* review CLAUDE.md requires
before an on-disk change is implemented (ADR-0013 §4). Nothing here is built.

**Why:** `TruncateLocal` records `truncatedUpTo` and calls `file.Truncate(0)` only when
`upTo >= local` — i.e. only when the checkpoint reached the end of the log. On a volume
that is being written to, that never happens, so **the reclaim path is a no-op and the
WAL is a file that grows until the volume goes idle.** Every other defence against a
full device is downstream of this one.

## The shape

One WAL becomes a directory of append-only segment files:

```
wal/<volume-id>/
  000000000000000001.seg     # zero-padded, monotonic, never reused
  000000000000000002.seg
  ...
```

- A segment is **sealed** when it reaches `SegmentBytes` (proposal: 64 MiB) or
  `SegmentAge` (proposal: 5 min), whichever comes first; a new one is created for the
  next record. Only the newest segment is open for append.
- **A record never straddles a segment.** A record that does not fit in the remaining
  space seals the segment and starts the next one. This is what makes replay of a
  segment a self-contained operation and what makes an unlink safe to reason about.
- The name is the **first sequence** the segment carries, zero-padded to 18 digits so
  lexicographic order is numeric order — the same rule the term claims use
  (`control-plane/terms/`), for the same reason: the directory listing is the index.

## Segment header (fixed, 64 bytes, little-endian)

```
Magic         [4]byte   "WS01"
Version       uint16
HeaderLen     uint16    // 64
VolumeID      [16]byte  // §14.1 record binding, at the file level too
Epoch         uint64
FirstSequence uint64
CreatedAtMs   uint64    // the injected clock, for SegmentAge
Reserved      [12]byte
HeaderCRC32C  uint32
```

`LastSequence` and `RecordCount` are deliberately **absent**: they would have to be
written back into the header at seal time, which turns a sealed file into a mutable one
and adds a torn-write case for no gain — replay already establishes both, and the WAL's
authority for "what is durable" is S3, not this file (§5.8).

`VolumeID` and `Epoch` at the file level are not redundant with the record binding
(wave 1, `ErrForeignVolume`): a directory restored to the wrong place is caught before
a single record is decoded.

## What changes semantically

| Today | With segments |
|---|---|
| `TruncateLocal(n)` truncates to 0 only if `n >= local` | unlinks every segment whose records are entirely `<= n`; the segment containing `n` stays whole |
| Replay reads one file | replays segments in name order, which is sequence order |
| A torn tail is the last record of the file | a torn tail is the last record of the **newest** segment; a torn tail in any older segment is corruption, exactly as a mid-file tear is today |
| `Resume` reopens the file | opens the newest segment for append, replays all of them for the view |

**Partial reclaim is the whole point:** the segment holding the published point is
never unlinked, so the floor granularity is one segment. That is why `SegmentBytes`
matters — it is the maximum space a published checkpoint cannot yet reclaim.

## The failure cases, and what must hold

1. **Crash between unlink and metadata update.** Unlink is the *last* step: the
   published watermark advances first, then segments are unlinked oldest-first. A crash
   mid-way leaves segments that a later truncation removes; the reverse order would
   remove data the published point does not yet cover. INV-13 unchanged.
2. **Crash mid-seal.** A segment is sealed by *creating the next one*, not by writing to
   the old one. So a crash leaves either "no new segment" (the next append creates it)
   or "an empty new segment" (replay skips a header-only segment). No state requires a
   write to an already-sealed file.
3. **A gap in the directory.** Segment 3 and 5 present, 4 missing: this is corruption,
   not a torn tail, and it must be a hard error — it is the local twin of the prefix
   floor in `recovery`, where a missing object ends the contiguous run rather than
   being skipped.
4. **ENOSPC while creating a segment.** Creating a segment must be charged against the
   device budget *before* the first append (ADR-0013), so the out-of-space state is
   reached at a segment boundary rather than mid-record. The existing `Degraded()` latch
   covers the reporting.
5. **fdatasync scope.** A sealed segment is synced once at seal; the open segment syncs
   as today. Creating a file also requires syncing the **parent directory**, which the
   single-file WAL never had to do — this is a new durability requirement, and the
   `real` disk must do it (the object store's filesystem twin already does, from wave 1).

## What it costs

- `wal.Log` gains a segment set and a name→first-sequence map. `Replay`, `Resume` and
  the property tests (`TestWALReplayProperty`) all change; the property to preserve is
  unchanged — truncate at every byte, bit-flip → exact state XOR detected error.
- `internal/simio/disk` gains nothing: `Create`/`Open`/`Remove`/`List` already exist,
  which is why this is a WAL-level change and not a simio one. The parent-dir fsync is
  the one addition (`Disk.SyncDir(name)` or an implicit sync inside `Create`).
- More open file descriptors: one per volume, plus retained sealed segments until the
  checkpoint catches up. Bounded by the device budget, not by volume count.

## The tests that would have to exist first

- A property test that for any interleaving of writes, seals, checkpoints and
  truncations, replay yields exactly the accepted records, in order, with no duplicate
  sequence — the current property, extended across segment boundaries.
- A table test for §"failure cases": torn tail in the newest segment (recoverable),
  torn record in an older one (hard error), a missing middle segment (hard error), a
  header-only segment (skipped), a segment whose VolumeID/Epoch is foreign
  (`ErrForeignVolume`).
- A reclaim test: with 5 sealed segments and a published point inside the third, exactly
  the first two are unlinked and the device's used bytes drop — the test that fails
  today, which is the reason for all of this.
- A DST scenario: crash at each boundary (before seal, after seal, before unlink,
  between unlinks, after the published advance) and assert INV-13 and the read view
  survive each one.

## Open questions for the reviewer

1. **`SegmentBytes` = 64 MiB?** It is the granularity of reclamation *and* the amount a
   crash can leave unsynced. Smaller reclaims sooner and costs more file descriptors and
   more directory churn.
2. **Do sealed segments stay open?** Keeping them open makes replay cheap and holds file
   descriptors; reopening on demand is simpler and pays a syscall per recovery.
3. **Is `SegmentAge` worth it?** It bounds how long an idle volume holds a partly-filled
   segment, at the cost of a timer per volume. An alternative is to seal on the
   checkpoint, which is when reclamation could happen anyway.
4. **Does the segment name carry the epoch?** `wal/<vol>/<epoch>/<first-seq>.seg` would
   make a foreign-epoch directory impossible rather than merely detected, at the cost of
   a directory per promotion.
