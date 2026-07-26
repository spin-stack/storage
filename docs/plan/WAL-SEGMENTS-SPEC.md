# WAL segmentation — format spec for review

**Status:** **approved 2026-07-26** by the human owner — this is the format review
CLAUDE.md requires before an on-disk change is implemented (ADR-0013 §4). The four open
questions at the end are settled. Implementation follows; the *diff* still gets a human
review before merge, as every data-loss-zone change does.

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

- A segment is **sealed** when it reaches `SegmentBytes` (**32 MiB**, see below) or when
  a checkpoint publishes; a new one is created for the next record. Only the newest
  segment is open for append.
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

## Decisions on the four open questions (2026-07-26)

**1. `SegmentBytes` = 32 MiB**, with the option to derive it from the volume's device
share once ADR-0013 exists (`clamp(share/8, 8 MiB, 64 MiB)`).

A correction to how this spec first framed it: segment size is **not** "the amount a
crash leaves unsynced" — that is bounded by `MaxUnflushedBytes`, since we fdatasync per
FLUSH. Segment size governs three things only: reclamation granularity, file
descriptors, and directory churn.

That makes the deciding cost scale with **volume count**, not with volume size: each
volume retains up to one segment the checkpoint cannot yet reclaim. At 64 MiB and 100
volumes on a host that is 6.4 GiB immobilised; at 32 MiB, 3.2 GiB.

The reference points, all with the same structure (append-only segmented log, reclaim by
segment): PostgreSQL WAL 16 MB, Cassandra/ScyllaDB commitlog 32 MB, etcd WAL 64 MB, TiKV
raft-engine 128 MB. The closest analogue is the Cassandra/Scylla commitlog — many
independent streams, a segment set per stream, reclamation tied to a progress point —
and it uses 32 MB. Kafka's 1 GB is not a counter-example: its granularity works because
retention is a policy, not a wait for a checkpoint.

**2. Sealed segments are closed.** Only the newest is held open. Descriptors would
otherwise scale with volumes × retained segments, which is the same quantity that
already makes segment size matter; replay is rare (crash, attach) and pays an open per
segment when it happens.

**3. No `SegmentAge` timer. Seal on checkpoint instead.** A timer per volume buys
nothing a checkpoint does not: sealing is only useful at the moment reclamation becomes
possible, and that moment *is* the checkpoint. An idle volume holds at most one
partly-filled segment either way.

**4. Yes — the epoch is in the path**: `wal/<vol>/<epoch>/<first-seq>.seg`. It mirrors
the S3 key layout (`wal/<vol>/<epoch>/`), makes a foreign-epoch directory impossible
rather than merely detected, and makes "which epochs still have local segments" a
directory listing. A promotion is rare, so a directory per promotion is free. The
file-level `Epoch` in the header stays: the path says where it was filed, the header
says what it is, and a restored directory can disagree with both.
