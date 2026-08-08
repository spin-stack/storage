# SEGMENT-RESERVATION-SPEC — DEV-0011, now that its blocker has cleared

**★ Nominally a HUMAN-REVIEW ZONE: on-disk format.** DEV-0011 asks for "a durable write
offset in the segment", and that is a format change, so this file exists before any code
does, per CLAUDE.md. **It ends by recommending that no format change be made** — which is
the outcome a spec-before-implementation is for.

**The recommendation, up front: close DEV-0011 as accepted, not fixed.** Its premise is
wrong in both halves. The budget is already charged *before* the record is written, so no
WRITE is ever half-accepted for want of it; and reserving device blocks does not require
growing the file, so nothing needs a durable write offset and no format changes. What is
left is a real but second-order device question, sized and priced in "Is it worth doing at
all" below, with one judgement call for the owner at the end.

## The blocker cleared in wave 3, and nobody noticed

DEV-0011 ends "**Waits on ADR-0013**, where the Agent knows a volume's share of the device
budget." That wait is over:

- `agent.Budget` divides the measured device (`agent.NewDiskUsage` → statfs → `Budget`),
  `Budget.Share()` is one volume's slice, and `Budget.Limits()` turns it into
  `wal.Limits{MaxLocalBytes, SegmentBytes}`.
- `agent.NewVolumeManager` refuses to start without one (`Budget.Share() <= 0`), so an
  Agent with no write-path bound is no longer reachable.
- ADR-0013 itself says **Accepted — 2026-08-03 (human owner)** in its own header.

`STATUS.md` still says ADR-0013 is `Proposed` — under "Decisions waiting on a human", and
again in the section on ADRs that describe deleted machinery. Both are stale, and they are
track A's to correct; this file only records that the dependency DEV-0011 named
is satisfied, so the entry is decidable now.

## What is charged today, and against what

Two different ceilings are in play, and DEV-0011 reads as though there were one.

**The budget ceiling — `wal.Limits.MaxLocalBytes`, enforced by `Log.backpressure`.** It is
evaluated in `Log.appendEncoded` *before* `segments.appendRecord` is called, against
`segments.retained()` plus the encoded record plus one `format.SegmentHeaderSize`. That
last term is exactly the thing DEV-0011 says is missing: the comment on it in
`Log.backpressure` says a segment header is charged on *every* WRITE although only a
rotation writes one, "where counting headers only when they are written would mean the
record that rotates a segment is the one record the bound does not cover — which is
precisely the record that makes the log exceed it."

So on this ceiling the guest's WRITE is refused whole, with `wal.ErrBackpressure`, before
a byte is written, at a WRITE boundary — which is *finer* than a segment boundary and
strictly better for the guest. Nothing here can be reached mid-record.

**The device ceiling — the filesystem's own ENOSPC.** This one is not evaluated at all; it
is discovered. `segments.create` writes the header and can fail (`Disk.Create`, the header
`Append`, `Sync`); `segments.appendRecord` can take a partial write and roll back with
`File.Truncate(before)`. `Log.noteAppendResult` latches `wal.DegradedOutOfSpace` on either.
`TestAFullDeviceAtASegmentBoundaryLeavesNoStub` pins the create-side case: the failed
create takes its own file back out, the log reports a full device, the directory still
replays, and the sequence the refused write did not consume is used by the next one.

`segments.retained()` — the number the budget is measured against — is maintained where the
number can change (`create`, `appendRecord`, `reclaim`, `adopt`) and counts the bytes the
segments actually hold, not of the blocks the device gave them. On today's disks those are
the same quantity. They stop being the same the moment anything reserves.

**This is the whole of DEV-0011's finding, restated accurately:** it is a claim about the
*device* ceiling only. The budget half was already right when the entry was written.

## What replay does with a tail it cannot decode

This is the question the entry turns on, and the answer is sharper than "the zeros decode
as a bad magic".

Measured, not reasoned: a scratch test builds a real segment through `wal.Log`, grows the
file with `File.Truncate` (the simulated disk zero-fills a grow, and charges it — see
`sim.simFile.Truncate`), and calls `wal.ReplaySegments`. The same test tears a record with
`sim.Disk.TornTail` + `Crash` first, to get the crash case. Every shape below is one a
preallocated segment would actually be in:

| the segment on disk | today | with the tail zero-filled to segment size |
|---|---|---|
| a healthy half-full open segment | replays, `nil` | 0 records, `format.ErrBadMagic` |
| a zero run shorter than `format.RecordHeaderSize` | replays, `nil` | replays, `nil` |
| a crash tore a record's **header** | intact prefix, `nil` | 0 records, `format.ErrHeaderCRC` |
| a crash tore a record's **payload** | intact prefix, `nil` | 0 records, `format.ErrPayloadCRC` |

Read the first row again: it is not a corruption case. **An ordinary, healthy, correctly
written segment becomes unreadable**, and since `wal.resume` returns the scan's error
unwrapped, the volume does not attach at all. A preallocated WAL without a delimiter is not
a WAL with a weaker guarantee; it is a WAL that cannot be opened.

The two crash rows are the interesting ones, and they are where the entry's framing breaks
down. Today a torn tail is `format.ErrShortBuf` from `format.UnmarshalRecordHeader` or
`format.DecodeRecord`, which `replayPrefix` turns into a clean stop — that is the whole of
INV-05's "a crash mid-append yields the intact prefix". Zero padding removes the shortness:
the buffer is now long enough, so the same tear presents as a CRC failure, which is
*corruption*, which `scanSegments` must treat as a hard error in every segment including
the newest. The distinction between "the crash caught us mid-write" and "this file is
damaged" is not weakened by padding — it is **deleted**.

**And here is the sentence the rest of this file rests on. The delimiter that makes that
distinction possible today is the file's own length.** It is a durable write offset that
costs nothing, that the kernel updates atomically with every append, that needs no CRC of
its own because it is not our bytes, and that cannot be torn. Growing the file to full size
spends it. Every replacement DEV-0011 could reach for is strictly worse than the thing it
would be replacing:

- **A write offset in the segment header.** `WAL-SEGMENTS-SPEC.md` already refused this
  shape once, for `LastSequence`/`RecordCount`: it "would have to be written back into the
  header at seal time, which turns a sealed file into a mutable one and adds a torn-write
  case for no gain". A write offset is worse than the fields that argument rejected,
  because it must be rewritten and made durable on *every* `Sync`, not once at seal — and
  the ordering between the record bytes and the offset that claims them becomes load-bearing.
  Reordered the wrong way it yields an offset asserting records that are not there, which is
  silent data loss, against today's failure mode of losing the last partial record and
  saying so. It would also drag the FLUSH/FUA ACK rule into the change: an ACK that today
  rests on one `fdatasync` would rest on two durable writes in a required order.
- **A commit record or an end-of-segment sentinel.** Same class, same torn-write case, plus
  a record type that means nothing to the reader of the format.
- **"Stop at the first byte that does not decode, and require the rest to be zero."** No
  format change, and it fails both crash rows: a torn record's own bytes are not zero,
  so the rule cannot fire, and the tear still presents as a CRC error. It also puts a
  region of the file — the padding — beyond the reach of INV-05's "any bit corruption is
  caught", which is currently unconditional.

## What `simio/disk` would need, and whether the two worlds can model each other

**None of the above is necessary, because reserving blocks does not require moving the
file's length.** Linux's `fallocate(2)` with `FALLOC_FL_KEEP_SIZE` allocates the blocks for
a range and leaves `i_size` where the appends put it; a subsequent write into that range is
guaranteed not to fail for want of space. `golang.org/x/sys/unix.Fallocate` and
`unix.FALLOC_FL_KEEP_SIZE` are in the module already — `internal/simio/real/disk.go` uses
the same package for `unix.Flock` — so this needs no new dependency and no new INV-01
exemption, because it lands where every other syscall in this tree lands.

The interface addition is one method on `disk.File` (`Reserve(n int64) error` — the name
should say what it promises, not which syscall it calls). What each side would have to do:

**Real.** `unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, n)`. It returns `ENOSPC` at call
time, which is the entire point, and `EOPNOTSUPP` on filesystems that do not implement
preallocation — NFS, and ZFS depending on version. Note also what it removes: `wal/degraded.go`
records that a filesystem with delayed allocation can report ENOSPC at `fdatasync` rather
than at write, that the simulated disk charges allocation at append time and so cannot model
it, and that `noteAppendResult` therefore deliberately does not classify sync errors. Inside
a reserved range that case cannot arise at all — the allocation has already happened. This
is the strongest technical argument in favour of doing it.

**Simulated.** `sim.Disk` derives `Usage.UsedBytes` from `len(c.cache)` per file, and says
why in its own comment: "derived from the files rather than tracked alongside them, so it
cannot drift from the disk it describes". A reservation that is deliberately invisible in
the file's bytes needs a second number per file, and `Usage`/`freeSpace` become a sum of
`max(len(cache), reserved)` — precisely the second source of truth that comment avoided. It
is one field and it is tractable; it is not free.

**And the honesty problem is not that one.** The simulator would model the *guarantee*
perfectly — an append inside a reservation never fails — while production on an
`EOPNOTSUPP` filesystem would have no guarantee at all, and nothing anywhere would fail.
Every test sees the simulated disk (INV-01), so every test would see a world production may
not be in. Two things make it honest, and they are not optional:

1. **Nothing may depend on the reservation for correctness.** It changes *when* ENOSPC is
   discovered, never what replay concludes or what the budget permits. The existing paths —
   the rollback in `segments.appendRecord`, the `DegradedOutOfSpace` latch, the stub removal
   in `segments.create` — all stay, and all stay tested, because they are what runs where
   the reservation is refused.
2. **The simulator must model the filesystem that refuses.** A fault that makes `Reserve`
   return `EOPNOTSUPP`, and the existing ENOSPC scenarios re-run under it. Without that, the
   `EOPNOTSUPP` branch is the untested one, and it is the branch half the fleet might run.

## The observables, if the owner takes it

With the real binary, against a real filesystem, in `integration/e2e` — none of these
assert on a Go value:

1. **A device sized so one volume's share does not fit is refused at attach, not mid-write.**
   The Agent starts, the volume attaches, the first segment's reservation fails, and the
   guest is told before it has written anything — instead of being told after it has
   written most of a segment.
2. **A guest writing into a reserved segment is never told "no space" inside it.** Fill the
   filesystem from outside the Agent while a volume is mid-segment; the writes into the
   already-reserved range complete, and the refusal lands at the next segment boundary.
   This is the whole claim, and it is the arm that fails today.
3. **The line that says which world this host is in.** On a filesystem that refuses
   preallocation the Agent prints, once, that it did not get reservations and that ENOSPC
   will therefore arrive mid-segment. A guarantee that varies by deployment and does not
   say which one it is in is worse than not having it.

**Planted bugs.** Make `Reserve` a no-op and watch (2) meet ENOSPC inside the segment —
which is today's behaviour, so the plant is a regression test for the finding itself. Make
the simulated `Reserve` succeed unconditionally and watch the `EOPNOTSUPP` scenarios pass
while proving nothing.

## §25.2: what the property tests need

**If nothing grows the file, they need nothing, and that is the load-bearing evidence that
this is not a format change.** `TestWALReplayProperty` (truncate at every byte, flip a bit)
and `TestWALSegmentReplayProperty` (any interleaving of writes, seals, checkpoints and
truncations) describe the on-disk byte sequence exactly as they do now, because with
`FALLOC_FL_KEEP_SIZE` the byte sequence is unchanged: same records, same file length, same
delimiter. A reservation is invisible to every reader of the format, which is the definition
of "not a format change".

Recorded for whoever revisits this: **if anyone does grow the file, §25.2 is not a small
extension.** `TestWALReplayProperty` would need a padding arm (`records || zeros`, not a
truncation of the serialized buffer — the shape it quantifies over is longer, not shorter),
`TestWALSegmentReplayProperty` would need segments that are preallocated rather than sized
by their contents, and property (3) — *any* single-bit flip is detected — would have to be
restated, because a flip inside the padding is either undetectable (narrowing an INV-05
property that is currently unconditional) or must be detected (making the padding a
CRC-covered region, i.e. bytes we have to write and sync after all). Neither answer is
cheap, and having to choose one is the clearest sign the file should not be grown.

## What this deliberately does not do

- **It does not add a durable write offset, in any location.** See "the delimiter is the
  file's own length". The offset already exists, for free, and is stronger than any field
  we could write.
- **It does not change the budget accounting to charge a whole segment at create.** That
  was the obvious small version and it buys nothing observable: `Log.backpressure` already
  runs before the append and already covers the rotating record's header, so charging
  `SegmentBytes` up front would only refuse guests earlier while `retained()` stopped
  describing the device it is named for. It would be worth doing only *together with* a
  real reservation, where the charge and the blocks would agree again.
- **It does not make an oversized record safe.** `segments.ensureRoom` lets a record larger
  than a whole segment go in alone rather than rotate forever, so such a record writes past
  any reservation taken at create. The guarantee would be "no ENOSPC inside the reserved
  range", not "no ENOSPC ever". In production the gap is unreachable — a guest request is
  bounded far below `Budget.Limits()`'s segment size — and it is reachable in tests that
  set a small `Limits.SegmentBytes` on purpose, which is where it should be pinned rather
  than argued away.
- **It does not touch the FLUSH/FUA ACK rule.** An ACK stays one `fdatasync` of records the
  device already has room for.

## Is it worth doing at all under ADR-0026

Stated plainly, because the answer is close and the entry has been open long enough to
deserve a real one.

**What the failure actually is today.** The device fills under a volume mid-record. The
partial write is rolled back by `segments.appendRecord`, the guest's WRITE fails with an
error it understands, `Degraded()` latches `OUT_OF_SPACE`, the sequence is not consumed,
replay is clean, and the next successful append clears the latch. No record is lost, no
record is half-applied, and `TestAFullDeviceAtASegmentBoundaryLeavesNoStub` and the DST arm
in `scenarios_harness.go` both pin it. DEV-0011 says so itself: *not data loss*.

**The one path where it costs a session.** If the *rollback* also fails, `Log.broken` is
set and the log refuses to serve anything against a tail it cannot describe — and under
ADR-0026 a volume that cannot serve to its stop is a volume whose whole session never
reaches the bucket. That is the only outcome in this area that is worth real money, and a
reservation makes its precondition unreachable: no partial append for want of space means
no rollback for want of space.

**How likely is the precondition.** By construction, small. `agent.GuestRatio` and
`agent.ReserveRatio` bound every guest on the host strictly below the device, and
`Budget.Share()` divides what is left by the fan-out, so `MaxLocalBytes` refuses a volume
long before the filesystem does. The device ENOSPC path is reached only when the budget's
own assumptions are wrong: another tenant grew on the same filesystem after startup,
`-max-volumes` was set below what the host actually runs, or the reserve was eaten by
something no accounting of ours covers. Those are real, and they are exactly the conditions
under which the failing rollback is itself most likely.

**What it costs.** A method on `disk.File`, two implementations, a contract test, a new
simulated fault (`EOPNOTSUPP`) with the ENOSPC scenarios re-run under it, a startup line,
and an `integration/e2e` arm that fills a real filesystem. No format change, no review
zone, no §25.2 work. Call it an ordinary increment of a day, most of it in the simulator.

**The verdict.** DEV-0011 as written — reserve by growing the file, add a durable write
offset, change the format — should be **closed as accepted, not fixed**, and this file is
the reasoning that lets it be closed rather than left open forever. The part of it that is
real needs none of that, is not a review-zone change, and stands or falls on the one
question below. Whichever way that goes, the DEV entry closes: it names a fix nobody should
make.

## The question for review

**Do we want a durability-adjacent property that holds on ext4 and XFS and is quietly
absent on NFS and some ZFS versions?**

That is the whole judgement call. Everything else here is settled by the code: the format
does not change, the budget is already charged before the record, and the file's length is
already the durable write offset. What is left is one defence — "the device cannot run out
of space inside a segment a guest is writing into" — that a filesystem is free to decline.

**The case for yes.** It makes `Log.broken`'s precondition unreachable, which is the only
outcome in this area that costs a session; it removes the delayed-allocation ENOSPC-at-fsync
case that `wal/degraded.go` records as unmodellable; and the hosts this actually runs on are
ext4 or XFS on NVMe, where it always holds.

**The case for no.** The property is invisible when absent, the simulator every test sees
would show it holding, and CLAUDE.md's rule about a component with no caller has a sibling
that applies here: a guarantee that cannot be observed to be missing is a guarantee an
operator cannot rely on and a reviewer cannot check. The failure it prevents is already
detected, latched, reported and lossless, and the budget is meant to make it unreachable
anyway.

**If the answer is yes, the third observable is not optional** — the Agent must say at
startup which world it is in, and the simulator must model the filesystem that refuses.
Without both, this is machinery that looks like a guarantee, which is the shape CLAUDE.md's
opening table is entirely made of.
