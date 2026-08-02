# Spec — who decides *when* to checkpoint and truncate (BUILD-INVENTORY increment 3)

**Status: awaiting human review. Not implemented.** Durability review zone. Unblocked by
increment 5 (`VIEW-ADOPTION-SPEC.md`), which had to land first: without it, truncating
local WAL and then restarting served zeros.

## What already exists, and why this increment is small

`checkpoint.Checkpointer.Create` does the hard part, and it does it carefully:

- it verifies this host may publish into the epoch **before** writing anything
  (§12.3–12.4: two hosts can hold the same epoch *number*);
- it takes the durable sequence from **S3's proof**, not from the log's watermark,
  because the watermark was set when the PUTs returned and the prefix can have fallen
  behind it since;
- it publishes create-only and treats an identical existing checkpoint as its own retry
  (a crash between the PUT and `AdvancePublished` must converge, or `published` sticks
  and the WAL can never be truncated);
- it verifies the epoch **again** after publishing and before `AdvancePublished`, because
  advancing published is what authorises discarding the last local copy.

`wal.Log.TruncateLocal` is equally done: `StrictOrder.AllowTruncate` refuses anything
above `published`.

**So nothing in this increment is about how to checkpoint. It is entirely about when —
and every question below is a "when" question with a data-loss or an availability edge.**

Today nothing calls `Create` in a running system. `published` stays 0 forever, not one
byte is ever reclaimed, and a host's NVMe fills until writes stall for that volume and
every co-tenant of the disk.

## The decisions

### 1. What triggers a checkpoint?

- **A fixed interval.** Simple, predictable, and wrong at both ends: an idle volume
  publishes checkpoints of nothing, a hot one lets the WAL grow between ticks.
- **Bytes appended since the last checkpoint.** Tracks the actual cost, and it is what
  the reclaim is measured in. Needs a floor so a trickle of writes still gets
  checkpointed eventually.
- **Local disk pressure (ADR-0013 thresholds).** Reactive: checkpoint when the device
  starts filling. Correct as a *safety net* and bad as the only trigger — by the time it
  fires the object-store round trip is on the critical path of a device that is nearly
  full.

**Recommendation: bytes since the last checkpoint, with an age floor, and disk pressure
as an additional trigger rather than the primary one.** Three numbers, all on
`VolumeManagerConfig`, none of them guessed at in code.

### 2. Where does it run, and what stops it starving the guest?

One goroutine per volume, owned by `Volume` and stopped with it — the runtime already
owns a serve goroutine and a supervisor, so this is the same shape.

**It must go through `ioclass.Scheduler` as `Background` (INV-17).** A checkpoint is a
LIST plus a PUT against the same object store the guest's FLUSH path uses, and INV-17 is
the invariant that says the guest wins. The scheduler exists and is currently wired to
nothing on this path; this is where it starts being load-bearing.

**Question for review:** one budget per host, or per volume? Per host is what INV-17
means by "the Agent's I/O", but it means one volume's checkpoint can starve another's.

### 3. How much local WAL is kept after a checkpoint?

`TruncateLocal(published)` reclaims everything the checkpoint covers. That is the maximum
reclaim and it has a cost increment 5 made visible: **a restart then has to rebuild the
entire read view from the object store**, because there are no local segments left to
replay. With the lazy base, that is a stall on the volume's first read.

- **Truncate to `published`.** Maximum space back, slowest restart.
- **Keep a retention window** — the last N bytes or N seconds of WAL below `published`.
  A restart replays those locally and only fetches the base underneath. Costs disk to buy
  restart latency.

**Recommendation: truncate to `published`, and treat retention as a later tuning knob.**
Restart latency is not yet measured on this path, and a retention window that exists
before anyone has measured what it buys is a number nobody can defend. Say so in the code
rather than leaving the reader to wonder.

### 4. What does a failed checkpoint do?

`Create` can fail four ways, and they are not the same event:

- **The store is unreachable.** Retry with backoff. The remote gap grows, which is the
  RPO an operator reads, and `wal.Limits.MaxRemoteGapBytes` already bounds it.
- **`ErrDurablePointMismatch`** — S3 proves more than this log ever ACKed. Another writer
  is publishing into this epoch. **That is a fencing signal, not a retry.**
- **`ErrCheckpointConflict`** — a *different* checkpoint exists at the same sequence.
  Two writers claiming one epoch. Also fencing.
- **The epoch moved between publish and `AdvancePublished`** — this host was fenced
  mid-operation. `Create` already refuses to advance; the scheduler must stop, not loop.

**Recommendation: the last three tear the volume's runtime down through the same path
`Fence` uses.** They are the data path discovering what the Control Plane's refusal would
have told it one heartbeat later, and INV-10 does not care which door the news came
through.

**Question for review:** should they also mark the fenced epoch, so `Apply` does not
restart the volume at the same epoch on the next cycle? I think yes — it is the same
reasoning as the report-refusal path — but it means the data path can fence a volume the
Control Plane still lists as this host's, and that asymmetry deserves a deliberate answer.

### 5. Is the lease checked before publishing?

`Create` verifies the *epoch object*, which is the fencing authority for publication
(§12.4). It does not consult the host lease. INV-06's note says the lease rule "still
governs checkpoint/manifest/snapshot publication" even in local mode.

**Question for review:** is `VerifyPublisher` sufficient, or must the scheduler also
require `Lease.Valid()` before calling `Create`? They fail differently — the epoch object
is a network read that can be stale-cached, the lease is local and monotonic — and the
cheap check is the one not being made.

### 6. Where do reclaimed bytes go?

`Log.TruncateLocal` already accumulates `reclaimedBytes`. `VolumeStatus` has no field for
it and neither does `VolumeReport`, so today the number exists and nobody can see it.
Adding it touches the proto (§26.2 metric `wal_reclaimed_bytes_total` is the other half).

**Recommendation: a metric first, a proto field only if the Control Plane needs to make a
decision with it.** Right now nothing does.

## Tests that must land with it

- **A DST arm** driving the full loop through the Agent: write → flush → checkpoint →
  truncate → restart → read. Increment 5's `scenarioTruncatedVolumeSurvivesARestart`
  already does the second half and truncates by hand; this replaces the hand-truncation
  with the scheduler and asserts segments actually disappear.
- **INV-13's checker under the scheduler**: `TruncateBelowPublishedChecker` exists and has
  never observed a scheduler, only hand-driven calls.
- **A planted bug**: a scheduler that truncates to the log's `durable` rather than its
  `published`. That is the single most tempting mistake here — they differ by exactly the
  window in which S3 has not confirmed — and the checker must catch it.
- **INV-17 under load**: a checkpoint in flight must not delay a guest FLUSH. Same shape
  as `TestAReadIsAnsweredWhileAFlushIsUploading`, which is the only reason the analogous
  regression in `blockdev` is impossible today.
