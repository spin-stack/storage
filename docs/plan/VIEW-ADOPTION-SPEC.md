# Spec — a rebuilt read view has no way in (BUILD-INVENTORY increment 5)

**Status: reviewed 2026-08-01; the `cow` + `wal` half is implemented, the Agent half is
not — see "What is left" at the bottom.** Durability *and* format review zone, so it was
reviewed before the code existed.

**Decisions taken:** (1) the seam goes in `cow.IntervalMap` — the type that owns the
extents learns about a base layer, so `wal` only needs a way to install one and
`blockdev` needs no change at all; (2) the base is fetched **lazily**, behind the first
read; (3) a base that cannot be built makes the volume **refuse loudly** rather than
answer; (4) *(not asked, taken with reasoning)* a resumed log starts `published` at the
durable point, because the base's objects are verified — which is what made the
truncation legal — and INV-13 would otherwise refuse to reclaim ranges the store already
holds.

This is the increment that must land before increment 3 (checkpoint and truncate), and
`STATUS.md` has said so since the 2026-07-26 audit — what it did not have until now is a
reproduction.

## The hole is real. Here is it happening.

Run on 2026-08-01 against `internal/wal` as it stands. Six segments, a flush, a
publish, a truncate, a restart, one read:

```
segments on disk before truncation: 6
after flush: local=6 durable=6 published=0
read from the LIVE log after truncation: first bytes abababab   ← correct
segments on disk after truncation: 1
read after RESTART:                 first bytes 00000000        ← zeros
```

Every one of those bytes was written, flushed, ACKed as durable and verified in the
object store. After the restart the volume serves **zeros, with no error anywhere** —
not a read failure, not a degraded flag, not a log line. A guest sees a hole where its
data was.

The reproduction is at the bottom of this file. It landed as
`internal/wal/base_adoption_test.go`, inverted: same setup, and now it asserts the data
comes back.

**Why it did not reproduce on the first try, which matters for the test that lands.**
With a single record nothing is reclaimed: `segments.reclaim` unlinks only *sealed*
segments, and one record sits in the still-open one, so `Resume` replays it from disk
and the read is correct. The hole needs `Limits.SegmentBytes` small enough that segments
seal and the truncation point to be above at least one sealed segment. A test that
truncates without checking that a file actually disappeared proves nothing — which is
exactly how nine `Resume` tests missed this.

## Why it happens

`Log.view` is a `*cow.IntervalMap`, and it is assigned in exactly one place:

```go
// NewLogAfter — the only assignment in the package
view: cow.NewIntervalMap(),
```

There is no setter. `Resume` rebuilds the view by replaying **local segments only**, and
`TruncateLocal` → `segments.reclaim` is what unlinks those segments. So the two are
exactly opposed: truncation reclaims the only source `Resume` has.

Meanwhile the correct view is already computable, from the objects that made the
truncation safe in the first place:

- `recovery.Recover(ctx, store, enc, volumeID, epoch) (*cow.IntervalMap, uint64, error)`
  — walks the whole epoch chain (§12.5) and returns the view up to the durable point.
- `materialize.FromCheckpoint / FromSnapshot / FromEpoch` — the same shape, for a
  destination host.

Both return precisely the object `Log` needs, and **nothing can install it**. That is the
whole increment: a way in.

## The decisions this needs

### 1. Where the seam goes — **DECIDED: in `cow.IntervalMap`**

- **A. A `wal` constructor that adopts a view** — `ResumeWithView(..., base *cow.IntervalMap)`,
  or an option on `Resume`. The Log keeps owning the read view and its locking, which is
  where every other invariant about it already lives; one type stays responsible.
  Local segments replay **on top of** the adopted base, because the local WAL is newer
  than anything the objects hold.
- **B. A read-through base in `blockdev`** — the Device consults the Log's view and falls
  back to a base for ranges it does not hold. Keeps `wal` untouched, but splits the read
  view across two types and two locks, and `cow.IntervalMap` has no "do you hold this
  range?" query today (`Read` fills zeros for gaps, indistinguishably from written
  zeros — which is the very confusion that produced this bug).

**DECIDED: neither A nor B as written — put the layering in `cow.IntervalMap` itself.**
That is better than my recommendation of A, and it dissolves the objection to B: the
distinction between "I hold nothing here" and "this was discarded" belongs to the type
that owns the extents, and once it exists `wal` needs only a setter and `blockdev` needs
nothing.

Implemented as `NewIntervalMapOver` + `SetBase`. `Read` paints the base first and
overlays this layer on top. `Clear` records a **tombstone** — a span with no data, so
discarding a terabyte costs one entry — because dropping the layer's extents would
otherwise uncover the base's older bytes, turning a DISCARD into a resurrection (§14.6).
Tombstones are recorded from construction, not from the moment a base arrives: a layer
that replayed a DISCARD before its base was installed would otherwise have recorded
nothing.

`TestLayeringEqualsFlattening` (rapid) states the whole contract — a layered map answers
exactly what one map would, had the base's operations and the layer's been applied in
that order — and it earned its place immediately by finding a real bug: `uncover` built
its result into `m.cleared[:0]`, and the split case appends two spans for one input, so
it overwrote the next element of the backing array before reading it. A tombstone
vanished, and the discarded range read back as the base's data.

### 2. Who fetches the base, and when — **DECIDED: lazy**

Resuming a volume means an object-store walk before the first guest read can be
answered. Two shapes:

- **Eager, at `Apply` time**: `agent.VolumeManager.start` calls `recovery.Recover` before
  the device is served. Simple and correct; the volume takes longer to come back, bounded
  by how much has to be replayed — which is what checkpoints exist to bound, and
  increment 3 has not landed yet, so today it is *every object in the epoch chain*.
- **Lazy, behind the first read**: serve immediately, block reads until the base is in.
  Faster to attach, and it puts object-store latency in the guest's read path — the thing
  §5.3/INV-18 keeps out of the write path.

**DECIDED: lazy.** The volume is served immediately and only its *reads* wait.
`wal.ResumeAwaitingBase` returns a log whose `Read` blocks until `InstallBase` or
`FailBase`, both safe to call from another goroutine. The caller owes it exactly one of
the two; a log that gets neither leaves its reads waiting for ever, and that is
documented on the method rather than left to be discovered.

### 3. What happens when the base cannot be built — **DECIDED: refuse, loudly**

The object store is unreachable, or an object fails validation. The choices are: refuse
to start the volume (the guest gets no device), or start it with an empty view (the guest
gets zeros — today's behaviour, silently).

**DECIDED: refuse, loudly.** `Log.Read` now returns an error, and `FailBase` makes every
subsequent read fail with `ErrBaseUnavailable`; `blockdev.Device.ReadAt` passes it to the
guest as IOERR. This is the whole point of the increment: an empty view where data should
be is indistinguishable from a fresh volume, and that is what has to stop being possible. `Apply` already collects per-volume failures and retries on the
next cycle, so a store that comes back cures it.

### 4. Does a resumed Log know it must not truncate below its base?

`StrictOrder.AllowTruncate` refuses anything above `Published`. After adopting a base at
sequence N, `published` should start at N rather than 0 — otherwise the first checkpoint
after a restart re-publishes work already published, and `TruncateLocal` refuses ranges
it should allow. Needs deciding as part of the constructor's contract.

## What is left: the Agent half

The `cow` and `wal` work is done and green. The Agent's is not, and it is **larger than
this spec assumed**, because of something found while wiring it:

**`internal/agent/volume.go` calls `wal.NewLog`, never `wal.Resume`. The Agent has never
resumed a WAL at all.** On a restart it builds an empty log over a root that already has
segments, leaves them unread, and starts appending at sequence 1. So the Agent half is
not "fetch a base and install it" — it is:

1. resume rather than create, when the root already holds segments;
2. use `ResumeAwaitingBase` and kick off `recovery.Recover` in a goroutine;
3. `InstallBase` on success, `FailBase` on failure — exactly one of the two, always,
   including on the paths that return early;
4. decide what a *fresh* volume does, since it has no objects to recover and must not
   wait for a base that will never come.

That is its own increment and its own review.

## Tests that must land with it

- **The reproduction below, inverted**: write → flush → publish → truncate (asserting a
  segment file actually disappeared) → resume → read the truncated range → get the data.
- **A DST arm.** `internal/dst` now has an Agent model (`scenarios_agent.go`), so this can
  drive a real restart: serve, write, flush, truncate, tear the runtime down, `Apply` it
  again, read. The checker is "no read returns zeros for a range this volume ACKed as
  durable" — the shape of INV-08/INV-13's promise from the guest's side, which no current
  checker states.
- **A planted bug that a checker catches**: adopt an *empty* base (today's behaviour) and
  the arm must fail. That is a behavioural proof, and it is the bug this increment fixes.

## The reproduction, verbatim

```go
package wal_test

// Six segments, a flush, a publish, a truncate, a restart, one read.
func TestZZHoleRepro(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	store := sim.NewObjectStore()
	vol := [16]byte{0x77}

	lm := lease.NewManager(clk, time.Minute)
	lm.Grant()

	// Small segments: reclaim only unlinks *sealed* ones, so a single record in the
	// still-open segment truncates nothing and the hole does not appear.
	limits := wal.Limits{SegmentBytes: 8192}
	l := wal.NewLog(d, "wal", clk, vol, 1, limits)
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 3), lm)

	payload := bytes.Repeat([]byte{0xAB}, 4096)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	w := l.Watermarks()

	if err := l.AdvancePublished(w.Durable); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateLocal(w.Durable); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2, err := wal.Resume(d, "wal", clk, vol, 1, w.Durable, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l2.Close() }()

	got := make([]byte, len(payload))
	l2.Read(0, got)
	if !bytes.Equal(got, payload) {
		t.Fatalf("a restart after truncation serves %x, not the written data", got[:8])
	}
}
```
