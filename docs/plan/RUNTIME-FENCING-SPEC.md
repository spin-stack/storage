# Spec — the per-volume runtime's review-zone half

**Status: reviewed and answered by the human owner on 2026-07-29; implemented, with one
gap named at the bottom.** The decisions taken are recorded inline below, each next to
the question it answers. What is left is a DST arm for item 2 — see "Outstanding".

Read with `internal/agent/volume.go` open. The runtime today serves a guest locally: it
appends to a `wal.Log`, answers reads from its view, and takes FLUSH as far as
`Log.Flush` will take it without remote mode. Every claim below is about what it must do
before it may say a write is *durable*.

---

## 1. The lease the Log is allowed to trust (fencing)

**What is missing.** `wal.Log.EnableRemote(batcher, uploader, lease)` is what turns on
remote mode, and the `lease` argument is a `wal.LeaseChecker` — `Valid() bool`. Until it
is called, the runtime has no uploader, no §14.4 ACK path, and `durable_sequence` stays
where a WRITE leaves it. `main` opens the object store and deliberately does not hand it
over (`_ = store`).

**The trap this is written down for.** `Loop` exposes `LeaseValid() bool`, and the
obvious wiring is to pass `Loop`'s `*lease.Manager` straight through. **That is wrong and
it fails silently.** `applyLease` allocates a *new* `lease.Manager` whenever the Control
Plane's TTL changes:

```go
if l.lease == nil || l.leaseTTL != ttl {
    l.lease = lease.NewManager(l.clk, ttl)   // a different object from here on
```

A Log holding the old manager would be gated by one that nobody renews: it would go
invalid at the old TTL and never come back, and the volume would self-fence while the
host is perfectly healthy. The adapter must read through `Loop.LeaseValid()` — a method
call, resolving the current manager every time — never a captured pointer.

**Second trap.** `EnableRemote` accepts a nil lease without complaining, and the failure
surfaces much later as `ErrNoLease` inside `durableStep` — at the first FLUSH, in the
guest's I/O path. Remote mode must be refused at construction when the lease is absent.

**Proposed shape.**

```go
// in internal/agent
type leaseFunc func() bool
func (f leaseFunc) Valid() bool { return f() }
```

The manager grows an optional `Lease func() bool` dep; `main` passes `loop.LeaseValid`.
A runtime built with a store but no lease is a construction error, not a warning.

**DECIDED (2026-07-29): yes — the host lease gates every volume's durable ACK, and the
per-volume case is item 2's.** Implemented as `leaseFunc`, a function resolved on every
call rather than a captured `*lease.Manager`, with `TestTheLeaseIsResolvedOnEveryAck`
flipping the answer between two FLUSHes — which a captured lease could not survive. A
manager built with an object store and no lease is refused at construction.

**Tests.** A DST arm where the lease lapses mid-flight: FLUSH must return `ErrSelfFenced`
and `durable_sequence` must not advance (INV-06 already has `DurableAckLeaseChecker`;
this adds the arm that drives it through the *Agent's* runtime). Plus a unit test that a
TTL change does not strand the Log on a dead manager — the planted bug being "pass the
`*lease.Manager`", which must fail.

---

## 2. `Fenced()` tears the runtime down (fencing)

**What is missing.** `Loop.report` already computes the fenced list from the Control
Plane's refusals (`STALE_EPOCH`, `NOT_PRIMARY`, `UNKNOWN_VOLUME`) and stores it in
`l.fenced`, where nothing reads it. Its own comment says the SELF_FENCED transition
"belongs to the data path". This is **DEV-0012's Agent half**, and it is what this spec
closes.

> The other half — that a log which self-fences on its own lease check keeps taking
> writes and serving reads — was **closed on 2026-08-02 as not a divergence**. §12.2
> line 641 grants exactly that behaviour and delegates the choice to policy; the policy
> is now written at `wal.Log`'s `fenced` field. The two triggers are different and only
> one of them is about stopping a guest.

**Proposed shape.** After `report`, the loop hands the fenced ids to the manager, which
stops those runtimes — the same path `Apply` uses when a volume leaves the desired state.

**DECIDED (2026-07-29): the safest option throughout.** Implemented as `VolumeManager.Fence`:
the runtime is torn down whole — log closed, socket closed, device gone — so neither reads
nor writes are answered, and the guest's I/O stalls rather than being served by a host with
no authority to serve it. The answers to each question follow.

- **Reads stop too.** A read of already-written bytes breaks no durability rule, but it
  is a stale read handed to a guest whose volume now has a different writer elsewhere,
  and the guest cannot tell. The cost is a stalled guest, which is the price of the safe
  side.
- **The device disappears.** The socket closes with the runtime. QEMU reconnects on its
  own and finds nothing listening; it retries, and the retry succeeds if and when the
  volume is granted to this host again at a higher epoch.
- **What un-fences it: a higher epoch, and nothing else.** This turned out to be the half
  that is easy to miss. The Control Plane refuses the *report* while `GetDesiredState`
  may keep listing the volume for this host — so without a memory of the fencing the very
  next `Apply` finds no runtime and starts one, re-serving a volume this host was just
  told it lost. `VolumeManager.fencedEpoch` records the epoch fenced out of, `Apply`
  skips anything at or below it, and a higher epoch clears it because a higher epoch *is*
  the Control Plane granting the volume again
  (`TestAFencedVolumeDoesNotComeBackAtTheSameEpoch`).

**Tests.** A DST arm: report refused → the runtime is gone → a write attempted against
the device fails → a later desired state at epoch N+1 starts a fresh runtime under the
new root. Plus the INV-10 single-writer checker driven through the Agent.

---

## 3. `blockdev.Device.mu` across the object-store round trip (durability)

**What is there now.**

```go
// mu serializes requests against the Log, which is not safe for concurrent use.
// It is held for the whole request, including the object-store round trip inside
// Flush: a WRITE that slipped past a FLUSH would be ACKed by that FLUSH's target
// sequence without having been uploaded.
```

**The comment's first sentence is stale.** `wal.Log` has been made safe for concurrent
use — that is what its two-mutex design is for. The consequence of the current code is
that `Flush` holds `d.mu` for an entire S3 round trip, so **every guest READ blocks on
S3**, which is exactly what the Log's design removed.

**The hazard is real, though, and it is not the one the comment names.** A WRITE that
lands between "capture target_seq" and "verify the uploads" must not be ACKed by that
FLUSH. §14.4's step 1 is *capture the target sequence*, and everything after is bounded
by it — so a WRITE arriving later gets a higher sequence and is simply not covered. The
question is whether `wal.Log.Flush` really captures its target before releasing anything,
or whether it re-reads state that a concurrent Write can have moved.

**DECIDED (2026-07-29): make it concurrent and safe. The precondition was checked first
and it holds.** `Log.Flush` captures `target := l.local` under the same `mu` that
`Log.Write` appends and bumps `local` under, so a WRITE returning afterwards has a
strictly higher sequence and `advanceDurable(target)` cannot reach it. `durableStep` runs
under `flushMu` and takes `mu` only in short stretches around the upload, never across
it, and the pending batch list is snapshotted precisely because a concurrent WRITE may
close a further batch onto its end. `wal` already proves all of this
(`TestAGuestWriteCompletesWhileAFlushIsUploading`, `TestConcurrentFlushesUploadEachBatchOnce`).

So `blockdev.Device.mu` was redundant, and it is gone. `TestAReadIsAnsweredWhileAFlushIsUploading`
is the regression guard, verified by putting the mutex back: the READ then blocks until
the test's timeout, with a stack pointing at it.

**But the spec named the wrong cause, and this correction matters more than the fix.**
"Every guest READ blocks on S3" is true, and removing that mutex does not fix it.
`vhost.Device.ProcessQueue` serves the ring **serially under the device's own mutex** —
one request at a time, in ring order, on the queue-loop goroutine. A FLUSH's object-store
round trip stalls every request behind it whatever `blockdev` does. See "Outstanding".

---

## Outstanding

Two things this increment did not close, both named rather than left implicit.

**~~A DST arm for item 2 (fencing).~~ Closed 2026-08-01.** `internal/dst/scenarios_agent.go`
drives the real `agent.VolumeManager` on the simulated clock, disk and socket through the
three moments that decide whether fencing means anything: a volume is served and takes a
write; its report is refused and the runtime must be gone; the desired state has *not*
caught up and repeating it must not bring the volume back, while a higher epoch must.
`FencedVolumeChecker` watches every seed, and its planted bug is the Agent not acting on
the refusal — DEV-0012 exactly as it stood. INV-10 is now proven at both levels.

**Concurrent request dispatch in `vhost` (durability review zone).** Making a FLUSH stop
blocking reads means `ProcessQueue` dispatching requests concurrently and completing them
out of order. virtio allows it — the used ring carries each chain's head index — but it
is a real data-path change: the used-ring publication needs its own lock, workers need
bounding (the ring is ≤128 by §4, so it bounds itself), and in-flight requests at a
session error interact directly with increment 3.3's inflight-shmfd tracking and RISK-10.
It needs its own spec and its own review; it is not a follow-on edit to this one.

## What is not in this spec

The uploader's scheduling (checkpoint/truncate) is increment 3, and it must not merge
before the view-adoption increment, because truncation without an adoptable read view
makes a restart serve zeros for everything it reclaimed. Both landed on 2026-08-01 in
that order (increment 5 then increment 3), so this constraint is satisfied rather than
pending; `VIEW-ADOPTION-SPEC.md` records how the seam was built.
