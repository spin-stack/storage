# Spec — the per-volume runtime's review-zone half

**Status: awaiting human review. Not implemented.** The five pieces of BUILD-INVENTORY
increment 2 that carry no review zone landed on 2026-07-28 (`internal/agent/volume.go`).
These three do, and CLAUDE.md requires a human review of the spec *before* the code
exists, not of the diff afterwards.

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

**The question for review.** This makes the *host* lease the gate on *every* volume's
durable ACK. §12.2 anchors fencing to the host lease, so that is right today — but a
volume promoted away from this host is fenced while the host lease is still perfectly
valid, and that case is handled by item 2 below rather than here. Is that split the
intended reading of §12.2 + §16, or should the runtime hold a per-volume gate that
combines both?

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
"belongs to the data path". This is **DEV-0012**: a self-fenced log still accepts WRITEs
and still serves reads.

**Proposed shape.** After `report`, the loop hands the fenced ids to the manager, which
stops those runtimes — the same path `Apply` uses when a volume leaves the desired state.

**The questions for review, and they are the reason this is a spec and not a patch:**

- **Does a fenced volume stop serving reads, or only writes?** Tearing the runtime down
  stops both, and the guest's I/O fails with IOERR. A read of already-written bytes is
  not a durability violation — but it *is* a stale read reaching a guest whose volume now
  has another writer, and the guest cannot tell. STATUS already flags this as undecided
  ("the exposure is a stale read reaching a guest, plus a device that refuses I/O to a
  guest still attached is the worse failure. Decide before increment ...").
- **Does the guest see a device that errors, or a device that disappears?** Closing the
  socket makes QEMU reconnect (11.0.2 does this on its own), which would loop: reconnect,
  get fenced again, reconnect. Keeping the socket and failing every request is honest but
  leaves a guest hung on I/O forever.
- **What un-fences it?** A volume can return to this host at a new epoch. `Apply` would
  then start a fresh runtime under the new epoch's WAL root, which is correct — but only
  if the fenced runtime was actually removed from the map and not merely stopped.

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

**Proposed shape.** Drop `d.mu` from `ReadAt`/`WriteAt` and hold nothing across
`Log.Flush`, *if and only if* `Log.Flush` is shown to capture its target sequence
atomically with respect to `Log.Write`. If it does not, the fix belongs in `wal`, not in
a mutex in `blockdev` that serializes the guest's entire I/O to hide it.

**This must not be a mechanical removal.** The property to establish first, by reading
`Log.Flush` and `Log.Write` together: *a WRITE that returns after a FLUSH captured its
target is never counted as covered by that FLUSH's ACK.*

**Tests.** A property/DST test with concurrent writers and flushers asserting that every
ACKed FLUSH's `durable_sequence` is covered by verified objects, and a benchmark or
timing assertion that a READ is not blocked by an in-flight FLUSH — the regression this
change exists to prevent, which no correctness test would catch.

---

## What is not in this spec

The uploader's scheduling (checkpoint/truncate) is increment 3, and it must not merge
before the view-adoption increment — see the truncation hole in `STATUS.md`.
