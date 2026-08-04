# SHUTDOWN-PUBLISH-SPEC — the one upload that carries V1's RPO

**★ HUMAN-REVIEW ZONE: durability.** Read before implementation, per CLAUDE.md. Five
review-zone increments have already landed their spec in the same commit as their code;
this one does not.

**The one thing it makes true:** an Agent that stops either saves every session it was
serving, or says so — loudly, in its exit code, and in time for an operator to act.

## REVIEWED AND DECIDED — 2026-08-04, human owner

**The open question is answered the other way: a host that cannot publish refuses to
release its data-directory lock.** The spec below argued for exit-and-release; the owner
chose hold-and-retry. What follows replaces §1, §2 and §3 of the decisions; everything
else stands.

**The mechanical consequence, and it is the whole decision.** A flock is released when
the process exits — the kernel does that, not us. So "refuse to release the lock" can
only mean **"do not exit"**. The Agent stays alive, keeps the volume's local WAL, keeps
the directory, and keeps trying.

**This is better than what the spec proposed, for a reason the spec undervalued.** An
Agent that exits non-zero is *indistinguishable from one that crashed*: the fleet sees a
dead host and cannot tell whether it is holding a session nobody has. An Agent that stays
up and says "I am holding unpublished data for volume X, attempt 14, last error: the
store refused the manifest" is a thing an operator can act on and the Control Plane can
still see, because the host keeps heartbeating. The exit code was honest; staying alive is
*legible*, which is what an incident needs.

### 1′. There is no give-up deadline — `-shutdown-grace` bounds one attempt, not the wait

`-shutdown-grace` (default 60s) becomes the timeout of a **single publish attempt**, so
one hung PUT cannot block forever. When an attempt fails the Agent retries with backoff,
**indefinitely**, and does not exit. It is not a budget after which data is abandoned;
nothing in this design abandons data.

### 2′. The process does not exit on failure — and what that costs

The teardown stops serving, publishes what it can, and then **blocks in a retry loop for
whatever is left**, holding the lock. It logs each attempt. It keeps heartbeating so the
Control Plane, and any operator watching the fleet, sees a host that is up and stuck
rather than a host that is gone.

**The cost, stated plainly because it is real:** a store outage leaves every affected
Agent alive and refusing to stop, so a rolling restart hangs fleet-wide until the store
comes back. That is the trade the owner accepted, and it is the right way round — a fleet
that will not restart during an outage is an operational problem with an obvious cause,
while a fleet that restarted and dropped a session each is a data problem with none.

**Three escapes, in order of preference:**

- **A second signal** still abandons and exits non-zero. It is now an explicit operator
  override rather than a convenience, and the line it prints says exactly which volumes'
  sessions are being left unpublished.
- **`SIGKILL`** (systemd's `TimeoutStopSec` will eventually send it) releases the flock
  and loses nothing: the records are on disk, and the next Agent on that host re-attaches
  at the same epoch (ADR-0024) and publishes them. Worth stating because it means the
  operational escape hatch is safe, which is what makes holding the lock affordable.
- **The store comes back** and the retry succeeds, which is the case this exists for.

### 3′. `ErrSuperseded` is the one failure that exits

Another writer published over us. Retrying would overwrite a newer image with an older
one, which is the single thing INV-10 exists to prevent — so this failure must **not** be
retried, and holding the lock buys nothing, because the volume has moved on to a host that
does not care what this directory contains. Exit **2**, release, and say so.

That asymmetry is the sharp edge of this decision: *every* failure is retried forever
except the one where retrying would destroy someone else's data.

### What an operator sees, revised

```
volume image publish started   volume_id=… attempt=1
volume image publish failed    volume_id=… attempt=1 error=… retry_in=2s
agent is holding unpublished data and will not release its data directory
                               volumes=2 attempts=14 oldest_wait=3m21s data_dir=…
```

The third line repeats, on a bounded interval, for as long as the condition lasts. A
process that is deliberately refusing to die must say so on a schedule; one that says it
once and goes quiet is indistinguishable from one that hung.

### The observables, revised

Observable (2) changes and gets sharper — it no longer tests an exit code, it tests that
the Agent **does not** exit:

> The object store is made unreachable and the Agent is SIGTERM'd. It **stays alive**,
> keeps the data-directory lock (a second Agent on that directory is still refused), and
> prints the holding line more than once. Then the store comes back **without the Agent
> being touched**, and the image appears — and *only then* does the Agent exit 0.

Its planted bug: let the teardown return on the first failure, and watch the Agent exit
while the bucket is still empty — which is today's behaviour, so the plant is a
regression test for the defect itself.



## What is broken, verified

Under ADR-0026, `Volume.publish()` at stop is the *entire* durability contract: nothing
else leaves the host, ever. Today that function:

- runs on `context.Background()` with **no deadline** (`internal/agent/volume.go:187`);
- returns **nothing** — every failure goes to `slog` (`volume.go:195-205`);
- is called from `stop()` → `remove()` → `Close()`, whose error is only *logged* by a
  deferred function in `main` that cannot change the process's exit status
  (`cmd/volume-agent/main.go:183-187`).

So the process **exits 0 whether or not the session reached the bucket**. `cmd/control-plane`
has a `-shutdown-grace`; `cmd/volume-agent` has none. Any stop timeout shorter than the
upload — `TimeoutStopSec`, a container runtime's grace period, an impatient operator —
loses the whole session with nothing anywhere reporting a failure.

**And there is a second, sharper path to the same loss.** `stop()` calls `v.cancel()`,
which cancels the context `fetchBase` is running under, and *then* `publish()` waits on
`baseDone`. Stopping a volume while its base is still loading therefore cancels the fetch,
sets `baseFailed`, and `publish()` correctly refuses to publish a view that is missing
everything the volume held before this session — so the session is dropped, silently,
because the Agent cancelled its own read. A volume attached shortly before a restart, or
one whose image is large, or a slow store, all reach it.

## What the data actually does when a publish fails

**It is not gone.** It is in the local WAL at `<data-dir>/wal/<volume-id>/<epoch>`, and a
restarted Agent re-attaches at the same epoch (ADR-0024) and can publish it. The loss is
"not in the object store", which becomes "gone" only if the host is also lost — which is
precisely the failure ADR-0026 accepts.

That changes what the right behaviour is: a failed publish is **a condition to retry, not
a catastrophe to report and forget**, and the local WAL must never be reclaimed on the
strength of a publish that did not happen. Nothing reclaims it today; the spec pins it.

## The decisions

### 1. A total deadline, not a per-volume one — `-shutdown-grace`

The Agent gets `-shutdown-grace` (default **60s**), mirroring `cmd/control-plane`. It
bounds the *whole* teardown, not each volume, because that is the number an operator
already has to choose: systemd's `TimeoutStopSec`, a container's grace period. A
per-volume bound cannot be reconciled with either — eight volumes at 30s each is four
minutes, and the operator set 90.

Volumes publish **serially**, each getting whatever remains of the budget. When it is
exhausted, the remaining volumes are **not attempted** and are named. Rejected:
publishing concurrently, which shortens the tail but multiplies the bandwidth a stopping
host takes from the ones still serving, and there is no io-class scheduler left to bound
it (deleted in 4.5).

**Default 60s and not 30:** an image is a whole session's writes. At 100 MB/s a 5 GiB
volume is 50 seconds, and the default should not be a number that fails on the first
realistic volume.

### 2. A publish that did not happen makes the process exit non-zero

`publish()` returns an error. `stop()` propagates it, `Close()` joins it, and `main`
returns it, so the process exits non-zero.

This is not only honesty. Under systemd a failed unit is *restarted*, and a restarted
Agent re-attaches at the same epoch with the local WAL intact and publishes on the next
clean stop — so the non-zero exit is the thing that turns a lost session into a retried
one. Exiting 0 tells the supervisor everything went fine and there is nothing to redo.

**One exception, and it is the interesting one.** `ErrSuperseded` — another writer
published over us — must **not** be retried and must still exit non-zero. Retrying it
would overwrite a newer image with an older one, which is the single failure INV-10
exists to prevent. It is a distinct exit code (below) so a supervisor can be told not to
restart on it.

### 3. Interruptible: the second signal abandons the publish

Today the first SIGTERM cancels the serve context and the publish deliberately ignores it;
a second signal does nothing at all, because `signal.NotifyContext` keeps handling signals
until `stop()` is called. An operator who has decided not to wait has no way to say so.

After this: the first signal starts the graceful teardown, and **a second signal abandons
the publish immediately** and exits non-zero. The rule to state plainly: *abandoning is
safe, because the data is in the local WAL and a restart republishes it.* What is not
safe is abandoning silently.

### 4. What an operator sees

Three lines, and they are the whole incident-time story:

```
volume image publish started   volume_id=… bytes=… volumes_remaining=…
volume image published         volume_id=… bytes=… duration=…
agent stopped                  volumes=8 published=6 not_saved=2 grace=60s exit=1
```

The **started** line is what makes a hang attributable to a volume rather than to "the
Agent is stuck". The summary line names the count that matters — `not_saved` — and it is
what a human greps for at 3am. Each unpublished volume also gets an `ERROR` line naming
it, its sequence, and the reason.

Exit codes: **0** all published · **1** one or more not saved (retry by restarting) ·
**2** superseded — another writer owns this volume's image, **do not restart**.

### 5. Stop must not cancel the base fetch it then waits for

`fetchBase` moves off the serve context. It gets its own context, cancelled by the
shutdown deadline rather than by the teardown that is about to wait for it. A volume
stopping while its base loads then either finishes the fetch and publishes a complete
view, or exhausts the grace and is reported `not_saved` — never "dropped because we
cancelled our own read".

### 6. The local WAL is never reclaimed on the strength of a publish that did not happen

Pinned with a test rather than left as a property nobody stated. It is what makes every
"retry by restarting" sentence above true.

## The observables

With real binaries, in `integration/e2e` — and note that the first three all assert on the
*process*, not on a Go value:

1. **A guest writes, the Agent is SIGTERM'd, the object appears and the exit code is 0.**
2. **The object store is made unreachable, the Agent is SIGTERM'd**: it exits **non-zero**
   within the grace, prints `not_saved=1`, and the local WAL still holds the records. Then
   the store comes back, the Agent is restarted and stopped cleanly, and **the image
   appears** — the retry path, end to end.
3. **A volume is attached and the Agent stopped immediately**, before the base can land:
   the image is published complete, or the volume is reported `not_saved`. It is never
   silently dropped. (This is the arm that fails today.)
4. **Two Agents race**: the loser exits **2** and the winner's image is intact.

**Planted bugs.** Restore `context.Background()` and watch (2) hang past its grace instead
of exiting. Drop the error from `Close()`'s join and watch (2) exit 0 with the data
missing — which is exactly today's behaviour, so this plant is a regression test for the
defect itself.

## What this deliberately does not do

- **No retry loop inside the Agent.** A publish that fails because the store is down will
  fail again in the next 200ms, and retrying inside the shutdown path spends the operator's
  grace on the case least likely to succeed. The retry is the restart, and it is
  supervised.
- **No partial publish.** The manifest is written last and CASed; a publish that runs out
  of grace mid-upload leaves orphan chunks and no manifest, which is the correct failure —
  the chunks are content-addressed, so the retry re-uses them instead of re-uploading.
  *(This makes the abandoned-upload case cheap, which is the argument for allowing it at
  all. It also means the bucket accumulates orphan chunks with nothing to collect them —
  INV-14 is `pending` and the sweeper is not in V1. Recorded here rather than discovered
  later.)*
- **No change to what a FLUSH promises.** The guest's `fsync` contract is untouched.

## ~~The question for review~~ — answered 2026-08-04

The judgement call was whether a host that cannot publish should refuse to release its
data-directory lock rather than exit. **The owner chose refuse.** The reasoning and every
consequence are in "REVIEWED AND DECIDED" at the top of this file, which supersedes
decisions 1, 2 and 3 below. The argument this section made for exiting — that a process
which refuses to die is harder to reason about at 3am — is answered by making it *say* so
on a schedule, and by the two escapes (a second signal, and `SIGKILL`, which is safe
because the records are on disk and a restart republishes them).
