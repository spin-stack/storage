# DATA-DIR-LOCK-SPEC — closing DEV-0014

**Human-review zone: fencing.** This is the increment spec, reviewed before
implementation per CLAUDE.md. It is a review of what will be built, with the doc's own
words where it has them.

## The hole

§10 opens with the assumption, in four words: **"Un proceso por host."** §1 repeats it —
"Un Volume Agent por host." Nothing enforces it, and two live Agents against one
`--data-dir` is not a degraded configuration, it is corruption:

- Both **resume the same segment directory** at the same epoch and both append through
  `disk.Open` (read + append), numbering from the same resumed point. Two writers, one
  file, interleaved records.
- `hostio.Listen` **unlinks a stale socket before binding**, which is right for the crash
  case (ADR-0022's sibling problem: a restart after `kill -9` must not need manual
  cleanup) and means the second incarnation **silently steals the socket** from the first
  rather than failing with `EADDRINUSE`. The guest follows the socket to the second Agent
  while the first still holds its open fds.

None of the four mechanisms ADR-0024 rests on touches this: they are all about what is in
S3, and this is a local-filesystem race. It predates that decision, and bumping the epoch
would not have helped — that only separates the two WALs if the second incarnation went
through the Control Plane, which is exactly what a stale supervisor restart does not do.

**It became urgent with increment 8.** The e2e lane now kills and restarts Agents under a
process supervisor, which is the first thing in this repository that can produce two live
Agents by accident.

## Why a lock, and why *this* lock

The doc is explicit that it distrusts one flavour of lock (§7):

> Esto cierra la carrera clásica del advisory lock ligado a sesión (conexión caída → lock
> liberado → dos CP creyéndose activos).

That warning is about a lock held by a **remote** server on behalf of a session: the
session dies invisibly, the server releases, and two processes each believe they hold it.
Correctness there comes from the CP term, not from the lock.

An `flock(2)` on a local file is the opposite arrangement and the failure it warns about
cannot occur: the arbiter is the kernel that owns both processes, the lock is held by an
open file description for exactly as long as the process holds it, and there is no
network in between. Nor is there a term to lean on instead — an Agent has no monotonic
token of its own, and the host lease cannot help, because two processes on one host share
one lease.

The property that makes it the right primitive rather than merely an available one: the
kernel releases it **when the process dies, by any means**. A `kill -9` leaves no stale
lock file to clean up, so ADR-0024's re-attach still works on the very next start. A lock
that had to be released explicitly would trade this bug for a worse one — an Agent that
refuses to start after a crash.

## What gets built

**1 — a lock primitive in `simio` (INV-01).** `disk.Disk` gains:

```go
// Lock takes an exclusive, non-blocking lock on name.
Lock(name string) (io.Closer, error)
```

with `disk.ErrLocked` for "someone else holds it". Two implementations, as always:
`real` (`unix.Flock` with `LOCK_EX|LOCK_NB`; `EWOULDBLOCK` → `ErrLocked`) and `sim` (a
set on the Disk, which is the right model — one `sim.Disk` is one host's filesystem).

Non-blocking is the whole design. A blocking lock would make the second Agent hang
silently instead of exiting with a message naming the directory, and "started but wedged"
is harder to diagnose than "refused to start".

**2 — `VolumeManager` takes it, not `main`.** The lock belongs to whatever owns
`DataDir`, and the manager is the only thing that knows the WAL root. Putting it in
`cmd/volume-agent` would leave it out of spin's runner when ADR-0021 lifts the manager
across — and "a field `main` forgot to set" is exactly how `HostID` went missing until
increment 8's lane read the log line about it.

`NewVolumeManager` takes `<data-dir>/agent.lock` and fails if it is held. `Close`
releases it, so a test (or a supervisor) that stops one manager and starts another in the
same directory works.

**3 — the failure says what to do.** Not `ErrLocked` bare: the message names the
directory and the fact that another Agent holds it, because the operator's next action is
to find that process.

## What this deliberately does not do

- **No pid file, no liveness check, no "is that process still alive?" logic.** The kernel
  already answers that question, and every hand-rolled version of it has the same race:
  read pid, process dies, new process reuses pid.
- **It is not fencing across hosts.** That is §12 and it is unchanged. This is one host's
  filesystem, and the guarantee is exactly the one §10 assumed and never enforced.
- **It does not make two Agents per host safe.** It makes the second one *fail*. If the
  fleet ever wants two (per-device sharding, blue/green), this is where the requirement
  surfaces, and ADR-0024 must be revisited with it.

## Tests that land with it

- **The primitive, in both implementations**, through the same table: a second lock is
  refused with `ErrLocked`, a released lock can be retaken, and the file surviving does
  not itself hold anything (a crash leaves no stale lock).
- **`NewVolumeManager` refuses a locked directory**, and accepts it again after the
  holder closes.
- **The e2e lane** starts a second `volume-agent` against a running one's `--data-dir` and
  asserts it exits, with the message naming the directory — the only place a real
  `flock` between two real processes is exercised.
- **Not a DST scenario.** The simulated lock would be checking the simulation's own map;
  what makes this true in production is the kernel, and the e2e arm is where that is
  observable.
