# Track C — the agent data path

**This file is track C's alone.** It was carved out of `STATUS.md` on 2026-08-04
because five lanes appending to one file is a collision every wave: in wave 3 one lane
committed a stale copy and deleted 121 lines of another's, ten seconds after they
landed, and only that lane looking again restored them. Ownership by *file* is a
control; ownership by *section of a file* is a convention, and a convention is what
`PARALLEL-PLAN.md` says is not a control.

`STATUS.md` remains the single answer to "what is true right now" — its head, its
tables and its DEV entries. This is the running log of one track's increments.

---

## Track C — the agent data path (open work, appended per increment)

*Only track C appends here* — it owns `internal/agent`, `internal/wal`,
`cmd/volume-agent`, `integration/e2e` and `integration/vhost/{lifecycle,wal}_test.go`. The
head tables are recounted once, at integration, by track A.

**Wave 0: `integration/e2e/e2e_test.go` is nine files (2026-08-03, `a5b3024`).** Eight
future increments each land an assertion in what was one 820-line file, and each also
mutates the shared deployment fixture inside it — eight three-way merges, or one split
now. Behaviour-identical, and proven so rather than assumed: the set of `func Test*` and
of unexported helpers is byte-identical to the parent commit, every file kept its
`//go:build e2e`, and the nine tests were run verbosely to confirm **PASS and not SKIP** —
a dropped build tag or an orphaned helper surfaces as a silent skip, not a failure, so
the exit code alone would have proved nothing.

**The spec for the shutdown publish is written and waiting on a human**
(`SHUTDOWN-PUBLISH-SPEC.md`, review zone: durability). Writing it surfaced a second path
to the same loss that no auditor had found: `stop()` cancels the context `fetchBase` runs
under and *then* waits on `baseDone`, so a volume stopped while its base is loading is
dropped — correctly, since the view would be incomplete — because the Agent cancelled its
own read.

### C1 — the publish assertion is made against the store the manager wrote to (2026-08-03)

`TestAVolumeWhoseBaseFailedDoesNotPublish` listed a **freshly constructed**
`sim.NewObjectStore()` rather than the store the rig handed the manager, so it asserted
that an empty bucket was empty and passed whatever `publish()` did. It was green with
`if v.baseFailed { return }` deleted — proven, not assumed.

Two things made it possible and both are fixed. `newPublishRig` took an
`objectstore.Store` and kept the concrete one only if a type assertion happened to succeed
(`sto, _ :=`), so a caller passing a double silently got `rig.store == nil`; it now takes
`*sim.ObjectStore`, which makes that unrepresentable. A test that genuinely needs a double
takes the new `newPublishManager` and asserts through the double it built — which is what
`TestARepeatedSnapshotRequestIsTakenOnce` already did with `countingStore`.

The fixture changed too, because the old one could not have failed even pointed at the
right store: the volume never wrote anything, and an object store that answers nothing
also fails `image.uploadChunks`' Head, so a wrong publish would have died of the double
rather than of the missing guard. The volume is now a **clone whose parent snapshot was
never published** — the shape of base failure that nothing else refuses, because the clone
has no image of its own and its publish CASes with an empty ETag, which is create-only and
*succeeds*. The other shape (the volume's own manifest unreadable) is unfalsifiable here:
that volume has a manifest in the bucket, so create-only loses the CAS and the bucket is
identical with or without the guard.

It writes a block before it stops, and the assertion is the absence of
`image/<vol>/manifest.json` — reporting, on failure, the chunk count it named, which is the
fact that separates "published a partial view" from "correctly published nothing". Planted
bug (the `baseFailed` guard deleted, in a scratch copy of HEAD, never in the tree):
*"a volume whose base never resolved published image/<vol>/manifest.json naming 1 chunk(s)
at sequence 1"*.

### C4 — three things that described a mechanism ADR-0026 deleted (2026-08-03)

**The e2e assertion is deleted, not repointed** (`263c26e`). `TestTheDeploymentServesAVolume`
scanned the Agent's output for `"no durability scheduler"` and failed the lane on it. No
code emits that string — the only occurrences in the tree were two comments — so the loop
body had been unreachable since increment 4.1 removed the scheduler. Proven rather than
argued: the check was inverted in place to fail when the string is *absent*, the lane was
run against the real binaries, and it failed on that line alone with everything else green.
**What is no longer covered: nothing.** It existed because `-host-id` was missing from the
binary and `checkpointsEnabled` refused a scheduler without one; there is no scheduler to
refuse, and the host id's remaining consumer — the heartbeat that creates the host row —
is already blocking four lines earlier in `waitForHost`, where a wrong id fails on the
foreign key.

**Five `VolumeManagerConfig` fields had no reader outside the code setting them**
(`1313296`): `UploadAttempts`, `HostID`, `CheckpointBytes`, `CheckpointInterval`,
`CheckpointPoll`. `DataDir`, `SocketDir` and `Limits` have one and stay. The call sites went
with them, which is the part that read as live configuration: `cmd/volume-agent` carried
an eight-line comment saying `HostID` is what stops the NVMe filling, four DST scenarios
set `CheckpointPoll: 24 * time.Hour` so a poller they no longer have would not fire, and
`integration/vhost/lifecycle_test.go` minted a UUIDv7 per run for a field that discarded
it. Dropping the DST host ids removes four draws from the seeded source, so ids downstream
of them change in those traces; the mandatory set is green.

**`-data-dir` stopped promising checkpoints** (`8750e3c`) — it names the WAL and
`agent.lock`, which are the only things in there. Verified from `volume-agent -h`, not from
the source. No other flag help in that binary describes withdrawn behaviour.

### C2 — stop no longer cancels the read it then waits for (2026-08-04)

SHUTDOWN-PUBLISH-SPEC §5, implemented after the human review of 2026-08-04. `fetchBase`
ran under the serve context, and `stop()` cancels that context and *then* waits on
`baseDone` — so a volume stopped while its base was still loading cancelled its own read,
`fetchBase` recorded a failed view, and `publish()` correctly refused to write an image
missing everything the volume held before this session. The whole session was dropped with
one log line. It now runs under `context.WithCancel(context.WithoutCancel(ctx))`, released
by `stop()` *after* publish has used its result. `WithoutCancel` and not `Background`: the
values (a trace span) are worth keeping and only the cancellation is wrong for this work —
and not a child of `ctx` either, because that is the Agent loop's context, which SIGTERM
cancels before `Close()` runs at all.

**What bounds the fetch now: nothing this increment owns.** `-shutdown-grace` does not
exist yet (the next increment of the same spec), so a stop waits on the store's own
timeouts. That is the safe direction of the two — a stop that waits too long is visible
and recoverable, a stop that publishes an image with a hole in it is neither — and it is
recorded here rather than discovered by whoever meets it.

The proof is a new mandatory DST scenario, `a-volume-stopped-mid-fetch-still-publishes`,
and it asserts what a *later guest reads back*, not that a context survived. Three
sessions: one writes and stops so there is a base worth waiting for; the second writes and
is stopped with its manifest read **blocked inside the store**; the third is a fresh Agent
on a data directory that has never seen the volume, so only the bucket can answer — and it
must answer with both patterns. Determinism comes from the release of the blocked read
being triggered by the *listener closing*, which happens only after the teardown has
cancelled the serve context; no sleep and no timeout. The hold is a `gatedStore` in the
scenario rather than a new `sim.ObjectStore` injector: every injector there returns an
answer, which is a fetch that has already finished, and `sim.ObjectStore`'s methods ignore
the context by design, so it could not model the cancellation half at all.

Planted bug (restored `context.WithCancel(serveCtx)`, reverted): red on all six seeds —
*"volume … read zeros at 4096: the image published by the interrupted session is missing
what the interrupted session wrote"*, with the Agent's own line above it reading *"the
volume's image could not be loaded … the read of image/…/manifest.json was cancelled while
it was in flight: context canceled"*.

### C5 — the shutdown publish holds the data directory and retries (2026-08-04)

SHUTDOWN-PUBLISH-SPEC's "REVIEWED AND DECIDED", implemented as the owner decided it and
**not** as the spec below that block proposed: a host that cannot publish does not exit
non-zero, it refuses to let go. The mechanism is forced — a flock is released by the
kernel when the process exits, so "refuse to release the lock" can only mean "do not
exit", which in this code means `VolumeManager.Close` does not return. It quiesces every
volume, then retries their publishes in rounds, indefinitely, holding the directory and
the process.

`-shutdown-grace` (new, 60s, mirroring `cmd/control-plane`) is the bound on **one
attempt**, not on the wait, and nothing here abandons data on a timer. It is armed on the
injected clock rather than with `context.WithTimeout`, which reads the real one — a
deadline the DST harness cannot advance either never fires in simulation or fires by
wall-clock accident. Zero means unbounded, which is what every in-process caller passes,
so no test or scenario gained a timer.

**Where the loop lives, and why the other two places are wrong.** In the `VolumeManager`.
Not in `Volume`: the decision is about the *data directory*, which a Volume cannot even
name, and a per-volume loop inside `stop()` would publish one volume to completion before
attempting the next, stranding every other session behind the slowest one. Not in `main`:
`Close` joins its errors, so `main` would have to re-derive which volumes were left, and
anything that lives only in `main` is what spin's runner does not inherit when it takes
the manager without the loop (ADR-0021) — which is exactly how `HostID` went missing.

**Volumes retry together, not one at a time.** A round tries every still-unpublished
volume once, serially within the round, then backs off (2s doubling to 30s). Concurrent
publishing multiplies the bandwidth a stopping host takes from the ones still serving,
with no io-class scheduler left to bound it, and interleaves the per-volume lines an
incident reads. One-to-completion is worse: a failure specific to volume A means volume B
is never attempted and the operator hears nothing about it.

**Three failures do not retry.** `image.ErrSuperseded` — another writer published over us,
so retrying would replace a newer image with an older one, which is the one thing INV-10
exists to prevent, and holding buys nothing because the volume has moved to a host that
does not care what this directory holds. Exit **2**, meaning *do not restart*. The new
`agent.ErrNoReadView` — the fetch that would have completed the image is over and this
process will not attempt another, so holding would be a wait with no event that could end
it. And the reconciliation teardown (`remove`, i.e. a fence or a promotion) makes exactly
one attempt and never holds: it runs on the reconcile goroutine, where a retry loop is a
lease not renewed and every *other* volume on the host fenced.

**The heartbeat keeps running while it holds** (`Loop.Sustain`), because "up and stuck" and
"gone" must not look the same to the fleet — that legibility is the whole reason the owner
chose holding over exiting. It is a reduced cycle: heartbeat plus a report of the volumes
still held (a held volume is still this host's, so the report is accepted), and
deliberately no `GetDesiredState` — applying it would restart the runtimes the teardown
just stopped — and no fencing, since nothing is left to stop and the manifest's CAS is the
authority anyway.

The proof is `integration/e2e/hold_test.go`, with the real binaries: the object store is
made unreachable by a TCP proxy the test can break (not by stopping the container, which
would come back on a different port the Agent was never told about), the Agent is
SIGTERM'd, and it **stays alive** — the holding line appears twice, a second Agent on that
directory is still refused by the kernel, the bucket is still empty, and the host's
`last_heartbeat` keeps advancing in the catalog. Then the proxy is restored, nothing
touches the Agent, the image appears and only then does it exit 0.

Planted bug (teardown returns on the first failure, which is what this increment replaced):
red, *"agent-1 exited (exit status 1) having printed "agent is holding unpublished data and
will not release its data directory" 0 time(s), wanted 2"* — a regression test for the
defect itself. The three unit arms in `internal/agent/hold_test.go` were planted
separately: giving up on the first failure ("*the bucket still holds no manifest*"),
retrying `ErrSuperseded` ("*the teardown waited to retry a superseded publish*"), and
reclaiming the local WAL on the strength of a publish that did not happen ("*…/wal/…/1 is
empty after an abandoned publish*" — SHUTDOWN-PUBLISH-SPEC §6, pinned).

**A defect the lane found by running the binaries.** The holding line printed
`data_dir=.`. In production `VolumeManagerConfig.DataDir` *is* `"."` — the Agent's real
Disk is rooted at `--data-dir` so the process cannot write outside it — so every message
naming the directory named nothing, on the one line whose entire purpose is to tell an
operator which directory on which host is stuck. Fixed with `DataDirLabel`, the same
directory spelled the way the operator spelled it. No in-process test could have seen it:
they all hand the manager a Disk spanning a whole filesystem, where the two spellings
agree — the same blind spot that hid `--data-dir` being applied twice.

**Not done here, and deliberately.** No DST scenario for the hold: the harness advances
its clock on quiescence, so a loop that retries forever is a scenario that never ends, and
the property under test ("the process is still there and the lock is still refused") is
about a process and a kernel, which is `integration/e2e`'s job. The retry's *effects* on
the data path — what publishes, what refuses, what stays in the WAL — are covered by the
unit arms above and by the existing publish scenarios.

### C9 — a snapshot taken while a guest writes is one point, not a smear (2026-08-04)

§19's whole claim is that a snapshot is a **sequence number, not an event**, and until now
nothing tested it: every snapshot, restart and clone in these lanes happened over a device
whose guest had already powered off, and the e2e lane's "snapshot of a live volume" asks
for its snapshot thirteen lines after the guest has gone. `TestASnapshotOfAWritingGuestIsOnePointAndNotASmear`
(`integration/vhost/lifecycle_test.go`) is the first test in this repository where a real
Linux guest is writing to a device *while* something else happens to it.

**The assertion is on bytes, and the weaker ones prove nothing.** "The snapshot object
exists" is satisfied by a smeared snapshot — it exists too, with the wrong bytes in it, and
a clone of it boots a state its parent never had. "The sequence is non-zero", or below the
volume's, tests a number the same function writes into the manifest next to the bytes;
nothing ties the two together, so `Freeze` could return the live map with a perfectly
correct sequence and every sequence assertion in the tree stays green. And "the guest's
data is in the snapshot" is the opposite half — completeness — which a copy of everything,
including writes made after the freeze, passes perfectly.

**The shape of the test is forced by two facts, and they are worth writing down because
the obvious design does not work.** (1) `integration/guestinit`'s hold mode writes one
constant pattern to eight fixed blocks, so a volume it is writing to stops changing about
400 ms into the run — every later moment looks identical, and a snapshot taken at any of
them is indistinguishable from a smear. The changing byte therefore has to be *filler the
guest then overwrites*, which means the region must already be in the frozen view:
`image.uploadChunks` evaluates `view.Ranges()` once, up front, so a range that did not
exist at the freeze is never uploaded and could never carry a late write. The volume boots
from an image the test publishes with `image.Publish`, with the guest's whole write region
pre-filled. (2) Whether a write lands inside the freeze→upload window cannot be left to
timing, so the Agent's object store is a double that **suspends the upload at its first
chunk `Head`** and holds it there while the kernel boots and writes. That turns "the guest
wrote while the copy was being made" from a race into an ordering the test enforces. A
second, unwritten region below the guest's exists only so the frozen view is two chunks:
`uploadChunks` reads a chunk's bytes and only then calls the store, so with one chunk the
whole copy is already in memory before the first call and there is no moment to suspend
the upload *at*.

Planted bug — the natural error in `wal.Log.Freeze`, taking `l.view` without swapping a
fresh layer over it, so the "frozen" map is the live one — red on the bytes:

    the snapshot carries a byte the guest wrote after it was frozen: at volume offset
    2097152 it holds 0x41, and the point it was frozen at (sequence 0) held 0xf5

0x41 is `'A'`, the first byte of the guest's pattern, in a snapshot whose every byte should
be filler. The other half of the test is what keeps that from being vacuous: a guest that
wrote nothing — a moved offset in `guestinit`, a device swallowing requests — also leaves a
snapshot full of filler, so the volume's **own** image, published when it stops, is read
back and required to differ from the filler in the same region.

Two things the first run of it established. The gate is on chunk keys and not on any
`Head`, because loading a volume's base image Heads `manifest.json` once for the ETag its
publish will CAS against — a gate on the first Head of any kind held the read view instead
of the snapshot, and the assertion on *which* key it stopped at is what said so. And the
lane, not `integration/e2e`, is where this can live: suspending an upload deterministically
needs a store the test owns, and over RustFS the equivalent is a paused TCP proxy, which
trades the deterministic window for an S3 client timeout — with the snapshot then failing
and never being retried (`ensureSnapshot` starts one at most once per id).

### C3 — the device budget on the Agent (2026-08-04)

`grep -rn "Limits" cmd/` returned nothing: **no Agent this repository has ever run had a
write-path bound of any kind**, while eight unit tests proved backpressure against limits
they set themselves. `agent.Budget` (`internal/agent/budget.go`) is the missing half —
`cmd/volume-agent` measures the device (`agent.NewDiskUsage(disk).Usage`, a statfs of
`--data-dir`'s filesystem), divides it by `-max-volumes` (default 16), and every `wal.Log`
the manager builds gets that share as its bound. An Agent with no budget does not start:
`NewVolumeManager` refuses `Budget.Share() <= 0`, in the manager and not in `main`, for
the reason the data-directory lock lives there.

**The quantity bounded is not `MaxUnflushedBytes`, and that is the point.** `Sync` clears
it on every guest fsync, so on any workload that fsyncs it reads zero while the segments
grow — it bounds a burst, not a session, and under ADR-0026 a session's whole WAL stays
local until the volume stops. The new `Limits.MaxLocalBytes` bounds the retained segments,
enforced against a counter `segments` maintains at the four places the number can change
(a created header, an accepted append, a reclaimed segment, an adopted directory);
`LocalBytes()` still stats the disk and is what the tests assert on, so a counter that
drifted fails rather than quietly loosens the bound.

**The reserve is subtraction, not a pool, and this is a departure from the ADR worth
reading.** The amendment re-aims it at the image publish at stop — but `image.Publish`
writes **no local byte**: it reads the view and PUTs. There is no machinery here to let one
writer spend what another may not, and building one would be exactly the "reserve nobody
can spend" the amendment warns against. So the guest budget is bounded strictly below the
device (`GuestRatio` 0.85, `ReserveRatio` 0.05) and a device full *for guests* still has
free blocks — which is what the stop's `fdatasync` needs on a delayed-allocation
filesystem (ADR-0013 gap 6, the one part DST cannot model) and what the filesystem's own
metadata needs. The ADR's **1 GiB floor is dropped** with the objects it was sized for;
keeping it would give every device under 20 GiB a budget of zero.

Two consequences that are decisions, not side effects. The share is **static** — dividing
among the volumes actually attached is retroactive (a volume that arrives puts another
volume's healthy guest into backpressure) — and because a share only bounds a device if
the number of shares does, `start` refuses the volume past `-max-volumes` (a local,
defensive power, ADR-0013 §5; the volume stays in the desired state and `Apply` retries).

Proven able to fail, four plants:

- `Share()` returning the whole budget — the DST arm `device-budget-holds-across-volumes`
  (four volumes on a simulated 4 MiB device, budget derived through `agent.NewBudget`):
  `volume ... met the device's ENOSPC after 819200 bytes: the budget did not bound it, the
  device did`, after the first volume held 3268608 of a 3355443-byte budget. The same plant
  turns the e2e arm (`integration/e2e/budget_test.go`, which reads the Agent's own start-up
  line) red with `each of 16 volumes may hold 13148287795 bytes of a 13148287795-byte
  budget: the budget is not divided`.
- the `MaxLocalBytes` check removed: `a thousand records fit inside the bound; the test
  proves nothing`.
- reclaim's accounting removed: `a WRITE after 4080 bytes were reclaimed: wal:
  backpressure`.
- adopt's accounting removed: `a log resumed over 8160 bytes of its 8192-byte share took a
  WRITE with <nil>`.

Production coverage after the increment: **90.1%** (`task cover`, floor 90%).
