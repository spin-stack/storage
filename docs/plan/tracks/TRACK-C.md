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

### C8 — an empty desired state is not a teardown order (2026-08-05)

`VolumeManager.Apply` stopped every volume the desired state did not list. That is right
for a volume missing from a list that names others, and it is the wrong reading of a list
that names *none* — and an empty list is not exotic. A Control Plane brought up against
an empty database sends it, a `GetDesiredState` that returns no rows for a reason that
has nothing to do with this host sends it, and an Agent whose `-host-id` stopped matching
after a config change is sent it too. Under ADR-0026 stopping a volume is what publishes
it, so a spurious empty list made a host upload every session it held and take every
guest's device away in one cycle, with nothing reporting an error anywhere; with wave 2's
holding rule, a host that then could not publish would hold its data directory and refuse
to stop as well.

**The rule is `len(live) == 0 && len(gone) > 0` stops nothing**, argued at the guard in
`Apply` together with the case where it is wrong. `live` and not `desired`, because a
list whose every entry `validateDesired` refused named nothing usable either, and reading
it as a full inventory would tear the host down on the strength of what it had just
rejected. A list that names something is unchanged: it is proof the Control Plane knows
this host and is deciding volume by volume, so an absence *inside* it is a decision about
that volume.

**This does not make a drain impossible, and that is checkable rather than hopeful.**
`cpserver.GetDesiredState` lists on `volumes.primary_host_id` and `cpserver.applyReport`
refuses on the same column, so every real detach, promotion or deletion that empties this
host's desired state also refuses this host's *report* of those volumes — and `Loop.fence`
tears them down in the same cycle, through the door that names the volume instead of the
one that names nothing. Where the rule is wrong is written at the guard: a desired-state
filter the report path does not share would leave a volume served here until something
names it (bounded, because handing it to another host is what changes `primary_host_id`),
and a Control Plane whose catalog is *gone* refuses every report anyway, so the guard buys
nothing in that case rather than harming.

**The guard took on a debt, and it is paid in the same commit.** A detach used to reach
this host as a shrinking desired state and be carried out by `Apply`, which sets no
fencing memory; now it arrives as a refused report, so `Fence` records the epoch. Since
`controlplane.Place` re-places a volume without bumping its epoch, `-detach-volume X`
then `-attach-volume X` naming this same host would arrive at exactly the remembered
epoch and be skipped for the life of the process — a guest whose device never returns,
silently. `Apply` therefore forgets the fencing memory of any volume the Control Plane
has stopped listing, which keeps DEV-0012's rule intact (there the volume *is* still
listed, so the entry survives).

Proven able to fail, three plants:

- the guard's condition forced false — the e2e arm
  (`TestAnEmptyDesiredStateDoesNotTearDownTheHost`, real binaries, real Postgres and
  RustFS, with a reverse proxy that answers only `GetDesiredState` with an empty
  response so the heartbeat and the report still reach the real Control Plane) went red
  in under four seconds with `the Control Plane listed no volumes and the Agent stopped
  volume ... and published [image/.../manifest.json] — an empty list is not a detach
  order, and this host is still the volume's writer`. The unit arm
  (`TestAnEmptyDesiredStateStopsNothing`, nil / empty / only-unparseable-entries) failed
  all three cases with `serving [] after a desired state that named nothing`.
- `Loop.fence` short-circuited — the counterweight
  (`TestADetachedVolumeIsStillStoppedAndPublished`, a real `control-plane
  -detach-volume`) went red with `timed out after 1m0s waiting for the detached volume's
  image to reach the bucket`. Without that scenario the guard would be
  indistinguishable from "this Agent never lets go".
- the fencing-memory forget removed —
  `TestAFencedVolumeIsForgottenOnceItLeavesTheDesiredState` failed with `volume ... has
  no device after being detached and attached back to this host: the fencing memory
  outlived the placement that caused it`. Its middle desired state names another volume
  rather than nothing, so it proves the forgetting on its own rather than through the
  empty-list path.

The Agent also *says* so, every cycle rather than once at the transition — the same
reasoning as `publishHeld`'s holding line: an operator arriving hours later needs the
state to still be announcing itself. `TestAnEmptyDesiredStateDoesNotTearDownTheHost`
waits for the line to repeat and then waits for it to stop once the Control Plane names
the volume again, which is what separates a guard from a permanent refusal to reconcile.

Production coverage at the last `task cover` run that completed during this increment:
**90.0%** against the same 90% floor, measured with the guard and both e2e scenarios in
place. The repeat run after `TestAFencedVolumeIsForgottenOnceItLeavesTheDesiredState` was
added could not build the tree at all — `internal/metadata/metadatatest` was mid-edit in
another lane — and a test that only adds exercised lines cannot lower the figure. Track A
recounts it at integration, which is the only place it means anything.

### C10 — what a second session does with the first session's WAL (2026-08-05)

A restarted Agent re-attaches at the **same epoch** (ADR-0024), so it opens
`<data-dir>/wal/<volume-id>/<epoch>` with the previous session's segments still in it.
Nothing in this tree had ever asked what happens to them, and the answer had a cost
nobody had noticed.

**What happens.** `wal.ResumeAwaitingBase` replays every record it finds into the read
view's top layer, `image.Load` installs the published image underneath as the base, and
`Log.Read` parks until one of the two arrives. So the second session's view is the
previous session's records *over* an image that already holds every one of them — the
same bytes twice, in sequence order, which is why it is not wrong: replay is ordered, so
a WRITE followed by a DISCARD lands as the tombstone either way. The records **above**
the image's sequence — a session that wrote after its last publish — are the only ones
the replay contributes, and they are what the retry story wave 2 built rests on.

**What was wrong is that nothing ever gave the redundant ones back.** `release()` deletes
nothing on purpose (SHUTDOWN-PUBLISH-SPEC §6), the next session adopts what it finds and
counts those bytes against its share (`segments.adopt`), and `TruncateLocal` has had no
production caller since ADR-0026 deleted the checkpoint. A volume started and stopped ten
times on one host held ten sessions of WAL, all of it inside its own image, against a
`Limits.MaxLocalBytes` **no session clears** — the field's own comment says so. The end
of that road is a guest whose WRITEs are refused with `ErrBackpressure` for good, on a
volume whose every byte is safe in the bucket.

**The reclaim lives in `wal.Log.InstallBase`**, because that is the only moment anything
in the process knows the segments are redundant: the caller knows an image covers this
sequence, the log knows which segments that sequence spans, and neither knows both
anywhere else. It is also what the published watermark set three lines above was always
for — that comment already said published must move so `AllowTruncate` would permit
exactly this, for a caller that never arrived. Rejected: doing it in the Agent once
`InstallBase` has returned, which puts one rule in two components, makes the Agent reach
through the log into a segment set it does not own, and leaves a window in which a guest
WRITE is weighed against a footprint the call has already superseded.

**Not a durability review-zone change, and the reason is stronger than "the base has
them".** A log resumed awaiting a base cannot answer a read from its segments at all —
`Read` parks on `baseWait` and fails with `ErrBaseUnavailable` if the base never arrives
— so an unreachable store already refuses to serve, with or without them. Segments below
the installed point are a fallback for nothing. Nothing about a FLUSH ACK, a publish or
a format changed.

Proven able to fail, in **opposite directions**, because either half alone is satisfied
by a wrong answer:

- the reclaim removed — the guest-backed arm
  (`TestASecondSessionGivesBackTheWALItsImageAlreadyHolds`,
  `integration/vhost/lifecycle_test.go`: a real kernel writes and fsyncs, the Agent
  stops and publishes, a second Agent starts on the untouched directory and a second
  boot reads the range back) went red on the filesystem — *"the second session kept 3
  segment file(s) holding 33792 bytes, out of the 3 files and 33792 bytes its own image
  already covers"*. Green it prints *"3 segments (33792 bytes) reduced to 1 (8464
  bytes), and the guest still reads its own pattern"*. It is deliberately the older
  `TestAGuestSurvivesAStopAndComesBackFromItsImage` **with its third step removed**: that
  one deletes the WAL by hand so only the image can answer, this one leaves it where a
  restart actually finds it.
- reclaim's published-point guard removed, so it unlinks everything but the newest file —
  the counterweight (`TestASessionThatNeverPublishedKeepsItsWAL`,
  `internal/agent/resume_test.go`) went red on the bytes: *"offset 524288 reads
  0x0000000000000000 where 0xb2b2b2b2b2b2b2b2 was written and ACKed"*.
- the reclaim's error swallowed and `baseWait` closed anyway —
  `TestALogThatCannotReclaimItsRedundantWALRefusesToServe` (`internal/wal`): *"installing
  a base over a WAL it could not reclaim reported success: the volume would serve from a
  device that has begun refusing operations"*.

**The counterweight needs four sessions and the reason is worth knowing: a wrong reclaim
is invisible to the session that performs it.** Resume has already replayed those records
into memory, so a session that unlinked segments it should have kept still answers every
read correctly; only the incarnation after it finds them gone. Two failed publishes in a
row is not a contrivance — re-attaching happens at the same epoch, so a host whose store
is down across two restarts is exactly this.

**A second thing the second session was keeping.** `Resume` re-encoded every record it
replayed into `Log.resumeTail` — a full second copy of the session's WAL, in memory, for
the life of the process — and `grep -rn resumeTail` found the declaration, one append and
no reader at all. It was the hand-back to the batcher, and the batcher went with the
remote durability chain in ADR-0026. Deleted, with `reencode`, its only caller;
`format.EncodeRecordRaw` stays because the encrypted write path uses it.

Production coverage after the increment: **90.0%** (`task cover`, floor 90%). The
reclaim's error path is what `TestALogThatCannotReclaimItsRedundantWALRefusesToServe`
exists for — without it the figure was 89.9%, since the increment also deletes covered
production code.

### Integration owner, 2026-08-05 — an encrypted clone, and a comment that promised a protection it did not provide

Every clone test in the tree ran **unencrypted** — the two DST scenarios and the e2e lane
all pass `nil` for the encryption — so a clone had never once opened a chunk its parent
sealed. `TestAnEncryptedCloneReadsItsParentsChunks` closes that: a parent writes, is
snapshotted, and a clone with its own id, its own data directory and its own empty WAL
reads the parent's bytes back through its device, with an INV-15 check in the middle so a
clone that succeeded *because nothing was sealed* cannot pass.

**Writing it corrected a false claim in production code.** `parentEncryption`'s comment
said the chunk AAD binds the volume id, so the DEK must be re-bound to the parent's, and
that handing the clone its own `Encryption` "would fail to open every chunk the parent
wrote". Planting exactly that changes **nothing**: `image.chunkAAD` takes the volume id as
a *parameter*, and `LoadSnapshot` is already handed the parent's, so what opens a chunk is
`enc.DEK` plus that argument — and the DEK is identical either way. The re-binding is
inert for image chunks.

The call is kept and the claim corrected, rather than the reverse: an `Encryption` bound
to the wrong volume is the wrong object to hold on a path whose subject is another
volume's data, and `wal.Encryption` *does* bind the id for WAL records. What is not kept
is a comment asserting a protection that is not there — that is the shape this repository
has spent four waves removing from its documents, found this time inside a function.

The plant that does work is passing the clone's own id to `LoadSnapshot`: the snapshot key
is derived from the volume id too, so it fails at once with "which was never published".

### C-a-clone-works-exactly-once — a clone that stopped once could never start again (2026-08-06)

`CHUNK-ADDRESSING-SPEC` §2 found it and this lane reproduced it before changing anything.
`image.Load` returns an **unlayered** view, `fetchBase` then slid the parent's snapshot
underneath it with `SetBase`, and `cow` refuses that — *"cow: this map was not built to
take a base (use NewIntervalMapOver)"*, the exact string the spec quotes. The error path
sets `baseFailed` and calls `FailBase`, so every read of the restarted clone came back
`ErrBaseUnavailable`, and the Agent then could not publish that session either
(`ErrNoReadView`), so it exited 1 with the session in a local WAL nobody would read. A
clone booted fine the first time — no image of its own, so the parent's snapshot *is* the
base — and never again.

**The fix is to stop layering, not to make the layering possible, and the refusal is the
reason.** A publish serialises `log.ViewAtRest()` and `image.uploadChunks` walks
`view.Ranges()`, which is the base's ranges minus this layer's tombstones plus its own
extents, read *through* the layering — so a clone's own image already holds its parent's
bytes, flattened, from its first stop. The parent is redundant once the manifest exists.
It is worse than redundant: tombstones do not survive publication as tombstones, a range
the clone discarded is expressed in the manifest as *absence*, and putting the parent back
underneath uncovers every DISCARD the guest issued — §14.6's failure. Weakening `SetBase`
to accept an unlayered map would have converted a total outage into silently resurrected
data. `SetBase` is untouched. `parentView` is now reached only in the `ErrNotPublished`
arm, which also stops a restarted clone from GETting its parent's whole snapshot on every
attach for a view it then threw away.

**Two arms, because the seam that missed it and the bytes are different questions.**
`TestACloneThatStoppedOnceStartsAgainAndReadsBothHalves` (`internal/agent`) drives a real
`VolumeManager`, a real WAL and a real device across two sessions on one disk and one
bucket, and asserts three offsets: one only the parent wrote, one only the clone wrote,
and one the parent wrote and the clone overwrote — a single offset cannot tell dropped
inheritance, a lost own-image and an inverted layer order apart.
`TestACloneThatStoppedOnceIsServedAgain` (`integration/e2e`) is the deployment: real
binaries, real Postgres, real object store, the clone stopped and its host restarted, with
the second Agent's recovery line *for the clone's volume id* and its exit status as the
observables.

**Both plants were run.** The unit arm was red against the tree as it stood, before any
production line changed, with `blockdev: READ of 512 bytes at 0 failed: wal: wal: the read
view's base could not be recovered: cow: this map was not built to take a base` — the
defect quoted by the spec, reproduced rather than inferred. Restoring the layering on top
of the fix reproduces it in the e2e lane too: agent-2 prints *"the clone's parent could not
be layered under its image; its reads will fail"* and the recovery line never arrives. A
second plant — never making the parent the base at all, the wrong fix that also gets past
`SetBase` — turns the first session's inherited range to zeros: *"offset 0 reads
0x0000000000000000, want 0xa1 repeated"*.

**No DST scenario was added, deliberately.** `DurableRangeChecker` watches for zeros and
foreign bytes and explicitly does *not* treat a refused read as a violation — which is the
right rule and means it could not have caught this. A third arm would have bought no
catching power for the cost the plan warns about: a new mandatory scenario edits
`pinnedMandatorySet`, and at most one lane may add one per merge window.

Nothing here touches the addressing decision `CHUNK-ADDRESSING-SPEC` §3 puts to a human.
This is §4's "the §2 fix now, on its own".

### C11-measured — what the duplicate actually costs, and three things the spec assumed that are not so (2026-08-07)

`CHUNK-ADDRESSING-SPEC` §8 asks a human whether `chain_depth` is a structure or a label and
recommends *label* — keep flattening, pay a duplicate per link. Nothing in the tree had ever
weighed that duplicate, so this increment weighed it. **The addressing is untouched: that is
the owner's decision and it is not made here.** What changed is that the price is now a set
of assertions rather than an impression, and that a stop paying it says so.

The scales are `internal/image/clone_cost_test.go`, and every number below is asserted by a
test rather than written down, so it cannot go stale in silence:

```
go test ./internal/image/ -run 'TestACloneFirstStop|TestASecondStop|TestTheBucketHolds|TestAWriteThatJoins' -v
go test ./internal/agent/ -run TestAnOperatorIsToldWhenAStopIsCopyingItsParentsDataset -v
```

Three numbers are reported for each shape, because one answers none of the questions
honestly: **transferred** (what the store was handed, from a decorator over `Put`),
**resident** (what the bucket then holds, from `List`) and **distinct** (what one
content-addressed namespace would have held, folded by the digest each key ends in — which
is computable from outside the process only because a chunk's key *is* the digest of its
plaintext). `resident - distinct` is exactly what the spec's option 2 would save.

**The headline is confirmed and it is exact.** A parent holds 8 MiB; a clone writes one
512-byte sector and stops. Its first stop transfers one object of 8388636 bytes — 8 MiB plus
the nonce and tag of one sealed chunk — for 512 bytes of new information, and the bucket then
holds 16777272. The cost is linear in the parent's dataset, so multiply by whatever a golden
image weighs.

**But the spec overstates when it recurs.** §3 prices option 1 as a duplicate "paid at the
clone's first stop **and at every snapshot of the clone**". The second half is not true: a
snapshot taken straight after the first stop transfers no chunk bytes at all, and neither
does an unchanged second stop, because `uploadChunks` skips a chunk whose key already exists
and by then the chunks are under the clone's own prefix. The duplicate is paid **once per
clone**. Planting a `Head` on a key that cannot exist takes that snapshot from 0 to 8388636,
which is what the spec describes and what the code does not do.

**And it overstates what option 2 would buy, which is the finding that changes the
decision.** §3 says a per-lineage chunk store makes "the storage cost of a clone
proportional to what the clone wrote". It does not. The saving is bounded by *chunk
granularity*, and with `MaxChunkBytes` at 64 MiB the golden image the whole workflow is built
around is **one chunk**:

| the parent's 8 MiB, laid out as | the clone writes | resident | distinct | option 2 would save |
|---|---|---|---|---|
| one contiguous run (one chunk) | nothing | 16777272 | 8388636 | all of it |
| one contiguous run (one chunk) | one 512-byte sector | 16777272 | 16777272 | **nothing at all** |
| eight regions (eight chunks) | one 512-byte sector | 16777664 | 9437436 | seven eighths |
| one contiguous run (one chunk) | every byte it inherited | 16777272 | 16777272 | nothing, and nothing is owed |

A single sector changes its chunk's content, so its digest, so no key space can make it
shared. The lever with the larger effect on the golden-image workflow is therefore the
**chunk size**, not the key space — and that is a much smaller change than moving the key
space and the AAD.

**The duplication is not a property of cloning.** Two volumes that were never related and
hold the same bytes hold 16777272 resident bytes and 8388636 distinct ones — the same
duplication as a clone, because `chunkKey` names the volume. A fleet booting N machines off
one image pays N copies whether they are clones or were created and written independently.
Answering §8 "structure" changes what a *clone* reads and touches none of that, so if the
owner's motive is bucket cost, `chain_depth` is the wrong variable to decide it on.

**And a cost nobody has priced is larger, in the limit, than the one being priced.** No
production caller deletes an object — `grep -rln '\.Delete(' --include=*.go internal/ cmd/`
reaches only test files: the object store's own conformance suite and one forwarding
decorator in a WAL test. So a superseded chunk stays for ever: after three stops a clone's prefix holds 8388636 bytes named by no manifest.
Chunk identity is fragile in a way that makes this worse than it sounds — `uploadChunks`
chunks a range relative to its own offset and `cow.Ranges` merges runs that touch, so a
512-byte write into the gap between two 1 MiB regions transfers 2097692 bytes as a single new
object and orphans the 2097208 it replaced. **The duplication is a cost per link, paid once;
the orphaning is a cost per stop, unbounded in time.** Neither option in the spec fixes it —
option 2 gives a re-chunked object a new digest too — and it belongs to
`DELETION-AND-RECLAIM-SPEC`, whose reclaim rule ("this volume's manifests, set difference")
would already collect it.

**One property nobody had written down, found by a fixture that was wrong.** The fragmented
parent originally wrote eight identical regions and produced *one* object, not eight: within
a single volume, identical content already dedups, because the key is the digest. The fixture
now uses different bytes per region, and the property is worth knowing when pricing a guest
filesystem full of zeroed or repeated blocks.

**Where this leaves the recommendation.** *Label* survives the measurement, but not for the
reason §4 gives. §4 defends it as "the storage cost is real but bounded per link"; the
measurement says it is bounded per link **and** that answering "structure" would not remove
most of it — the one-chunk case and the unrelated-volumes case are both untouched by a chain
walk. So the sentence §20 owes a reader is not "a clone costs a duplicate per link". It is:
*a volume costs a full copy of everything it holds, and content addressing dedups only within
its own prefix and only at chunk granularity.* That is a bigger and simpler claim, and it is
true of every volume rather than only of clones. The owner's decision is unchanged in
substance and much better priced.

**Making it visible** is `Volume.sayWhatThisImageCosts`. `fetchBase` records what the read
view took from a parent's snapshot rather than from an image of its own, and the publish
prints it **before** the upload — the operator's question is asked while the stop is hanging,
and a line printed when the publish finishes cannot answer a question about why it has not
finished. The size is a fold over `Ranges()`, the O(extents) walk `cow.Cost` deliberately
avoids; that is right here and wrong there, because `Cost` is recorded on every guest fsync
and this runs once per stop, immediately before an upload of exactly these bytes. `Cost().Bytes`
was the alternative and is wrong for a second reason: it sums the layers, so a clone that
rewrote its parent would report 16 MiB of an 8 MiB upload.

**Two series are owed to track E, and this lane did not write them**, because
`internal/obs/metrics.go` is track E's and `Recorder.Gauge` silently no-ops on a name that is
not in `obs.Catalog()` — recording them first would be a metric that looks recorded and emits
nothing:

- **`image_bytes`**, gauge, labels `[volume]` — bytes of guest data in the image a volume last
  published. There is no series for what the bucket holds; `image_publish_duration_seconds`
  says how long it took and never how much.
- **`image_inherited_bytes`**, gauge, labels `[volume]` — of those, the bytes that came from a
  parent's snapshot rather than from this volume's own image. Nonzero only in the one session
  before a clone's first stop, which is the only session that pays. Both would be recorded at
  the call sites of `sayWhatThisImageCosts`, which already holds both numbers.

`chain_depth` still has no producer and the Agent is the wrong place for one (ADR-0021 keeps
it from knowing what a Control Plane is); it is owed by whoever owns `controlplane.Clone`,
and §8's answer decides whether it should exist at all.

**No `integration/e2e` arm, deliberately.** No guest boots in that lane, so a clone's parent
there has no data and the inherited number would be zero — an assertion that could not
distinguish "the line is right" from "the line is empty". The deployment statement this
increment can honestly make is the one in `internal/agent`, over a real `VolumeManager`, a
real WAL and a real device, asserting the clone's prefix holds exactly what its parent's
holds **and** the line that says so. The line's arm asserts the parent's stop too, and that
is not padding: a message printed on every publish satisfies any assertion about the clone's.

**Every measurement was planted.** Keying chunks bucket-wide takes the clone's first stop to
0 objects and 0 bytes and the bucket to a single copy — and the chain-of-three arm then fails
with `crypto: authentication failed`, which is the AAD blocker §3 predicts for that naive
form, reproduced rather than argued. Disabling the `Head` skip makes the snapshot pay 8388636
where the assertion wants 0. Inverting the condition in `sayWhatThisImageCosts` makes the
clone print a line with no `inherited_bytes` and the parent claim it is copying an inherited
dataset, and four assertions go red.

---

## C — DEV-0011's blocker cleared, and the entry should be closed rather than fixed

`docs/plan/SEGMENT-RESERVATION-SPEC.md`. A spec, no code: the deliverable of a
review-zone item whose review concludes that the review zone is not entered.

**The wait is over and nothing noticed.** DEV-0011 ends "**Waits on ADR-0013**, where the
Agent knows a volume's share of the device budget". Wave 3 landed exactly that —
`agent.Budget`, `Budget.Share()`, `Budget.Limits()` producing `wal.Limits.MaxLocalBytes`,
and `NewVolumeManager` refusing to start without one — and ADR-0013's own header has said
**Accepted** since 2026-08-03. `STATUS.md` still calls it `Proposed` in two places; that is
track A's file and track A's correction, noted here so it is not lost.

**The finding does not survive contact with the code, in either half.** DEV-0011 asks that
a segment be charged before its first append "so out-of-space is reached at a segment
boundary rather than mid-record", and concludes that needs `fallocate` plus a durable write
offset — a format change. Both halves are wrong:

- The *budget* is already charged before the record. `Log.backpressure` runs in
  `Log.appendEncoded` ahead of `segments.appendRecord`, against `segments.retained()` plus
  the encoded record plus one `format.SegmentHeaderSize` — that last term exists precisely
  so the record that rotates a segment is not the one record the bound misses, and its
  comment says so. A WRITE is refused whole, at a WRITE boundary, which is finer than a
  segment boundary. Only the *device* ceiling can be met mid-record.
- Reserving device blocks does not require growing the file. `FALLOC_FL_KEEP_SIZE` reserves
  the range and leaves `i_size` alone, and `unix.Fallocate` is already reachable from
  `internal/simio/real` (it is where `unix.Flock` comes from). No zero tail, no delimiter
  problem, no format change, no §25.2 work — the on-disk byte sequence is identical, which
  is the definition of not being a format change.

**What replay does with a padded tail was measured, not argued**, with a scratch test that
grows a real segment via `File.Truncate` (the simulated disk zero-fills a grow) and tears
one with `sim.Disk.TornTail` + `Crash`. A healthy half-full segment becomes
`format.ErrBadMagic` — not a corruption case, an ordinary one, and `wal.resume` surfaces it,
so the volume does not attach. A torn header becomes `format.ErrHeaderCRC` and a torn
payload `format.ErrPayloadCRC`, where today both are `format.ErrShortBuf` and a clean stop.
Padding does not weaken the distinction between "the crash caught us mid-write" and "this
file is damaged": it deletes it.

**The sentence the spec turns on:** the durable write offset already exists and it is the
file's own length — free, updated atomically by the kernel, needing no CRC because the bytes
are not ours, and impossible to tear. `WAL-SEGMENTS-SPEC.md` already rejected writing back
into a segment header once, for `LastSequence`/`RecordCount`; a write offset is the same
shape and worse, because it would be rewritten on every `Sync` rather than once at seal, and
its ordering against the record bytes would pull the FLUSH/FUA ACK rule into the change.

**The recommendation is to close DEV-0011 as accepted, not fixed**, and the residue is one
ordinary increment the owner can take or leave: reserve at `segments.create` with
`FALLOC_FL_KEEP_SIZE`, which makes `Log.broken`'s precondition — a rollback that fails after
a partial append, the only outcome here that costs a whole session under ADR-0026 —
unreachable, and removes the delayed-allocation ENOSPC-at-fsync case `wal/degraded.go`
records as unmodellable. The question left for a human is the one the code cannot settle:
whether to want a property that holds on ext4 and XFS and is quietly absent on NFS and some
ZFS versions. If yes, the Agent must say at startup which world it is in and the simulator
must model the filesystem that refuses — otherwise every test sees a guarantee production
may not have.

## C — `parentView` walks the lineage (step 1 of the chunk-addressing chain)

**What landed.** `parentView` follows `parent_snapshot_id` upward until a volume with no
parent, loading each ancestor's snapshot manifest and layering them oldest-first, where it
resolved exactly one link before. `image.LoadSnapshotOver` is the loader that hands a
manifest a base; `cow` needed nothing — `NewIntervalMapOver` nests arbitrarily and both
`Ranges` and `Read` already recurse. `descriptor.json` gains `parent_volume_id`, and
`controlplane.Clone` writes it.

**Why the descriptor grew a field, which the spec did not anticipate.** §3 says to read
each ancestor's `descriptor.json` for the next link, and cites `ParentSnapshotID` as
carrying it. It does not: a snapshot id addresses nothing without the volume it lives
under (`image.SnapshotKey`), so the descriptor recorded half a link and the other half was
in the catalog as `snapshots.volume_id`. The bucket could not state a lineage the spec
assumes it is the authority on. It is an on-S3 format change, additive, and the §25.2
property test covers it by drawing the link rather than leaving it empty — `omitempty`
keeps an unset field out of the stored bytes, so a field that is never non-empty in the
generator is a field no truncation and no bit flip ever reaches.

**The thing step 3 cannot omit, and the spec does not mention.** A manifest states a
DISCARD as *absence* — `cow.Ranges` subtracts tombstones and its comment says the
distinction cannot survive an image that does not carry them — and absence in a layered
chain means "ask the layer below". So the walk answers a range an intermediate clone
discarded and then snapshotted with its grandparent's older bytes, where the snapshot it
descends from reads zeros (§14.6). It is the same failure `fetchBase` refuses one level
down, and it needs depth 2 to reach, which nothing outside the tests added here builds.
**Stopping the flattening therefore cannot land without a tombstone in the manifest**: at
that point absence carries the whole meaning of the format — it is how a delta says "the
layer below has this" — so a discard needs a way to say the opposite. It is a format
change and it belongs to the step that makes the manifest a delta, not to this one. The
alternative considered was to keep the single link until then, which leaves a walk with no
caller and moves the discovery into the step least able to afford it.

**"Harmless while publishing still flattens" is true in one direction only**, and it is
worth writing down because the forced order rests on it: while every manifest is
self-contained, layering the ancestors below the nearest changes the answer on exactly one
set of ranges — the ones a nearer ancestor discarded. Everywhere else the nearest
ancestor's own extents shadow them.

**Failing closed is the whole of the error handling.** A cycle, a descriptor that is not
there, a snapshot that was never published, a snapshot id with no volume: each is refused
by name. A walk that stopped early on any of them would install a base holding however
much of the lineage it read, and a partial view is indistinguishable from a complete one
to the guest reading it. `maxChainWalk` is a termination guard and deliberately not a
depth limit — it sits far above the design document's ceiling of five, because refusing at
attach would make a volume the fleet created successfully unreadable. Step 4 makes the
refusal, at create.

**What the tests had to fabricate, and why that is honest.** A depth-3 lineage built by
driving three Agents proves nothing: publishing flattens, so the nearest ancestor's
manifest alone answers every read and a walk that stops after one link changes not one
byte. So the ancestors' snapshots are published through the real publisher from *unlayered*
views — each manifest naming only that volume's own ranges, which is what step 3 makes the
publisher produce. Planted, a walk that stops after one link reads
`0x0000000000000000` at offset 0 where the great-grandparent wrote `0xa1`; stopping after
two leaves that offset wrong and the grandparent's right. The DST arm
(`a-clone-of-a-clone-reads-its-grandparents-bytes`, pinned in the same commit) fails the
same plant with "read zeros at 0 for a range its grandparent wrote" and a trace carrying
`zeros_after_restart=true`.

**Three clone tests and two DST scenarios were fabricating a parent no Control Plane could
have created** — a volume with a published snapshot and no descriptor. They now write the
one a provisioner writes. That is the walk's new precondition made visible rather than
worked around.

**`inherited_bytes` was double-counting the moment a chain existed.** It was
`Cost().Bytes`, which sums every layer, so a range an ancestor wrote and a nearer one
rewrote counted once per layer; a three-link lineage would report three times the dataset
an operator is watching upload. It is now the fold over `Ranges()` that
`sayWhatThisImageCosts` already used, which is the bytes the publish actually moves.

## C — the chunk store moves to the lineage (step 2 of the chunk-addressing chain)

**What landed.** Chunks are keyed `chunks/<lineage-root>/<digest>` and the AAD binds the
lineage root; `image.Ident{Volume, Lineage}` is how the two ids travel, `image.Prefix`
still owns the manifests and snapshots, and `image.ChunksPrefix` is new. `agent.fetchBase`
resolves the root from the walk step 1 built — `parentChain` returns the ancestry
nearest-first, so its last link is the root — and does it *before* it loads anything,
because a clone cannot find one byte of its own image without it. `task ci` green, the
integration and e2e lanes green including the real-kernel one, production coverage 90.35%.

**The nonce argument still holds, and it holds better than it read.** This was the part
the decision block flagged as not mechanical, and the answer is not "it survives" but "it
was true at the wrong scope and the key space is what fixed it". The doc rested on two
facts together — the nonce is drawn, and a chunk is sealed exactly once ever because an
existing key is skipped — and the second held per *volume* while the DEK has always been
per *lineage*. A clone re-sealed everything it inherited under its parent's DEK with a
fresh nonce, once per link: same plaintext, N nonces, one key. Never a reuse, never
unsafe, but the sentence claiming safety was not the sentence providing it. What provides
it is that a chunk's key is the digest of its plaintext, so two seals landing on one key
are two seals of the same bytes; the skip keeps the *number* of nonces drawn under one DEK
proportional to distinct content rather than to activity, which is what a drawn nonce's
birthday bound needs. Both rejected derivations keep their reasons, and the
generation-counter one gets worse: a lineage now has several volumes publishing into one
key space, each with its own generation.

**The one thing that would break it, written at the code and repeated here because it is
somebody's future decision:** two DEKs inside one chunk prefix — a DEK rotation applied to
a volume rather than to a lineage. Nothing rotates a DEK anywhere in this repository
today. If one is added it must be per lineage; if it is not, the failure is closed rather
than silent, because the create-only PUT means the first ciphertext owns the key and the
other volume's load fails its authentication.

**Where the root comes from, and the descriptor walk was enough.** The task asked to say
if it was not. It is: every case the Agent has is answered by the walk it already does —
a volume with no parent link never touches the store, a clone with no image walks anyway
for `parentView`, and a clone *with* an image now walks too, which is the one behaviour
this step adds and the one cost it adds (one small GET per link at attach, bounded by the
ceiling step 4 will enforce). Recording the root in the volume's own descriptor was
rejected: one GET whatever the depth, but it duplicates a fact the chain already states,
and a duplicate that can disagree with the chain is a way for a volume's chunks to be
written under a prefix its own ancestry says is the wrong one. No Control Plane lookup was
added and `DesiredVolume` did not grow a field, so ADR-0021 is untouched.

**Not under `image/<root>/chunks/`, and the difference is a delete.** The spec's literal
`chunks/<lineage-root>/<digest>` is what landed, top-level. These bytes outlive the volume
that first wrote them — a clone reads them after its parent's manifest is gone — so a
delete that clears `image/<root>/` must not be able to take a descendant's data with it.

**The measurements were re-weighed, not deleted, because they are what the decision was
made on.** A clone that stops without writing now transfers **zero** where it transferred
its parent's whole dataset; one sector into an eight-chunk parent costs one region, not
eight; one sector into a one-chunk parent still costs the whole chunk, and no key space
can give that back — with `MaxChunkBytes` at 64 MiB that is the golden-image case, so
"proportional to what the clone wrote" is true at *chunk granularity* and should be quoted
that way. A chain of three holds one copy of its dataset; two unrelated volumes holding
identical bytes still hold two objects, which is not a defect to fix later: they share no
key, so they cannot share an object.

**Four plants, because the format's whole claim is a pair of directions.** Binding the
seal to the writing volume: *"a clone could not open the chunks its parent sealed: crypto:
authentication failed"*. Dropping the lineage from the AAD: *"an unrelated volume opened a
chunk sealed for another lineage: the blast radius of a chunk is the bucket, not the
lineage"*. Keying chunks by the volume again: the round trip still passes — which is the
point of asserting on the bucket — while the location assertion and every cost number go
red. Never resolving the root in the Agent: four clone tests and two DST scenarios fail
with `chunk … not found`, which is the seam that no assertion inside `internal/image`
could have caught, since they all supply the pair by hand.

**Assertions that had quietly stopped covering anything, found by moving the objects.**
Every "the bucket holds nothing for this volume yet" check in the e2e lanes and the
plaintext-leak checks in `internal/agent` and the DST leak scenario listed `image/<vol>/`,
which held the chunks until this change and now holds a manifest. They would all have gone
on passing over nothing — including INV-18's "a guest WRITE is zero PUTs", whose whole
subject is a chunk appearing where none should. The DST `hidingStore` had the same hole:
it hid `image/` and would have left the chunks readable. Fixed in the same wave, in their
own commit.

**One test-only affordance is on the deadcode allow-list**, `image.OwnLineage`: production
never builds one, because the Agent derives the pair from the walk and for a root the walk
leaves the volume's own id in place. Tests, DST and the lanes build root volumes
constantly.

**What step 3 must know.** A manifest still means *everything this volume can read* — the
publish still flattens, so every manifest is self-contained and the walk is still only
load-bearing for ranges an intermediate ancestor discarded (step 1's entry says why). When
step 3 stops flattening, a manifest becomes *what this volume itself wrote*, and three
things stop being true. First, a manifest can no longer be read alone: `image.Load`'s
result becomes a layer, and the caller must layer it over the ancestry — which is why
`Load` returns an unlayered map today and why that is a protection, not an obstacle
(`agent.fetchBase`). Second, absence in a manifest stops meaning "this range is empty" and
starts meaning "ask the layer below", so **a discard needs a tombstone in the manifest** —
step 1's entry calls this the thing step 3 cannot omit, and it is a second on-S3 format
change with its own §25.2 work. Third, the `TestASecondStopPaysOnlyForWhatItTouched` fold
is the shape reclaim now needs: a chunk is unreferenced only when *no manifest in the
lineage* names it, across both volumes' images and both volumes' snapshots, and that fold
exists nowhere in production code.

**Two documents still describe the old key space and this lane did not edit them**, since
neither is track C's: `STATUS.md`'s DEV-0020 note about chunks under
`image/<vol>/chunks/`, and `DELETION-AND-RECLAIM-SPEC`'s object table and its §
establishing that `chunkKey` embeds the volume id. The second matters most — its reclaim
rule is bounded by a volume's own prefix, which is exactly what stopped being true.

## C — publishing stops flattening (step 3 of the chunk-addressing chain)

**What landed.** `image.uploadChunks` walks `view.DeltaOver(inherited)` where it walked
`view.Ranges()`, so a manifest states the layers *this* volume holds and leaves its
ancestry to the chain. `agent.fetchBase` composes the ancestry on every attach and hands
`image.Load` a base where it used to pass none; `Volume.inherited` is what publish and
snapshot must not write down. `cow.Delta`/`DeltaOver` is the new serialising half of the
layering, and `Ranges` is now written in terms of it so the rules have one implementation.
`task ci` and `task ci:full` green — including `db:verify`, the RustFS backend
conformance, the real-kernel `integration/vhost` lane and `integration/e2e`. Production
coverage 90.40%.

**The cost fell where the spec said it would not, and that is the finding.** Every number
below is an assertion in `internal/image/clone_cost_test.go`, the same scales the decision
was weighed on, re-run through the three key spaces this repository has had:

| a clone of an 8 MiB parent that… | per-volume keys | per-lineage keys | a delta |
|---|---|---|---|
| stops without writing anything | 8388636 | 0 | 0 |
| writes one sector, one-chunk parent | 8388636 | 8388636 | **540** |
| writes one sector, eight-chunk parent | 8388636 | 1048604 | **540** |
| overwrites every byte it inherited | 8388636 | 8388636 | 8388636 |
| writes past what it inherited | 528 | 528 | 528 |

The second row is the golden-image case — `MaxChunkBytes` is 64 MiB, so a golden image is
one chunk — and `C11-measured` and step 2's entry both concluded that no key space could
recover it: a sector written into a chunk changes that chunk's content, so its digest, so
there is nothing to share. Both were right, and both were about the wrong lever. Stopping
the flattening recovers it completely, because the manifest then names the sector rather
than the chunk the sector fell in. **"Proportional to what the clone wrote" is now true as
`CHUNK-ADDRESSING-SPEC` §3 stated it, rather than true at chunk granularity**, and the
weaker form was the honest one to insist on right up until this step. The seam agrees: in
`internal/agent`, a clone that writes one block into one of a parent's two regions adds 512
bytes to its lineage's chunk store where it added 2048, and its publish line carries
`image_bytes=512` beside `inherited_bytes=4096`.

The two rows that did not move should not have. A clone that overwrote everything it
inherited wrote everything it uploaded, and a disjoint write costs the disjoint write.

**The tombstone was the core of the step, not a footnote, and it is a second on-S3 format
change.** In a flattened manifest, absence meant zeros. In a delta it means *ask the layer
below* — that is what a delta is — so the same absence hands a guest its ancestor's older
bytes at every offset it freed. That is §14.6 and it is worse than losing the erasure: a
filesystem has already returned those blocks and may have handed them to something else.
`Manifest.Discarded` carries them, `loadManifest` replays them as `cow` tombstones, and
`cow.DeltaOver` reports none at all when nothing is inherited, because with no layer below
absence and an erasure are the same statement and storing the second in every manifest for
ever buys a distinction no reader can act on.

**`image.Load` therefore returns a layerable map, which is the exact reversal of the fix in
`C-a-clone-works-exactly-once`, and the reversal is safe for the reason that entry gave.**
That entry refused to weaken `cow.SetBase`, on the grounds that tombstones did not survive
publication and sliding a parent under a published image would uncover every DISCARD — "a
total outage converted into silently resurrected data". The premise was true and it is the
premise this step changed. The protection has not been dropped; it moved into the format,
where it also survives a restart, which the refusal never did.

**What a clone stopped being, and where that lands.** A clone is no longer self-contained
and never becomes so, which touches four things, checked rather than assumed:

- **A restart.** Covered, and it was the thing most likely to break: `image.Load` now needs
  the ancestry to answer anything the clone did not itself write.
  `TestACloneThatStoppedOnceStartsAgainAndReadsBothHalves` and the e2e
  `TestACloneThatStoppedOnceIsServedAgain` pass unchanged, which is what one wants of a test
  written before the change it survives.
- **`-rebuild-metadata`.** Unaffected, and worth saying why rather than leaving it: it reads
  descriptors and `image.ReadSnapshotManifest` for a sequence number, never a view, so a
  manifest that names less says nothing different to it. The parent link it restores comes
  from the descriptor (step 1 added `parent_volume_id`), and `cpserver` still resolves the
  parent *volume* from the snapshot row, so a rebuilt catalog produces a walkable desired
  state.
- **A snapshot of a clone** is a delta too — `snapshot` passes the same `v.inherited` — so a
  clone of that snapshot walks through it to the same root. Measured: a chain of three holds
  one copy of its dataset, and the middle link's snapshot manifest names nothing of the
  root's.
- **A delete of a parent** now takes its descendants' data with it, permanently, and there is
  no window in which it does not. That is `DELETION-AND-RECLAIM-SPEC`'s answer B — flatten
  the descendants first — and its FLATTEN does not exist yet. **Between here and step 4 a
  parent with clones cannot safely be deleted at all**, which the decision block predicted
  and which nothing in the code refuses, because nothing in the code deletes.

**What depth costs at attach, which is step 4's number.** `TestWhatDepthCostsAtAttach`
drives a real `VolumeManager` over lineages of one, two and three links and counts the reads
an attach issues:

| links | first attach (no image of its own) | restart (with one) |
|---|---|---|
| 1 | 5 | 7 |
| 2 | 9 | 11 |
| 3 | 13 | 15 |

Four object reads per link — the ancestor's descriptor, a Head and a Get for its snapshot
manifest, and one Get per chunk that manifest names (one, in this fixture) — plus one for
the Head that finds no image of its own, or three for a manifest, its Get and its chunk. **A
real golden image is many chunks, and that is where the depth cost actually lives**: the
per-link constant is `3 + chunks named`, and only the 3 is bounded by anything a ceiling
controls. At the design document's ceiling of five, a restart is 23 round trips plus the
chunk Gets; at `agent.maxChainWalk`'s termination guard of 64 it is 259, which is what that
guard being far above the ceiling means in practice.

The restart row is the one that changed. It used to be a constant — a flattened image
answered every read at any depth — and it is now the same slope as the first attach, which
is what makes a ceiling a bound on a volume's whole life rather than on its first boot.

**No guest can issue a DISCARD, and the tests say so rather than pretending otherwise.**
`VIRTIO_BLK_F_DISCARD` is not offered (`internal/vhost/features.go`) and `wal.Log.Discard`
has no production caller, so the erasure cannot be driven through a device. What a DISCARD
*is*, when the bit is eventually negotiated, is `cow.IntervalMap.Clear` on the log's read
view — `Log.appendClear` calls exactly that — so that is what `internal/image`'s arm drives,
on a view composed the way `fetchBase` composes one. The Agent lane reaches the other side
of the same format question and asserts it through a real manager: an ancestor whose
published snapshot carries an erasure, read through the chain, across a stop and a restart
(`TestARangeErasedInTheMiddleOfALineageStaysErased`). The format has to be right before the
feature bit is offered, not after.

**Four plants, all run.** Dropping the tombstone at the *publisher* takes the §25.2 round
trip red at the first draw and leaves the erasure absent from the bucket. Dropping it at the
*loader* makes a restarted clone read `0xa1a1a1a1a1a1a1a1` where it must read zeros — in
`internal/image`, and again through a real `VolumeManager`, which is data resurrection
observed from outside the process and the outcome this whole step is arranged around.
Removing the tombstone handling from `cow.delta` fails the delta-plus-ancestry property with
`byte 0: the view holds 0, the delta over its ancestry holds 1`. Restoring the flattening in
the Agent's publish takes the seam measurement from 512 bytes back to 2048 and the depth-3
restart from 15 object reads to 18 — the second of which is the one worth having, because a
cost regression that only shows up in bytes is invisible to every correctness test here.

**What §25.2 covers and what this format still cannot detect, stated because the second half
is a real hole and it is one this step widened.** Truncation is total: a manifest is JSON, so
every prefix fails the load. **A bit flip inside a manifest is not detected.** The digest
protects a chunk's *contents*; nothing protects the manifest naming them, and flipping one
bit of a decimal digit yields another decimal digit — `"offset":1024` becomes
`"offset":1025` and the load succeeds with data in the wrong place. That exposure is not new
(`chunks[].offset` has had it since the format existed) and `discarded` inherits exactly it,
but it is **worse in consequence** now: a moved tombstone uncovers an ancestor's bytes rather
than misplacing this volume's. Closing it is `descriptor.frame`/`unframe` applied to this
object — a digest line over the bytes *as stored*, which `DESCRIPTOR-DIGEST-SPEC` already
proved cannot be over a re-encoding — and it was deliberately not folded in here: it is an
independent on-S3 format change in a review zone, and `PARALLEL-PLAN.md` says not to land a
sixth spec in the same commit as its code. It is owed by whoever takes the next format
increment.

**No DST scenario was added, deliberately, and this is the second lane in a row to say so.**
The mandatory set may grow by one per merge window and step 1 already spent it on
`a-clone-of-a-clone-reads-its-grandparents-bytes` — which is now exercising the real
publisher's output rather than a fixture anticipating it, so it got stronger for free. The
erasure has no arm there either way: `DurableRangeChecker` watches for zeros and foreign
bytes, and resurrection *is* foreign bytes, so a scenario is worth having — it is the
cheapest thing left on this subject and it belongs to the next window.

**DEV-0020 is implemented and this lane cannot close it.** Its subject — "removing the
flattening before the chain read exists converts a storage-cost defect into silent zeros at
chain depth 2" — is exactly what steps 1 to 3 answered, in that order and for that reason.
Striking the heading is an edit to `STATUS.md`, which is track A's file, and
`hack/dev-entries-open.txt` must lose its line in the *same* commit or `task dev:entries`
fails both ways. So it stays open here, and closing it is one commit for whoever owns
`STATUS.md`. DEV-0024 waits on the same answer.

**What step 4 must know.** The table above is the ceiling's arithmetic. Two things beyond it:
`agent.maxChainWalk` is 64 and is a termination guard, not a policy — refusing at attach
would make a volume the fleet created successfully unreadable, which is why the refusal
belongs at create. And FLATTEN is now load-bearing for two separate reasons rather than one:
it is the only way back under the ceiling, *and* it is the only way to delete a parent that
has clones without destroying them.

## C — FLATTEN, the operator one-shot (step 4b of the chunk-addressing chain)

**What landed.** `control-plane -flatten-volume <id>` writes an image that states the whole
dataset, re-seals it under `chunks/<that volume>/`, and rewrites the volume's descriptor so
the bucket stops saying it descends from anything. The walk and the composition moved out of
`internal/agent` into `internal/lineage`, which is the new package's whole reason to exist —
`agent.fetchBase` and the flatten compose the same chain, and two implementations of a
lineage walk is the KEK-file defect with more corners. `task ci` green, the e2e lane green,
production coverage 90.28%.

**Where it runs, and the argument is not convenience.** In the Control Plane's binary, as a
one-shot, alongside `-seed-volume`, `-snapshot-volume`, `-clone-snapshot`, `-detach-volume`,
`-cordon-host` and `-rebuild-metadata`. What settles it is the *precondition*: a flatten
CASes the volume's manifest, so a host serving that volume — holding the ETag it loaded,
about to compare against it at its stop — would have its publish fail with `ErrSuperseded`,
which its teardown deliberately does not retry, and the session would be lost. So the volume
must be detached, and a detached volume is in no host's desired state: **there is no Agent to
ask.** Giving one a flatten verb means handing it a volume it is not serving, with no device
and no data-directory lock, driven by a Control Plane ADR-0021 §4 keeps it from knowing
about. It is the one admin one-shot here that borrows no term, because it writes no catalog
row; the comment at the dispatch says so rather than leaving a reader to notice the absence.

**It costs a full copy of the volume's dataset, and that is forced rather than chosen.** A
volume's lineage root is *derived from its chain* (`lineage.Root`), so a volume with no chain
is its own root by definition — leaving the bytes under the ex-parent's prefix would leave a
manifest naming chunks no reader will ever look under. It is also the only version of the
operation that means anything for a delete: a flattened clone whose bytes still lived in its
parent's chunk store would keep every one of them reachable, and deleting the parent would
free nothing, which is the point of DELETION-AND-RECLAIM-SPEC's answer B. *Rejected: naming
the ancestors' chunk objects from the flattened manifest and moving no bytes at all.* The
shared chunk store makes it almost possible and one thing stops it — a `Chunk` names a whole
object and the format cannot express a slice of one, so a run composed from a parent's 8 MiB
chunk and a clone's 512-byte write needs re-chunking. Giving `Chunk` a `skip` field is the
cheap FLATTEN somebody could build later; it is an on-S3 format change in a review zone with
its own §25.2 work, and folding it into the increment that first wants it is how a format
grows a field nobody reviewed.

**The catalog cannot say a lineage ended, and that is what changed the read path.**
`volumes.parent_snapshot_id` is write-once by construction — `CreateVolume`'s conflict path
COALESCEs it so a converging rebuild can never drop a clone's link — which
`DELETION-AND-RECLAIM-SPEC` §9 found and handed to FLATTEN. It means no catalog write can
produce a row saying "this volume descends from nothing any more", so an Agent taking its
first link from the desired state would, after a flatten, walk to an ancestry the volume no
longer reads through *and* resolve a lineage root that is no longer its own — looking for its
own chunks under its ex-parent's prefix and finding none. So the desired state is now the
trigger and the bucket is the answer: a volume whose desired state names a parent has its
whole ancestry read from `volumes/<vol>/descriptor.json` upward. ADR-0021 is untouched — it
is a read of an object, not a lookup against a Control Plane, and it is the authority
`-rebuild-metadata` trusts precisely when the database is what was lost. The cost is one GET
per attach **for clones only**, recorded in `TestWhatDepthCostsAtAttach` as a constant rather
than a slope. Eleven fixtures gained the descriptor a Control Plane would have written.

**Two refusals, and both are statements of fact rather than caution.**

*A volume with published snapshots of its own is refused.* Those snapshots are immutable
(§5.2, INV-16) deltas over the ancestry; clearing the link would leave a future clone of one
of them reading zeros for everything the ancestry held — while being admitted at depth 0,
because the ceiling compares the parent volume's depth. That is exactly the sentence track
D's D13 asked FLATTEN not to break, and this is the answer to it: **the way to make such a
volume flattenable is to delete its snapshots**, which is the snapshot-deletion decision
`DELETION-AND-RECLAIM-SPEC` §6.4 already carries. There is no way to flatten a snapshot;
rewriting one is what create-only forbids.

*A volume whose own chunk prefix is not empty is refused by name.* That prefix is empty until
a flatten writes into it, so a non-empty one is the fingerprint of a flatten that did not
finish. It converts the one unrepairable window into a message that names the state instead
of a confusing "chunk not found".

**The order, and the window it leaves.** Chunks, then the manifest, then the descriptor. Each
failure leaves reads refused rather than wrong, and the middle state — a self-contained image
under a bucket that still claims an ancestry — is the one a re-run does not repair. The
reverse order was considered and is worse: clearing the descriptor first destroys the only
record of what the volume descends from, so a flatten that then failed could never be resumed
by anyone.

**The flattened manifest carries erasures a root volume's never would, and a plant is what
made that claim honest.** `cow.DeltaOver` drops tombstones when it is given nil, so the
flatten publishes over an empty map instead. The first version of that reasoning claimed the
window protected every erasure; planting "publish over nil" went **green**, because an
erasure an *ancestor* made is stated by that ancestor's own manifest and the composed
ancestry answers it either way. What is actually at stake is a range **this volume**
discarded over bytes an ancestor holds — the fixture gained the offset that can tell the
difference, and the plant then reads `0xa1` where the guest freed the blocks (§14.6). The
comment says the narrower true thing.

**Four plants, each watched go red and reverted textually.**

- Publishing the delta instead of the flattened view (the task's "skip a range"): in
  `internal/lineage`, *"offset 0 reads 0x0000000000000000..., want 0xa1 repeated"* at both
  ranges only the ancestors wrote; through a real `VolumeManager`, *"offset 0 reads
  0x0000000000000000, want 0xa1 repeated — a range only the parent ever wrote, read after the
  parent's objects were deleted"*.
- Publishing over nil, so the volume's own erasures are dropped: *"offset 20480 reads
  0xa1a1a1a1a1a1a1a1..., want 0x0 repeated"* on the reading that still lays the image over
  the chain.
- Making the Agent believe the catalog's link again (three lines in `parentChain`): the seam
  refuses the read outright — *"lineage: snapshot … was never published, so the chain below it
  cannot be read"* — which is the flattened volume resolving its ex-parent's root.
- Removing the flatten from the e2e scenario and changing nothing else: agent-2 prints *"the
  volume's ancestry could not be materialized; its reads will fail"* and the lane is red in 66
  seconds.

**What the e2e lane cannot say, found by running it rather than by reasoning.** No guest boots
there, so nothing in it holds a byte — every `image_bytes=` is 0 — and the assertion that the
flatten moved chunks under the clone's own prefix is *unsatisfiable*, not merely unmet. It was
written, run against the deployment, and removed rather than weakened until it passed. That
lane says the deployment performs the operation (the parent's snapshot manifest is deleted and
the clone is still served, exiting 0); `internal/agent` says what the operation preserves,
over a real WAL and a real device, at three offsets that fail differently.

**No format change, and therefore no §25.2 obligation.** Not one byte of the manifest, the
descriptor or the chunk sealing moves: the flatten writes through `image.Publish` and
`descriptor.Write` with the fields they already have. The only new thing a manifest can now
contain is a `discarded` span in a volume that descends from nothing, which the format has
always allowed and `image_property_test.go` already draws.

**No DST scenario, and this is the third lane in a row to say so.** The mandatory set may
grow by one per merge window, and the property here — an operator's one-shot rewrites two
objects — has no fencing or data-path arm a checker could hold that the two lanes above do
not already assert on bytes. The erasure scenario step 3 asked for is still the cheapest thing
left on this subject.

### What is owed, and to whom

**One catalog write, and it belongs to track D.** The flatten leaves
`volumes.parent_snapshot_id` and `volumes.chain_depth` as they were, because no verb can
change them: the column is write-once by construction and `metadata.Store` has no clearing
verb at all. **Nothing reads wrong because of it** — the Agent takes the link from the
descriptor, which is exactly why that change was made — but everything that *counts* lineage
still counts the old one: `controlplane.Clone`'s ceiling refuses clones of a volume that is
now at depth 0, and a delete of the old parent would still see a descendant. The staleness is
in the safe direction (refusing what it should admit), and `-rebuild-metadata` already fixes
both, because it reconstructs the row from the descriptor this command rewrites. The command
says so in a `WARN` on every successful run rather than in a document.

What track D needs to add is one term-guarded statement and its two implementations:

```sql
UPDATE volumes SET parent_snapshot_id = NULL, chain_depth = 0, updated_at = now()
 WHERE volume_id = $1 AND (SELECT term FROM control_plane_leader WHERE singleton) = $2
```

as `metadata.Store.ClearVolumeParent(ctx, term, volumeID)` — `metadata.go`, `sim/sim.go`,
`pg/pg.go`, `metadatatest/contract.go`, `db/queries/volumes.sql` — with `cmd/control-plane`'s
`flatten` calling it after `lineage.Flatten` returns and before it prints. Its contract case
should assert on `GetDesiredState`'s answer rather than on the column, the way
`VolumePlacementIsChangeableAndClearingIsIdempotent` does. Once it exists, the `WARN` and the
`-rebuild-metadata` sentence come out of `cmd/control-plane/flatten.go`.

**Two things for whoever writes the delete.** A delete of a volume with descendants flattens
them first, and "descendants" has to be read out of the *bucket* as well as the catalog now:
a flattened volume is still a descendant in the row until the write above exists. And a
descendant with published snapshots cannot be flattened at all, so the delete's plan for it is
to delete those snapshots first — which is that spec's decision 4, and the reason the refusal
here names them.
