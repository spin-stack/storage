# Track B — the gate runs

**This file is track B's alone.** It was carved out of `STATUS.md` on 2026-08-04
because five lanes appending to one file is a collision every wave: in wave 3 one lane
committed a stale copy and deleted 121 lines of another's, ten seconds after they
landed, and only that lane looking again restored them. Ownership by *file* is a
control; ownership by *section of a file* is a convention, and a convention is what
`PARALLEL-PLAN.md` says is not a control.

`STATUS.md` remains the single answer to "what is true right now" — its head, its
tables and its DEV entries. This is the running log of one track's increments.

---

## Track B — the gate runs (open work, appended per increment)

*Only track B appends here* — it owns `.github/workflows/`, `Taskfile.yml`, `hack/`,
`internal/testinfra`, `integration/guestinit` and `integration/vhost/guest_test.go`. The
head tables are recounted once, at integration, by track A.

**Wave 0: the mandatory DST set cannot select nothing (2026-08-03, `d58c19c`).**
`go test -run TestNoSuchNameAtAll ./internal/dst/` exits 0 — "ok, no tests to run" — and
`task dst` selected its scenarios with `-run` regexes, so a renamed test or a mistyped
pattern turned the mandatory gate into a no-op that reported success. The set is now
pinned by **name** (`internal/dst/mandatory_set_test.go`, 17 entries) rather than by a
count, because a count is one integer every future branch bumps.

**And the fix found a second no-op underneath it.** `internal/dst`'s `TestMain` audits,
after the package runs, that every default checker was actually shown to catch a planted
bug — and it skips that audit whenever `-run` is non-empty, so a single-test invocation is
not spuriously red. `task dst` always passed `-run`. **The gate whose purpose is running
the DST proofs was the one place the "these checkers can fire" audit was switched off.**
Removing the filter entirely — rather than making it fail on an empty selection — costs
0.02s and re-enables it.

**Wave 0: the simulable analyzer sees the build-tagged surface (2026-08-03, `2d75842`).**
INV-01's authoritative layer ran without build tags, so every file under `integration/`
and `internal/testinfra` was invisible to it. Two things were not what the task assumed:
the analyzer's `-tags` flag is a deprecated no-op in `singlechecker` (tags reach it only
through `GOFLAGS`, which the `go list` child inherits), and there were **zero real
findings** — all 15 sites are build-tagged harnesses already exempt under DEV-0016, so the
exemption was ported to the analyzer rather than the code being fixed. The net new
coverage under `integration/` is therefore zero files; what it *did* buy is that tagged
code elsewhere — `internal/metadata/pg`'s integration tests, for one — is now governed by
the authoritative layer for the first time. The exemption is per **file**, not per
package, because `integration/vhost` holds host-side non-test code beside its harnesses,
and the fragment matching was changed to segment-anchored: plain `strings.Contains` would
have handed the exemption to any package merely *named* like an exempt one.

**Wave 0: the workflow calls `task ci:full` (2026-08-03, `784404f`).** The premise was
wrong in a way worth recording — CI's 13 steps were already set-equal to `ci:full`, so
nothing was missing; the defect was structural, two copies of the gate with nothing
holding them equal. Doing the permissions work found two real ones instead, both in the
guest lane's registry access: `guest-lane-image` logs in to ghcr.io with only
`contents: read`, so against a private package the lookup fails closed and the lane
disables itself **silently** — the exact failure ADR-0025's preflight exists to make
visible — and `guest-lane` is a container job, whose image is pulled before any step runs,
so no `docker login` step could ever authenticate it and it had no `container.credentials`.
Both fixed. None of it is executed: these workflows have never run.

**B9: a guest that stays alive (2026-08-03, `0ee78a6`).** `RunLinuxGuest` ended in
`cmd.Run()`, so **no test in this repository had ever had a live guest concurrent with any
other event** — every snapshot, restart and clone in the lanes happened over a device whose
guest had already powered off, and the e2e lane's "live" snapshot is requested thirteen
lines after the guest is gone. `testinfra.StartLinuxGuest` returns while QEMU runs (on the
same `Process` the binaries use, so the console is waitable line by line, with no second
copy of the pump), and `RunLinuxGuest` is now the thin blocking wrapper — its `ctx`
parameter went with the rewrite, because a guest that is killed by the test's own cleanup
has no use for one; the five call sites in `integration/{e2e,vhost}` are the only edits
outside this track's files. `integration/guestinit` grew `spin.mode=hold`: write, fsync,
print `GUESTINIT-ALIVE <n>`, repeat, and stop when the **host** sends `GUESTCTL-STOP` down
the other direction of the serial line — a channel rather than a signal to QEMU, because
killing the VM proves nothing about the guest and leaves the volume mid-write. An
unrecognised `spin.mode=` is now an error instead of falling back to the write path.

`TestAGuestStaysAliveWhileTheHostWatchesItWrite` is the deliverable: it asserts liveness
three ways — the WAL's durable watermark moves *after* the host read it, a heartbeat
numbered above any seen before that arrives afterwards, and the guest answers the stop by
powering off and reporting `GUESTINIT-PASS`. **Both planted bugs go red**: a `hold()` that
returns after one iteration fails at "the WAL to grow under a running guest" (the first
heartbeat and the first watermark still arrive, which is why one observation would not
have been enough), and a guest deaf to the console keeps printing heartbeats until `Stop`
gives up. What this unblocks is track C's snapshot-mid-write, an Agent restart under an
attached guest, and RISK-10's reconnect path; none of them are written here.

**B8a: "components with no production caller" is computed (2026-08-04).** The list under
that heading above was hand-written, and in two consecutive waves it was wrong — it went
stale inside one increment, and wave 2 added an OTLP exporter and a provider nobody listed.
`task deadcode` (`hack/deadcode.sh` + `hack/deadcode-allow.txt`, on the pinned
`golang.org/x/tools/cmd/deadcode` — the module already required x/tools, so the pin is
go.mod's and `tools:deadcode` refuses to install when the two disagree) answers it from the
call graph instead. Roots are the binaries and only the binaries — `./cmd/...` plus
`integration/guestinit`, which is PID 1 in the guest; `-test` was rejected because it makes
every test's own subject reachable and answers a question nobody asked.

**Its output today: 78 reported, 61 explained by the allowlist, 17 unexplained, exit 1.**
The 17 are `internal/simio/real`'s entire network implementation (9 — a real `Listen`/
`Dial` whose only caller is the simio contract test; the transport is Connect over HTTP),
`internal/lifecycle`'s Agent volume-state machine (7 — §16's `AgentVolumeState`, tested and
referenced by nothing outside its own file), and `lease.Manager.Revoke`, a verb nothing
performs since ADR-0026 removed the lease-gated ACK. They belong to tracks E and D, so this
track reports them rather than deleting them. **What would make it blocking in `ci:full`:
that number reaching zero** — each finding deleted, or in the allowlist with its reason.
Until then it stays out of the gate deliberately: a step that is red the day it lands is a
step someone removes.

**The allowlist is the part that had to be built carefully**, because an allowlist is where
findings go to be forgotten. Every entry carries its reason on the same line and the task
**fails** on an entry with no reason and on an entry that no longer matches a finding —
which caught its own author twice within minutes: `lifecycle.SnapshotStates` is called by
`SnapshotState.Valid` and was never dead, and `package integration/guestinit` cannot be
reported because it is a root. Neither would have been noticed by a human writing a list.

**Two blind spots, both stated in the script, both real.** RTA marks every method of a type
that reaches `reflect` as live, so `wal.TruncateLocal` — the flagship entry of the
hand-written list — is *not* reported (`-whylive` answers "reachable only through
reflection"); and a package no binary imports is not in the program at all, which is where
`metadata.BumpVolumeEpoch`, the other entry, lives. A second pass names those packages
(10, all explained) at package granularity. So the tool is a floor: absence from its report
is not evidence of a caller, and the hand-written list and the tool disagree in *both*
directions — which is the argument for having the mechanical one, not against it.

**Planted:** an exported `PlantedUnusedVerb()` in `integration/guestinit/main.go` (a root,
so the plant tests the analysis and not just the parser). Reported went 78 → 79 and
unexplained 17 → 18, with `integration/guestinit.PlantedUnusedVerb` at the top of the
unexplained list. Reverted.

**B8b: `guest:verify` fails when it cannot check (2026-08-04).** The lane's preflight had
four states in which it printed OK having proven nothing, and three of them were measured
on this machine before the change rather than argued from the source. `test -f` on the
initramfs: emptying `_output/guest/initramfs.cpio.gz` left `task guest:verify` green and
exit 0 — and `build:guest` did **not** rebuild it, because its `sources:`/`generates:`
fingerprint compares the sources, so a truncated artefact is "up to date". An empty
`GUEST_KERNEL_SHA256`: `task guest:verify GUEST_KERNEL_SHA256=` printed the full `OK:
Linux version 7.1.0 — PVH ELF, ... present`, verifying a kernel against no pin. A missing
`readelf`: the Xen PVH note check was wrapped in `if command -v readelf`, so a machine
without binutils skipped it silently. And a kernel with no embedded config printed
`warning: ... unchecked` and returned success, which is the case where all four
`CONFIG_*` assertions stop running.

All four now fail, and each message names the input and the task that produces a good one.
The initramfs is opened rather than stat'ed — gzip-tested, then listed for `./init`, whose
absence is a kernel panic ("no working init found") that reads like a kernel bug rather
than a missing build step. `fetch` still honours an empty pin, because bisecting a kernel
change needs to acquire an unpinned one; *verifying* against no pin is a contradiction.

**Planted, all four, and quoted here because a preflight is exactly the kind of check that
is never exercised:** zero-byte initramfs → `... is not a valid gzip stream — a truncated
or interrupted build; rebuild: task build:guest`; a valid archive holding only `dev/` →
`... contains no ./init — the kernel would mount it and panic with no PID 1`; empty pin →
`GUEST_KERNEL_SHA256 is empty: there is no pin to verify ... against`; `readelf` off the
PATH → `readelf is missing, so ...'s Xen PVH note cannot be checked`; and the config branch
planted by pointing `ikconfig` at a marker the kernel does not carry →
`has no embedded config (CONFIG_IKCONFIG=n), so CONFIG_VIRTIO_BLK ... cannot be checked`.
Each exits non-zero; with good inputs the target is green.

**One of these plants was the check crying wolf at its author**, which is worth recording
because it is the failure mode that gets a preflight disabled: the first `./init` matcher
was `grep -qx './init'`, and `cpio --list` prints the stored `./init` back as `init`, so a
perfectly good initramfs failed. Caught by running the good path, not by reading it.
