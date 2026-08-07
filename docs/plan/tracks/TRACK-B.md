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

**B10: a skip stops looking like a pass, and CI runs the guest-backed proofs
(2026-08-05, `8c9d296`, `927f79d`, `d27c988`, and the commit this entry lands in).** The
item is one sentence — `task ci:full` reported success on a machine that had booted no
guest — and it took four commits because the lie had three layers.

*The exit code.* `test:integration:qemu` printed `SKIP: no QEMU in _output` and exited 0,
and every test under both guest-backed lanes skipped itself for the same reason, so the
merge gate was green on every machine but one laptop while running none of the proofs a
real Linux kernel carries: that a guest's `fsync` puts zero objects in the bucket, that a
snapshot taken while a guest writes is one point and not a smear, that an Agent that
cannot publish keeps its data directory. The decision now belongs to whoever makes the
claim: a caller passes `REQUIRE_PROOFS`, the lanes export it as `SPIN_REQUIRE_PROOFS`, and
`testinfra.missingInput` — one helper behind all three "input is not built" sites, so a
lane cannot acquire a new way to skip without going through it — turns a skip into a
failure under it. `ci:full` sets it; `ci:noguest` is the *same* `ci:lanes` list with it
unset and ends by printing what it did not prove. A second target name rather than an
opt-out variable, because the sentence people exchange is "ci:full was green" and an
environment variable is invisible inside it. The alternatives, including the
machine-readable skip summary, are written at the gate targets in `Taskfile.yml`.

*The workflow.* CI had never executed one of these proofs and could not have: the `ci`
job ran `ci:full` on a runner with no QEMU, and `guest-lane` would have died at
`fetch:kernel`, which had no source to fetch from. `guest-inputs` now resolves both
artefacts this repository does not build — the pinned QEMU runtime image and the mirrored
kernel, both named from the Taskfile via `qemu:version` and `guest:kernel:tag` so no
version is spelled twice — and hands the kernel on as an artefact; `guest-lane` and
`guest-e2e-lane` run the two lanes with `REQUIRE_PROOFS=1` in container jobs on that
image; `ci` runs `ci:noguest`, which is what a runner with Docker and no QEMU can honestly
claim. `gate` is the job to require on the branch: `needs` alone enforces nothing, because
a *skipped* needed job leaves its dependents free to run and a workflow of green-and-grey
boxes reports as a passing run — the same defect as a target exiting 0 after printing
SKIP, one level up. It now enumerates its lanes with `toJSON(needs)` instead of keeping a
hand-written result map beside `needs:`, since two lists with nothing holding them equal
is that defect one level in again, and it fails on an empty enumeration.

*The reading.* Both of those were verified by reading, because no workflow can execute
here — and reading is what goes stale. `task workflows:verify` (`hack/workflow-tasks.sh`,
in `task ci`) resolves every `task <name>` in `.github/workflows` with `task --summary`,
which also refuses an `internal:` target a workflow may not call. Its first finding was
immediate: `guest:proofs` described itself as "what CI's guest jobs run" and no workflow
named it — nor could one, since a hosted runner has the pinned QEMU or a Docker daemon and
not both (ADR-0025). Deleted rather than re-described.

**Verified both ways, on the machine that has the pinned QEMU.** With it present,
`task test:integration:qemu REQUIRE_PROOFS=1` is `ok ... 8.260s`, exit 0. Pointed at an
empty tree (`OUTPUT_DIR=/tmp/spin-empty-out`) the same command exits **201** at
`guest:ready` — `missing .../bin/qemu-system-x86_64 — run: task build:qemu` — and without
`REQUIRE_PROOFS` it prints SKIP and exits **0**, which is the developer path the design
keeps. Below the preflight, where CI's jobs actually live, the tests themselves go red:
`SPIN_REQUIRE_PROOFS=1 QEMU_OUTPUT_DIR=<empty>` over `integration/vhost` exits **1** with
every guest test FAIL naming its missing input and the remedy. The `gate` job's body was
run against the shapes GitHub produces for `needs`: all-success 0, a failed `guest-inputs`
with two skipped lanes 1, `{}` 1.

**What is still blocked, and it is not a failure — it is two artefacts nobody has
published.** The workflow is red until both exist, deliberately, and its preflight prints
the command for each:

- **The QEMU runtime image**, `ghcr.io/<owner>/<repo>/qemu:$(task qemu:version)`. Nothing
  is missing but a run: `.github/workflows/qemu.yml` publishes it, it has
  `workflow_dispatch`, and anyone with write access can trigger it. It is not on the
  per-push path because the build is tens of minutes.
- **The mirrored guest kernel**, `guest-kernel:$(task guest:kernel:tag)`. This one needs a
  human: storage never builds a kernel (ADR-0021), so it has to come from a machine with a
  sibling spinbox checkout that has already built the pinned artefact —
  `task fetch:kernel && task guest:kernel:push GUEST_KERNEL_IMAGE=...`, with
  `packages: write`. Until then `vars.GUEST_KERNEL_IMAGE` can point at any registry path
  carrying it; `hack/guest-kernel.sh` re-hashes whatever it pulls against
  `GUEST_KERNEL_SHA256` regardless of where it came from.

Both are private packages by default, so a pull request from a fork gets a token that
cannot read them and its gate will be red. There are no forks and no runner yet; the
choice when there are is to make the two packages public, and the comment at
`guest-inputs` is where having made it should be recorded.

### Integration owner, 2026-08-05 — the coverage floor was decorative at the boundary

`hack/coverage.sh` compared the **rounded string** `go tool cover -func` prints. On
2026-08-05 it reported `90.0%` and `OK` for a tree whose real figure was 3929/4366 =
**89.9908%** — under the floor, passing on 0.0092pp of rounding. It now computes the ratio
from the profile and compares exactly; the printed percentages stay rounded, because that
is what a human reads. It also fails when the production profile has no statements at all,
which would otherwise pass having measured nothing.

This is the same defect as the one wave 4's gate work was about — a check reporting
success for something it did not establish — one level down, inside the check that guards
the others. Turning it on immediately made the tree red, which is the correct outcome and
the reason it was worth finding: the previous wave had deleted tested production code and
recorded that coverage "did not fall below", which was true of the printed number and
false of the number.

**B11: `task deadcode` binds, over a set (2026-08-06).** The check existed and gated
nothing: it was landed outside `ci`/`ci:full` on the argument that a step which is red the
day it lands is a step someone deletes, and the plan was to wire it in "the moment the
unexplained count is zero". That plan cannot arrive on purpose. The count drifts as other
lanes delete code — it has fallen once already since the entry above was written, and no
one decided anything to make it — so the gate would have switched itself on by accident,
and until then every newly unreachable symbol was landing unnoticed. The entry above is
also the demonstration of `PARALLEL-PLAN.md`'s rule about numbers in documents: every
figure in it is now wrong, and the mechanism that computes the right ones is `task
deadcode`.

**The shape is a second list, `hack/deadcode-pending.txt`, and the split is the decision.**
`deadcode-allow.txt` claims "unreachable, and that is correct forever"; the pending list
claims "unreachable, and nobody has finished deleting it yet". Both are parsed by one
reader in `hack/deadcode.sh`, so no rule can apply to one and quietly not the other, and
the gate now fails on four things: a finding in neither list (the set cannot grow), an
entry in either list that is no longer reported (it cannot outlive its code), a symbol
claimed by both, and an entry with no reason. The rejected alternative was one list: move
the outstanding findings into the allowlist with a reason each. That is green immediately
and costs the allowlist its meaning — the file that says which unreachable symbols are
*correct* would have been carrying, at that moment, mostly ones that are not. Pinning a
bare integer was rejected for the reason this repository already paid for: `wantBehavioural`
is the ratchet that causes merge conflicts, and a number is satisfied by deleting one
finding and adding another.

**What is pinned, and whose it is.** Everything in the pending list belongs to another
lane, so this track recorded them rather than deleting them: `internal/simio/real`'s whole
TCP transport (track E — a complete listen/dial/framed-send implementation reachable from
no binary, because the wire is Connect over HTTP; its only callers are the simio contract
test and DST's simulated counterpart), and `lease.Manager.Revoke` (a production verb whose
two callers — the fence and the lease-gated ACK — ADR-0026 deleted; `internal/lease`
belongs to no track in `PARALLEL-PLAN.md`'s table, and the decision travels with track D's
detach). The consequence to know about: **a lane that deletes one of these must delete its
line in the same commit**, or the gate goes red on a stale entry. That is deliberate and it
is the same rule `internal/dst/mandatory_set_test.go` already imposes; the file says so in
its header.

**Planted, and it went red.** An exported `PlantedUnreachableVerb()` in
`integration/guestinit/main.go` — a root, so the plant exercises the call-graph analysis
and not just the parser. `task deadcode` printed `unexplained: 1` with
`integration/guestinit.PlantedUnreachableVerb` named, and exited 1; reverted, and the
target prints `OK: every unreachable symbol has a recorded reason` followed by the
still-owed list, which is printed on *green* runs precisely because after this change a
green run is the only kind anyone sees. The other three failures were exercised too, with
`PENDING=` pointed at a doctored copy: a pending entry deadcode does not report → *"the
deletion happened. Remove the lines, in the commit that removed the code"*; a symbol in
both lists → *"which claims they are permanently fine and must be deleted at the same
time"*; an entry with no `#` → *"entries with no reason"*. `task --summary ci` shows the
step between `lint` and `test`.
