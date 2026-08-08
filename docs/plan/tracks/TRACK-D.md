# Track D — the catalog

**This file is track D's alone.** It was carved out of `STATUS.md` on 2026-08-04
because five lanes appending to one file is a collision every wave: in wave 3 one lane
committed a stale copy and deleted 121 lines of another's, ten seconds after they
landed, and only that lane looking again restored them. Ownership by *file* is a
control; ownership by *section of a file* is a convention, and a convention is what
`PARALLEL-PLAN.md` says is not a control.

`STATUS.md` remains the single answer to "what is true right now" — its head, its
tables and its DEV entries. This is the running log of one track's increments.

---

## Track D — the catalog (open work, appended per increment)

*Only track D appends here* — it owns `internal/controlplane`, `internal/cpserver`,
`internal/metadata/**`, `internal/db`, `internal/schema`, `internal/lifecycle` and `api/`,
and it runs as one sequential lane. The head tables are recounted once, at integration, by
track A.

**D1: the per-volume durability mode is gone, end to end (2026-08-04).** ADR-0026 made
§14.8's local ACK the only contract; the enum outlived the mechanism by a whole increment,
in nine places — `lifecycle.Durability`, `volumes.durability` with its CHECK, the proto's
`Durability` enum and `DesiredVolume.durability`, `metadata.Volume.Durability`, the
descriptor's `durability` field, `controlplane.VolumeSpec.Durability`, the
`-seed-local-durability` flag, and about thirty fixtures. **The claim that nothing reads
it was checked before anything was deleted, not after**: `Durability.Remote()` had exactly
one caller and it was its own test; `DesiredVolume.GetDurability()` had exactly one caller
and it was `cpserver`'s own test; the Agent never looked at the field it was sent. The
three remaining reads were the validations of a value nobody consumed — `Valid()` in
`provision.validate`, and the default-then-validate in each of the two `CreateVolume`s.
`wal.DurabilityMode`, which `lifecycle.Durability`'s doc comment named as its data-path
counterpart, had already been deleted; the comment was the last thing describing it.

**−220 lines of hand-written code and SQL** (−347 counting the regenerated `api/gen` and
`internal/db`). The schema change is `ALTER TABLE volumes DROP COLUMN durability`
(`migrations/20260804020927_drop_volume_durability.{sql,json}`, planned against the dev
database and applied to it); `task db:verify` and `task test:integration` are green, so
the column is gone from both the declared state and the database the pg contract runs on.
Proto field 6 is `reserved`, not renumbered — a peer built before this change decodes 6 as
an enum, and a later field reusing the number is the one way a wire format lies to a
reader that is otherwise correct. The top-level `Durability` enum is deleted outright:
proto3 has no file-scope `reserved`, so the note lives in `DesiredVolume` where the
`reserved 6` is.

**D2: a volume's placement is changeable, so attach is no longer permanent (2026-08-04).**
`volumes.primary_host_id` was write-once. `CreateVolume` set it and its converging upsert
protected it with `COALESCE(existing, excluded)`; `BumpVolumeEpoch` is the only other
writer and has no production caller. So a volume could not be detached, could not be
re-placed, and the volumes `-rebuild-metadata` restores with no host — which it says out
loud on the way out — could never be given one. Three well-tested teardown paths had no
condition in production that reached them: `VolumeManager.Apply`'s gone-branch,
`VolumeManager.Fence`, and the `NOT_PRIMARY` report outcome. All three are now reachable.

`metadata.Store.SetVolumePrimaryHost(ctx, term, volumeID, primaryHostID)` places a volume
or clears its placement, in both implementations and one term-guarded statement, with
`cmd/control-plane -detach-volume` and `-attach-volume`/`-attach-host` as the callers.
Three decisions, each written at the code rather than here:

- **The §7 state moves with the ownership, in the same write** (`metadata.PlacedState`):
  no writer means `DETACHED`, a writer means `ACTIVE`. Two writes have a window and
  neither order is harmless — a volume left `ACTIVE` with no host is one
  `controlplane.RequestSnapshot` accepts (it checks only the state) and no Agent can ever
  take, so the snapshot sits `CREATING` for ever.
- **The epoch is not touched, in either direction.** An epoch is a fencing token granted
  by a compare-and-set to a writer that won it; a detach grants it to nobody. Nothing
  needs the bump either: `cpserver.applyReport` compares `primary_host_id` against the
  reporting host *before* it looks at the epoch, so a cleared volume answers `NOT_PRIMARY`
  — the outcome that makes the Agent fence and tear down — and `""` matches nobody
  because the RPC refuses an empty `host_id` at the boundary. And a bump would cost a
  re-attach its local data: the WAL lives at `<data-dir>/wal/<volume-id>/<epoch>`, so a
  new epoch is a fresh empty root and detach has to be reversible.
- **A straight hand-over A → B is refused** (`metadata.ErrAlreadyPlaced`); the caller
  detaches, observes the host has stopped, and places. This is what keeps the increment
  out of the mutual-exclusion review zone rather than dragging it in: a host serves what
  `GetDesiredState` lists and learns it lost a volume on its *next* poll, so one write
  naming a new owner would have two Agents serving one volume for a poll interval, both
  publishing an image over the same manifest with one losing its session to the CAS. The
  refusal does not make the two-step exclusion — an operator who re-places within a poll
  interval rebuilds the window by hand — it only guarantees no single catalog write opens
  it. Closing it properly is the §7 machine's job and is not done.

No schema change: the column was already nullable with an FK. The contract case
`VolumePlacementIsChangeableAndClearingIsIdempotent` asserts on `ListVolumesByHost` —
what `GetDesiredState` answers an Agent with — rather than on the column, so a store that
wrote the column and answered the listing from elsewhere would still fail. **Four planted
bugs, each watched go red:** dropping the sim's `checkTerm` (stale-term subtest and both
`everyMutation` sweeps), dropping the sim's hand-over guard (the A → B subtest), dropping
the sim's `v.State = state` (the DETACHED assertions), and replacing the SQL term
predicate with a tautology in the pg lane (stale-term subtest, `<nil>` instead of
`ErrStaleTerm`). `task ci` and the pg integration lane are green.

**Not proven end to end.** `integration/e2e` is track C's file set in this wave, so no lane
drives `-detach-volume` against a running Agent and asserts the socket disappears and the
image lands. That proof is the seam this repository keeps losing defects at, and it is
owed.

**D4: admission counts what a host is using, not only what it was promised (2026-08-04).**
ADR-0013's second surviving gap (its amendment of 2026-08-03). The Agent has always
shipped the measured `UsedBytes` — a `statfs` of the filesystem holding `--data-dir` — and
`cpserver` has always stored it in `hosts.nvme_used_bytes`; **nothing read it**. So the
§28.2 bound protected against over-promising and not at all against filling, and under
ADR-0026 that is worse than when the ADR was written: a session's whole WAL stays local
until the volume stops, so the device holds everything every attached volume has written,
with no mid-session reclaim and no reservation covering a byte of it.

`placement.Policy` gains `MaxUsedRatio` (zero means `DefaultMaxUsedRatio` = ADR-0013 §3's
85%, never "unbounded" — a policy literal written before the field existed never decided
that a full device may keep receiving volumes), and `metadata.CapacityBound` gains
`UsedLimit`. Both arms travel with the write (ADR-0017): the SQL predicate now reads
`nvme_used_bytes` from the same `hosts` row lookup that already proved the host exists, in
both `CreateVolume` and `UpdateOperationPhase` — a drain's plan entry is a reservation too,
and it moves whole hosts' worth of data. `placement.Policy.Bound` is the only builder of a
bound, so the two ceilings are derived together; `cmd/control-plane` exposes
`-max-used-ratio` next to `-max-oversubscription`.

**The rule, and what it is not.** The measured arm charges the request *nothing*:
`used <= UsedLimit`, a gate, not an accounting. `used + size <= UsedLimit` was rejected
because it assumes a volume occupies its declared size the moment it is placed, which is
the assumption oversubscription exists to deny — a 1 TiB volume would be unplaceable on a
half-empty 2 TiB device. A single occupancy number over `max(committed, used)` was
rejected because the physical ceiling is *below* the promise ceiling by construction, so it
subsumes it and `MaxOversubscription` stops meaning anything. And the catalog cannot
predict what a volume adds physically anyway: what lands is what the guest writes.
`best()` still ranks on committed, deliberately — the measurement lags placement by a
heartbeat plus however long a guest takes to write, so a host handed ten volumes still
measures empty and a used-bytes ranking would keep choosing it.

**Two traps, both decided rather than tripped.** `used` includes the other tenants of that
filesystem, which is the point (`agent.DiskUsage`): no truncation of ours frees them. And
"the host has not measured yet" is *not* a third state — total and used come from one
`statfs` in one heartbeat, and `DiskUsage.Usage` returns an error rather than a zero when
it fails, so the existing `NVMeTotalBytes <= 0` guard already refuses the unmeasured host.
Reading `used == 0` as "unknown, refuse" would have refused the emptiest host in the fleet.

**Four planted bugs, each watched go red.** Dropping the `&& h.NVMeUsedBytes <=
p.UsedLimit(h)` from `Admits` (`Admits = true, want false` in three table cases, and
`Choose = "h-source"` where a source host at 95% must fall through); `if false &&` on the
sim's arm and `>= -1 *` on each of the two SQL predicates (`CreateVolume onto a device at
900/1024 GiB = <nil>, want ErrCapacityExceeded`, and the same for the plan entry, in both
the sim and pg lanes). `task ci` and `go test -tags integration ./internal/metadata/pg`
are green.

**D4b: the bound was a predicate of the write and still not a bound (2026-08-04).** Found
while writing D4's verification, and it is the reason that case exists. ADR-0017 moved the
§28.2 ceiling into the statement that places the bytes, and the claim written next to it —
"two operations that chose the same destination against the same fleet read produce one
reservation and one `ErrCapacityExceeded`" — **is false in PostgreSQL**. READ COMMITTED
fixes a statement's snapshot before the statement runs, and the derived committed value is
an aggregate over rows the statement does not lock, so two `INSERT`s that overlap in time
each evaluate the bound against a fleet the other is not in yet. Measured before anything
was written: two `psql` sessions, one 100-byte ceiling, two 100-byte volumes, 200 committed
afterwards. Nothing in the repository could have caught it — every capacity case was
sequential, and a sequential case cannot tell a predicate of the write from a check in
front of it.

`internal/db/queries/hosts.sql` gains `LockHostPlacement`
(`pg_advisory_xact_lock(hashtextextended(host))`) and `pg.Store.placing` runs every bounded
write as *two* statements in one transaction: the lock, then the write. The lock has to be
its own statement — an advisory lock taken inside the INSERT would change nothing, because
that statement's snapshot is already taken — and READ COMMITTED is what makes it work: the
loser blocks on the lock and the INSERT it then runs takes a fresh snapshot containing the
winner's row, so it refuses itself. Rejected: SERIALIZABLE (a retry loop in every caller of
the Store for a two-row hot spot) and `SELECT ... FOR UPDATE` on the host row (locks the
wrong rows — every heartbeat writes that one, and the counted rows are in `volumes` and
`operations`). An unbounded write is not a placement decision and pays nothing: no
transaction, no lock.

The contract case is `placements racing for the last slot leave the host inside its
ceiling` — five rounds of 32 concurrent `CreateVolume`s onto a host with room for one,
asserting the host's committed bytes afterwards rather than which caller won. Rounds and
racers because a scheduler is not an oracle. **Planted in both lanes:** replacing `placing`
with `boundRefused` in Go followed by the write reddens the pg lane 5 runs out of 5
(`round 0: 2 of 32 racing placements landed, want exactly 1`); moving the sim's
`boundLocked` out of its critical section reddens the sim lane 16 runs out of 20 — the
sim's window is a mutex hand-off, so its detection is high but not certain, while the lane
where the defect is real detects every time.

`internal/metadata/pg` (`-tags integration`, the whole package) and `task dst` are green.
`task ci` is **not** green in this tree, for a reason this branch did not cause:
`cmd/volume-agent/main.go` is unformatted and `internal/agent` hangs in a publish retry
loop, both track C's uncommitted work in progress. `task test` for every package this
branch touches is green, as is `task lint` apart from that one file.

**D5: a cordon on device pressure has an actor, a reason and hysteresis (2026-08-04).**
ADR-0013 §3's first row, the last of the three the amendment approved. Nothing cordoned on
pressure: `SetHostState` had no non-production caller at all, and no code path read a
device measurement to decide anything. `cpserver.Heartbeat` now does — the heartbeat
carries the only measurement of the device that exists, and reacting to it is the Control
Plane's alone (ADR-0013 §5: the Agent gets local defensive powers only, and moving volumes
stays here, because two actors evacuating one host is the class of bug two earlier waves
closed). The Agent-local half — refusing attaches at 85%, the reserve — is deliberately
not here; it is `internal/agent`'s and a later increment's.

**The hysteresis is a band, not a dwell** (`cpserver.CordonUsedRatio` = 0.70,
`UncordonUsedRatio` = 0.65). One threshold is a flapping cordon: a host sitting on the line
crosses it in both directions on consecutive heartbeats, and every crossing is a write, a
state every placement decision in the fleet reads, and a line in whatever an operator is
watching — placement becomes non-deterministic for reasons nothing records. A Schmitt
trigger fixes that with **no state at all**: the decision is a pure function of the host
row, because the previous decision is read back from the state and the reason it wrote.
Coming back requires freeing 5% of the device, which heartbeat jitter does not produce; the
5%–gap dead zone (a host cordoned while admission's 85% ceiling would still take it) is the
price, and the band is deliberately the *smallest* gap that is unambiguously real.
**A dwell was rejected**: "clear for N heartbeats" needs a per-host timestamp, which is
either a column written on every heartbeat of every host — the busiest RPC there is — or
memory a leader change discards, so a failing-over CP would hold hosts cordoned
indefinitely with each new leader restarting the count. It also puts a clock into a
decision that is otherwise two numbers already in the row (INV-01 makes every clock an
injected dependency). What it catches and the band does not is a device that frees 5% and
refills it between two heartbeats — not noise, a host doing exactly what the cordon is for.

**The reason is also the authority** (`lifecycle.CordonReason`, `hosts.cordon_reason`,
`migrations/20260804112514_host_cordon_reason.{sql,json}`, planned against and applied to
the dev database; `task db:verify` green). `CORDONED` stopped being evidence that a human
meant it, so an operator needs the cause — and, in the other direction, the automatic loop
must never clear a cordon set for a cause the fleet cannot see. One column serves both:
`OPERATOR` outranks `DEVICE_PRESSURE`, `DEVICE_PRESSURE` may only replace `''` or itself,
and `CordonReasons().OverwritableNames()` is that table as the `SetHostState` predicate —
in Go it would be read, compare, write, and an operator's cordon landing between the read
and the write would be cleared anyway. A second `cordoned_by` column was rejected: two
columns that must agree are two columns that can disagree. `SetHostState` gains the
parameter; `CordonNone` is not an actor and is refused, so no write is authorless. A table
constraint (`state = 'CORDONED' OR cordon_reason = ''`) keeps a reason from outliving its
cordon, because the next reader of a stale reason is the pressure loop deciding whether it
may act.

**Three planted bugs, each watched go red.** `UncordonUsedRatio = 0.70` (the hysteresis
gone) reddens `TestHeartbeatCordonsAndUncordonsAcrossTheBand` on a one-byte oscillation:
`after 769658139443/1099511627776 used: state = "ACTIVE" reason = "", want "CORDONED"` —
one byte off a 1 TiB device flips the fleet state. Adding `CordonOperator` to
`cordonOverwrite[CordonPressure]` reddens the contract case in both lanes
(`un-cordoning an operator's cordon: want ErrCordonHeld, got <nil>`), and passing
`CordonOperator.OverwritableNames()` in the pg params reddens it in the pg lane alone —
which is where the rule is actually enforced, since pg's Go check only diagnoses a 0-row
write. Dropping the reason clause from `pressureTarget` on top of the first reddens
`TestPressureNeverTouchesAnOperatorsCordon` (`a heartbeat cleared an operator's cordon`);
either alone leaves it green, which is the defence in depth working and is why the store
half has its own case.

No DST scenario: this is neither data path nor fencing nor GC, and it adds no checker (the
merge protocol allows one per window). `task ci`, `task cover` (90.0%) and
`go test -tags integration ./internal/metadata/pg` are green.

**D6: an operator can read the fleet, and a rebuilt volume can be placed (2026-08-04).**
Two findings, one increment. `cmd/control-plane` had a one-shot for every *write* an
operator needs — seed, snapshot, clone, rebuild, detach, attach — and none for looking, so
"which hosts are cordoned and why", "which volumes has nobody got" and "which snapshots
are stuck" were a psql session and four hand-written joins that lived nowhere. And the
reason the catalog could not answer them is structural: every read the Control Plane
serves is scoped to a host, because every one of them answers an Agent. A volume with no
primary matches no host id — not even `""`, which is `ErrInvalidID` at the boundary — and
`ListPendingSnapshots` reaches a snapshot *through* `volumes.primary_host_id`. After
`-rebuild-metadata`, which restores no placement, every volume in the catalog is in that
blind spot.

`metadata.Store` gains `ListVolumes` and `ListUnfinishedSnapshots` in both
implementations, with the contract case `FleetWideReadsSeeTheRowsNoHostOwns` stating the
contrast rather than assuming it (it detaches the fixture volume, then asserts the
per-host read has gone blind and the fleet-wide one has not). Unfinished is CREATING and
DELETING, defined once as `lifecycle.SnapshotState.Unfinished` and handed to the SQL as an
array — the `allowed_states` move — so the §19 vocabulary keeps one authority. It is
deliberately not "the states with no successor": PUBLISHED has one and is finished, while
DELETING has none and is not, because ADR-0026 deleted the reclaim that was supposed to
consume it. No index on `snapshots.state`: this is a fleet-wide scan an operator runs by
hand, and an index would be maintained by every snapshot write for its benefit (written in
the query).

`-fleet-status` prints hosts with state/reason/fill, volumes with host/state/epoch, and
the unfinished snapshots with the host that is supposed to take each — `-` when nobody is,
which is the column that says whether anything will ever happen. Text, not JSON, and a
flag rather than a subcommand tree: ADR-0021 says these binaries are test harnesses and
the operator interface belongs to `spin`, so the bar is "someone running this repository's
lanes can see the fleet". It takes no term and no `-holder-id`, and runs before the object
store is opened — a read guards nothing, and refusing to show the catalog because nothing
is leading (or because no bucket was named) removes the view exactly when it is the only
thing left. Ages are differences against `metadata.Store.Now`, the catalog's own clock.

The placing half: `-attach-host` is now optional, and `controlplane.Place` chooses with
`placement.Choose` when it is absent — §20's third rule, under the same two ceiling flags
`-clone-snapshot` uses. **Both, and neither direction is an accident**: an operator naming
a host is overriding the admission rule on information the catalog does not have (a volume
restored to the machine whose device still holds its bytes goes to a host cordoned for
exactly that fill), so a named host is honoured without an admission check — and not
silently, since the placement line carries `host_state` and `chosen_by`.

**Owed, and it is a real gap: the bound is checked by `Choose` and is not carried into the
write.** `SetVolumePrimaryHost` takes no `CapacityBound`, so two attaches racing onto one
host both evaluate the ceiling against a fleet the other is not in yet and both commit —
the same defect D4b closed for `CreateVolume` by making the bound a predicate of the
statement (and, in Postgres, by the advisory lock in front of it). Closing it is a
signature change to a store method in both implementations plus its contract; until then
this path is one human running one command from a shell.

**Also owed: nothing drives `-fleet-status` as a process.** The report is asserted on the
bytes it writes, against the sim store, in `cmd/control-plane/fleet_test.go`; the three
lines in `main` that reach it from the flag are covered by nothing, and `integration/e2e`
is track C's file set this wave. That is the seam this repository keeps losing defects at.

**Six planted bugs, each watched go red.** Dropping the state filter from the sim's
`ListUnfinishedSnapshots`: the printed report says `SNAPSHOTS NOT FINISHED (2)` and lists
a PUBLISHED snapshot; the contract case fails with `a PUBLISHED snapshot is still
outstanding`. The same tautology in SQL (`WHERE TRUE OR state = ANY(...)`, regenerated)
fails the pg lane identically. Making the sim's `ListVolumes` skip the unplaced:
`ListVolumes = [b1], want [b1 b2]`. Narrowing `Unfinished` to CREATING:
`"DELETING".Unfinished() = false, want true`. Replacing `Choose` with `hosts[0]` — the
fixture's admitted host sorts *last* so that this plant cannot pass — `placed on ...072,
want the one host that admits it (...074)`. Ignoring the named host: `Place returned
...074/ACTIVE, want the named host and the state that says it is out of service`.

No schema change (two new queries, no new object), so no migration and no `db:plan`. No
DST scenario and no new checker: this is neither data path nor fencing. `task ci`,
`task cover` (production 90.0%) and `go test -tags integration ./internal/metadata/pg` are
green.

**D8a: the operations subsystem is retired, and ADR-0017's second capacity term with
it (2026-08-05).** An audit found twelve `metadata.Store` methods with no non-test
caller; four of them — `RecordOperation`, `UpdateOperation`, `GetOperation`,
`ListLiveOperationsByHost` — were the whole interface of the `operations` table, and
**nothing outside a test has ever written a row**. They arrived with the drain and the
reconciliation loop ADR-0026 withdrew. Deleted: the table and its four indexes, the
`operations.sql` queries and their generated half, the four store methods in both
implementations, `metadata.Operation`, `ErrDrainInProgress`, `PlanReservation` /
`PlanReservations`, and `lifecycle.OperationKind` / `OperationPhase` — a vocabulary
whose only remaining reader was the CHECK constraint on a column that no longer
exists.

**The capacity view was the part to think about, and the answer is that removing the
second term changes no number this catalog has ever produced.** ADR-0017 derives
`committed(host)` as the volumes whose primary is the host *plus* what an in-flight
operation plan reserved there and had not yet placed. The lateral join that computed
the second half ran over an empty relation for every host in every state a V1 catalog
can reach, because the only writer of `operations.current_state` was
`UpdateOperationPhase` and it had no caller. What is removed is headroom for a move
that spans two hosts, and V1 performs none — ADR-0017's own "the tests that enforce
it" section already says so, in the past tense, of the behavioural cases that went
with the drain. The one move a V1 catalog can make, detach-then-attach, makes the
volume primary in the same statement that places it, so there is no interval between
"reserved" and "primary" for the term to cover. **It does not touch wave 2's admission
work**: `CreateVolume` still carries the bound, the advisory lock in front of it
(D4b) still serializes racing placements, and the arithmetic both stores agree on is
now one term in Go and one in SQL instead of two and two. The gap that remains is the
one D6 already recorded and this does not widen: `SetVolumePrimaryHost` carries no
`CapacityBound` at all.

`TestCommittedBytesIsDerivedInOnePlace` needed a new marker and that is the
interesting test change. It detected an inlined copy of the derivation by looking for
`jsonb_array_elements` — the plan-unnesting half was the only thing in the project
that unnested anything — and after this nothing can write that construct, so the check
would have passed for ever by having had its subject deleted. It now looks for `SUM(…
size_bytes`, which is what the derivation *is* now.

**Three planted bugs, each watched go red.** Inlining the sum back into
`volumes.sql`'s bound predicate (`SELECT COALESCE(SUM(bv.size_bytes),0) FROM volumes
bv WHERE bv.primary_host_id = …` in place of the view read): `queries carrying their
own copy of the committed-bytes derivation instead of reading host_committed_bytes:
[volumes.sql]`. Replacing the view's correlation with `WHERE v.primary_host_id IS NOT
NULL` so every host is charged for the fleet: `committed = 2147483648000, want
10737418240 (the volumes it holds)` in `TestPGCommittedBytesViewDoesNotDeriveTheWhole
Fleet`, plus four contract subtests including `an empty host reports 1073741824
committed bytes`. The same plant in the sim (`v.PrimaryHostID != ""`) reddens the
contract in the unit lane with the identical line — which is the point of the shared
contract: a term one implementation keeps and the other drops is a proof about the
wrong program.

Schema change: `DROP TABLE operations CASCADE` plus the view
(`migrations/20260805113004_drop_operations.{sql,json}`, planned against the dev
database and applied to it). `task ci`, `task db:verify` and
`go test -tags integration ./internal/metadata/pg` are green.

**D8b: the revocation window is deleted, the lease keeps its one rule, and
`ResizeVolume` is a decision left to a human (2026-08-05).** Five more
`metadata.Store` methods had no non-test caller. Three of them —
`BlockHostRenewals`, `UnblockHostRenewals`, `RevokeHostLease` — are ADR-0016 stage
1: the Control Plane revoked a source's lease to fence it and refused that host's
renewals for the length of one promotion, so the source's next heartbeat could not
re-arm what the fence took away. ADR-0026 withdrew the promotion. The ADR's own
amendment already states the consequence — the window "is currently empty, because
a revocation stops nothing on the data path" — so what was left was three writers
with no caller, a `hosts.renewals_blocked_until` column that could only ever be
NULL, and a predicate in `RenewHostLease` that could only ever be true. All of it
is gone, along with `metadata.ErrRenewalsBlocked`, `Host.RenewalsBlockedUntil` and
the `HostExists` query, whose only reader was `RevokeHostLease`'s ErrNotFound
disambiguation.

**Kept for stage 2 was the alternative and it is worse than it looks.** Stage 2 is
a fence that follows the *volume*; what it needs is not this column with a caller
added, it is a different granularity. A column no write ever sets is
indistinguishable, to the next reader, from one whose writer is broken. The
schema carries that reasoning where the column was.

**`GetHostLease` stays, against the audit, and the reason is what it is for.** It
has no binary caller either, but it is the only way to observe what
`RenewHostLease` did from outside the store, and `cpserver`'s heartbeat test uses
it exactly that way — the heartbeat renewed the lease is asserted by reading the
lease back, not by trusting the handler's return. Deleting it would delete an
observation and leave the assertion resting on an error value, which is the shape
CLAUDE.md names.

**`ResizeVolume` is not deleted, and that is a decision, not an omission.** §3 makes
volumes grow-only and resize a product verb; the store method is the bottom half of
it and refuses a shrink with `ErrShrinkNotAllowed`. What does not exist is
everything above it: no RPC in `api/`, no `controlplane` function, no operator
surface — and no Agent-side path either, since a guest learns its device size at
attach. Deleting thirty lines that already state the §3 rule correctly, so that a
future resize can restate it, is the trade this note refuses to make on its own.
**Waiting on a human: does V1 offer resize, or is §3's grow-only rule a promise with
no verb behind it?** Either answer is cheap; assuming one is not.

The contract's `hostLeases` case is rewritten rather than trimmed. It had proved
"a host holds no lease" by revoking one, and with nothing to revoke that branch
would have had no case at all — a store inventing a zero-valued lease would have
passed. It now proves it on a registered host that has never renewed, and both
renewal claims read the lease back and compare the stored TTL, because a store that
returns nil and writes nothing satisfies any assertion on `err`.

**Three planted bugs, each watched go red.** Making the sim's `GetHostLease` return
the zero lease instead of `ErrNotFound`: `a host that has never renewed: want
ErrNotFound, got <nil>`. Dropping `l.TTLSeconds = int32(ttlSeconds)` from the sim's
renewal: `lease = {…TTLSeconds:0}, want host … with a 10s TTL`, and the
cordoned/draining case with `lease TTL 0 -> 0, want the renewal to have landed as
20`. The same defect in SQL — `ON CONFLICT … DO UPDATE SET last_renewal = now()`
without `ttl_seconds`, regenerated — reddens the pg lane alone with `CORDONED host:
lease TTL 10 -> 10, want the renewal to have landed as 20`, which is the case the
sim could not have caught for it.

Schema change: `ALTER TABLE hosts DROP COLUMN renewals_blocked_until`
(`migrations/20260805114343_drop_renewal_window.{sql,json}`, planned against the dev
database and applied to it before this was written). `task ci`, `task db:verify`,
`task test:integration` and `go test -tags integration ./internal/metadata/pg` are
green.

**Owed to track A:** ADR-0016 is still `Accepted` and its stage 1 now has no
implementation, and `STATUS.md` still names `RenewalsBlockedUntil` as a live
mechanism. Neither file is track D's to edit this wave.

**D8c: two orphan types are deleted and the third is not one (2026-08-05).**
`lifecycle.AgentVolumeState` is §16's Agent-side per-volume machine — nine
constants, a transition table, `Serving()`, `CanTransitionTo`, `Transition`,
`AgentVolumeStates`, `ParseAgentVolumeState`. It was written first, explicitly so
that "Phases 02/03 implement the doc's machine rather than reinventing one", and
then the Agent was built and reinvented nothing: `internal/agent` imports exactly
one symbol from `internal/lifecycle` (`VolumeActive`) and tracks a volume's serving
state next to the WAL and the lease it actually depends on. The audit's claim of
"zero references anywhere" was wrong in the way that matters — there were two whole
test functions, `TestAgentVolumeStateMachine` and `TestServingStates`, and their
only subject was the machine itself. `placement.CommittedRatio` is the same shape
four lines long: the §26.2 catalog declares `host_nvme_committed_ratio`, nothing
records a value for it, and the division is two lines at whatever recording site
eventually exists.

**`controlplane.Elector.HighestClaimedTerm` is kept, and the audit is wrong about
it.** It is not read only by its own test: `internal/metadata/pg`'s
`TestATermIsNeverIssuedTwiceAcrossADatabaseRestore` uses it as the *observation*
that every term ever issued is in the bucket whatever the database says — the
property ADR-0011 rests on, proven against a real PostgreSQL that has just been
rewound. Deleting it would either delete that assertion or put a hand-written copy
of the bucket walk inside the test. What it genuinely lacks is the operator surface
its own doc comment promises ("what an operator, or a startup check, reads"), and
`-fleet-status` is the obvious home — `fleetReport` takes only a `metadata.Store`
today, so wiring it means handing the report the object store as well. That is an
increment, not a line, and it is not this one.

The allowlist entries for `OperationKinds`, `OperationPhases`, `PlanReservations`,
`PlanReservation.Reserves` (D8a) and `CommittedRatio` (here) went with the symbols;
`hack/deadcode.sh` fails on a stale entry, which is how they were found rather than
remembered. `task deadcode` reported 74 findings with 17 unexplained before D8 and
66 with 10 after it; every one of the remaining ten is another lane's
(`internal/simio/real`'s network, `lease.Manager.Revoke`).

**One planted bug for the one assertion that changed.** `TestValidRejectsTheZeroValue`
walked four vocabularies and now walks three, so its index-based message moved:
making `SnapshotState.Valid()` return true unconditionally prints `zero value 2
reported itself valid`, which is the third entry and therefore the right one.

No schema change. `task ci` is green and `task cover` reports production 90.0%
against the 90% floor — deleting production code that was fully covered is what
keeps that number from rising, and it did not fall below.

**D9: resize is deleted, and V1 does not resize (2026-08-06).** `ResizeVolume` was
the last term-guarded verb with no caller outside a contract test, and D8b left it
open as a question for a human: does V1 offer resize, or is §3's grow-only rule a
promise with no verb behind it? The answer is the second, and it is not a preference
— **a row that grows is not a volume that grows**, and the rest of the path is
missing rather than untested. Establishing that came first:

- `cpserver.GetDesiredState` already sends `size_bytes` to the Agent on every poll,
  and `agent.VolumeManager.Apply` returns at its `existing.epoch == d.GetEpoch()`
  check before it reads the field. A grown row therefore reaches every Agent serving
  it, every few seconds, and changes nothing.
- `blockdev.New` fixes a Device's capacity at construction and `blockdev.Device`
  offers no way to change it, so even a restart-driven resize means the teardown
  path — which publishes the session and takes the guest's device away.
- The guest cannot be told in any case. A new capacity has to arrive as
  `VHOST_USER_BACKEND_CONFIG_CHANGE_MSG` on the backend request channel, and
  `internal/vhost` does not offer `VHOST_USER_PROTOCOL_F_BACKEND_REQ` — there is a
  test pinning that it is not offered. Without it QEMU raises no virtio
  configuration-change interrupt and the guest never re-reads `capacity`.
- The image does not care, and that is the one part that was already fine: an
  `image.Manifest` is a volume id and a chunk list with no size in it, so a larger
  volume genuinely is the same chunks plus unwritten space.

**Keeping it was not neutral, which is what decided this.** `descriptor.json` carries
`size_bytes` and is written only by `controlplane.Provision` and `controlplane.Clone`;
`-rebuild-metadata` reads exactly that object to reconstruct a volume the database no
longer describes (INV-20). A resize that landed in the catalog and not in the bucket
was therefore the one way to make the two disagree about a volume's size, with nothing
to notice — and the descriptor's own doc comment claimed it was "updated on resize,
epoch change, and snapshot-lineage changes", which was true of none of the three. Thirty
correct lines that a future resize would have to restate cost nothing to hold; a verb
that reads as existing, with that behind it, does.

**Wiring it instead was the alternative, and it is five lanes wide.** It needs the
backend request channel and the configuration-change message (`internal/vhost`), a
Device whose capacity can move under in-flight requests (`internal/blockdev`), an
`Apply` that acts on a size change with no epoch bump (`internal/agent`), a guest-lane
proof that reads `/sys/block/vda/size` before and after (`integration/vhost`), and only
then this lane's half: an RPC, a `controlplane.Resize` that rewrites the descriptor in
the same operation, and a flag on `cmd/control-plane`. That is a wave, not an item, and
four of those files belong to other tracks. Bringing the catalog half back is one commit
— the query, the two store methods, the contract cases — and it belongs in that wave.

**What replaced it is a property, not a gap.** `metadatatest`'s
`VolumeGeometryIsImmutable` runs *everyMutation* — the whole mutating surface, the same
list that already carries "a method added to the Store without a line here is a method
whose term guard nobody checks" — each against a fresh world, and reads the volume's
size and block size back through `GetVolume`. A resize brought back as a store method
and nothing else, the exact shape this deleted, fails there instead of passing a suite
that never looked. `queries/volumes.sql` now has no statement that writes `size_bytes`
after `CreateVolume`, and `schema.sql`'s column says so where the column is.

**Three planted bugs, each watched go red.** `v.SizeBytes = v.SizeBytes * 2` inside the
sim's `UpdateWatermarks`: `UpdateWatermarks changed the volume's geometry:
1073741824/65536 -> 2147483648/65536 (V1 has no resize)`. The same defect in SQL —
`SET size_bytes = size_bytes + 1` in `UpdateVolumeWatermarks`, regenerated — reddens
the pg lane alone with `... -> 1073741825/65536`, which is the half the sim cannot
prove for it. And the case this exists for: `ResizeVolume` put back on `sim.Store` with
a line in `everyMutation` and nothing else, which fails as
`ResizeVolume changed the volume's geometry`.

**`task deadcode` did not move, and that is worth writing down.** It reported 66
findings before this and 66 after: `metadata/sim` is explained at package granularity,
and `deadcode -whylive` answers "reachable only through reflection" for
`internal/metadata/pg.Store.ResizeVolume` — blind spot 1, which `hack/deadcode.sh`
documents. Nothing in the catalog layer's pg or `internal/db` half is visible to that
tool, so "the gate is green" was never evidence about this method, and the audit that
found it read the code.

**Handed to track A, because §3 is track A's file.** Four places in
`arquitectura_mvp_volumenes_remotos_v5.md` promise a resize V1 does not have: the
objective list ("Resize online (grow) del volumen persistente"), §9's guest feature list
("el grow se propaga vía actualización del config space + notificación; el guest expande
con `resize2fs`"), §30's roadmap item that pairs resize with snapshots and clones, and
§31's success criterion "Resize (grow) online end-to-end" — which is now a criterion V1
can neither meet nor fail, the same shape ADR-0026 left on four others.
`STATUS.md`'s phase-09 row and its "no production caller" list name `ResizeVolume` too.

No schema change (the column's comment moved; `task db:verify` re-plans to empty).
`task ci` is green, `task cover` reports production 90.2091% against the 90% floor, and
`go test -tags integration ./internal/metadata/pg` is green.

**D10: the operator half of the cordon has a command (2026-08-07).** ADR-0013 §5 splits
cordon between two actors, and only one of them could act. The pressure loop has cordoned
and un-cordoned hosts since D5; the human half — `lifecycle.CordonOperator`, the
`cordonOverwrite` authority table, `ErrCordonHeld`, and the `overwritable_reasons`
predicate the `SetHostState` statement carries so the loop cannot clear what a person set
— was reachable from no binary. `grep -rn CordonOperator --include='*.go' . | grep -v
_test.go` named the constant's declaration, the table, and the store contract, and stopped
there. So the mechanism that exists to protect an operator's decision from the automatic
loop could only ever be exercised *by* the automatic loop, and an operator about to reboot
a host had two options: leave it taking new volumes, or hand-write an UPDATE that skips
the term guard and the transition table.

`-cordon-host` and `-uncordon-host` are that command, in `cmd/control-plane/cordon.go`,
one-shots under the current term like every other admin verb in that binary. **The reason
is not a flag**, and that follows from the type rather than from brevity: `CordonReason`
answers "who is asking", there is exactly one human actor, and its own comment rejects a
second column that would have to agree with the first. What an incident needs from here is
the state, and `-fleet-status` already prints it as `OPERATOR` beside `DEVICE_PRESSURE`.

**The line names what the host is still serving**, because "cordoned" answers a question
an operator usually does not mean: a cordoned host keeps serving everything it holds
(`HostState.Serving`), so somebody who reads "cordoned" and reboots the machine has
stopped those volumes rather than protected them. Detaching them stays a separate command
and a separate decision.

**One planted bug.** `TestAnOperatorsCordonOutranksThePressureLoop` asserts on the row
`fleetReport` prints, never on the store's error, because a write with the wrong authority
returns nil exactly as happily as the right one and the difference is only visible to the
next reader. Writing `lifecycle.CordonPressure` instead of `CordonOperator` in `setCordon`
turns it red at the first step: `after -cordon-host the report says state=CORDONED
reason=DEVICE_PRESSURE; want CORDONED/OPERATOR`. Reverted by textual replacement.

Verified against a live deployment as well as in the test: a host at 47% used — well under
the 65% the loop un-cordons at — was cordoned by hand and left for four heartbeats, and
stayed `CORDONED OPERATOR`. `docs/plan/RUNBOOK.md` §3 carries that transcript.

No schema change, no new query. `task ci` is green.

**D11: the incident path is written down, and every step was run (2026-08-07).**
`docs/plan/RUNBOOK.md`. CLAUDE.md's maturity table names "runbook times measured" among its
criteria for production-verified and there was no runbook to time: the pieces an
incident uses all existed — `-fleet-status`, the hold-and-retry teardown, the self-cordon,
`-rebuild-metadata`, detach and attach — and nothing said how they fit together when
something is actually wrong.

**The constraint was that every step is a command that exists and an output somebody has
seen**, so it was built by running a deployment rather than by reading the tree: the pinned
Postgres with `schema.sql` applied, a `control-plane` serving, one Agent on the host
filesystem and a second in a container with a 64 MiB device so the ADR-0013 band could be
crossed in both directions for real. Four failures were then walked end to end — a host
holding unpublished data, a host that cordoned itself, a lost catalog, and a volume that
will not attach — and what the processes printed is pasted in. The deployment's one
honest gap is stated in the file's header: no guest booted, so every session in it is
empty and every published manifest says `"sequence":0`.

**Four things the run established that no document said, and two of them are hazards.**

- **Nothing answers "which hosts are holding unpublished data".** An Agent in its retry
  loop is `ACTIVE` with a one-second heartbeat in `-fleet-status`, indistinguishable from a
  healthy one; the only witness is that host's own log. That is the cost of the owner's
  hold-and-retry decision being *legible per host* rather than fleet-wide, and closing it
  means a field on the heartbeat — `api/`, `internal/agent`, a column — so it is an
  increment, not a line.
- **`LEADER … renewed Xm ago` is not liveness.** `renewed_at` is written by
  `AcquireLeadership` and nothing else, so a healthy Control Plane's age grows at exactly
  the same rate as a dead one's. The run read "renewed 12m22s ago" off a process that had
  been serving for twelve minutes. The cheap honest fix is for the word to be "elected".
- **Placement never looks at a heartbeat.** `Policy.Admits` reads `AcceptsPlacement` and the
  two device numbers; `LastHeartbeat` is not in it. So `-attach-volume` places a volume onto
  a host whose Agent has been down for minutes and reports `host_state=ACTIVE`, and the
  socket never appears. Measured. It matters most immediately after `-rebuild-metadata`,
  when an operator is placing every volume in the fleet by hand.
- **Electing a Control Plane against an emptied catalog is what stops the fleet serving**,
  not the loss of the catalog itself. Agents keep serving through the outage; the moment
  heartbeats succeed again the Control Plane refuses their reports for volumes it has never
  heard of and every Agent fences, publishes and drops its socket. So the rebuild is not a
  background repair — between the election and the last `-attach-volume` the fleet serves
  nothing, and the runbook orders the three steps for that reason.

The term continuity ADR-0011 exists for was observed rather than argued: a Control Plane
elected against a truncated catalog took term 2, because the bucket held
`control-plane/terms/00000000000000000001` and the Elector reads the bucket.

Two paths were run end to end from a clean state and are quoted in full — the catalog-loss
recovery (elect, rebuild, place, serving again) and the holding host (SIGTERM, hold for
five attempts, second signal and `SIGKILL` as the two escapes, store restored, publish,
exit 0). Exit codes are measured: 1 on abandon, 137 on `SIGKILL`, 0 on a publish that
landed. Exit 2 — superseded — was not reached in this run and the file says so rather than
claiming it.

`docs/plan/README.md` is track A's to update; the runbook is not linked from the map yet.

**D12: `block_size` says what it is, and half of DEV-0024 stays open (2026-08-08).** The
declared schema commented the column as `CoW segment granularity (64 KiB)`. Every claim
DEV-0024 made about that was re-checked against the tree before anything moved, and all of
them hold: `controlplane.VolumeSpec.BlockSize`'s own doc says "the logical block size
reported to the guest", `VolumeSpec.validate` refuses one that is not a multiple of
`sectorSize` (512), `control-plane -seed-block-size` defaults it to 4096 with the help text
"logical block size", and the Agent carries the desired volume's value into
`vhost.Config.BlockSize`, which `NewDevice` writes into the virtio-blk config as `blk_size`.
The entry says "hands it to `blockdev`"; that is the one thing in it that is not so —
`internal/blockdev` has no block size at all, and the value reaches the guest through
`internal/vhost`. The comment now cites the symbol that actually carries it.

**Why the comment and not the column.** The value stored has always been the guest's block
size, so nothing migrates. The fixtures in `internal/metadata/metadatatest` that pass 65536
are legal because 65536 is a sector multiple, not because anyone meant a segment, and that
coincidence is the likeliest reason the wrong comment survived a reader.

**`task db:plan` produced nothing, and that is the fact worth recording rather than the
step worth skipping.** A `--` comment in `schema.sql` is not a database object: pgschema
diffs the declared state against a live database, and PostgreSQL never stored this line, so
the diff is empty by construction. The run printed `No changes detected.` and then the
Taskfile's own branch, `nothing to plan: the database already matches
internal/schema/schema.sql`, and wrote no file under `migrations/`. So this increment adds
no reviewed plan, because there is no DDL to review. `task db:verify` still passes — the
declared state applies to an empty Postgres 18 and re-planning against the result is empty.
A schema comment that *does* reach the database would be `COMMENT ON COLUMN`, which this
file does not use anywhere; adopting it to make comments plannable would put the same
sentence in two places, which is the thing a single source of truth exists to prevent.

**What was not closed.** DEV-0024's second half — whether the 64 KiB CoW granularity is V2
or simply dead — is left open, and narrowing the entry without saying so is the failure this
paragraph exists to prevent. The tree is unambiguous that it does not exist *now*:
`cow.IntervalMap` works on the guest's real extents with no grid (its package comment
records `ActiveMap`'s deletion and the `SegmentSize` constant that went with it), and what
leaves the host is a chunk of up to `image.MaxChunkBytes` keyed by the digest of its
plaintext. But "does not exist" is not "is not coming back": `CHUNK-ADDRESSING-SPEC.md` §8
is unreviewed, and its *structure* answer moves the chunk key space and the AAD so that a
clone's publish writes only new digests — at which point how large a unit is addressed stops
being an implementation detail and becomes the thing being decided. A 64 MiB chunk dedups
almost nothing for a clone that wrote 512 bytes. Declaring the granularity dead today would
be guessing the answer to a question a human has been handed, which is what DEV-0024 was
opened to avoid.

**A seam nothing observes, found by planting rather than by reading.** In a scratch
worktree, `m.supervise(serveCtx, v, ln, d.GetBlockSize())` was replaced with a hardcoded
`512` — an Agent that ignores the catalog's `block_size` entirely — and `go test
./internal/...` stayed green, every package. Nothing in the tree asserts that this column
reaches the guest as `blk_size`. `internal/vhost`'s `GET_CONFIG` test asserts the field, but
against a `Config` whose `BlockSize` is zero, so it is reading the `SectorSize` default; and
`integration/vhost` sets `BlockSize: 512` in its desired volume, which is that same default,
so the fixture agrees with the plant by coincidence — the shape CLAUDE.md lists as "two
Agents pointed at the same wrong bucket so they agreed". Closing it is one arm in a lane
this track does not own (a desired volume with a non-default block size, and the guest's
`blockdev --getss` or the config's `blk_size` read back), so it is reported rather than
written here.

**D13: the ceiling refuses, and `chain_depth` gets the producer it was declared with
(2026-08-08).** `controlplane.Clone` refuses past `MaxChainDepth` with `ErrChainTooDeep`
instead of incrementing a number nobody read; the refusal names FLATTEN, the volume to run
it on, and the fact that the snapshot to clone is one taken afterwards. `cmd/control-plane`
gained the telemetry wiring it has never had — exporter, Provider, deferred flush — and
`-fleet-status` gained a `DEPTH` column and a header count. `task ci` green, production
coverage 90.4%.

**What the number is chosen against, since the task said not to take 5 because a document
says 5.** Step 3's measurement is the input: at attach each ancestor costs three fixed
object reads — its descriptor, then a Head and a Get for its snapshot manifest — plus one
Get per chunk that manifest names. Only the three are depth's own cost; the chunk Gets are
the dataset, which a volume downloads whatever its lineage looks like. At the ceiling that
is fifteen fixed round trips where a root pays none, and the only wall in that direction is
`agent.awaitBase`'s ShutdownGrace bounding one wait for a read view, which at any plausible
per-request latency sits an order of magnitude further out — which is what
`agent.maxChainWalk` being far above the ceiling means in practice. **So the attach does not
choose the number.** What does is the steady state: `cow.IntervalMap.Read` recurses into its
base unconditionally, so at depth D *every* guest read scans D+1 extent lists for the life
of the volume, and the host holds D+1 interval maps per attached clone. Both are linear and
nothing distinguishes four from five from six.

**There was therefore no cliff to derive a number from, and that is the finding rather than
an evasion.** A number invented from a benchmark would have dressed a policy up as a
measurement. What is left to choose on is what an operator has already been told — §20.1,
§10's `max_chain_depth` and the design document all say five — and the comment at
`MaxChainDepth` states the two measurements that would move it: a per-link constant that
stops being small (links whose manifests each name many chunks make the attach the binding
cost rather than the read), or an index in `cow` that stops a read from scanning every
layer. A flag was rejected: a ceiling an operator can raise per invocation is one that gets
raised during the incident it exists to prevent.

**It refuses at create because that is the only place it can.** Refusing during the walk
would turn a volume the fleet created successfully into one nothing can read, with a guest
already booting and the only remedy a FLATTEN of something that cannot be attached;
`agent.maxChainWalk`'s comment says the same thing from the other side. Refusing here costs
an operator a command they have not run yet, and it happens before a host is chosen, so a
refusal leaves no row, no descriptor and no charged byte to clean up.

**A thing FLATTEN must not break, which nothing enforces.** The comparison is against the
parent *volume*'s depth, and that describes the read path only while a snapshot is at its
volume's depth. It is today, because a snapshot is a delta published by the volume it
belongs to. FLATTEN is exactly what can break it: a flattened volume returns to depth 0
while the snapshots it published beforehand are still deltas over the old lineage, so a
clone of one of *those* would be admitted at depth 1 while reading through as many ancestors
as ever. Whatever FLATTEN does about its volume's earlier snapshots — rewrite them, refuse
to clone them, or something else — it has to leave that sentence true, or this comparison
stops bounding the thing it was chosen to bound. It is written at the comparison as well as
here.

**Where the producer went, and why not the Agent.** `read_view_layers` already reports what
a read walks — every layer, including the ones a Freeze adds and no lineage explains — from
the process that walks it, at the cadence it changes. A second series recorded there would
be a subset of the first. `chain_depth` is the catalog's number: the one the ceiling refuses
on and the one FLATTEN reduces, changed by nothing but the Control Plane. So it is recorded
at the change and not polled — between a clone and a FLATTEN a volume's depth cannot move,
and a poller would need a loop this process does not have, at fleet cardinality, to
re-report a number that is not allowed to have changed. An operator comparing the two series
is comparing a claim with what the object store made of it, which is only possible while
they are two.

**What a change-recorded series does not answer, and what was built because of it.** It goes
quiet, so "which volumes are deep *now*" is not a question it can take — and after a
`-rebuild-metadata` it has never been recorded at all for rows that came back from the
bucket. That is the whole reason `-fleet-status` grew the `DEPTH` column and the header
count: the operator who meets the refusal, or who wants to not meet it, needs the list of
volumes waiting on a FLATTEN, and the catalog is the only thing that holds it. Saying this
out loud is cheaper than discovering it during an incident with a dashboard that shows five
volumes cloned last Tuesday.

**`cmd/control-plane` recorded nothing at all before this**, which STATUS.md had already
noticed: no exporter, no Recorder. A producer added to `Clone` with that still true would
have written into a no-op for ever — the shape of every row in CLAUDE.md's "build it thin"
table — so the binary got the same wiring `cmd/volume-agent` has, built before anything
takes a Recorder.

**Three plants, each watched go red, each reverted textually.** Deleting the refusal (which
is exactly the code as it stood before this increment): *"the catalog's deepest volume is at
depth 6, the ceiling is 5"*, alongside a row, a descriptor and 1073741824 charged bytes for
a volume that should not exist — the test uses `Errorf` and not `Fatalf` at the sentinel
check precisely so a build with no ceiling reports what it did rather than only that it did
not refuse. Recording the parent's depth instead of the clone's: *"the collector decoded
chain_depth=0 for the clone the Control Plane created at depth 1"* — the assertion reads
each of the five links back from a collector, with a fresh reader per link, because the
samples differ only in their volume label and one reader would leave the value at the mercy
of which data point the SDK returned last. Counting the ceiling with `>` instead of `>=` in
the report: *"report does not say VOLUMES (3, 1 with no primary host, 1 at the depth ceiling
of 5"*.

**A comment this spec said was owed here, and it was.** `Clone`'s doc claimed a same-host
clone reads local NVMe while a cross-host clone pays the download. Nothing provides it:
`agent.parentView` GETs the ancestry on every attach, on the host that took the snapshot
exactly as on any other, and the only cache in `internal/agent` holds `VolumeKeys`. The
locality was EROFS plus a checkpoint plus a cached WAL, which ADR-0026 deleted; the
placement preference outlived it. The preference stays — it costs nothing, it is still the
right destination if that cache is ever built, and removing it would remove the only reason
`source_host_id` reaches placement — and the claim goes.

**What this increment did not do.** No lane asserts that the *binary* refuses: the e2e
fixture's `cloneSnapshot` would need five publish cycles to build a lineage at the ceiling,
which is a slow arm in a lane this track does not own, so the refusal is proven at the
Control Plane seam and the flag is proven only to compile. **The Control Plane's telemetry
path is wired and has never been run against a collector** — `internal/simio/real`'s
process-level export test covers `cmd/volume-agent`, and the equivalent for this binary
needs a database, so what is proven here is that `Clone` records the right number into a
real SDK reader, not that a byte leaves the process. That is the honest boundary and it is
the next thing anyone should distrust. No DST scenario: the ceiling is an admission
decision with no fencing or data-path property to violate, and the mandatory set's one slot
per window is better spent on the erasure arm step 3 asked for. No DEV entry opened;
DEV-0020 and DEV-0024 remain track A's to close.

### Placement policy lives in two processes' flags, and CI found the seam (2026-08-08)

A fleet refuses to place a volume for two independent reasons, and they are configured in
two different places:

- **the cordon band** (`cpserver.Band`, `-cordon-used-ratio`) — a flag on the process that
  *serves*, applied when a heartbeat arrives;
- **the fill ceiling** (`placement.DefaultMaxUsedRatio`, `-max-used-ratio`) — a flag on
  whichever process runs the *placing command*, applied by `placement.Admits`.

The first CI run failed every clone because a runner's disk is 87% full and the cordon
fires at 70%. That was fixed by letting the caller state the band, and the next run failed
every clone again — same message, different gate: `Admits` refuses above 85% and nobody had
told the one-shot otherwise.

**An operator can hit exactly this.** Tune the band and not the ceiling and you get a fleet
that never cordons and still answers "no host with capacity", with nothing in either
process's output relating the two. The lane now passes both, from one helper rather than
from a literal per call site, because the failure mode of a literal is a new placing
command that forgets it.

Worth changing rather than documenting: placement policy that lives in two processes'
flags is policy nobody can read back. The catalog is where a fleet's own rules belong —
`-fleet-status` could then print them, and a rule an operator cannot read is a rule they
will tune twice and reconcile never.
