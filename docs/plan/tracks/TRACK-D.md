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
