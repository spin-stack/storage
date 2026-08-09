# RUNBOOK — what an operator does when something is wrong

**Every command here was run, and every block of output is what it printed.** Nothing in
this file is illustrative. Where a question has no command, it is under
[What nothing can answer](#what-nothing-can-answer) and marked **not-yet-possible** rather
than left out — a runbook is read under pressure by somebody who trusts it, so an invented
step is worse than a missing one.

**Verified 2026-08-07** against a single-machine deployment: the pinned `postgres:18-alpine`
with `internal/schema/schema.sql` applied, a `control-plane` serving, one `volume-agent`
on the host filesystem and a second in a container with a deliberately small device, and
`-object-store-dir` as the object store. What that deployment does *not* have is a guest:
no QEMU booted, so every session below is empty and every published manifest carries
`"sequence":0`. Two steps depend on a session having bytes in it, and they say so where
they appear. The lane that runs the same paths with a real kernel writing real blocks is
`integration/e2e` (`hold_test.go` for the holding case).

Throughout: `$DSN` is the catalog's connection string and `$STORE` is the object store —
`-object-store-dir <path>` here, `-s3-bucket <name>` in a real deployment. Ids are the ones
the verification run generated; yours differ.

---

## 0. The three things to know before touching anything

**`-fleet-status` is the only fleet-wide read, and it reads the catalog, not the fleet.**
It needs `-database-url` and nothing else — no `-holder-id`, no object store, no leader —
because the moment an operator most needs to look is the moment nothing is leading
(`fleetReport` in `cmd/control-plane/fleet.go` carries that reasoning). What it prints is
what PostgreSQL says. A host that is up and stuck looks identical to a host that is fine.

**Every write is a separate one-shot run of the same binary, and each borrows the
*current* term.** `-seed-volume`, `-snapshot-volume`, `-clone-snapshot`, `-rebuild-metadata`,
`-detach-volume`, `-attach-volume`, `-cordon-host`, `-uncordon-host` all call `GetLeader`
and fail if nobody is leading. They deliberately do not elect: `AcquireLeadership`
increments the term for the same holder id too, so an admin command that took a term would
leave the *serving* Control Plane refused as stale on every write it made afterwards.

**The data path does not stop when the control path does.** A volume already being served
keeps being served through a Control Plane outage, a catalog outage and an object-store
outage. What stops is placement, snapshots, and the ability to move anything.

---

## 1. First look

```
$ control-plane -fleet-status -database-url "$DSN"
as of 2026-08-07T11:23:44Z (the catalog's clock)

LEADER  019fdbeb-a8f2-7d75-92f6-17a3b2a01851  term 2  renewed 12m22s ago

HOSTS (2, 0 not taking placements)
HOST_ID                               STATE   REASON  USED           TOTAL    COMMITTED  HEARTBEAT
019fdbe7-7baa-7553-a781-7e2998b211a1  ACTIVE  -       1.3GiB (8%)    15.3GiB  1.0GiB     2s
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  ACTIVE  -       30.0MiB (47%)  64.0MiB  0B         0s

VOLUMES (1, 0 with no primary host, 0 at the depth ceiling of 5 — a clone of one is refused until it is flattened)
VOLUME_ID                             PRIMARY_HOST                          STATE   EPOCH  SIZE    DEPTH  PARENT_SNAPSHOT
019fdbe8-e65f-773b-8362-48a20fc3a034  019fdbe7-7baa-7553-a781-7e2998b211a1  ACTIVE  1      1.0GiB  0      -

SNAPSHOTS NOT FINISHED (0)
SNAPSHOT_ID  VOLUME_ID  STATE  EPOCH  ON_HOST
  (none)
```

Read it in this order.

**The header counts are the triage.** "N not taking placements" is why a placement
would fail; "N with no primary host" is how many volumes nobody is serving — after
`-rebuild-metadata` it is every volume there is; "N at the depth ceiling" is how many
volumes a clone request would now be refused for, and the `DEPTH` column names them. That
last one is the only place the fleet's lineage depth is visible while nothing is changing
it: `controlplane.Clone` records the `chain_depth` series when it creates a link, so the
series says what was created and this says what is being held.

**`HEARTBEAT` is the only liveness signal in the report.** Seconds means the Agent's
reconciliation loop is running. Minutes means it is not, and *the row still says `ACTIVE`*:
fleet state is a placement decision, not a health check, and `placement.Policy.Admits` reads
the state and never the heartbeat. Section 5 is what that costs.

**`LEADER ... renewed Xm ago` is not liveness.** The Control Plane that printed the line
above had been up and serving for twelve minutes when it was read. `renewed_at` is written
by `AcquireLeadership` and by nothing else — `grep -rn renewed_at internal/db/queries/`
finds one file, whose `INSERT … ON CONFLICT` is reached only at election and whose `SELECT`
is the read this line prints. The age is therefore the time since the *last election*, so a
healthy Control Plane's grows forever and a dead one's grows at exactly the same rate. To
find out whether one is running, look at the process or its port.

**If the catalog itself is unreachable, this is the first line you see** — the report reads
the catalog's clock before anything else, so a dead database fails here rather than after
three empty sections:

```
$ control-plane -fleet-status -database-url "postgres://cp:cp@localhost:55999/cp?sslmode=disable"
ERROR control-plane exited error="reading the catalog's clock: failed to connect to `user=cp database=cp`: 127.0.0.1:55999 (localhost): dial error: dial tcp 127.0.0.1:55999: connect: connection refused"
exit=1
```

---

## 2. A host is holding unpublished data and will not stop

### What it looks like

The Agent was sent `SIGTERM`, and it did not die. Its log repeats, on a widening backoff:

```
INFO  volume image publish started   volume_id=019fdbe8-… attempt=1 volumes_remaining=1
ERROR volume image publish failed    volume_id=019fdbe8-… attempt=1 retry_in=2s error="agent: volume 019fdbe8-…: publishing its image at sequence 0: image: publishing the manifest: open …/store/image/019fdbe8-…/manifest.json.470841859.tmp: permission denied"
WARN  agent is holding unpublished data and will not release its data directory volumes=1 volume_ids=[019fdbe8-…] attempts=1 oldest_wait=136.102µs data_dir=…/data
…
WARN  agent is holding unpublished data and will not release its data directory volumes=1 volume_ids=[019fdbe8-…] attempts=5 oldest_wait=30.007872403s data_dir=…/data
```

This is the decided behaviour, not a hang: the owner chose
hold-and-retry over exit-and-release on 2026-08-04. A flock is released by the kernel when
the process exits, so "do not release the data directory" can only mean "do not exit". The
`oldest_wait` and `attempts` fields are how long, and the `error` is why.

### What the rest of the system says about it

Nothing. Verified while the Agent above was in its retry loop:

```
HOST_ID                               STATE   REASON  USED         TOTAL    COMMITTED  HEARTBEAT
019fdbe7-7baa-7553-a781-7e2998b211a1  ACTIVE  -       1.2GiB (8%)  15.3GiB  1.0GiB     1s
```

`ACTIVE`, heartbeating every second. That is deliberate — the whole argument for holding
over exiting is that a stuck host stays *visible* — but it means **the catalog cannot tell
you which hosts are holding**. See [What nothing can answer](#what-nothing-can-answer).

Two checks that do work, from the host itself:

```
$ ps -o pid,stat,etime,args -p <pid>
    PID STAT     ELAPSED COMMAND
 921948 Ssl        03:48 /…/volume-agent -host-id 019fdbe7-… …
```

```
$ volume-agent -host-id <any> -data-dir <the held directory> …
ERROR volume-agent exited error="agent: another Volume Agent is already using /…/data (§10: one Agent per host): simio/disk: the lock is held by another process: /…/data/agent.lock"
exit=1
```

That refusal is the *kernel's* answer, not a flag the holding process set, and it is the
proof that the directory is still owned. It is also what a supervisor's restart will hit,
which is why a held Agent cannot be "just restarted".

### The options, and what each costs

**(a) Fix the store. Cost: the wait, and nothing else.** This is the case the design exists
for. Nothing touches the Agent; it notices on its next attempt:

```
store writable again at 11:13:32
INFO volume image publish started   volume_id=019fdbe8-… attempt=6 volumes_remaining=1
INFO volume image published         volume_id=019fdbe8-… sequence=0
INFO volume-agent stopped
```

and the process is gone, exit **0**, with the manifest in the bucket:

```
$ find "$STORE" -type f | sort
…/store/control-plane/terms/00000000000000000001
…/store/control-plane/terms/00000000000000000002
…/store/image/019fdbe8-e65f-773b-8362-48a20fc3a034/manifest.json
…/store/volumes/019fdbe8-e65f-773b-8362-48a20fc3a034/descriptor.json
```

**(b) A second `SIGTERM`. Cost: this session reaches the bucket only when this host runs
again.** It is an explicit override and it names what is being left behind:

```
ERROR abandoning this volume's unpublished session on request; it stays in this host's local WAL and a restart republishes it volume_id=019fdbe8-… epoch=1 local_sequence=0 data_dir=…/data wal=wal/019fdbe8-e65f-773b-8362-48a20fc3a034/1
ERROR volume-agent exited error="agent: the shutdown publish was abandoned; these sessions exist only in this host's local WAL: volume 019fdbe8-… at sequence 0"
exit=1
```

The `wal=` field is the path, relative to `data_dir`, that must survive. Exit **1** is the
supervisor's instruction to restart — `exitCode` in `cmd/volume-agent/main.go` documents
every code it can return, and **2** ("another writer published over us; do not restart") was not
reached in this verification run.

**(c) `SIGKILL`. Same cost as (b), and it is safe.** Measured: the process ends with 137,
the kernel drops the flock, and the next Agent on the same directory takes it and serves
the same epoch.

```
--- A is holding:
2                       # occurrences of the holding line before the kill
--- SIGKILL
A exit=137
--- B on the same data dir:
INFO volume-agent starting host_id=019fdbe7-… data_dir=…/data …
INFO serving volume volume_id=019fdbe8-… epoch=1 socket=/tmp/rbsock/019fdbe8-….sock resumed=false encrypted=true
```

`resumed=false` there because this verification's session was empty. A session with bytes
in it comes back as `resumed=true` and republishes on the next clean stop; that arm is
`integration/e2e/hold_test.go`, not this file.

**(d) Reboot, reimage or wipe `-data-dir`. Cost: the session, permanently.** (b) and (c) are
safe *only* because the records are on that disk. This is the one action to refuse until
either (a) has succeeded or somebody has decided the session is expendable.

---

## 3. A host cordoned itself

### Which query says why

`-fleet-status`, the `STATE` and `REASON` columns. Verified against a host whose device was
filled to 78%:

```
HOST_ID                               STATE     REASON           USED           TOTAL    COMMITTED  HEARTBEAT
019fdbe7-7baa-7553-a781-7e2998b211a1  ACTIVE    -                1.2GiB (8%)    15.3GiB  1.0GiB     1m7s
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  CORDONED  DEVICE_PRESSURE  50.0MiB (78%)  64.0MiB  0B         4s
```

`REASON` is the actor, and there are exactly two. `DEVICE_PRESSURE` is the Control Plane's
own reaction to the heartbeat, and it says so as it happens:

```
INFO device pressure changed a host's fleet state host_id=019fdbf0-… from=ACTIVE to=CORDONED nvme_used_bytes=52428800 nvme_total_bytes=67108864
```

`OPERATOR` is a human. No writer can produce a `CORDONED` row with `-` in `REASON`:
`SetHostState` refuses a reason with no authority, and the `hosts_cordon_reason_belongs_to_a_cordon`
constraint in `internal/schema/schema.sql` holds the other direction — a host that is not
cordoned has no reason.

**A cordoned host keeps serving everything it already holds.** Cordon stops *placement*
(`lifecycle.HostState.AcceptsPlacement`), not I/O; the lease is still renewed
(`HostState.Serving`). A cordon is not a way to take a host's volumes away.

### What clears it

**A `DEVICE_PRESSURE` cordon clears itself when the device drops below 65%** — and not
below 70%, because one threshold is a cordon that flaps (the band is in
`internal/cpserver/pressure.go`, `CordonUsedRatio` / `UncordonUsedRatio`). The dead zone
between the two is real and was measured. At 69% the host is still cordoned:

```
tmpfs                    64.0M     44.0M     20.0M  69% /data
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  CORDONED  DEVICE_PRESSURE  44.0MiB (69%)  64.0MiB  0B  0s
```

At 47% the next heartbeat returns it:

```
tmpfs                    64.0M     30.0M     34.0M  47% /data
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  ACTIVE  -  30.0MiB (47%)  64.0MiB  0B  0s

INFO device pressure changed a host's fleet state host_id=019fdbf0-… from=CORDONED to=ACTIVE nvme_used_bytes=31457280 nvme_total_bytes=67108864
```

Freeing space is the fix. The used figure is the *device's*, from one `statfs` behind
`-data-dir` (`agent.DiskUsage`), so another tenant of that filesystem counts and no
truncation of ours reclaims it.

**An operator forces it either way with `-cordon-host` / `-uncordon-host`.** These are new,
and until they existed the human half of the cordon authority had no command at all — see
[What nothing can answer](#what-nothing-can-answer) for what that means about the rest of
this file.

```
$ control-plane -cordon-host 019fdbe7-… -holder-id <uuidv7> -database-url "$DSN" -object-store-dir "$STORE"
INFO host fleet state changed by the operator host_id=019fdbe7-… state=CORDONED still_serving=1 volume_ids=[019fdbe8-e65f-773b-8362-48a20fc3a034]
```

`still_serving` is on that line because "cordoned" does not answer the question an operator
usually means. Those volumes keep running; detaching them is section 5's separate command.

**An operator's cordon outranks the loop, and this was measured, not assumed.** A host at
47% used — well under the 65% the loop un-cordons at — was cordoned by hand and left for
four heartbeats:

```
$ control-plane -cordon-host 019fdbf0-… …
INFO host fleet state changed by the operator host_id=019fdbf0-… state=CORDONED still_serving=0 volume_ids=[]
### four heartbeats later:
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  CORDONED  OPERATOR  30.0MiB (47%)  64.0MiB  0B  2s
```

It stayed. That is ADR-0013 §5 — the loop may not clear what a human set — enforced in the
`SetHostState` statement's `overwritable_reasons` predicate rather than by a read and a
write. `-uncordon-host` releases it:

```
$ control-plane -uncordon-host 019fdbf0-… …
INFO host fleet state changed by the operator host_id=019fdbf0-… state=ACTIVE still_serving=0 volume_ids=[]
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  ACTIVE  -  30.0MiB (47%)  64.0MiB  0B  4s
```

`-uncordon-host` also clears a `DEVICE_PRESSURE` cordon, which is how an operator overrules
the band. It does not lift the ceiling: the next heartbeat above 70% cordons the host
again, because pressure may overwrite "no cordon".

---

## 4. The catalog is gone

Read this section **before** running `-rebuild-metadata`, not after. The rebuild is
faithful about what it restores and the fleet does not resume on its own.

### What happens the moment the catalog empties

The serving Agents keep serving. Their heartbeats start failing, because the Control Plane
still holds a term the catalog no longer records, and every term-guarded write is refused:

```
WARN reconciliation cycle failed error="agent: heartbeat: aborted: cpserver: upserting host \"019fdbe7-…\": metadata: stale control-plane term" retry_in=4s host_id=019fdbe7-…
$ ls /tmp/rbsock
019fdbe8-e65f-773b-8362-48a20fc3a034.sock      # still there: the guest is unaffected
```

The Control Plane prints nothing at all while this is happening. `-fleet-status` is the
diagnosis:

```
LEADER  none — no Control Plane has ever been elected

HOSTS (0, 0 not taking placements)
  (none)
VOLUMES (0, 0 with no primary host, 0 at the depth ceiling of 5 — a clone of one is refused until it is flattened)
  (none)
```

### Step 1 — elect a Control Plane. Running the rebuild first does not work

```
$ control-plane -rebuild-metadata -holder-id <uuidv7> -database-url "$DSN" -object-store-dir "$STORE"
ERROR control-plane exited error="-rebuild-metadata needs a Control Plane to be leading (start one first): metadata: not found"
exit=1
```

Start one. **The term does not restart at 1, and that is the point** — the Elector reads the
bucket, not the database (ADR-0011), so a restored or emptied catalog cannot re-issue a term
some host still believes in:

```
$ control-plane -holder-id <uuidv7> -database-url "$DSN" -object-store-dir "$STORE" -listen …
INFO control-plane elected holder_id=019fdbeb-… term=2 version=dev
INFO control-plane serving listen=127.0.0.1:18080 lease_ttl=30s

$ ls "$STORE"/control-plane/terms/
00000000000000000001
00000000000000000002
```

If the object store is also gone, stop here: there is no way to prove a term has never been
issued, and the Elector fails closed rather than issue one anyway.

**Electing a leader is what stops the fleet serving.** As soon as heartbeats succeed again,
the Agents report volumes the fresh catalog has never heard of, the Control Plane refuses
each report, and the Agents fence:

```
WARN the control plane listed no volumes for this host; the ones already being served are kept, because an empty desired state names nothing serving=1 volume_ids=[019fdbe8-…] listed=0
WARN volume fenced; tearing its runtime down volume_id=019fdbe8-… epoch=1
INFO volume image published volume_id=019fdbe8-… sequence=0
$ ls /tmp/rbsock          # empty
```

The session is published on the way down, so nothing is lost — but the socket goes with it
and the guest's I/O stalls (`VolumeManager.Fence` explains why reads stop too). Between this
step and step 3, the fleet serves nothing.

### Step 2 — rebuild

```
$ control-plane -rebuild-metadata -holder-id <uuidv7> -database-url "$DSN" -object-store-dir "$STORE"
INFO catalog rebuilt from the object store; no volume has a primary host — place them to resume serving volumes=1 snapshots=0
exit=0
```

**What it restores:** every volume that has a `volumes/<id>/descriptor.json`, and every
published snapshot, with their sizes, block sizes, epochs, wrapped DEKs and lineage. Hosts
come back on their own, by heartbeating.

**What it does not restore: placement.** No object in the bucket records which host was
serving which volume, so every volume comes back with none:

```
VOLUMES (1, 1 with no primary host, 0 at the depth ceiling of 5 — a clone of one is refused until it is flattened)
VOLUME_ID                             PRIMARY_HOST  STATE   EPOCH  SIZE    DEPTH  PARENT_SNAPSHOT
019fdbe8-e65f-773b-8362-48a20fc3a034  -             ACTIVE  1      1.0GiB  0      -
```

It also does not restore anything about a volume that has no descriptor. A volume
provisioned by a Control Plane that never wrote one is not in the bucket and does not come
back.

### Step 3 — the human part: place every volume

Nothing does this for you. One run per volume:

```
$ control-plane -attach-volume 019fdbe8-… -holder-id <uuidv7> -database-url "$DSN" -object-store-dir "$STORE"
INFO volume placed volume_id=019fdbe8-… host_id=019fdbe7-… host_state=ACTIVE chosen_by=placement

INFO serving volume volume_id=019fdbe8-… epoch=1 socket=/tmp/rbsock/019fdbe8-….sock resumed=false encrypted=true
INFO read view recovered from the object store volume_id=019fdbe8-… epoch=1 durable_sequence=0 reclaimed_local_bytes=0 cloned_from=""
```

Omitting `-attach-host` is the intended form: `placement.Choose` decides, so an operator with
forty volumes does not have forty host ids to invent. `chosen_by=placement` versus
`chosen_by=operator` records which happened.

**Read section 5's warning before doing this in bulk.** Placement does not look at
heartbeats, so a rebuild that runs while a host is down can place volumes onto it and
report success.

---

## 5. A volume will not attach

The catalog says a volume is placed and no socket appears, or the attach itself refuses.
In order of how often each is the answer.

**First: is anything serving it?** A volume's runtime is a unix socket in the Agent's
`-vhost-socket-dir`, named for the volume. `ls` on the host the catalog names is the fastest
answer there is, and if the socket is absent the Agent's log says why. Every case below was
produced against a live Agent.

**The Control Plane is unreachable.** The Agent never gets a desired state at all:

```
WARN reconciliation cycle failed error="agent: heartbeat: unavailable: dial tcp 127.0.0.1:19999: connect: connection refused" retry_in=2s host_id=019fdbf6-…
```

**The host holds a different KEK from the one the volume was provisioned under.** Both key
ids are in the line, and neither is a secret:

```
WARN reconciliation cycle failed error="agent: applying the desired state: agent: volume 019fdbe8-… is wrapped under KEK \"kek-cf688370f908b849\"; this host holds \"kek-df05c96cdd005c7f\"" retry_in=4s host_id=019fdbe7-…
```

Fix the host's `-kek-file`. Do not re-provision the volume: the DEK is wrapped under the
original KEK and nothing else can unwrap it.

**The socket path is too long.** A unix socket path has a hard kernel limit, and
`-vhost-socket-dir` plus a UUID plus `.sock` can exceed it. The error names the syscall and
not the cause, which is why it is here:

```
WARN reconciliation cycle failed error="agent: applying the desired state: agent: volume 019fdbe8-…: opening /very/long/path/019fdbe8-….sock: hostio: listening on …: bind: invalid argument" retry_in=5s host_id=019fdbe7-…
```

Meanwhile `-fleet-status` shows the volume placed on an `ACTIVE` host with a fresh
heartbeat. Nothing in the catalog is wrong.

**The host in the catalog has not heartbeated.** This is the one that looks like nothing at
all, because `-attach-volume` succeeds. Placement admits on state and capacity —
`placement.Policy.Admits` reads `AcceptsPlacement` and the two device numbers, and never
`LastHeartbeat` — so a host whose Agent has been down for minutes is a candidate. Measured,
with the Agent stopped:

```
HOST_ID                               STATE   REASON  USED           TOTAL    COMMITTED  HEARTBEAT
019fdbe7-7baa-7553-a781-7e2998b211a1  ACTIVE  -       1.2GiB (8%)    15.3GiB  0B         2m56s
019fdbf0-54d8-7ba8-9201-87e09d8cc5d3  ACTIVE  -       30.0MiB (47%)  64.0MiB  0B         3s

$ control-plane -attach-volume 019fdbe8-… …
INFO volume placed volume_id=019fdbe8-… host_id=019fdbe7-… host_state=ACTIVE chosen_by=placement
$ ls /tmp/rbsock          # empty
```

`host_state=ACTIVE` on that line is the fleet's state, not the host's liveness.
**Read the `HEARTBEAT` column before placing anything.** If the host you meant is stale,
`-cordon-host` it first: placement then skips it, and the cordon is `OPERATOR` so the
pressure loop will not undo it.

### When the attach itself refuses

These are the refusals as printed, all exit **1**.

```
$ control-plane -attach-volume <placed volume> -attach-host <a different host> …
ERROR control-plane exited error="metadata: volume is already placed on another host: volume 019fdbe8-… is placed on 019fdbe7-…, detach it before placing it on 019fdbf0-…"
```

A volume moves in two runs and never one. The store refuses the hand-over because a host
learns it has lost a volume only on its next poll, so a single write would have two Agents
serving. Passing both flags at once is refused for the same reason:

```
$ control-plane -detach-volume <v> -attach-volume <v> …
ERROR control-plane exited error="-detach-volume and -attach-volume are two separate runs: a volume moves host by being detached, observed to have stopped, and then placed"
```

The move, verified end to end:

```
$ control-plane -detach-volume 019fdbe8-… …
INFO volume detached; its host stops serving it on its next poll, and publishes the session's image as it does volume_id=019fdbe8-…

WARN volume fenced; tearing its runtime down volume_id=019fdbe8-… epoch=1
INFO volume image published volume_id=019fdbe8-… sequence=0
$ ls /tmp/rbsock          # empty — this is the "observed to have stopped" step
$ find "$STORE" -type f
…/store/image/019fdbe8-e65f-773b-8362-48a20fc3a034/manifest.json
…/store/volumes/019fdbe8-e65f-773b-8362-48a20fc3a034/descriptor.json

$ control-plane -attach-volume 019fdbe8-… …
INFO volume placed volume_id=019fdbe8-… host_id=019fdbe7-… host_state=ACTIVE chosen_by=placement
```

Waiting for the socket to disappear is not optional politeness. It is the step that makes
the release safe — the teardown is what puts the session in the bucket.

```
$ control-plane -attach-volume 019fdbe8-… -max-used-ratio 0.05 …
ERROR control-plane exited error="controlplane: placing volume 019fdbe8-…: placement: no host with capacity"
```

`no host with capacity` covers all three refusals at once: no `ACTIVE` host, none under
`-max-oversubscription` on committed bytes, none under `-max-used-ratio` on measured fill.
`-fleet-status`'s `STATE`, `COMMITTED`/`TOTAL` and `USED` columns are the three to compare
against, in that order.

```
$ control-plane -attach-volume <volume> -attach-host <unknown host> …
ERROR control-plane exited error="controlplane: host 019fdbf1-…: metadata: not found"

$ control-plane -attach-volume <unknown volume> …
ERROR control-plane exited error="metadata: not found"
```

A named host is honoured **without an admission check** — the fleet's ceilings are not
applied to a decision an operator made by hand — which is why the success line reports
`host_state`. Placing onto a `CORDONED` or `DRAINING` host works and says so.

---

## 6. A volume has to be deleted — and the retention window is not ours

**Verified 2026-08-08** on the same shape of deployment as the rest of this file, with the
same gap: no guest booted, so the volume below had a descriptor and no published image
(`descriptor=true manifest=false`). What that leaves untested here is the size of the
numbers, not the steps.

### What a delete means, in one sentence, because it is not reversible by anything here

Deleting a volume means no VM can be created or started from it: the objects stop
answering and the catalog rows go. **The couple of days of recovery come from the bucket,
not from this repository.** Every object is marked, never removed — `objectstore.Store`
has no permanent delete on it, and `real.NewS3Store` refuses a bucket without versioning —
so what actually expires a deleted volume is the bucket's own lifecycle policy. If nobody
configured one, nothing ever expires and a delete costs storage forever. If somebody set
it to a day, a delete is irreversible after a day. **This code cannot see that setting and
cannot enforce it**, which is why it is written down here and in the next subsection rather
than validated at startup.

### What the deployment owes the bucket, before the first delete

Three things, on the bucket the Agents and the Control Plane are pointed at:

- **Versioning enabled.** Not optional and not merely recommended: `real.NewS3Store` calls
  `GetBucketVersioning` at construction and fails closed, so a bucket without it is a fleet
  that will not start. That is the *only* part of this the code checks.
- **A lifecycle rule expiring non-current versions**, whose retention is the deployment's
  recovery window. The design document's §5.11 is where the shape comes from — *"El borrado
  real lo ejecuta el lifecycle del bucket sobre versiones no-actuales tras el grace
  period"* — and the number is a decision nobody in this repository is allowed to make for
  a deployment. Write the number down next to the bucket, and write it here.
- **A rule expiring the delete markers themselves**, or a deleted volume leaves a marker per
  object for ever. It costs no storage and it does make a `LIST` of the bucket slower and a
  bill line item confusing.

**Shortening the retention shortens the only recovery path a deleted volume has.** There is
no second copy, no snapshot of the catalog that helps, and no undo verb in any binary here:
recovery is "the previous versions are still in the bucket, so put them back and rebuild".
When they are gone, they are gone.

### Deleting one

Two preconditions, and the command refuses on each rather than working around it. The
volume must be **detached** — a host learns it has lost a volume only on its next poll, so
deleting a placed volume takes the objects out from under a guest that is still writing:

```
$ control-plane -delete-volume 019fe2b1-… -database-url "$DSN" -object-store-dir "$STORE" -holder-id cp-del
ERROR control-plane exited error="volume 019fe2b1-… is placed on host 019fe200-…: detach it first (-detach-volume 019fe2b1-…) and let that host publish its session. Deleting it now would take the objects out from under a guest that is still writing, and the host would not find out until its next poll"
```

and **nothing may descend from it**. That question is asked of the bucket, not of the
catalog, and the answer is each other volume's `descriptor.json`. A clone reads its
ancestry on every attach — publishing stopped flattening — so a descendant is flattened
first, by this command, calling the same one-shot as `-flatten-volume`. It needs
`-kek-file` for exactly that reason and for no other: a flatten opens every chunk the clone
reads and re-seals it under the clone's own lineage. A descendant that is still placed, or
that has published snapshots of its own, stops the delete with the flatten's own refusal.

Then it works, and says what it marked:

```
$ control-plane -detach-volume 019fe2b2-… …
INFO volume detached; its host stops serving it on its next poll, and publishes the session's image as it does volume_id=019fe2b2-…

$ control-plane -delete-volume 019fe2b2-… …
INFO volume deleted from the object store; every key it owned now carries a delete marker, and the bucket's lifecycle policy is what expires them volume_id=019fe2b2-… descriptor=true manifest=false snapshots=0 chunk_objects=0 chunk_bytes=0 flattened_descendants=0
INFO volume and its snapshots removed from the catalog; recovering it means restoring the objects and running -rebuild-metadata volume_id=019fe2b2-…
```

`chunk_bytes` is what the delete actually reclaims, and for a **clone** it is nearly always
zero — a clone writes into its *ancestry's* chunk store, so its data is not under its own
name and deleting it frees only its manifests. The command says so on the way out
(`chunks_belong_to_lineage_of=…`). To reclaim a clone's data, flatten it first — which
re-homes its dataset under its own id — and then delete it.

Running it again is not an error. Every step treats "already gone" as done, so an operator
who is not sure the first run finished re-runs it:

```
$ control-plane -delete-volume 019fe2b2-… …
WARN no catalog row names this volume; deleting whatever the bucket still holds under its names volume_id=019fe2b2-…
INFO volume deleted … descriptor=false manifest=false snapshots=0 chunk_objects=0 chunk_bytes=0
```

### What the fleet says afterwards, and what the bucket still holds

`-fleet-status` no longer lists it, and `-rebuild-metadata` — the one path that brings a
volume back from the object store — does not resurrect it, which is the point of deleting
the descriptor first:

```
$ control-plane -fleet-status -database-url "$DSN"
VOLUMES (0, 0 with no primary host, 0 at the depth ceiling of 5 …)
VOLUME_ID  PRIMARY_HOST  STATE  EPOCH  SIZE  DEPTH  PARENT_SNAPSHOT
  (none)

$ control-plane -rebuild-metadata …
INFO catalog rebuilt from the object store; no volume has a primary host — place them to resume serving volumes=0 snapshots=0
```

And the bytes are still there. In this verification run the object store is a directory,
where a delete marker is a sibling file, so the window is visible directly:

```
$ find "$STORE/volumes" -type f
volumes/019fe2b2-…/descriptor.json.deleted
volumes/019fe2b2-…/descriptor.json
```

### Undeleting one, inside the window

There is **no `-restore-volume`** (see [What nothing can answer](#what-nothing-can-answer)).
The recovery is two steps, and the first is done with the bucket's own tooling:

1. **Remove the delete markers**, oldest object first — chunks, then `manifest.json`, then
   `snapshots/*.json`, then `descriptor.json`. The order is the inverse of the delete's, and
   it is not decoration: the descriptor is what makes the volume exist to the rebuild, so it
   goes back last, once the bytes it describes are already readable. On S3 that is
   `aws s3api delete-object --version-id <the delete marker's version>` per key; against
   `-object-store-dir` it is removing the `.deleted` file.
2. **`-rebuild-metadata`**, which reconstructs the volume and its snapshots from those
   objects. It restores no placement — no object records one — so finish with
   `-attach-volume` per volume, exactly as in [section 4](#4-the-catalog-is-gone).

```
$ rm "$STORE"/volumes/019fe2b2-…/descriptor.json.deleted
$ control-plane -rebuild-metadata …
INFO catalog rebuilt from the object store; no volume has a primary host — place them to resume serving volumes=1 snapshots=0

$ control-plane -fleet-status -database-url "$DSN"
VOLUME_ID                             PRIMARY_HOST  STATE   EPOCH  SIZE    DEPTH  PARENT_SNAPSHOT
019fe2b2-…                            -             ACTIVE  1      1.0GiB  0      -
```

**A restore that runs after the lifecycle expired the versions finds nothing and reports
success on the rebuild with `volumes=0`.** That is the only signal there is, so check the
number.

---

## What nothing can answer

Each of these is a question this verification run asked and could not answer with a command.

**Which hosts are holding unpublished data.** *(not-yet-possible.)* Section 2's Agent looked
exactly like a healthy one in `-fleet-status`: `ACTIVE`, heartbeat `1s`. The only witness is
that host's own log. Answering it fleet-wide means the heartbeat carrying the count of
held-back sessions, which is a field in `api/`, a change in `internal/agent`, and a column —
so it is an increment, not a line, and it is the largest hole in this file. Until then, the
answer to "did the rolling restart lose anything?" is a log search across the fleet.

**Whether the Control Plane is alive.** *(not-yet-possible.)* `LEADER … renewed Xm ago` is
the time since the last election and grows on a perfectly healthy process (section 1). A
serving Control Plane writes nothing periodically, so the catalog holds no evidence of its
liveness at all. Making the line mean what it reads as means renewing on a schedule, which
is a lease and a loop this system does not have; the cheaper honest fix is for the line to
say "elected" rather than "renewed".

**How to undelete a volume with a command.** *(not-yet-possible.)* `objectstore.Store` has
`Restore`, both implementations honour it, and no binary calls it — so section 6's recovery
is `aws s3api` per key, in an order a human has to get right, during the one incident where
getting it wrong is expensive. It is a `-restore-volume` flag over the same deterministic
key set the delete walks, plus one rule that is not obvious and is why this is not a
one-line addition: `ErrRestoreSuperseded` on a chunk is *success* (a chunk is
content-addressed, so a re-published one is a different ciphertext of the same plaintext)
and on `manifest.json` it is fatal.

**Whether the bucket's lifecycle policy is what the deployment thinks it is.**
*(not-yet-possible, and structurally so.)* The retention window behind every "recoverable
for a couple of days" sentence in section 6 is a bucket policy. Nothing here reads it,
nothing validates it, and a delete run against a bucket with no such rule looks exactly
like one run against a bucket with a thirty-day rule. The only thing this repository checks
is that versioning is *on* (`real.NewS3Store` fails closed without it), which is the
difference between "recoverable for some period" and "not recoverable at all" — not the
period. Closing this means reading `GetBucketLifecycleConfiguration` at startup and
refusing, or logging, a bucket whose rule does not exist; it is small, and it is a decision
about whether a fleet should fail to start over an object-store policy.

**Whether a volume is actually being served.** *(not-yet-possible.)* `-fleet-status` prints
placement, which is intent. The socket is the fact, and it exists only on the host. Every
"is anything serving it?" in section 5 is an `ls` over ssh.

**How long any of this takes at fleet scale.** *(not-yet-possible.)* CLAUDE.md's maturity
table names "runbook times measured" as a criterion for production-verified, and this file
does not measure any: the verification deployment had two hosts and one empty volume. What
it does establish is the *shape* — one `-attach-volume` run per volume after a rebuild, no
batching, no parallelism — which is the part that will hurt at forty volumes.

**~~How an operator cordons a host.~~** Closed by this increment: `-cordon-host` and
`-uncordon-host` on `cmd/control-plane`. It is worth recording why it was missing, because
the shape recurs. Every mechanism behind an operator cordon existed and was tested —
`lifecycle.CordonOperator`, the `cordonOverwrite` authority table, `ErrCordonHeld`, the
`overwritable_reasons` predicate in `SetHostState`, and store-contract cases for all of it —
and `grep -rn CordonOperator --include='*.go' . | grep -v _test.go` reached no binary. The
half of ADR-0013 §5 that protects a human's decision from the automatic loop could only ever
be exercised by the loop.
