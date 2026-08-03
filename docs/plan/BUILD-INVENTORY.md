# BUILD INVENTORY — one volume, one host, real binaries

> **Rewritten 2026-08-02 for ADR-0026.** The previous target slice — "FLUSH uploads a
> verified object to a real object store; a checkpoint publishes and the local WAL
> truncates" — is no longer the definition of done, because the SLO that required it was
> withdrawn. Increments 0-8 of the old road landed and their record is in git; nothing
> below re-does them.

**The target slice, restated.** `cmd/volume-agent` receives a volume via
`GetDesiredState`, fetches its DEK via `GetVolumeKeys`, serves a vhost-user-blk socket
backed by `wal.Log`; a real guest writes and `fsync`s, and the ACK is local; the volume is
**uploaded once when it stops**; a snapshot of a running VM is an `fsync` plus a copy at a
§19 sequence number; a new VM boots from either, preferring the host that already holds
the data.

**The RPO is one session.** A host that dies mid-session loses everything written since
the volume attached. That is accepted, stated to the user, and the reason half of what
follows is deletion.

---

## Read this before starting: the gate will block

CLAUDE.md says an observed doc↔code divergence is a DEV entry, and an open one blocks the
gate. **§2 of the architecture document still declares "RPO 0 bajo el modelo de fallas
probado por DST".** Every deletion below contradicts it, so every increment below opens a
divergence against a document that has not been revised.

**Increment 0 is therefore the document, and it is not optional.** It is also the one
piece of work that is not the implementer's to do — the architecture document is the
owner's. Until §2's SLO table and §14.8 are revised, the rest of this inventory cannot
pass its own gate.

---

## Increment 0 — the architecture document says what was decided *(owner)*

| Piece | Where | Why |
|---|---|---|
| §2's SLO table: RPO becomes "one session; writes covered by FLUSH are durable against process/Agent/QEMU crash, not host loss" | `arquitectura_mvp_volumenes_remotos_v5.md` §2 | It is the assumption every deletion below contradicts. Leaving it makes the gate unpassable. |
| §14.8: the dual mode collapses to one contract | §14.8 | `remote` is withdrawn, not kept as an option: a mode nobody selects is a second contract to keep correct for free. |
| §19 stays, and gets one sentence: a snapshot's frozen view *is* its sequence number | §19 | It already says "a snapshot is a number, not an event". That is what makes `fsync`-plus-copy work while the source VM keeps writing, and it is worth making explicit rather than inferred. |
| §12: fencing is scoped to "two writers must not both upload", not to every ACK | §12 | The protocol shrinks to a compare-and-set; the sections describing lease-gated ACK describe something that will not exist. |

**Done when:** the document no longer contradicts ADR-0026.

---

## Increment 1 — the free deletions *(no replacement needed)*

Nothing in production reaches these. They can go before anything is built, and going first
makes every later diff smaller.

| Piece | Lines | Why it is free |
|---|---|---|
| `internal/gc` + `scenarios_gc.go` | 381 + 223 | **Zero production callers**, verified: the only mention outside the package and the DST harness is a comment in `provision.go`. It was already dead weight before this ADR. |
| `wal.WriteSummary`, `wal.SummaryObject`, `Log.uploaded`, exported `wal.ReadSummary` | ~120 | Increment 12 of the old cleanup plan, subsumed. The writer has no production caller; `coveredLocked` is replaced by an incrementally maintained `covered uint64`, which is O(1) and cannot hold entries from a previous loop. Keep `recovery.readSummary` for now — it goes with `recovery` in increment 4. |

**Review zone:** no. **Done when:** `task ci:full` green, `task cover` still over the floor.

---

## Increment 2 — a volume is uploaded when it stops

The first half of the new path, and the one that makes the rest deletable.

| Piece | Where | Why |
|---|---|---|
| Upload the volume's state on a clean stop | `internal/agent` (`Volume.stop`, `VolumeManager.Close`) | Today `stop()` closes the log and nothing leaves the host. This is the whole durability contract of V1. |
| ★ Compare-and-set at stop, so two incarnations cannot both upload | `internal/epoch` (shrunk) | **The only fencing that survives**, and the one thing that must not be assumed away: two hosts both uploading is a silent lost update with no error anywhere. A CAS on one object replaces a lease renewed every three seconds gating every ACK. |
| Boot from the uploaded copy | `internal/agent` (`fetchBase`) | Replaces `recovery.RecoverOver`. One object read, not a chain reconstruction. |

**Review zone:** yes, the CAS (fencing). Its own spec, its own review, its own DST arm.

**Observable:** a guest writes, the Agent stops, an object appears; a second Agent starts
the volume and the guest reads its own bytes back. Two Agents racing to stop the same
volume produce one object and one loud refusal — not two objects.

---

## Increment 3 — a snapshot is an `fsync` and a copy *(mechanism done 2026-08-03)*

**Done:** `wal.Log.Freeze` (§19's capture + seal), `image.PublishSnapshot`/`LoadSnapshot`
(create-only, chunks shared with the volume's image), `agent.VolumeManager.Snapshot`, and
`parentView` reading the *snapshot* rather than the parent's live image — which was a live
defect, not a refactor. **Increment 3b (done 2026-08-03) is the trigger:** `pending_snapshot_id` on
`DesiredVolume`, the id/sequence/error back on `VolumeReport`, `ListPendingSnapshots` and
`PublishSnapshot` on the store, `controlplane.RequestSnapshot`, and
`control-plane -snapshot-volume`. No schema change was needed — the `snapshots` table
already held every column. Only §19's two metrics are left unrecorded.

| Piece | Where | Why |
|---|---|---|
| Snapshot of a *running* VM: `fsync`, freeze at the current sequence, upload a copy | `internal/snapshot` (simplified) | §2's primary use case is clone-from-snapshot. Under this ADR it no longer assembles a checkpoint plus the WAL objects after it — there are none. |
| The frozen view is the sequence number (§19) | same | The source VM keeps writing immediately after the `fsync`. Without freezing at a number, the copy is whatever the WAL held when the upload finished, which is not a snapshot of anything. This is also how §2's "pausa de I/O por snapshot ~0" survives. |

**Review zone:** yes (on-S3 format). **Observable:** snapshot a running VM, keep writing
to it, clone the snapshot, and the clone reads what was there at the `fsync` — not the
later writes.

---

## Increment 4 — cut the old path

**Only after 2 and 3 run end to end.** Deleting first would leave no way to serve a
volume, which is the mistake the old plan's ordering was written to avoid.

Ordered by the dependency graph, leaves first — `recovery` is imported by `snapshot`,
`materialize`, `drain`, `checkpoint` and `agent/volume`, so it cannot go early.

| Order | Piece | Lines | Note |
|---|---|---|---|
| 4.1 | the durability scheduler's checkpoint/truncate/drain | ~200 in `internal/agent` | `drainOnce`, `checkpointOnce`, `checkpointLoop`. The WAL lives one session; there is nothing to reclaim mid-session. |
| 4.2 | `internal/checkpoint` | 215 | Imported by `materialize`, `gc` (gone in 1) and `agent/durability` (gone in 4.1). |
| 4.3 | `internal/materialize` | 293 | Imported by `drain` and `agent/volume`. Its `FromSnapshot` is the closest thing to what increment 3 needs — read it before deleting it, do not assume it is all waste. |
| 4.4 | `internal/recovery` | 890 | The largest, and last because everything above imports it. |
| 4.5 | the remote half of `internal/wal` | part of 3 290 | Uploader, `durableStep`'s S3 branch, the batcher's role in it, `EnableRemote`. The **local** half stays entirely: append, replay, the torn tail, segments, the read view. |
| 4.6 | the fencing half of `internal/controlplane` | part of 1 841 | Promotion, `FENCING_WAIT`, the lease-gated paths. `internal/lease` shrinks to liveness if the Control Plane still needs it for placement and cordon — **check, do not assume it goes**. |

**Review zone:** yes, throughout. **Each of 4.1-4.6 is its own increment with its own
diff review**; they are listed together because the order between them is fixed, not
because they are one change.

**Watch for:** every deletion takes DST scenarios with it (`scenarios_recovery.go` 623,
`scenarios_drain.go` 801, and parts of `scenarios.go` 1 382 and `scenarios_agent.go`
1 589). A scenario deleted with the thing it proved is correct; a scenario deleted because
it went red is a stop signal.

---

## Increment 5 — the clone starts where the data already is *(done 2026-08-03)*

`Clone` takes a `placement.Policy` and no host; `control-plane -clone-snapshot` is the
production caller both it and `Choose` were missing. The snapshot's `source_host_id` is
what makes step 1 real, and increment 3b is what stamps it.

| Piece | Where | Why |
|---|---|---|
| `controlplane.Clone` asks `placement.Policy.Choose` instead of taking a host | `internal/controlplane/clone.go` | `Choose` already implements §20's three steps with `SourceHostID` first; its only production caller is the drain. Under this ADR a cross-host clone pays a full download with no warm standby and no lazy loading to shorten it, so this carries most of the boot-time story. |

Two properties to preserve: same-host is a **preference** — `Choose` already falls through
when the source host is full, cordoned or gone, and making it mandatory would couple
scheduling to a host with no obligation to be up — and the locality is **time-bounded**,
since the source host holds the data only while it still holds the volume.

**Review zone:** no. **Observable:** a clone of a snapshot taken from a running VM is
placed on that VM's host when it has capacity, and elsewhere when it does not, with the
boot time of each measured rather than promised.

---

## The invariants, after

Not a deletion list — a statement of which properties still have meaning, and it wants a
pass of its own against `INVARIANTS.md`.

**Survive untouched**, because none of them is about remote durability: INV-01 (simulable
interfaces), INV-02 (deterministic replay), INV-03 (ordered watermarks), INV-04 (unflushed
bounds), INV-05 (WAL serialize/replay), INV-15 (nothing leaves the host in cleartext),
INV-18 (S3 is not in the write path — now trivially true), INV-22 (uuidv7).

**Change meaning or go:** INV-06 (durable ACK requires a lease), INV-07 (FLUSH/FUA
ordering — the six steps become two), INV-08 (S3 is the recovery authority — it becomes
the *boot* authority), INV-09 (no ACKed write lost across failover — there is no
failover), INV-11 (promotion wait), INV-12 (recovery point), INV-13 (objectize before
truncate — there is no truncation), INV-21 (idempotent PUT — still true, far less
exercised).

**INV-10 (effective single writer) is the one to be careful with.** It does not go: it
becomes the compare-and-set of increment 2. It is the property that stops two hosts
silently overwriting each other's upload, and it is the last thing anyone should relax
while simplifying.

---

## What this inventory does not cover

- Any volume that must survive host loss mid-session. That is V2 and it comes back from
  git, against a real requirement (ADR-0026, "what would reverse it").
- Cross-host live migration and warm standby, which were the other reason the remote
  chain existed.
- Whether `internal/lease` disappears or shrinks to liveness. Recorded as an open question
  in increment 4.6 rather than answered here, because the Control Plane's need for a
  liveness signal is independent of fencing and has not been checked.
