# Spec — who decides *when* to checkpoint and truncate (BUILD-INVENTORY increment 3)

**Status: awaiting human review of the plan below. Not implemented.** Durability review
zone. Unblocked by increment 5 (`VIEW-ADOPTION-SPEC.md`), which had to land first:
without it, truncating local WAL and then restarting served zeros.

**Corrected 2026-08-01.** The first version of this file asked six questions. Five of them
are answered by `arquitectura_mvp_volumenes_remotos_v5.md`, with numbers, and I had not
opened it — I grounded the spec in the code and in `BUILD-INVENTORY` instead, which is
exactly the re-designing-against-the-doc that CLAUDE.md forbids. What follows is what the
design says, what that means in this codebase, and the **one** thing it genuinely does not
settle.

## What already exists, and why this increment is small

`checkpoint.Checkpointer.Create` does the hard part, and it does it carefully:

- it verifies this host may publish into the epoch **before** writing anything
  (§12.3–12.4: two hosts can hold the same epoch *number*);
- it takes the durable sequence from **S3's proof**, not from the log's watermark,
  because the watermark was set when the PUTs returned and the prefix can have fallen
  behind it since;
- it publishes create-only and treats an identical existing checkpoint as its own retry
  (a crash between the PUT and `AdvancePublished` must converge, or `published` sticks
  and the WAL can never be truncated);
- it verifies the epoch **again** after publishing and before `AdvancePublished`, because
  advancing published is what authorises discarding the last local copy.

`wal.Log.TruncateLocal` is equally done: `StrictOrder.AllowTruncate` refuses anything
above `published`.

**So nothing in this increment is about how to checkpoint. It is entirely about when —
and the design already says when.**

Today nothing calls `Create` in a running system. `published` stays 0 forever, not one
byte is ever reclaimed, and a host's NVMe fills until writes stall for that volume and
every co-tenant of the disk.

## What the design already decides

| Question I asked | Where it is answered | The answer |
|---|---|---|
| What triggers a checkpoint? | §21.1 "Objectization", §10 config | `checkpoint_interval_bytes: 256 MiB`, `checkpoint_interval_time: 2m`. Both, whichever comes first. |
| Is the io-class budget per host or per volume? | §10 config, §11 | Per **host**: `background_net_budget: 30% de NIC`, `background_nvme_budget: 30% de IOPS/BW`. Those are host resources; there is no per-volume budget to argue about. |
| How much local WAL is kept? | §21.1 step 7, §14.7 | Everything with `sequences <= published_sequence` becomes eligible. No retention window. |
| Must the lease be valid to publish? | §12.6 (line 641) | Yes, explicitly: a `SELF_FENCED` Agent "deja de ACKear durabilidad, **deja de publicar checkpoints/manifests**". `VerifyPublisher` is not a substitute. |
| Where do reclaimed bytes go? | §26.2 | Metrics, not a proto field: `checkpoint_duration_seconds`, `objectization_pending_bytes`, `gc_reclaimed_bytes_total`. |

**So the plan is:**

1. One goroutine per volume, owned by `Volume`, stopped with it — the same shape as the
   serve loop and its supervisor.
2. It fires when **256 MiB of WAL has accumulated or 2 minutes have passed**, whichever
   is first. Both land on `VolumeManagerConfig` with those defaults.
3. It acquires `ioclass.Background` before touching the object store (INV-17). One
   `ioclass.Scheduler` per Agent, because the budget is a share of the host's NIC and
   NVMe.
4. It requires `Lease.Valid()` before calling `Create`, on top of the epoch verification
   `Create` already does. The two fail differently — the epoch object is a network read
   that can be stale-cached, the lease is local and monotonic — and §12.6 requires the
   cheap one.
5. `checkpoint.Create` → `TruncateLocal(published)`. No retention window, per §21.1.
6. Reclaimed bytes and checkpoint duration go to `obs`, not to `VolumeReport`.

Note this is consistent with the fencing already built, and the two paths are meant to
differ. §12.6 says a lease-expired Agent "puede seguir sirviendo reads de su caché
mientras QEMU siga conectado, según política" — and that is what happens: `wal` self-fences
the *durability* path (FLUSH fails) and reads continue. `VolumeManager.Fence` is the other
case entirely, where the Control Plane has said **another host is the writer**, and there
stale reads are the hazard, which is why that path tears the runtime down.

## The one thing the design does not settle

`checkpoint.Create` can fail with `ErrDurablePointMismatch` (S3 proves more than this log
ever ACKed) or `ErrCheckpointConflict` (a different checkpoint exists at the same
sequence). Both mean **two writers are in this epoch**. The design covers a writer that
learns it is fenced from its lease or from the Control Plane; it does not say what a
writer should do when it learns it from the object store.

Doing nothing is not an option — the scheduler would loop on a volume that has already
lost. The choices are to tear the runtime down as `Fence` does, and if so, whether to
record the fenced epoch so `Apply` does not restart it next cycle. Recording it means the
**data path can fence a volume the Control Plane still lists as this host's**, which is a
new authority and not one the design grants anywhere.

**That is an ADR, not a question**: it extends the doc rather than interpreting it, and
CLAUDE.md says an extension needs an ADR before merge. It will be written as one with a
recommendation (tear down and record, because the object store is a more authoritative
witness of a second writer than a heartbeat that has not failed yet), not carried as an
open question.

## Tests that must land with it

- **A DST arm** driving the full loop through the Agent: write → flush → checkpoint →
  truncate → restart → read. Increment 5's `scenarioTruncatedVolumeSurvivesARestart`
  already does the second half and truncates by hand; this replaces the hand-truncation
  with the scheduler and asserts segments actually disappear.
- **INV-13's checker under the scheduler**: `TruncateBelowPublishedChecker` exists and has
  never observed a scheduler, only hand-driven calls.
- **A planted bug**: a scheduler that truncates to the log's `durable` rather than its
  `published`. That is the single most tempting mistake here — they differ by exactly the
  window in which S3 has not confirmed — and the checker must catch it.
- **INV-17 under load**: a checkpoint in flight must not delay a guest FLUSH. Same shape
  as `TestAReadIsAnsweredWhileAFlushIsUploading`, which is the only reason the analogous
  regression in `blockdev` is impossible today.
