# ADR-0013 — Local device pressure: thresholds, a reserve, and who is allowed to react

- **Status:** Accepted 2026-08-03, *as amended by ADR-0026*. §3 and §5 shipped; §1 and §2
  are open gaps; §4's subject was deleted with the WAL. **The section numbers are cited
  from ~45 places in the tree — they are never renumbered.**
- **Extends:** §5.7, §28.1–28.2 (cordon/drain, capacity accounting), INV-04.
- The per-volume quota is *soft* — it never fails a guest write. So the
  device is the only hard limit on the write path and the only source of an ENOSPC a
  guest can see.

## The problem

A host fills its NVMe and the GC cannot help: the GC sweeps the **bucket** and frees no
local byte, ever. Under ADR-0026 a session's data stays local until the volume stops, so
the device holds everything every attached volume has written, with no mid-session reclaim
at all — which makes the device the load-bearing limit, not a backstop.

## Decision

**§1. Backpressure is per device, not per volume.** *Open gap.* N volumes on one device,
each inside its own bound, exceed the device together and nothing sums them. The Agent
owns one device budget and hands each volume a share; admitting a volume requires budget,
not just catalog capacity — a host can sit inside its §28.2 bound with no room for the
volumes it already holds. The simulated disk models the ceiling per *device*
(`sim.Disk.SetDeviceBudget`, distinct from the per-file `InjectENOSPC`), which is what
makes the case testable at all.

**§2. A reserve the data path may never touch.** *Open gap.* 5% of the device, floor
1 GiB, unallocatable to volume data. Its consumer is no longer the checkpoint chain
(deleted with the uploader): it is the **image publish at stop**, because a volume that
cannot publish loses its whole session (ADR-0026). A device that is full for guest writes
must still be able to run the write that un-fills it.

**§3. Thresholds with distinct actions, not one cliff.**

| Device usage | Action | Who | State |
|---|---|---|---|
| ≥ 70% | **cordon** the host, term-guarded | CP, on the Agent's heartbeat | shipped (`cpserver.DefaultBand`) |
| ≥ 85% | refuse new placements on this host | CP, in the placement policy | shipped (`placement.DefaultMaxUsedRatio`, `-max-used-ratio`) |
| ≥ 85% | refuse new attaches locally | Agent | open gap |
| ≥ 95% | only the reserve's writer may write | Agent, locally | open gap, with §2 |

The measured input is `host.nvme_used_bytes`, written by the heartbeat. The ADR names one
cordon number and one number flaps, so the uncordon line (65%) is the code's; the reason
is at its declaration.

**§4. Reclamation means unlinking whole files, not truncating one growing file.** *Subject
removed.* This proposed segmenting the WAL; there is no WAL, no `OUT_OF_SPACE` degraded
state and no mid-session reclaim path. What survives is the property the simulated disk
asserts: truncating or removing a file gives the bytes back to the device budget, so a
reclaim path can be measured rather than assumed. **Known limitation, recorded here
because the file that held it is gone:** a filesystem with delayed allocation reports
ENOSPC at `fdatasync`, not at `write`; the simulated disk charges allocation at append.

**§5. Who is allowed to react.** *Shipped.* The Agent has local, defensive authority only:
backpressure, refusing attaches, marking itself degraded. Moving volumes is exclusively
the Control Plane's, driven by the Agent's reports — the Agent sees the device in real
time but not the fleet, the CP sees the fleet but arrives late, and if both may evacuate
then two actors drain one host. The same asymmetry covers cordons: the automatic loop may
clear only its own cordon, never a human's. Automatic drain picks by **remote gap**, not
by size — the backlog that is not closing is the one that keeps growing, and the largest
volume may be perfectly healthy.

## Alternatives rejected

- **Let it fill and rely on ENOSPC.** Every write then fails with an error the guest
  cannot act on, and the recovery path fails at the same instant as the data path.
- **A static per-volume quota enforced at attach.** Wrong variable: what grows is driven
  by how long the session runs and by object-store availability, not by the volume's size.
- **Reclaim the oldest local data regardless of what is published.** Those bytes exist
  nowhere else; this is the one thing reclamation may never do.
- **Detach the largest volume under pressure.** Turns a local, recoverable condition into
  an availability event for a guest that may be doing nothing wrong.
