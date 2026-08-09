# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## What runs end to end

Driven by real binaries in `integration/e2e`, with a real Linux kernel in `integration/vhost`:
a guest boots off a vhost-user-blk device, writes, `fsync`s (ACK is local `fdatasync`, zero
objects in the bucket — INV-18), stops, and its image appears sealed (INV-15) and CASed over
the manifest it booted from; a fresh data directory reads it back. Verified by hand on a
real machine 2026-08-09, binaries and a real kernel, including a clean stop and a restart on
an empty data directory. INV-10 holds across processes on both
backends since 2026-08-09, and the conformance suite can now see it when it does not. A live volume can
be snapshotted without pausing, cloned from that snapshot, flattened, and deleted. A guest can
DISCARD. An Agent that cannot publish holds rather than exits.

## Do this next

From a readiness run that broke the system seven ways against real binaries and real Linux
guests: 82 findings, 79 reproduced. **Six of the nine blockers closed 2026-08-09** — the
cross-process CAS, the KEK-less Agent, the epoch per placement, the partitioned Agent, the
silent torn tail, and the dead-things-look-alive family. `git log` has each. Three remain.

1. **The catalog knows a volume's data is not where the Agent is looking, and nobody
   compares.** Two shapes of the same hole, both reproduced. `-detach`/`-attach` after an
   unclean kill is accepted with no warning and the guest reads **zeros** where fsync
   returned success, because the new host's read view came from a manifest that predates
   the lost session. Delete `image/<vol>/manifest.json` while the catalog says
   `published_sequence=8` and the volume serves as a blank 256 MiB device, then the next
   publish makes that permanent. In both cases the number that would catch it is already in
   Postgres. **Fix:** carry `published_sequence` in the desired state and refuse to serve
   when the read view is older than the catalog says, instead of booting empty.
2. **Losing the local WAL silently rolls a volume back to its last publish**, and the
   catalog's `GREATEST` hides it — `local_sequence` keeps the old high-water mark for ever.
   `wal.ResumeReport` now carries what replay actually recovered (wave 1); nothing compares
   it to `durable_sequence`. **Fix:** report the resumed sequence on the heartbeat and
   refuse or alarm when it is below what the catalog last acknowledged.
3. **Agent memory is unbounded and unmeasured from outside.** Measured on one volume:
   RSS 464 MB at 53k distinct 4 KiB writes, **1.55 GiB at 195k** — about 2.18x the guest's
   working set, with the OOM killer as the only limit. `read_view_bytes` exists and is now
   scrapeable, so the number is visible; nothing bounds it. **Fix:** decide what a volume
   does when its read view crosses a bound — backpressure is the honest answer and it is
   the mechanism that already exists.

## Thin paths that shipped without being deepened

- **A same-host clone downloads its whole ancestry**, exactly like a cross-host one: the
  placement preference buys nothing measurable. A per-host chunk cache keyed by digest would
  make it real, and content addressing makes that nearly free.
- **No `-delete-snapshot`.** One published snapshot disables FLATTEN for that volume forever.
- **An interrupted `-flatten-volume` refuses every later flatten** (`ErrAlreadyStarted`), with
  no resume path.
- **No bucket lifecycle is configured by any code**, so a delete marker is reversible for as
  long as nobody sets one — which is the whole recovery window.
- **No alerting artifact exists** (no rules file, no threshold comparison in code).
- **No distributed tracing.** Metrics reach a collector over OTLP; `request_id` correlates
  nothing.
- **No `/healthz`**, and a Control Plane that lost its term serves broken forever (the term is
  a closure over a constant; there is no renew loop).
- **The Connect API is unauthenticated and binds `:8080`**, with `GetVolumeKeys` on it.
- **Crypto-shred is partial**: deleting a volume drops both copies of the wrapped DEK, but a
  lineage shares one DEK and `crypto.KMS` has no destroy verb.
- **PITR has no artefact** — no tooling, no config, no rehearsal.

## Divergences (DEV entries)

The ratchet is `hack/dev-entries.sh` + `hack/dev-entries-open.txt`, and it turns one way: an
entry open here and unpinned there fails the gate, and a pin whose entry closed fails it too.
**0 open.** Resolved ones keep a struck heading so the check cannot pass vacuously; the
reasoning is in `git log`.

## ~~DEV-0007~~ — the spine's second half *(withdrawn 2026-08-03)*
## ~~DEV-0011~~ — a segment's space is charged as used, not reserved *(2026-08-08: accepted, not fixed)*
## ~~DEV-0012~~ — a self-fenced log still accepts WRITEs *(2026-08-02: not a divergence)*
## ~~DEV-0019~~ — a restarted encrypted volume served its guest ciphertext *(2026-08-02)*
## ~~DEV-0020~~ — a clone chain is flattened by copying, and nothing decided that *(2026-08-08)*
## ~~DEV-0021~~ — the recovery-point floor was fictional *(2026-08-02, by deletion)*
## ~~DEV-0022~~ — the §26.2 catalog described a withdrawn system *(2026-08-03)*
## ~~DEV-0023~~ — the design document still had resize *(2026-08-07: V2)*
## ~~DEV-0024~~ — the 64 KiB CoW granularity is retired *(2026-08-08)*
## ~~DEV-0025~~ — a manifest could not meet §25.2's bit-corruption half *(2026-08-08: framed)*
