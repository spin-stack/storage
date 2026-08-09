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
DISCARD. An Agent that cannot publish holds rather than exits, and one that cannot find a
volume's data publishes no device at all.

**Measured under sustained load** (2026-08-09, one guest, one volume, 70 minutes, 142
samples): Agent RSS flat at ~27 MB, file descriptors flat at 17, the object store constant
at 355 bytes while the volume served (INV-18 holds over an hour, not just over a test), the
Agent's log flat at 5 lines for the whole run, and the guest's fsync rate steady at ~19.5/s
end to end — no drift. The local WAL grows about 4.7 MiB/min and nothing reclaims it
mid-session, which is the documented shape (§5.7) and the number nobody had.

## Do this next

Nine pilot blockers closed 2026-08-09; a readiness re-run then found 22 regressions those
fixes caused and the worst are closed too; `CarryForward` now has a mandatory DST arm that
crashes at every write it performs. A 70-minute soak with a real guest found nothing that
degrades — see "What runs end to end". `git log` has all of it. What is left:

1. **The soak could not reach the read-view bound, and that is a hole in the tooling, not
   in the bound.** `guestinit`'s hold mode rewrites eight blocks in rotation, so the read
   view stays pinned at 32 KiB however long it runs — the 256 MiB bound is unreachable by
   the only sustained-load generator this repo has. A hold variant that writes distinct
   offsets is a few lines in `integration/guestinit/main.go`, and without it nothing has
   ever driven the bound that stands between a guest and the OOM killer.
2. **No DST arm for "a volume that fails closed has no socket".** The behaviour landed with
   unit and e2e coverage; the deep gate wants a scenario.
3. **`published_sequence` still reaches the catalog one session late** for a volume that
   keeps serving. The teardown now reports it, so the window is narrow.
4. **`internal/lineage/flatten.go` reads `ErrNotPublished` as "never published".** Flattening
   a clone whose own image vanished writes down the ancestry and drops the clone's own layer.
   The attach path closed this; the flatten path was never on it.
5. **The read view's bound is a constant.** `MaxViewBytes` defaults to 256 MiB where the
   device bound is a share of a measured `statfs`; the honest counterpart is a share of
   measured RAM.
6. **Bring-up has three sharp edges**, all hit walking it by hand: `-holder-id` is required
   and documented only in the error; a `-vhost-socket-dir` over ~107 bytes fails as an opaque
   `bind: invalid argument` retried for ever (`sun_path` is 108); and applying `schema.sql`
   to a fresh database needs a `psql` nothing in the repo provides.

## Thin paths that shipped without being deepened

- **A same-host clone downloads its whole ancestry**, exactly like a cross-host one: the
  placement preference buys nothing measurable. A per-host chunk cache keyed by digest would
  make it real, and content addressing makes that nearly free.
- **No `-delete-snapshot`.** One published snapshot disables FLATTEN for that volume forever.
- **An interrupted `-flatten-volume` refuses every later flatten** (`ErrAlreadyStarted`), with
  no resume path.
- **No bucket lifecycle is configured by any code**, so a delete marker is reversible for as
  long as nobody sets one — which is the whole recovery window.
- **No alerting artifact exists** (no rules file, no threshold comparison in code) — including
  for `volumes.refusal`, which is now the fleet's one machine-readable "this volume is down".
- **No e2e scenario asserts a refusal reaching `-fleet-status`.** It is proven by the seam
  tests and by hand against real binaries (2026-08-09, NO_KEY and IMAGE_MISSING); the lane
  that boots a guest is where a refusal *caused by a guest's own history* belongs.
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
