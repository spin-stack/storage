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

**Nothing here blocks a pilot.** Nine blockers closed, then 22 regressions those fixes caused
found by a re-run and closed, then the six residuals. `git log` has all of it. What is left
is one derivation and a set of things only a running tenant can answer.

1. **`MaxViewBytes` is a constant.** It defaults to 256 MiB where every other bound in the
   Agent is derived from something measured — `agent.Budget` reads the device with `statfs`,
   takes `GuestRatio`, subtracts `ReserveRatio` and divides by `-max-volumes`. The
   counterpart is one volume's share of measured RAM, read from `/proc/meminfo` **and the
   cgroup**, because an Agent in a container with a 2 GiB limit on a 256 GiB host must not
   size itself from the host. Operational, not correctness: the bound exists, fires, and has
   a proven escape hatch.

## What only a pilot can answer

Named here so nobody mistakes them for things that were checked.

- **Nothing has run against real S3.** Every lane used the filesystem store or RustFS. The
  `If-Match` CAS we fixed is ours; S3 evaluates its own, and that path has never carried a
  publish.
- **Two hosts under real load for hours** has never been run. The takeover lanes were minutes.
- **An upgrade of a running fleet** has never been run. A restart resumes at the same epoch;
  two versions serving at once is untested, and INV-19 becomes binding exactly there.
- **Sustained multi-volume load.** The soak was one volume; the device budget divides by
  `-max-volumes` and that division has never been under pressure from more than one guest.

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
