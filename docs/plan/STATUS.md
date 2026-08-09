# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## What runs end to end

Driven by real binaries in `integration/e2e`, with a real Linux kernel in `integration/vhost`:
a guest boots off a vhost-user-blk device, writes, `fsync`s (ACK is local `fdatasync`, zero
objects in the bucket — INV-18), stops, and its image appears sealed (INV-15) and CASed over
the manifest it booted from (INV-10); a fresh data directory reads it back. A live volume can
be snapshotted without pausing, cloned from that snapshot, flattened, and deleted. A guest can
DISCARD. An Agent that cannot publish holds rather than exits.

## Do this next

Ordered by the thin-path rule. The first four came out of an audit of what a first alpha
needs; the last two out of a doc-vs-code audit.

1. **Stand up a deployment on a clean box from the built binaries, writing down each
   command.** Nobody has. Every lane runs on this tree with TestContainers and RustFS, so a
   real-S3 or bring-up gap would be invisible today, and it would outrank everything below.
   Two ordering traps already known: `-seed-volume` fails until the Agent has heartbeated
   (`hosts` is created by `UpsertHost` and `primary_host_id` is an FK), and `-host-id` is
   validated only by Postgres's `uuidv7` domain, so a v4 starts and fails inside the CP.
2. **An Agent started without `-kek-file` serves an encrypted-provisioned volume in the
   clear.** `encryptionFor` returns `nil, nil` when `KMS == nil` *before* calling `deps.Keys`,
   so the catalog's `kek_id` is never compared; the only guard is a start-up WARN. Refuse
   when `keys.KEKID != ""` and no KMS.
3. **A guest wedges at its device share with no signal at the moment it happens.** The bound
   is logged at start-up and `-max-volumes 1` widens it, but nothing fires when a log crosses
   it, and `blockdev`'s message to the guest is wrong: a FLUSH does not clear `MaxLocalBytes`.
4. **Nothing states the durability bound to an operator.** A host that dies mid-session loses
   everything written since attach. `-fleet-status` has the watermarks available and does not
   print them, and no runbook page says `-snapshot-volume` is the lever that bounds it.
5. **A volume returned to a host that served it before replays stale records over the newer
   image.** `resume` replays into the top layer, `InstallBase` installs the store's image
   underneath and only raises the counter when `durable > local`. Silent wrong data, reachable
   with two hosts — and `-attach-volume` is shipped and ungated.
6. **`-detach-volume` returns before the Agent publishes**, and `remove` calls `quiesce()`
   before `publishAttempt`, so the socket unlinks while the upload runs. A flatten or delete
   racing it can strand a session with `ErrSuperseded`, which is logged and nothing else.

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
