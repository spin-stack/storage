# STATUS — what is true right now

State only. What shipped is `git log`; why a decision was made is a comment where the
decision is made. If something here is finished, delete it.

## What runs end to end

Driven by real binaries in `integration/e2e`, with a real Linux kernel in `integration/vhost`:
a guest boots off a vhost-user-blk device, writes, `fsync`s (ACK is local `fdatasync`, zero
objects in the bucket — INV-18), stops, and its image appears sealed (INV-15) and CASed over
the manifest it booted from; a fresh data directory reads it back. Verified by hand on a
real machine 2026-08-09, binaries and a real kernel, including a clean stop and a restart on
an empty data directory. **INV-10 is claimed but does not hold on the filesystem store —
see item 1.** A live volume can
be snapshotted without pausing, cloned from that snapshot, flattened, and deleted. A guest can
DISCARD. An Agent that cannot publish holds rather than exits.

## Do this next

From a readiness run that broke the system seven ways against real binaries and real Linux
guests: 82 findings, 79 reproduced. Ranked by consequence — silent loss first. Everything
here is demonstrated, not reasoned.

1. **`If-Match` is not atomic across processes in the filesystem store, so INV-10 does not
   hold.** `real.ObjectStore.Put` implements create-only with `os.Link` — atomic — and
   implements the CAS as `ReadFile` → compare → stage → rename, guarded by a `sync.Mutex`
   that means nothing to a second process. Measured: 4 processes, same prevETag, 9 of 20
   rounds admitted more than one winner at a 4096-byte body. Reproduced end to end: two
   Agents serving one volume both logged `volume image published`, both exited 0, and one
   guest's fsynced session is in no manifest and no error. The blocking conformance suite
   cannot see it — both concurrency cases spawn goroutines inside one process, where the
   mutex covers it. **Fix:** hold an `O_CREAT|O_EXCL` lock file over read-compare-rename,
   and make the suite's concurrency cases fork processes.
2. **A KEK-less Agent writes guest plaintext into the bucket.** Started without `-kek-file`
   against a volume provisioned with one, it prints one startup WARN, serves
   `encrypted=false`, and publishes: `xxd` on the chunk shows raw guest bytes. Restarted
   against an *encrypted* volume it logs that reads will fail, then serves anyway and
   accepts writes that land in the WAL in cleartext. **Fix:** in `encryptionFor`, fetch the
   keys before deciding, and refuse any volume whose row carries a `kek_id`.
3. **The epoch never changes across a placement, so a returning host replays its stale WAL
   over the newer image.** Confirmed: A→B→A, `current_epoch` stayed 1 through four
   placements, A's old `wal/<vol>/1/` is the directory the new session opens, and the guest
   reads A's older bytes over B's published image. **Fix:** call `BumpVolumeEpoch` from
   `-attach-volume`. It is already implemented and contract-tested and has no caller.
4. **A partitioned Agent never stops serving.** 75 s past a 30 s lease TTL its socket is
   still bound; the operator moves the volume; both sockets exist and two real guests boot
   and fsync concurrently against one volume. **Fix:** tear the runtime down when the lease
   expires.
5. **Attach to a new host with unpublished data serves zeros, silently.** `-detach`/`-attach`
   after an unclean kill is accepted with no warning and the guest reads zeros where fsync
   returned success. Same shape when a manifest is missing: the volume serves as a blank
   device and the next publish makes it permanent. **Fix:** carry `published_sequence` in
   the desired state and refuse to serve when the read view is older than the catalog says.
6. **Losing the local WAL silently rolls a volume back to its last publish**, and the
   catalog's `GREATEST` hides it: `local_sequence` keeps the old high-water mark forever.
   **Fix:** report the resumed sequence and refuse or alarm when it is below the stored one.
7. **A torn WAL tail is dropped with no log line at all.** The only component that noticed
   was the tenant's guest. **Fix:** one WARN at resume naming volume, segment, offset and
   bytes discarded.
8. **Nothing surfaces a dead host, a dead Control Plane, or a stuck volume.** A killed Agent
   reads `ACTIVE` in `-fleet-status` forever; a Control Plane dead for 25 s still prints as
   `LEADER` (the term is taken once and never renewed); a superseded CP keeps listening and
   never exits; an unlinked socket makes a volume permanently unreachable with zero log
   lines. There is no scrapeable endpoint — the Agent holds no listening TCP socket, and
   every capacity series needs an OTLP collector no documented step stands up.
9. **Agent memory is unbounded and unmeasured from outside.** Measured on one volume:
   RSS 464 MB at 53k distinct 4 KiB writes, 1.55 GiB at 195k — ~2.18× the guest's working
   set, with the OOM killer as the only limit.

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
