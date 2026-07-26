# ADR-0020 — vhost-user host I/O is the one INV-01 exception outside `simio`

- **Status:** Accepted (Phase 03, increment 3.1)
- **Date:** 2026-07-26
- **Deciders:** human owner + implementer agent
- **Implements:** §3, §16, roadmap §30.3
- **Amends:** ADR-0003 (INV-01 enforcement), by adding exactly one exempt path

## Context

INV-01/§25.1 says production code reaches time, the network, the disk and the object
store only through `internal/simio`, and ADR-0003 enforces it with three layers plus a
custom analyzer. Phase 03 has to serve a virtio-blk device to QEMU 11.0.2 over
**vhost-user**, and vhost-user is not a message transport that happens to run on a
socket. Three of its primitives have no counterpart in `simio`:

1. **A `SOCK_STREAM` Unix socket.** `simio/network` carries length-delimited messages
   between named endpoints. vhost-user frames its own 12-byte header over a byte
   stream, and the framing matters: the descriptors below ride with the *first byte* of
   a message.
2. **File-descriptor passing over `SCM_RIGHTS`.** The memory table, the kick and the
   call are file descriptors, not bytes. There is no `simio` verb that transfers a
   kernel object.
3. **`mmap` of the front-end's memory.** The central act of the protocol is mapping
   QEMU's guest RAM into this process with `MAP_SHARED`. There is nothing to *simulate*
   about "the guest's RAM is now our RAM" that is not simply the mapping; a simulated
   version would be a byte slice, which is what the tests already use on the other side
   of the interface.

Modelling these inside `simio` would mean inventing an interface whose only
implementation is the real one, and whose "simulation" is a fiction nobody could use to
find a bug.

## Decision

**`internal/vhost/hostio` is exempt from INV-01. `internal/vhost` is not.**

- `internal/vhost` is pure: message framing, feature negotiation, guest-address
  translation, split-virtqueue walking and virtio-blk request handling all operate on
  ordinary byte slices behind four small interfaces it defines and consumes —
  `Conn`, `Listener`, `Mapper`, `EventFD`, plus `Backend` for the storage side. A
  simulated front-end drives the whole protocol in unit tests with no VM, no socket and
  no shared memory.
- `internal/vhost/hostio` is the only place the kernel is touched: `net.Listen` on the
  Unix socket, `recvmsg`/`SCM_RIGHTS`, `unix.Mmap`, `eventfd`, and the raw-file
  `Backend` that increment 3.1 serves (below).
- The exemption is a **leaf, not a subtree**. Both enforcement layers carry it and both
  carry a fixture proving the parent package is still checked:
  `.golangci.yml` (`depguard`, `forbidigo`) and `hack/analyzers/simulable`
  (`exemptPathFragments`, with `testdata/src/notexempt/internal/vhost`).

### The raw-file backend lives here too

Increment 3.1 serves a **raw file** through `vhost.Backend`. It does not go through
`simio/disk`: `disk.File` is append-only (`Append`, `ReadAt`, `Truncate`, `Sync`)
because that is what a WAL needs, and a block device needs random writes at arbitrary
offsets. The missing primitive is `WriteAt(p []byte, off int64) (int, error)`.

Widening `simio/disk` for this would be the wrong trade twice over: it would add a verb
to a durability-critical interface (and to both of its implementations, and to its
contract tests) for a backend that is **scaffolding** — Phase 04 puts `wal.Log` behind
`vhost.Backend` and the raw file becomes a test fixture. So `hostio.OpenRawFile` uses
`os.OpenFile`/`WriteAt`/`Sync` directly, inside the already-exempt package, and is
documented as what it is.

If a later phase needs random-write durable files in the data path — not as
scaffolding — that is the moment to add `WriteAt` to `simio/disk` with its own crash
model, not now.

## Consequences

- One more exempt path, with the same shape as the `simio` one: a package that exists
  *so that the exception has an address*.
- The part of vhost-user that has bugs worth simulating — the negotiation, the ring, the
  request handling — stays under INV-01 and is tested without a VM.
- The part that cannot be simulated is instead verified **by execution against real
  QEMU 11.0.2** (`integration/vhost`, `task test:integration:qemu`). That lane is the
  only thing that anchors the wire format to reality: a decoder tested against its own
  encoder proves only that it is self-consistent.
- `hostio` is thin on purpose. Every line in it is a line the unit tests cannot reach,
  so it is measured by the integration lane and by its own socket/eventfd/mmap tests,
  and anything genuinely unreachable without a VM is excluded from the coverage floor
  in `hack/coverage.sh` with the reason written down.
