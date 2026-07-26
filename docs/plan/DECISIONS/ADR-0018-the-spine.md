# ADR-0018 — The spine: Agent first, Connect RPC in `api/`, one volume end to end under a real guest

- **Status:** Accepted 2026-07-26
- **Date:** 2026-07-26
- **Deciders:** human (decided), implementer agent (proposed the options)
- **Implements/Extends:** §3 (data path), §16 (Agent lifecycle), §14.8 (ACK contracts),
  §25.4 (integration lanes). Closes the shape question of **DEV-0007**.
- **Related:** ADR-0013 (device pressure), ADR-0014 (quota), ADR-0015/0016 (fencing).

## Context

Everything in this repository is a library. There is no `cmd/`, no `api/`, no process
that a guest can talk to — which is why the rebaseline calls every phase a *model*
rather than *integrated*, and why several closed findings end at "needs a caller that
knows its epoch". The remaining open items are all Agent-side: epoch-qualified
watermarks, ENOSPC to the guest, the quota gate on the write path, and the device budget
of ADR-0013.

## Decision

### 1. Agent first

The Control Plane already exists as a library and is drivable from tests; the Agent does
not exist at all, and every open item lives there. The first spine increment builds the
Agent, with the CP driven in-process (or stubbed) until the Agent's shape is known.

### 2. The vertical slice that defines "done"

**One volume, one host, one real QEMU guest attached over vhost-user-blk**, exercising
the full cycle:

```
guest write → WAL append → FLUSH → verified S3 object → checkpoint → TruncateLocal
```

Not a benchmark and not a feature set: the point is that the durability chain runs
end to end against real components — the pinned QEMU (already built and published by
its own workflow), a real object store (RustFS, already certified by the §6.1 suite),
and a real filesystem. Anything that only works because a test held it in memory shows
up here.

It is also the first time `TruncateLocal` matters in production terms, which is where
the reclamation gap (ADR-0013) stops being theoretical.

### 3. `api/` is Connect (connectrpc/connect-go)

The RPC surface is defined as protobuf in `api/` and served with
[connect-go](https://github.com/connectrpc/connect-go): one schema, HTTP/1.1 and
HTTP/2, gRPC-compatible wire format, and a plain-HTTP client that `curl` and a browser
can hit — which matters more than it sounds for an operator debugging a fenced host at
3am. Code generation joins the pinned toolchain (`buf` + `protoc-gen-connect-go`) and
runs through a Taskfile target with a `generate:check` twin, exactly as sqlc does.

### 4. The Agent pulls; the Control Plane never pushes

The Agent heartbeats and reconciles against the CP's desired state. It does not accept
commands.

This is the fencing decision in disguise: a pushing CP needs its own path to reach a
host, and a host it cannot reach is precisely the host being fenced — so push invents a
second fencing story for exactly the case where the first one matters. With pull, a
partitioned Agent simply stops learning, its lease lapses, and §12 does the rest.
It also means the CP needs no inbound reachability to hosts, which is the deployment
this design has assumed everywhere else.

## What this unblocks, in order

1. **Epoch-qualified watermarks** — a reporter that knows its epoch exists.
2. **ENOSPC / quota to the guest** (ADR-0014 §3) — there is a guest to return it to.
3. **The device budget and its thresholds** (ADR-0013) — the Agent is the component that
   owns a device.
4. **Per-volume fencing** (ADR-0016 stage 2) — the holdership cache lives in the Agent.

## Alternatives considered

- **Control Plane first (a `cmd/control-plane` around the existing libraries).** Faster
  to a running binary and proves nothing new: the CP's logic is what the last three
  waves hardened, and none of the open items are in it.
- **gRPC directly.** Fine, and gives up the plain-HTTP client for no benefit here; the
  wire format is compatible either way.
- **A REST/JSON hand-rolled API.** No schema, no generated client, and every field a
  fresh chance to disagree with the store's vocabulary.
- **Push from the CP.** See §4: it needs a second answer to the question fencing already
  answers.

## The tests that would enforce it

- An integration lane (`-tags integration`, Docker + the published QEMU image) that
  boots a guest, writes a known pattern through the virtio device, flushes, kills the
  Agent, and asserts the pattern is readable from S3 alone — the INV-09 property, for
  the first time end to end.
- A crash matrix over the same slice: kill the Agent at each boundary of the cycle and
  assert the guest's ACKed writes survive (INV-06), and that a resumed Agent continues
  the sequence space rather than restarting it (`wal.Resume`, wave 1).
- `generate:check` for the Connect stubs, so a stale generated client fails the gate the
  way stale sqlc output does.

## Consequences

- The repository gains `api/` (protobuf + generated Connect code, committed) and
  `cmd/volume-agent/`; `cmd/control-plane/` follows once the Agent's needs are known.
- The pinned toolchain grows `buf` and the Connect plugins, installed by `task tools`
  into `./.tools/bin` like everything else.
- The integration lane grows a QEMU-dependent job. It uses the image the QEMU workflow
  publishes rather than building it, so the per-push gate stays fast.
- `wal.Limits`, the quota gate and the degraded state all acquire their first real
  caller, which is when their contracts get tested rather than asserted.
