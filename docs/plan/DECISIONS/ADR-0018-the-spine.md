# ADR-0018 — The spine: Agent first, Connect RPC in `api/`, one volume end to end under a real guest

- **Status:** Accepted 2026-07-26
- **Implements/Extends:** §3 (data path), §16 (Agent lifecycle), §25.4 (integration lanes).

## Decision

### 1. Agent first

The Control Plane already existed as a library and was drivable from tests; the Agent did
not exist at all, and every open item lived there. The first spine increment builds the
Agent, with the CP driven in-process until the Agent's shape is known.

### 2. The vertical slice that defines "done"

**One volume, one host, one real QEMU guest**, exercising the full durability cycle from a
guest write to a state reconstructible from the object store alone. Not a benchmark and
not a feature set: the point is that the chain runs against real components — the pinned
QEMU, a real object store (RustFS), a real filesystem — so anything that only worked
because a test held it in memory shows up here.

(The cycle was written here as WAL append → FLUSH → S3 object → checkpoint →
`TruncateLocal`. v6 moved the local CoW format to qcow2 and removed the WAL; the milestone
is unchanged, its steps are not the ones above.)

### 3. `api/` is Connect (connectrpc/connect-go)

Protobuf in `api/`, served with connect-go: one schema, HTTP/1.1 and HTTP/2, a
gRPC-compatible wire format, and a plain-HTTP client `curl` and a browser can hit — which
matters for an operator debugging a fenced host at 3am. Generation joins the pinned
toolchain (`buf` + `protoc-gen-connect-go`) behind a Taskfile target with a
`generate:check` twin, exactly as sqlc has.

### 4. The Agent pulls; the Control Plane never pushes

The Agent heartbeats and reconciles against the CP's desired state. It does not accept
commands.

This is the fencing decision in disguise: a pushing CP needs its own path to reach a host,
and a host it cannot reach is precisely the host being fenced — so push invents a second
fencing story for exactly the case where the first one matters. With pull, a partitioned
Agent stops learning, its lease lapses, and §12 does the rest. It also means the CP needs
no inbound reachability to hosts.

## Alternatives considered

- **Control Plane first.** Faster to a running binary and proves nothing new: the CP's
  logic is what the last three waves hardened, and none of the open items were in it.
- **gRPC directly.** Fine, and gives up the plain-HTTP client for no benefit; the wire
  format is compatible either way.
- **A hand-rolled REST/JSON API.** No schema, no generated client, and every field a fresh
  chance to disagree with the store's vocabulary.
- **Push from the CP.** See §4.
