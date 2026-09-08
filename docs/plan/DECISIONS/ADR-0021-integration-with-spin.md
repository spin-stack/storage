# ADR-0021 — storage integrates into spin; until then it must run end to end on its own

- **Status:** Accepted 2026-07-27
- **Affects another repository:** `github.com/aledbf/spin` migrates from Atlas to pgschema
  (§3). That half is spin's work; storage cannot close it and does not depend on it.

## Decision

### 1. spin imports storage. storage never imports spin.

The dependency direction is fixed now, and it is the one rule that must not bend: the
moment storage knows what a workspace is, it stops being testable on its own — which is
the property the rest of this ADR exists to protect.

At integration, spin's **controlplane** grows volume administration by importing storage's
control-plane packages, and spin's **runner** — the long-lived per-host daemon that
already owns the QEMU VMs — grows a volume subsystem that owns the per-volume data path.
Neither of storage's binaries is deployed.

### 2. storage's binaries are test harnesses, and they must stay fully runnable

`cmd/control-plane` and `cmd/volume-agent` will not be deployed, and are not throwaway
demos either. They must run the complete chain against real infrastructure: a real
PostgreSQL, a real object store, a real QEMU guest. Integration tests need a system that
can be stood up in one process tree and driven to failure, and it must exist **before** the
code moves into spin — otherwise the first time the durability chain runs end to end is
inside a product, where a fencing bug is a customer's data.

What they must not acquire is a second identity: no operator-facing polish, no
configuration surface that implies production, nothing that invites a deployment.

### 3. spin migrates from Atlas to pgschema

One database, one tool. ADR-0019's reasons apply unchanged, and spin has the one storage
did not: it will host storage's tables, so the alternative is two tools diffing one schema
— a state-based tool that recomputes and a versioned chain that replays, against the same
catalog.

### 4. A narrow public surface, promoted out of `internal/`

Two facades, not the package list. Names settle when the first consumer exists; the shape
is fixed here:

- **Host side** — "serve this volume": given a volume id, its geometry, its epoch, its key
  material, a disk root and an object store, return something that owns the local chain and
  can be stopped. **It is written as a self-contained type that `internal/agent`'s loop
  *uses*, never as a method on the loop**, so spin's runner can take the same type without
  the loop, its heartbeat, or its Control Plane client. (Today: `qcow.Manager`, with
  publishing injected — a local chain needs no Control Plane and no object store.)
- **Control-plane side** — provisioning, promotion, capacity: what spin's controlplane
  invokes over a `metadata.Store`. Already the shape of `internal/controlplane`.

Everything else stays internal and stays free to change. A narrow surface is what keeps the
format work (a human-review zone) from becoming a compatibility obligation to another
repository.

## What this does not decide

- **When.** Integration starts when storage serves one volume end to end, not before.
- **Whether the repositories merge.** Separate repositories are what keep storage's DST
  harness, its checkers and its 90% floor meaningful. Revisit after the first integrated
  release.
- **`volctl`.** No operator CLI while spin's `cmd/cli` exists and storage's surface moves.
- **Module path / Go version.** storage is on Go 1.26, spin on 1.25; a consumer must be at
  or above its dependency, so spin moves to 1.26 before importing storage.
