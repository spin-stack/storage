# ADR-0021 — storage integrates into spin; until then it must run end to end on its own

- **Status:** Accepted — 2026-07-27
- **Date:** 2026-07-27
- **Deciders:** human owner (approved), implementer agent (proposed)
- **Relates to:** ADR-0018 (the spine), ADR-0019 (pgschema)
- **Affects another repository:** `github.com/aledbf/spin` migrates from Atlas to
  pgschema. That half is recorded here because the decision was taken here; it needs a
  matching note in spin.
- **Status of that half, checked 2026-08-02: not started.** spin still has `atlas.hcl`,
  `migrations/atlas.sum` and an ordered migration chain. Nothing in storage depends on
  it — this repository's schema tooling is settled by ADR-0019 — so it is not a blocker
  here, but it is also not something storage can close. It is spin's work, and until
  spin does it the "one database, one tool" reason below is a plan rather than a state.

## Context

`spin-stack/` holds three repositories, not two:

- **spin** (`github.com/aledbf/spin`) — the product. `cmd/controlplane` (API + web),
  `cmd/runner` (**the long-lived per-host daemon that runs the QEMU VMs**),
  `cmd/supervisor`, `cmd/proxy`, `cmd/cli`.
- **spinbox** — the containerd runtime spin builds on: a shim per VM, a kernel that
  boots in ~90 ms, and `cmd/vminitd`, a static Go init.
- **storage** — this repository.

storage was designed without reference to spin and arrived at the same answers on the
questions that matter. Both use `connectrpc.com/connect` over a proto schema in `api/`.
Both have the host-side component connect **outbound** to the control plane and pull —
storage wrote that down as ADR-0018 ("the Agent pulls, the Control Plane never pushes"),
spin's README states it as "runners connect outbound; no inbound ports required". Both
use sqlc against PostgreSQL 18 with the desired state in `internal/schema/schema.sql` —
the same file path, independently.

That convergence is the reason this ADR is short. What remains is three facts:

1. **The binaries duplicate spin.** `storage/cmd/control-plane` is a second control
   plane with the same stack against the same database. `storage/cmd/volume-agent` is a
   second long-lived host daemon pulling from a control plane, on a host that already
   runs `spin/cmd/runner` — which is the process that owns the QEMU VMs and should
   therefore own the vhost-user sockets and the WALs behind them.
2. **spin cannot import any of storage today.** Every package is under `internal/`, and
   Go's rule confines those to `github.com/spin-stack/storage/…`. All twenty-seven
   packages are unreachable from the product. This is not a detail to settle at
   integration time: it decides what the public surface is, and therefore what may
   change freely.
3. **Two schema tools would govern one database.** spin uses Atlas with a migration
   chain and `atlas.sum`; storage moved to pgschema (ADR-0019) because Atlas Community
   cannot diff a view and `atlas migrate lint` needs a licence.

## Decision

### 1. storage integrates into spin. The dependency direction is fixed now.

**spin imports storage. storage never imports spin.** Everything below follows from
that sentence, and it is the one rule that must not bend: the moment storage knows what
a workspace is, it stops being testable on its own, which is the property the rest of
this ADR exists to protect.

Concretely, at integration: spin's **controlplane** grows volume administration by
importing storage's control-plane packages, and spin's **runner** grows a volume
subsystem that owns the per-volume data path. Neither of storage's binaries is
deployed.

### 2. storage's binaries are test harnesses, and they must stay fully runnable

`cmd/control-plane` and `cmd/volume-agent` are **not products and will not be
deployed** — but they are not throwaway either, and they are not allowed to rot into
demos. They must run the complete chain against real infrastructure: a real PostgreSQL,
a real object store, a real QEMU guest.

That is the whole point. Integration tests need a system that can be stood up in one
process tree and driven to failure, and it must exist **before** the code moves into
spin — otherwise the first time the durability chain runs end to end is inside a
product, where a fencing bug is a customer's data. The harness is how we find out
whether the design works while it is still cheap to change.

So: the binaries stay, the gate covers them, and their honesty is load-bearing. What
they must not acquire is a second identity — no operator-facing polish, no
configuration surface that implies production, nothing that invites a deployment.

### 3. spin migrates from Atlas to pgschema

One database, one tool. The reasons that moved storage (ADR-0019) apply unchanged to
spin, and spin has the one storage did not: it will host storage's tables, so the
alternative is two tools diffing one schema — a state-based tool that recomputes and a
versioned chain that replays, against the same catalog.

The migration is spin's work and follows ADR-0019's shape: `schema.sql` stays the
declared state, `pgschema plan` produces reviewed DDL committed under `migrations/`,
`pgschema apply` runs a *saved* plan, and CI asserts that `schema.sql` applied to an
empty database leaves an empty plan. Atlas's ordered chain is replaced by the committed
plans; `atlas.sum` by that CI check, which compares the real result rather than the
file's bytes.

### 4. A narrow public surface, promoted out of `internal/`

Two facades, not twenty-seven packages. The exact names are settled when the first
consumer exists; the shape is fixed here:

- **Host side** — "serve this volume": given a volume id, its geometry, its epoch, its
  key material, a disk root and an object store, return something that owns a
  `wal.Log`, serves a `vhost-user-blk` socket, and can be stopped. This is the keystone
  runtime. **It is therefore written as a
  self-contained type that `internal/agent`'s loop *uses*, never as a method on the
  loop** — so spin's runner can take the same type without the loop, its heartbeat, or
  its Control Plane client.
- **Control-plane side** — provisioning, promotion, drain, capacity: the operations
  spin's controlplane invokes, over a `metadata.Store` implementation. Already the
  shape of `internal/controlplane`.

Everything else — `wal`, `cow`, `crypto`, `recovery`, `checkpoint`, `materialize`,
`gc`, `epoch`, `vhost` — **stays internal and stays free to change**. A narrow surface
is what keeps the format work (a human-review zone) from becoming a compatibility
obligation to another repository.

### 5. The guest kernel is consumed as an artefact, not as code

storage's test lane needs a Linux guest that can issue FLUSH. spinbox already builds
one (`task build:kernel`, `task build:initrd`) and already demonstrates the pattern for
a static Go init (`cmd/vminitd`). storage consumes **the built kernel image**, pinned
the way `RUSTFS_IMAGE` is pinned — a build-time dependency on a binary, reversible by
changing a path, with no Go import in either direction.

## What this does not decide

- **When.** Integration starts when storage serves one volume end to end
  at that milestone, not before. There is nothing to integrate until
  then, and moving code earlier would mean debugging the data path inside spin.
- **Whether the repositories merge.** Keeping them separate is what keeps storage's DST
  harness, its invariant checkers and its 90% floor meaningful. Revisit after the first
  integrated release, never before.
- **`volctl`.** No operator CLI while spin's `cmd/cli` exists and storage's surface is
  still moving.
- **Module path / Go version.** storage is `github.com/spin-stack/storage` on Go 1.26,
  spin is `github.com/aledbf/spin` on Go 1.25. A consumer must be at or above its
  dependency, so spin moves to 1.26 before importing storage. Noted, not scheduled.

## Consequences

- The keystone increment is written as a promotable type from the first commit. Same
  work, different shape; free now, a refactor later.
- storage keeps two binaries nobody deploys, and pays for them in the gate. That cost
  buys the only place the whole chain can be broken on purpose.
- spin pays for a schema-tool migration it did not ask for, before storage's tables
  arrive rather than after.
- The moment a package is promoted out of `internal/`, changing it costs a coordinated
  change in two repositories. That is the reason the surface is two facades and not the
  package list.
