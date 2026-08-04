# REFERENCE — resolve any symbol the code cites, without opening another file

> Whether this file still keeps its promise is a command, not a claim:
> ```
> comm -23 <(grep -rhoE 'ADR-[0-9]{4}' --include='*.go' . | sort -u) \
>          <(grep -ohE 'ADR-[0-9]{4}' docs/plan/REFERENCE.md | sort -u)
> ```
> Empty means every ADR the code cites resolves here. Swap the pattern for `INV-` or
> `DEV-` for the other two. This replaces the dated "up to date as of …" note that used
> to serve the same purpose and could only ever be true on the day it was written.
>
> The same check for the architecture document, which matters while ADR-0026 removes
> sections — a `§` the code cites must still exist:
> ```
> D=arquitectura_mvp_volumenes_remotos_v5.md
> grep -rhoE '(virtio 1\.2 )?§[0-9]+(\.[0-9]+)*' --include='*.go' . |
>   grep -v '^virtio' | sed 's/§//' | sort -u |
>   while read s; do
>     # §14.3.1 and §14.3.4 are numbered *rules inside* §14.3, and §29.4/§30.3 are
>     # items inside a numbered list rather than headings — so a match on the parent
>     # counts. Anything else that does not resolve is a citation left dangling by a
>     # section this document no longer has.
>     grep -qE "^#+ ${s%.*}[ .]|^#+ ${s}[ .]" "$D" || echo "dangling §${s}"
>   done
> ```
> Two things it deliberately does *not* flag, and both would make it noise if it did: the
> `virtio 1.2 §N` citations, which are a different specification entirely (see the warning
> above), and sub-rule numbering. A check that cries wolf gets ignored, which is worse than
> no check — this repository has learned that one more than once.
>
> Sections leave the document *with the code that cited them*, in the same commit, so this
> stays empty rather than being repaired afterwards.

The code carries **2.106 references** across 228 of its 264 Go files: `§14.4`, `INV-13`,
`ADR-0017`, `DEV-0007`. They are deliberate — they are what lets a reader check that a
comment about fencing says what §12.4 actually says, and they are how four audits found
real bugs. What they lacked was a way to *resolve* one without opening a 1.455-line
Spanish document.

That is this file. One line per symbol. If one line is not enough, the last column says
where the full answer lives.

> **`§` is overloaded — check the prefix.** A bare `§N` is the architecture document
> (`arquitectura_mvp_volumenes_remotos_v5.md`). `virtio 1.2 §N` is the **VirtIO 1.2
> specification**, a different document entirely (`internal/vhost`, `internal/blockdev`).
> The code always writes the `virtio 1.2` prefix when it means that one.

---

## `§` — the architecture document

Section numbers are stable; the doc is in Spanish, these glosses are not a translation.

| § | What it says |
|---|---|
| §1 | Executive summary: the shape of the whole system. |
| §2 | Use case and target SLOs. |
| §3 | MVP objectives, and the explicit non-objectives. |
| §4 | Principal decisions (EROFS + CoW + vhost-user-blk + PostgreSQL + WAL in S3). |
| §5 | **The invariants.** The subsections below are the ones the code cites. |
| §5.2 | Snapshots are immutable once PUBLISHED (→ INV-16). |
| §5.3 | S3 does not participate in every WRITE (→ INV-18). |
| §5.4 | Locality is not durability — a local write is not a durable one. |
| §5.5 | The ephemeral device may be lost; nothing durable may depend on it. |
| §5.6 | Watermarks are ordered: `published ≤ durable ≤ local` (→ INV-03). |
| §5.7 | Bounds on unflushed local WAL — backpressure, never a silent NVMe fill (→ INV-04). |
| §5.8 | **S3 is the recovery authority**; PostgreSQL watermarks are informative only (→ INV-08). |
| §5.9 | Background I/O always yields to foreground/flush (→ INV-17). |
| §5.10 | Nothing leaves the host in cleartext (→ INV-15). |
| §5.11 | The GC may not cause the worst incident — it marks, it never deletes (→ INV-14). |
| §6 | Deployment architecture. |
| §6.1 | **Object-store backend requirements** — the conformance suite (`task backend:conformance`). |
| §6.2 | Execution modes. |
| §7 | **Control Plane + PostgreSQL**: the verified term, and the volume failover states. The most-cited section in the code (96 references). |
| §8 | The minimal PostgreSQL model (tables). |
| §9 | Guest layout: the three devices and OverlayFS. |
| §10 | The Volume Agent. |
| §10.1 | The Agent's memory budget. |
| §11 | Internal I/O classes (foreground / flush / background). |
| §12 | **Fencing and leases — the full protocol.** |
| §12.1 | Explicit assumptions (chiefly: the monotonic clock is the only trusted one). |
| §12.2 | The lease cycle, Agent side: no durable ACK without a valid lease (→ INV-06). |
| §12.3 | Promotion, Control-Plane side: the FENCING_WAIT before granting epoch N+1 (→ INV-11). |
| §12.4 | Belt and braces: the epoch object with CAS in S3 (→ INV-10). |
| §12.5 | What happens to the old writer's late PUTs — the recovery point (→ INV-12). |
| §12.6 | Why the lease is per **host** and not per volume (fencing scalability). |
| §13.1 | Decoupled granularities: 64 KiB CoW segments vs the WAL record. |
| §13.2 | The read path. |
| §13.3 | The active map, with a memory bound. |
| §14 | **The exact WAL format and batching rules.** |
| §14.1 | WAL Record layout (header is **104 bytes**, not the doc's 96 — ADR-0005). |
| §14.2 | WAL Object layout (the remote batch). |
| §14.3 | Batch close + PUT rules. The `.N` suffixes are the numbered rules in that list: **§14.3.1** = close immediately on FLUSH/FUA; **§14.3.4** = close on age. |
| §14.4 | **The order of operations in FLUSH/FUA.** Six steps, and the reason ACK is last (→ INV-07). |
| §14.5 | PUT idempotency (→ INV-21). |
| §14.6 | DISCARD and space reclamation. |
| §14.7 | The WAL's local layout (now a directory of segments — `WAL-SEGMENTS-SPEC.md`). |
| §14.8 | **Per-volume durability modes** (`remote` / `local`) and what each ACK means. |
| §15 | Encryption at rest. |
| §15.1 | Key model (DEK per volume, KEK in the KMS). |
| §15.2 | Data encryption (AES-256-GCM). |
| §15.3 | Operational consequences of encryption. |
| §16 | **The Volume Agent's state machine** — ACTIVE, SELF_FENCED, FENCED, RECOVERING… |
| §18 | Idempotency (of operations, not of PUTs — that is §14.5). |
| §19 | Pause-free snapshots: a snapshot is a number, not an event. |
| §20 | Clone, locality and chains. |
| §20.1 | Chain flattening. |
| §21 | Objectization, compaction and GC. |
| §21.1 | **Objectization**: checkpoint first, then truncate — never the reverse (→ INV-13). |
| §21.3 | **Safe GC**: mark-and-sweep with no direct deletion (→ INV-14). |
| §22 | Recovery, standby and reconstruction. |
| §22.1 | Determining the durable point, authority S3: the longest contiguous prefix (→ INV-08). |
| §22.3 | Host loss, with a warm standby. |
| §22.4 | Lazy loading — designed now, implemented later. |
| §22.5 | `rebuild-metadata`: rebuilding PostgreSQL from S3 (→ INV-20). |
| §23 | Edge cases. |
| §24 | The S3 client as a subsystem (hedged GET, retry budget, circuit breaker). |
| §25.1 | **Deterministic Simulation Testing** and the simulable-interfaces rule (→ INV-01, INV-02). |
| §25.2 | WAL property tests (→ INV-05). |
| §25.3 | Fault injection on real hardware — a complement, not a substitute. |
| §25.4 | The backend conformance suite. |
| §26 | Observability. |
| §26.1 | Distributed tracing. |
| §26.2 | **The metrics catalog** — the names `internal/obs` registers. |
| §27 | **Format versioning and a mixed fleet** (→ INV-19, still pending). |
| §28.1 | Cordon / drain (→ ADR-0008, ADR-0016). **The drain is withdrawn with ADR-0026**; cordon survives in `placement.Admits`, which never places on a cordoned host. |
| §28.2 | Capacity and placement (→ ADR-0017). |
| §29.4 | Residual weakness: cold cross-host materialization RTO (→ RISK-04). |
| §30.3 | Roadmap item 3 — vhost-user, whence Phase 03. |

## `INV-` — the invariants

Full statement, checker, and activation increment: **`INVARIANTS.md`**.

The dated aggregate that used to sit here ("State as of 2026-07-26: 21 active, INV-19
pending") is gone rather than refreshed. A count is the part that rots first and the part
nothing checks; the per-row state below is resolved against `INVARIANTS.md`, which is the
file that owns it.

| INV | One line | State |
|---|---|---|
| INV-01 | Simulable interfaces only — no `time.Now()`/sockets/syscalls outside `simio`. Three exemptions, all narrow and all with a fixture proving they did not widen: `internal/vhost/hostio` (host code, ADR-0020), `integration/guestinit` (not host code — PID 1 inside the guest, DEV-0013), and build-tagged test harnesses (`integration/**/*_test.go`, `internal/testinfra` — they drive real processes and containers, so there is no clock to inject; DEV-0016). | active |
| INV-02 | Deterministic replay: same seed ⇒ identical trace. | active |
| INV-03 | Ordered watermarks: `published ≤ durable ≤ local`. | active |
| INV-04 | Unflushed bounds: backpressure rather than a silent NVMe fill. | active |
| INV-05 | WAL serialize/replay is total and safe — corruption is detected, never applied. | active |
| INV-06 | No durable ACK without a valid lease at the instant of ACK (`remote` mode). | active |
| INV-07 | FLUSH/FUA ordering — ACK only after the six §14.4 steps. | active |
| INV-08 | S3 is the recovery authority; the durable point is the longest contiguous prefix. | active |
| INV-09 | No ACKed-durable write is lost across a failover. | active |
| INV-10 | Effective single writer: a stale-epoch writer publishes nothing. | active |
| INV-11 | Promotion waits `lease_ttl + max_clock_skew` before granting epoch N+1. | active |
| INV-12 | The recovery point is the epoch boundary; late PUTs fall outside it. | active |
| INV-13 | **Never truncate local WAL above a verified `published_sequence`.** | active |
| INV-14 | The GC cannot permanently delete — it marks; the bucket lifecycle removes. | active |
| INV-15 | Nothing leaves the host in cleartext. | active |
| INV-16 | Published snapshots are immutable (compaction may replace objects with logically-equal ones). | active |
| INV-17 | Background I/O always yields to foreground/flush. | active |
| INV-18 | S3 is not in the WRITE path — only FLUSH/FUA touch it. | active |
| INV-19 | Fleet-mixed format gating: no writer at format v+1 until every host can read it. | **pending** |
| INV-20 | `rebuild-metadata` reconstructs PostgreSQL from S3. | active |
| INV-21 | PUT idempotency: one sequence span, one object. | active |
| INV-22 | **Every UUID is v7** — `internal/ids` only, enforced by lint and a DB domain. | active |

## `ADR-` — the decisions

Each ADR is a dated, standalone record; that is why they are separate files and stay
that way. Full text: `DECISIONS/ADR-NNNN-*.md`.

| ADR | Decision | Status |
|---|---|---|
| ADR-0003 | The simulable-interfaces rule is enforced by a custom analyzer, not by intent. | Accepted |
| ADR-0005 | **WAL headers are 104 bytes**, not the doc's 96 — every field is load-bearing. | Accepted |
| ADR-0007 | PostgreSQL 18 + UUIDv7. *(Its tooling half — Atlas — is superseded by ADR-0019.)* | Partly superseded |
| ADR-0008 | A drain moves a volume from its **durable prefix in S3**, not from a snapshot. | **Withdrawn** — ADR-0026 (no drain, no durable prefix) |
| ADR-0009 | Lifecycles are typed (`internal/lifecycle`): compile time, store boundary, DB CHECK. | Accepted |
| ADR-0010 | One S3 client wrapper; RustFS is the certified dev backend. | Accepted |
| ADR-0011 | The Control-Plane term is anchored **outside PostgreSQL**, in a create-only S3 claim. | Accepted |
| ADR-0012 | GC anchors: every WAL object of a closed epoch is a root; Phase 12 is born with a by-key index. | **Withdrawn** — ADR-0026 (`internal/gc` deleted; INV-14 pending) |
| ADR-0013 | Local device pressure: a device budget, a reserve, and who may react. | **Proposed** |
| ADR-0014 | Volume quota: soft, per-lineage, content-addressed snapshots, and squash. | **Withdrawn** — ADR-0026 (no squash, no lineage ledger; the soft/hard distinction survives) |
| ADR-0015 | The fencing wait is a **monotonic dwell**, not a comparison of wall clocks. | Amended — nothing promotes; the durable half (`fencing_started_at`) survives |
| ADR-0016 | Fencing granularity: a revocation window bounded to one promotion. | Amended — the lease is liveness only; the per-host granularity stands |
| ADR-0017 | **Capacity is derived from state, not an incremental ledger.** | Accepted |
| ADR-0018 | The spine: Agent first, Connect RPC in `api/`, the Agent pulls and the CP never pushes. | Accepted |
| ADR-0019 | Schema tooling is **pgschema**, not Atlas; `schema.sql` is the declared state. | Accepted |
| ADR-0020 | `internal/vhost/hostio` is the one documented INV-01 exception (SCM_RIGHTS + mmap). | Accepted |
| ADR-0021 | storage integrates into **spin**; spin imports storage and never the reverse. The two binaries are test harnesses that must stay runnable end to end. spin migrates to pgschema. | Accepted |
| ADR-0026 | V1 accepts an RPO of one session: §14.8 local becomes the only ACK contract, a volume is uploaded once at stop, and a snapshot is an fsync plus a copy at a §19 sequence number. Withdraws the remote durability chain. The product question behind it — has anyone asked for a VM to survive host loss mid-session? — was answered no. | Accepted |
| ADR-0025 | The QEMU guest lane runs **inside the published runtime image** (`ghcr.io/<repo>/qemu:<version>`) as a CI container job, rather than installing QEMU's dynamic dependencies on a bare runner, so the dependency set has one definition in `Dockerfile.qemu`. The job is gated on the image existing and skips with a notice rather than failing the gate. | Accepted |
| ADR-0024 | A restarted Agent re-attaches at the **same epoch** — no bump, no CP round trip, no FENCING_WAIT. **Its justification was rewritten with ADR-0026**: not a prefix in S3 and a record-by-record agreement check, but the simpler fact that nothing leaves the host mid-session, so a second incarnation has nothing to overwrite until it stops — where the manifest CAS catches it. Two *live* Agents on one data dir are the flock's job (DEV-0014), not an epoch policy. | Accepted (amended) |
| ADR-0023 | The object store is a fencing witness the data path may act on. **The witness moved with ADR-0026**: not a checkpoint mid-session but the manifest's compare-and-set at stop, which tells a host its predecessor published while it was running (`image.ErrSuperseded`) instead of letting it overwrite. | Accepted (amended) |
| ADR-0022 | The guest kernel is pinned by sha256 and obtained by `task fetch:kernel` into `_output/guest/vmlinux` — never resolved from a sibling checkout's path. storage may *mirror* spinbox's artefact into a registry; mirroring is not building (ADR-0021 stands). | Accepted |

## `DEV-` — doc↔code divergences

Open ones are in **`STATUS.md`**, because an open divergence is part of the current
state. Resolved ones are listed here so a reference in the code still resolves; the
commit named is where the fix landed.

| DEV | What it was | Where |
|---|---|---|
| DEV-0001 | The WAL header's fields sum to 104 bytes, not the documented 96. | resolved → ADR-0005 |
| DEV-0002 | A drain moves from the durable prefix, not from a snapshot. | resolved → ADR-0008 |
| DEV-0003 | Recovery accepted an unvalidated WAL object as durable. | resolved `6d5655e` |
| DEV-0004 | Fencing was fail-open, and promotion was neither atomic nor resumable. | resolved `6d5655e`, `f9f5885` |
| DEV-0005 | Not every Control-Plane mutation was term-guarded. | resolved `93b70aa`, `15cb1e2` |
| DEV-0006 | The object store exposed permanent deletion; the GC did not mark. | resolved `cd17e0b` |
| DEV-0007 | Several phases marked done are partial models — the spine. | **ADR-0018's chain closed 2026-08-02** (`TestAGuestSurvivesCheckpointAndTruncation`: a real guest writes, fsyncs, the Agent checkpoints and truncates, and a second boot reads it back from S3). The rest — background snapshot sealing, the clone chain link, segment objects, cross-host materialization — is still open → STATUS.md |
| DEV-0008 | The drain was not idempotent across every crash boundary. | resolved `f9f5885` |
| DEV-0009 | `rebuild-metadata` rebuilt volumes only. | resolved `2b09d1e` |
| DEV-0010 | Observability was registered but never recorded. | resolved `fc02579` |
| DEV-0011 | A segment's space is charged as used, not reserved at creation. | **open** → STATUS.md |
| DEV-0012 | A self-fenced log still accepts WRITEs and still serves reads. | resolved 2026-08-02 — not a divergence: §12.2 delegates it to policy, and the policy is now written at `wal.Log`'s `fenced` field |
| DEV-0020 | A clone chain deeper than one link cannot be materialized: `materialize.FromSnapshot` resolves only the objects the parent's manifest lists, all under the parent's own id, so a clone of a clone never fetches its grandparent's extents. Unrelated to encryption — the same hole exists for a plaintext volume. | open — §19/§20 must say whether a chain is walked or flattened |
| DEV-0019 | A restarted encrypted volume served its guest **ciphertext**: `agent.fetchBase` passed a literal `nil` `*wal.Encryption` to `recovery.RecoverOver` for a volume whose DEK it had just unwrapped, and `ApplyRecord` folded the sealed payload into the read view at exactly the plaintext's length, with no error anywhere. | resolved 2026-08-02 — `recovery.ErrSealedWithoutKey` makes it unrepresentable; `Volume` carries its `enc`; `parentView` re-binds the DEK to the parent's id; mandatory DST arm `encrypted-volume-survives-a-restart` |
| DEV-0018 | Three documents claimed a Linux-guest lane that no test performed — every `integration/vhost` test boots a 512-byte SeaBIOS boot sector, and INT 13h has no flush verb. Writing the lane found a real hang: a guest re-initialises the device (firmware → OS hand-off) with a **new kick eventfd**, and the queue loop stayed parked on the first one. | resolved 2026-08-02 — `queueLoop.ensure` restarts on a new kick; `TestAReinitialisedDeviceIsStillServed` + `TestALinuxGuestIssuesFLUSH` |
| DEV-0017 | `cmd/volume-agent` rooted its Disk at `--data-dir` *and* passed the same absolute path as `DataDir`, so every WAL landed under `<data-dir>/<data-dir>/wal/...`. | resolved 2026-08-02 — the binary passes `DataDir: "."`; the e2e lane asserts the doubled directory does not exist |
| DEV-0016 | `task lint` ran golangci-lint with no build tags, so the whole `integration/`+`internal/testinfra` surface was never parsed by it. | resolved 2026-08-02 — `--build-tags integration,e2e`, plus a harness exemption whose narrowness is checked by planting a `time.Now()` in a non-test file |
| DEV-0015 | The volume descriptor had no integrity check: truncation was caught, a flipped bit inside a number was not, and §22.5 would rebuild the volume at the wrong size. | resolved 2026-08-02 — the object is `<sha256>\n<json>`, digested over the bytes as stored (a digest *field* cannot work: Go matches JSON keys case-insensitively). `DESCRIPTOR-DIGEST-SPEC.md` |
| DEV-0014 | Two Agents could share one `--data-dir`: both resumed the same segment directory at the same epoch, and `hostio.Listen` unlinks the stale socket so the second *silently stole* it instead of failing with `EADDRINUSE`. | resolved 2026-08-02 — `disk.Lock` (flock, non-blocking) taken by `NewVolumeManager`; `DATA-DIR-LOCK-SPEC.md` |
| DEV-0013 | `task lint` red since `e8bbdab`: the guest-side `integration/guestinit` tripped the INV-01 lint layer. | resolved 2026-07-28 — third INV-01 exemption, narrowness fixture |

## `RISK-`

`RISKS.md`. Ten risks, each with a trigger that forces a re-review. RISK-10 (QEMU
inflight-shmfd) is the one with an execution log attached.
