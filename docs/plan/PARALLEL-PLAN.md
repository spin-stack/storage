# PARALLEL-PLAN — what is left, and how to run it concurrently

**Written 2026-08-03** from a seven-lens audit of the tree (not of the documents), a
synthesis, and two adversarial critics — one checking every claim against the code, one
looking only for merge collisions. Both critiques changed the plan; where they did, the
correction is written next to the item rather than silently applied.

## How much is left

**31 increments: 15 S, 14 M, 2 L.** Roughly 8 developer-days of S work, 14 of M, and two
L items that are a phase each. **Nine of the 31 are pure deletion.**

The system is closer than it feels. The thin path already runs end to end with real
binaries and a real kernel: a guest boots, writes, `fsync`s, stops, and its bytes come
back from S3; a snapshot of a live volume lands; a clone of that snapshot boots on the
host that took it. What is left is five real defects in the data path, two lifecycle
verbs V1 genuinely lacks (detach and delete), about fifteen surfaces with no caller, and
a gate that has never executed a single guest-backed proof.

## The two corrections the critics made

**1. Increment 0 is not blocking, so the documents do not go first.** The plan's own
sequencing rested on `BUILD-INVENTORY.md`'s "until §2's SLO table and §14.8 are revised,
the rest cannot pass its own gate". Both *are* revised —
`arquitectura_mvp_volumenes_remotos_v5.md:92` (the RPO row) and `:788` (§14.8). The
residual contradictions elsewhere in that document are real and worth fixing, but they
are not a gate. **The day-one item is C6/C7, not A1.**

**2. C11 and DEV-0020 are one mechanism, and separating them was a mistake.**
`image.uploadChunks` iterates `view.Ranges()`, and `cow.Ranges()` returns the base merged
with the layer — so a clone's first stop re-uploads its parent's whole dataset under its
own prefix. That is the storage cost C11 wants gone, **and it is the only reason a
depth-2 clone reads anything but zeros today**. Removing the flattening without building
the chain read converts a cost defect into a silent-zeros correctness defect. C11 is
therefore blocked on a chain-depth decision, and `controlplane.Clone` still does
`ChainDepth: parent.ChainDepth + 1` with no refusal.

## The one that loses data

**`Volume.publish()` is V1's entire durability contract and it is unbounded,
uninterruptible and silent.** It runs on `context.Background()` with no deadline
(`internal/agent/volume.go:187`), returns nothing, and sends every failure to `slog`; the
process then exits 0 whether or not the session reached the bucket. `cmd/volume-agent`
has no `-shutdown-grace` — `cmd/control-plane` does. Any stop timeout shorter than the
upload loses the whole session with no non-zero exit anywhere.

This is a **durability review zone**: it needs a spec reviewed by a human before
implementation, not after.

## The five tracks

Ownership is by **file**, not by package — every real collision in this repository is a
single file or a hand-maintained registry.

| Track | Goal | Owns |
|---|---|---|
| **A — the documents** | Nothing in `docs/` or the design document describes a mechanism ADR-0026 deleted | the architecture document, `STATUS.md` head, `REFERENCE.md`, `RISKS.md`, `INVARIANTS.md` |
| **B — the gate runs** | `ci:full` and CI execute every proof this repo claims, including the guest-backed ones | `.github/workflows/`, `Taskfile.yml`, `hack/`, `internal/testinfra`, `integration/guestinit`, `integration/vhost/guest_test.go` |
| **C — the data path** | The one upload that carries V1's whole RPO is bounded, observable and correct | `internal/agent`, `internal/wal`, `cmd/volume-agent`, `integration/e2e`, `integration/vhost/{lifecycle,wal}_test.go` |
| **D — the catalog** | A volume can be detached, re-placed and deleted; a host can be cordoned | `internal/controlplane`, `internal/cpserver`, `internal/metadata/**`, `internal/db`, `internal/schema`, `internal/lifecycle`, `api/` |
| **E — observability** | A metric leaves the process, or the machinery that pretends to emit one is deleted | `internal/obs`, `internal/vhost`, `internal/blockdev`, `internal/cow` |

**Files no track owned, added after wave 1 found them the hard way.**
`internal/dst/scenarios_agent.go` (edited by D and C), `cmd/control-plane/main.go` (D
only, but unassigned), `integration/vhost/lifecycle_test.go` (edited by *three* lanes in
one wave), and `internal/descriptor`. Assignment: `scenarios_agent.go` and
`integration/vhost/lifecycle_test.go` to **C**, `cmd/control-plane/main.go` and
`internal/descriptor` to **D**. A file with no owner is a file every lane feels entitled
to.

**Wave 2 found three more, and got lucky in all three rather than protected:**
`internal/simio/real` and `go.mod`/`go.sum` → **E** (the exporter's socket code belongs
there, so INV-01 needs no new exemption; route any other dependency through E).
`internal/placement` → **D**. And `internal/dst/mandatory_set_test.go`, which is a
hand-maintained registry that **must** be edited in the same commit as any new mandatory
scenario — `TestMandatorySetIsPinnedByName` fails otherwise, which is its whole purpose.
It goes to whichever lane adds a scenario, and **at most one lane may add one per merge
window**, the same rule the DST checkers already have.

**Wave 1's real lesson: ownership by file is necessary and not sufficient.** Track E
committed two files it did not own (`internal/agent/publish_test.go`,
`snapshot_test.go`) from a *stale pre-edit copy*, wholesale reverting the fix track C had
just landed — an assertion that could not fail, restored to being unable to fail. C
noticed and restored it; nothing but that noticing stood between the wave and shipping the
defect it had just removed. The rule that would have prevented it is mechanical, not
social: **stage by explicit pathspec, and never `git commit -a` or `git add` a directory**
while another agent is working. Track D did exactly that on purpose and said so.

**Track D has no exploitable internal parallelism.** Seven of its eight items edit
`metadata.go` + `sim.go` + `pg.go` + `metadatatest/contract.go` together, and five of them
edit `schema.sql` and regenerate `internal/db`. Plan it as one sequential lane.

## Wave 0 — the enablers, before any track branches

Without these, the merge cost exceeds the implementation cost for C and D.

1. **Split `integration/e2e/e2e_test.go`** (820 lines, one file, eight of C's twelve
   observables land in it) into a fixture file plus one file per scenario. Zero behaviour
   change.
2. **Pin the mandatory DST scenario set by NAME, and make `task dst` fail when its `-run`
   selects nothing.** `go test -run TestNoSuchNameAtAll ./internal/dst/` exits 0 today. Pin
   names, never a count — a count is one integer every branch bumps.
3. **Pre-seed `STATUS.md`** with one empty subsection per track at the end of the open-work
   log. Every branch appends only inside its own; the head tables are recounted once, at
   integration.
4. **`ci.yml` calls `ci:full`.** Every other CI item adds a step to a gate CI does not run.
5. **The simulable analyzer sees the build-tagged surface.** It will flag `integration/*`
   and `internal/testinfra` — fix them once, before C and E branch, not at merge.

## The merge protocol

- **Commit by pathspec, not by index: `git commit -m "…" -- path1 path2`.** Wave 3 proved
  that "stage by explicit pathspec" is not enough — `git add <path>` stages into an index
  **every agent shares**, so a commit sweeps whatever another lane staged seconds earlier.
  One commit in wave 3 carries another lane's entry, and another lane's own entry landed
  inside somebody else's commit. Nothing was lost, and attribution was scrambled. The
  pathspec form of `git commit` bypasses the index entirely and is the mechanical fix.
- **Each track's running log is its own file** (`docs/plan/tracks/TRACK-*.md`, carved out
  of `STATUS.md` on 2026-08-04). Five lanes appending to one region collided in every
  wave; in wave 3 a lane committed a stale copy and deleted 121 lines of another's, ten
  seconds after they landed, and the only thing that restored them was the other lane
  happening to look again. Ownership by *file* is a control; ownership by *section* is a
  convention.
- **A number written into a document is stale by the next commit.** Wave 3's document lane
  recounted the mandatory set at 18 and the ADR-0013 citations at 54; twenty minutes later
  another lane made them 19 and 68, and several cited line numbers moved by 47. Cite the
  *symbol* and the mechanism that computes the count — `pinnedMandatorySet`, `task
  deadcode` — not the number and the line.
- **Generated output is regenerated at integration, never merged.** `internal/db/models.go`
  is one file for every table and `api/gen/` is committed; `task generate:check` is in
  `ci:full`, so a text merge that succeeds still has to be regenerated.
- **One open schema branch at a time.** `migrations/` plans carry a fingerprint of the
  database they were planned against; two branches planning from one baseline both claim
  it, and the second is stale at `db:apply` — while `db:verify` still passes, because it
  rebuilds from `schema.sql` and never reads `migrations/`. The failure is invisible to the
  gate.
- **At most one track adds a DST *checker* per merge window.** A new checker touches three
  registries — `scenarios.go`, `checkers.go`, and `planted_bug_test.go`'s `plantedProofs`
  map *and* its `wantBehavioural` constant. Scenarios without a new checker are safe.
- **During the wave a branch runs `task ci` plus the lanes it touches.** `ci:full` is
  enforced on the integration branch only, until wave-0 item 4 lands.
- **The 90% coverage floor is only meaningful on the integration branch.** Nine items are
  pure deletion of production code that carries tests; the ratio moves under every branch.
- **Four hand-maintained counters get one owner and one recount after the wave:**
  `wantBehavioural`, the DST scenario-set pin, STATUS.md's "Components with no production
  caller", and "Decisions waiting on a human".

## Review zones needing a human before implementation

Three, and none may land its spec in the same commit as its code — five already did.

- **The shutdown-publish contract** (durability): what a bounded publish promises, what a
  failure does to the exit code, and what an operator sees.
- **Chunk addressing per volume vs per bucket, with the chain-depth decision** (on-S3
  format): C11 and DEV-0020 are the same mechanism.
- **Volume and snapshot deletion, and reclaim** (GC): the first thing in this repository
  that deletes an object.
