# CHUNK-ADDRESSING-SPEC — where a chunk lives, and the chain-depth decision it cannot be separated from

## DECIDED — 2026-08-07, human owner: `chain_depth` is a **structure**

**Options 2 and 3 together**, which is the opposite of §4's recommendation. §4 argued
*label* on the grounds that flattening is what the code already does and the storage cost is
bounded per link. The owner chose structure. What follows is what that obliges, so nobody has
to reconstruct it from a one-word answer.

**The cost argument §4 rested on had already collapsed**, which makes the decision cheaper to
defend than §4 does. The measurement lane established, with tests, that the duplicate is paid
**once per clone and not once per snapshot** (§3's premise was wrong), and that under option 2
the saving is bounded by *chunk granularity* rather than by what the clone wrote — a clone
that writes one sector saves nothing. So structure is not being bought for bytes. It is being
bought because a lineage that reads through its ancestors is a thing the system can then
*talk* about: a ceiling that refuses, a FLATTEN that means something, a `chain_depth` that
describes the read path rather than a lineage nobody walks.

### What is now decided

1. **Chunks move to `chunks/<lineage-root>/<digest>`**, and the AAD binds the lineage root
   instead of the volume. Not bucket-wide: the DEK is per volume and the whole chain already
   shares the root's (`clone.go`), so the lineage is exactly the scope where sharing is
   already safe. Bucket-wide would need a bucket-wide key and would make the design
   document's *volume delete = crypto-shred* meaningless.
2. **`parentView` walks the chain**, reading each ancestor's `descriptor.json` for the next
   link rather than receiving a list in `DesiredVolume`. §3's own reasoning stands: it keeps
   ADR-0021 intact and the bucket is already the authority a rebuild trusts.
3. **Publishing stops flattening.** That is what makes 1 and 2 worth anything, and it is what
   makes a clone's cost proportional to what it wrote.
4. **The ceiling becomes a refusal.** `controlplane.Clone` refuses past the depth limit
   instead of incrementing a number nobody reads, and `chain_depth` acquires the producer it
   has never had.
5. **FLATTEN becomes real**, because it is now the only way back under the ceiling — and,
   per the deletion decision, the only way to delete a parent that has clones.

### Three consequences that are not optional

**The nonce argument must be rewritten, not adjusted.** `internal/image`'s package doc rests
the encryption safety case on two facts *together*: the nonce is drawn rather than derived,
and a chunk is sealed exactly once ever because an existing key is skipped. The skip rule is
unchanged but now spans volumes, so "who sealed this, under which nonce" stops being answered
by the key space. That is three paragraphs of a package doc and it is the reason the format is
safe. It is a review-zone edit in its own right.

**The read path degrades linearly with depth**, which is what the design document predicts and
why §20.1 asks for FLATTEN at all. The ceiling is what bounds it; the refusal is therefore not
a nicety.

**FLATTEN is an operator-run one-shot, not a background operation.** The `operations` table
that would have carried it was retired in wave 4 with ADR-0017's second capacity term, and
bringing it back for one verb is a schema change, a term guard and a reconciliation loop for
something every other admin action in this repository does as a one-shot flag
(`-seed-volume`, `-snapshot-volume`, `-clone-snapshot`, `-detach-volume`,
`-rebuild-metadata`). ADR-0021 says these binaries are harnesses; a one-shot is the shape that
fits. *(Rejected: reviving `lifecycle.OpFlatten`. If spin later needs it scheduled, it
schedules the one-shot.)*

### The order, which is not the order §4 assumed

§4 sequenced this as "a later increment with its own spec". It is now the increment, and it
has a forced order because each step breaks reading until the next lands:

1. the chain walk (option 3) — harmless while publishing still flattens, and it is what makes
   a depth-2 clone read from its ancestors rather than from a flattened copy;
2. the key space and the AAD (option 2) — the format change, with the §25.2 property test and
   the rewritten nonce argument;
3. stop flattening — the moment the cost actually falls, and the moment a clone stops being
   independent;
4. the ceiling as a refusal, and FLATTEN.

**Between 3 and 4 a parent with clones cannot be deleted at all.** That window is the reason
this order is written down rather than left to whoever picks it up.


**★ HUMAN-REVIEW ZONE: on-S3 format.** Anything here that moves the key space or the AAD is a
format change, which means a human review of this spec before implementation *and* of the diff
before merge, plus the §25.2 serialize/replay property test with arbitrary truncations and bit
corruptions. Five review-zone increments in this repository landed their spec in the same
commit as their code; `PARALLEL-PLAN.md` says do not make it eight.

**The one thing it makes true:** a clone's storage cost is a number this repository chose, and
the depth at which the read path stops working is one it *refuses at* rather than degrades
past.

**A correction to make before anything else, because an earlier draft of this spec quoted a
promise the design document does not contain — and getting this right changes what the
reviewer is being asked.** §20 never says "no data copy". What it says is about the
*download* at clone time: *"Same-host: sin descarga; reutiliza EROFS, checkpoint y WAL
cacheado"* (`arquitectura_mvp_volumenes_remotos_v5.md:998`), *"Clone en el mismo host sin
descargar nuevamente el snapshot"* (`:114`), *"Clone same-host sin transferencia
significativa"* (`:1342`).

**Verified: the code does not honour those either, and one of its own comments says it does.**
`parentView` calls `image.LoadSnapshot` unconditionally (`internal/agent/volume.go:950`),
which GETs every chunk the parent's snapshot manifest names (`loadManifest`, `image.go:308`)
— on *every* attach, including one on the very host that took the snapshot. There is no local
reuse and no code path that could provide it: the only cache in `internal/agent` holds
`VolumeKeys` (`loop.go:60-65`, `:352`), not data, and nothing in `fetchBase` reads another
volume's local segments. Yet `controlplane.Clone`'s doc comment states the opposite as
settled fact — *"a cross-host clone pays a full download from the object store … while a
**same-host clone reads local NVMe**"* (`clone.go:23-25`) — and placement is built around it
(`policy.Choose` with `SourceHostID`, `clone.go:62-65`).

**Both halves of that sentence are one download in the code as it stands.** The locality the
comment describes was EROFS + checkpoint + cached WAL, which is precisely what ADR-0026
deleted; the placement preference outlived the thing it was a preference for. That is a
DEV-shaped divergence in a *code comment* — the place CLAUDE.md says a decision should live,
which is exactly why a wrong one there is expensive — and it is **not this spec's subject**,
not fixed by any option below, and owed to whoever owns `internal/controlplane`. It is
recorded here because a reviewer pricing a clone will otherwise price it from that comment.

So there are three costs, and the document accounts for none of them: a download the
document says will not happen, a *second* download on the next attach, and — the subject of
this spec — an **upload** of the whole inherited dataset at the clone's first stop, which the
document does not mention in either direction. The defect this spec proposes to fix is
therefore not a broken promise but a cost nobody costed, which is the weaker claim and the
true one.

**On citations.** Every claim names the file and the symbol, and every line number was
re-checked against `26a9525` after four other lanes landed during the writing of this spec.
One claim in an earlier draft did not survive that re-check (§7, the FLATTEN vocabulary that
`c81d83d` deleted), and one quotation turned out not to exist in the document it was
attributed to (the header). Where the symbol is enough, trust the symbol. Every probe output
below was re-run against the tree, not copied forward.

---

## 1. The mechanism, established from the code and then reproduced

### 1.1 The chain, link by link

- **A chunk key names its volume.** `chunkKey(volumeID, digest)` is
  `Prefix(volumeID) + "chunks/" + digest`, and `Prefix` is `image/<uuid>/`
  (`internal/image/image.go:101,106-108`). The digest is of the **plaintext**
  (`uploadChunks`, `image.go:217-218`), which is what makes dedup survive encryption.
- **The seal binds the volume too.** `chunkAAD = "image-chunk" || volumeID || digest`
  (`image.go:369-371`), used by `seal` and `open` (`image.go:344-353,357-367`).
- **A clone reads through its parent.** `agent.parentView` loads
  `image.LoadSnapshot(store, penc, parentVolume, snapshotID)`
  (`internal/agent/volume.go:923-960`, the load at `:950`), with the DEK re-bound from this
  volume to the parent (`parentEncryption`, `:965`; `wal.NewEncryption` keeps the DEK and
  swaps the id, `internal/wal/crypto.go:41-46`). That works because a clone inherits its
  parent's DEK, `KEKID` and `DEKKeyID` (`internal/controlplane/clone.go:92-98`).
- **`fetchBase` layers it under the clone's own image** (`volume.go:886-895`), or makes it the
  base outright when the clone has never published (`:872-878`, the `ErrNotPublished` arm),
  then hands it to `log.InstallBase` (`:902`), which does `l.view.SetBase(base)`
  (`internal/wal/log.go`, `InstallBase`).
- **Publishing flattens the whole stack.** `Volume.publish` takes `v.log.ViewAtRest()`, the
  layered view (`volume.go:206`, view at `:222`, `image.Publish` at `:232`).
  `uploadChunks` iterates `view.Ranges()` (`image.go:211`), and `cow.IntervalMap.Ranges`
  returns *"the base's ranges minus this layer's tombstones, plus this layer's own extents"*
  — it recurses with `subtract(m.base.Ranges(), m.cleared)` (`internal/cow/ranges.go`,
  `Ranges`).

### 1.2 Reproduced, not inferred

Run against the real packages through a `go test -overlay` probe, so nothing entered the
working tree (`git status internal/image/` is clean after the run). A parent writes 3 MiB,
publishes, and snapshots; a clone layered over that snapshot writes **512 bytes** and
publishes:

```
parent: 1 chunk objects, 3145756 bytes under image/00000000-0000-7000-8000-000000000000/chunks/
clone:  1 chunk objects, 3145756 bytes under image/aa000000-0000-7000-8000-000000000000/chunks/
        (its own write was 512 bytes)
```

**A clone's first stop re-uploads its parent's entire dataset under its own prefix.** 3145756
is 3 MiB plus the 12-byte nonce and 16-byte tag of one sealed chunk (`<nonce:12><ct><tag:16>`),
and the clone paid all of it for a 512-byte write, because its 512 bytes overwrite part of a
range the base already covers and `Ranges()` merges the two into one 3 MiB run. That is
`PARALLEL-PLAN.md`'s C11 and the live half of DEV-0020, and it is a cost the design document
never accounts for in either direction (see the header).

*(An earlier draft of this spec quoted `2 chunk objects, 3146240 bytes` from a probe whose
clone wrote at a disjoint offset and used no encryption. Both runs show the same thing; this
one is the one that was re-run against the tree as it stands, and it shows the cost more
sharply — the duplicate is not proportional to what the clone wrote, and can be the whole
inherited dataset for a single sector. The earlier draft also called this a violation of
§20's "no data copy"; the header explains why that quotation was wrong and what §20 actually
promises.)*

And the chunks genuinely cannot be shared as they stand — copying volume A's chunk objects
*and its manifest* verbatim under B's prefix and loading B fails on the AAD, not on the key:

```
CONTROL loading volume A (same bytes, own identity):
  err = <nil>
loading volume B over a chunk sealed for volume A:
  err = image: chunk 6896d9ea3f73a443…c8a6f735daa41b1 at offset 0: crypto: authentication failed
```

The control line matters: the probe hand-builds B's manifest, so a failure on its own would
also be consistent with a manifest this probe got wrong. The same objects opening under A's
identity leaves the volume id as the only difference — which is what makes this the embryo of
§5's "the binding is still a binding" test rather than an anecdote.

### 1.3 Why removing the cost, on its own, breaks reading

`parentView` resolves **exactly one link**: `image.LoadSnapshot` reads one manifest
(`image.go:164-167` → `loadManifest`, `:300-330`) and follows nothing from there. A depth-2
clone reads its grandparent's bytes only because the depth-1 snapshot manifest it loads was
*flattened when it was published*. Take the flattening away without building a chain walk and a
cost defect becomes a silent-zeros correctness defect — which is exactly what DEV-0020 was
opened for the first time, before `internal/materialize` was deleted.

Meanwhile the ceiling that would have bounded this does not exist: `controlplane.Clone` does
`ChainDepth: parent.ChainDepth + 1` with no refusal (`clone.go:91`), the design document's
ceiling is 5 (`arquitectura_mvp_volumenes_remotos_v5.md:180`, and `max_chain_depth: 5` at
`:586`), the metric `chain_depth` is declared in the §26.2 catalog with **no producer**
(`grep -rn chain_depth --include=*.go internal/ cmd/` outside `internal/db` and `metrics.go`
reaches only the descriptor field and one log line in `cmd/control-plane/main.go:290`), and
**nothing in the tree exercises `chain_depth > 1`**.

*(Housekeeping, owed to track A rather than done here: the task that commissioned this spec
says `STATUS.md`'s DEV-0020 cites a symbol that no longer exists. It did — `materialize.FromSnapshot`
— and track A rewrote the entry on 2026-08-04, so it now describes the mechanism above. What
is still stale inside it is three line numbers: it cites `internal/agent/volume.go:836` for
`parentView`'s `LoadSnapshot`, which is `:950`, and `internal/image/image.go:180,142` for the
two `uploadChunks` calls, which are `:181` and `:143`. Those citations are in track A's file
and this lane does not touch it.)*

## 2. A defect this uncovered, which no option may leave in place

**A clone that has published its own image cannot be restarted. Its reads fail entirely.**

`image.Load` returns an **unlayered** view: `loadManifest` builds `cow.NewIntervalMap()`
(`image.go:306`), and `NewIntervalMap` returns a map with `layered` false
(`internal/cow/intervalmap.go`, `NewIntervalMap`). `fetchBase` then calls
`base.SetBase(parent)` (`volume.go:886-895`), and `SetBase` refuses on an unlayered map —
*"an unlayered one has been discarding its tombstones, so giving it a base now would uncover
every range it was told to discard"* (`cow.IntervalMap.SetBase`).

Reproduced:

```
SetBase on the map image.Load returned:
  err = cow: this map was not built to take a base (use NewIntervalMapOver)
```

In `fetchBase` that error path is `v.baseFailed = true; v.log.FailBase(err)` and the log line
*"the clone's parent could not be layered under its image; its reads will fail"*
(`volume.go:889-893`). Every read then returns `ErrBaseUnavailable`. **A clone works exactly
once.**

Nothing covers it, which is why it survived: the DST clone scenarios
(`a-clone-reads-through-its-parent` and `a-snapshot-of-a-live-volume-is-frozen`,
`internal/dst/scenarios_agent.go`) start a clone and never restart it, and
`integration/e2e/clone_test.go` contains one test, `TestACloneIsPlacedWhereItsDataIs`, which
never stops the clone. This is the "seams, not parts" failure the top of CLAUDE.md is a table
of.

**It matters to the decision** because the branch is dead in the only sense that counts — it
has run and has never succeeded — and each option below either deletes it or fixes it:

- under **flattening**, the parent layer is redundant once the volume has its own image (the
  image already contains everything the parent held), so the fix is to *not layer the parent*
  in that branch — and that is correct **only** because publishing flattens, which is a
  sentence that has to be written next to the code;
- under a **chain walk**, the fix is the opposite: `image.Load` must return a layerable map,
  and the layering becomes load-bearing rather than redundant.

## 3. The options, with what each costs and what it breaks

### Option 1 — keep flattening; stop pretending `chain_depth` is a structure

**What it is.** The status quo, plus the §2 fix (do not layer a parent under a published
image), plus honesty about `chain_depth`: with eager flattening, every published manifest is
self-contained, so the number describes a *lineage*, not a read path. There is nothing to walk
and nothing to refuse. §20.1's FLATTEN is, in effect, already implemented — eagerly, at every
publish, on the host that is stopping.

**Cost.** A full duplicate of the inherited data per link, paid at the clone's first stop and
at every snapshot of the clone. For the golden-image workflow the design document's §2 is
built around — N clones of one image — the bucket holds N+1 copies.

**Breaks.** Nothing that works today. It is the cheapest correct option and the baseline the
others must beat.

**What it obliges.** §20 must say what a clone costs — it currently says only what a clone
does not *download* (`:998`, `:114`, `:1342`), and a reader takes the silence for cheapness.
And `chain_depth` must either be deleted or documented as observability-only: leaving a
ceiling of 5 (`:180`, `:586`) in a document that bounds nothing is how DEV-0020 stayed open
for two waves.

### Option 2 — move chunks out of the volume's prefix

**The naive form — one bucket-wide `chunks/<sha256>` namespace — does not work, and the reason
is not the key.** Two things block it, both verified:

1. **The AAD binds the volume id** (`chunkAAD`), so a chunk written by one volume does not open
   for another; §1.2's probe is that fact. The sealing rule would have to change, which is the
   format review this document is a prerequisite for.
2. **The DEK is per volume.** Chunks are sealed with the volume's DEK (`image.seal` via
   `wal.Encryption`), so two volumes with different DEKs produce different ciphertexts for the
   same plaintext under the same content-addressed key. Whoever writes first wins the key and
   nobody else can open it. Fleet-wide dedup therefore needs a bucket-wide key — which gives
   up per-volume crypto isolation and makes the design document's *"borrado de volumen =
   crypto-shred"* (`arquitectura…v5.md:1348`) meaningless.

**The form that does work is per *lineage*, not per bucket:** `chunks/<lineage-root>/<digest>`,
where the lineage root is the volume whose DEK the whole chain already shares
(`clone.go:92-98`). Bind the lineage root in the AAD instead of the volume. That is the sharing
that actually matters — a clone and its parent — at a fraction of the blast radius.

**What it buys.** A clone's publish writes only genuinely new digests — the storage cost of a
clone becomes proportional to what the clone wrote, which is the property the golden-image
workflow needs and the one no document currently claims.

**What it costs.**
- **A format change**: the chunk key space and the AAD. §25.2 property test, human review of
  the diff, and — because nothing is deployed (CLAUDE.md, "Formats before the first
  deployment") — changed in place, with no v2 alongside v1.
- **Deletion becomes cross-prefix.** DELETION-AND-RECLAIM-SPEC's reclaim rule is "read this
  volume's own manifests and take the set difference", which is bounded precisely because
  chunks live under one volume. Under a lineage prefix the naming manifests live under several
  volumes' prefixes, and reclaim needs the lineage's whole manifest set — or reference
  counting, which that spec rejects.
- **The nonce argument must be re-derived, not assumed.** `internal/image`'s package doc rests
  the whole encryption safety case on two facts together — *"The nonce is drawn rather than
  derived, and a chunk is **encrypted exactly once, ever** — a chunk whose key already exists
  is not re-sealed, it is skipped. Together those two facts are what make nonce reuse
  impossible"* (`image.go:31-33`), with both alternative derivations considered and rejected
  right there (`:37-41`). The skip rule is unchanged, but it now
  spans volumes, so "who sealed it, under which nonce" becomes a question the key space no
  longer answers. That argument is three paragraphs of a package doc and it is the reason the
  format is safe; it must be rewritten deliberately.
- **It is useless without option 3.** Sharing a chunk store does not make a manifest
  self-contained. If publishing still flattens, per-lineage keys save the *bytes* and keep the
  manifest bloat; if publishing stops flattening, the chain must be walked.

### Option 3 — walk the chain in `parentView`

**What it is.** Follow `parent_snapshot_id` upward, loading each ancestor's snapshot manifest
and layering them, until a volume with no parent. `cow` already supports it: `NewIntervalMapOver`
nests arbitrarily, `Ranges` recurses through `m.base.Ranges()`, `Read` recurses through
`m.base.Read`. The DEK holds across the whole chain transitively, because each clone inherits
its parent's (`clone.go:92-98`), so `parentEncryption`'s re-binding generalises to N links.

**What it buys.** The only thing that makes a chain readable when publishing does not flatten.
Necessary for option 2; pointless with option 1.

**What it costs.** N manifest loads and N chunk downloads at attach — a read path that degrades
linearly with depth, which is exactly what the design document predicts and why it asks for a
flatten operation at all (§20.1: *"el replay y el read path degradan linealmente con la
profundidad"*). And it needs the ceiling to become a **refusal** in `controlplane.Clone` rather
than an increment, plus the `chain_depth` metric to acquire the producer it has never had.

**Where the ids come from is already decided and should stay decided.** `parentView` takes the
link from the desired state, not from a lookup, because ADR-0021 keeps the Agent from knowing
what a Control Plane is — *"The ids come from the desired state rather than a lookup: ADR-0021
keeps this type from knowing what a Control Plane is, so the Control Plane is what tells it"*
(`volume.go:921-922`). A chain walk must not quietly reverse that: it
either receives the whole chain in `DesiredVolume`, or it reads each ancestor's
`descriptor.json` from the bucket (which carries `ParentSnapshotID`, `internal/descriptor/descriptor.go:39`).
The second keeps ADR-0021 intact and costs one more GET per link; the first makes the
`DesiredVolume` message carry a list whose length the Control Plane must bound. **Prefer the
descriptor walk** — the bucket is already the authority a rebuild trusts.

### Option 4 — flatten at clone time

**What it is.** Materialise the clone's own complete image when the clone is created, so every
volume is self-contained from birth: no `parentView`, no chain, and per-volume deletion stays
trivial.

**Why it is listed and why it is rejected.** It is the option people reach for, and this
repository has the scar: `CloneCrossHost` was **280 lines, fully tested, doing expensive work
on the wrong machine, and called only by its own test** — CLAUDE.md names it as the canonical
example of a component with no caller. It also moves the cost to the worst moment: a clone
that cannot boot until a full copy finishes turns a create into a wait proportional to the
parent's size, where flattening pays the same bytes at a *stop*, when nothing is waiting.

**The argument this section is not allowed to make, and an earlier draft made it:** that a
full copy before boot would waste the same-host placement preference. It would not, because
that preference already buys nothing at read time — the header establishes it: `parentView`
downloads the parent's snapshot from the object store on every attach, same host or not.
Rejecting option 4 for spoiling a locality the code does not have would be arguing from a
comment instead of from the code, which is the failure mode both these specs exist to break.

## 4. Recommendation

**Option 1 plus the §2 fix now; option 2-as-per-lineage together with option 3 as a later
increment with its own spec.**

- The §2 defect is a live outage — a clone works once — and belongs to whichever increment
  lands first, on its own, with the e2e arm that would have caught it.
- Options 2 and 3 change the sealing rule and the attach path. That is a format review and a
  §25.2 property test, and it must not ride along with a bug fix.
- The storage cost is real but bounded per link, and nothing is deployed.

**The trigger for doing 2+3:** when the duplicate is *multiplied* rather than doubled — a
golden image with N clones, or lineages routinely deeper than two. Until then the flattening is
the cheapest correct thing in the tree.

## 5. What a format change here would have to prove (§25.2)

If the key space or the AAD moves:

- **Serialize/replay with arbitrary truncations and bit corruptions** over the manifest:
  round-trip exactly, or return a detected error — never silently wrong. The shape already
  exists in `internal/image/image_property_test.go` (`TestPublishLoadRoundTrip`,
  `TestLoadRefusesCorruptedChunks`).
- **The binding is still a binding.** A chunk placed under the new key space with the wrong
  identity in its AAD must fail to open. §1.2's probe is that test in embryo, and it is the one
  assertion that distinguishes "we moved the key" from "we removed the protection".
- **The lesson `internal/descriptor` already paid for.** Its digest is over *the bytes as
  stored*, because a hash recomputed from a decoded struct cannot see what the decoder
  normalised away — a single bit flipped in `"volume_id"` yields `"Volume_id"`, Go matches
  field names case-insensitively, and the re-marshalled digest matches
  (`descriptor.go`, the comment above `frame`/`unframe`). Any new framing here inherits that
  rule rather than rediscovering it.

## 6. The observables

Outside-observable, each with the plant that proves it can fail.

1. **A clone is stopped and started again, and reads its own bytes back** (`integration/e2e`).
   **This is the arm that fails today** — §2's defect is the plant, already in the tree, so
   this is a regression test for a live bug rather than a hypothetical.
2. **A depth-2 clone reads its grandparent's bytes** (DST, a new scenario such as
   `a-clone-of-a-clone-reads-its-grandparents-bytes`): ranges written only by the grandparent,
   never rewritten below. It passes today *by copying* and must pass after whatever is chosen.
   *Plant:* make `parentView` return `nil` and watch the grandparent's ranges read as zeros.
   **Merge rule:** a new mandatory scenario must edit `pinnedMandatorySet` in the same commit,
   and at most one lane may add one per merge window.
3. **The storage cost is asserted in bytes in the bucket.** After a clone's first stop, sum the
   sizes under `image/<clone>/chunks/`. Option 1 asserts it is ≈ parent + own; option 2 asserts
   it is ≈ own. Asserting the *bytes* is what turns "a clone is cheap" from an impression the
   design document leaves into a property — and §1.2 shows the current numbers a test would
   start from (3145756 for both parent and clone, on a 512-byte write).
   *Plant:* under option 2, restore the flattening and watch the byte count jump by the
   parent's dataset.
4. **The ceiling refuses.** `control-plane -clone-snapshot` of a snapshot whose volume is at
   the ceiling exits non-zero and creates **no catalog row and no descriptor**.
   *Plant:* remove the guard and watch a depth-6 chain come into existence.
   (Only meaningful if the answer to §8 is "structure".)

## 7. What this deliberately does not do

- **No background FLATTEN operation.** §20.1 asks for it as a reconciled background operation
  with an I/O budget — a budget ADR-0026 increment 4.5 deleted along with the io-class
  scheduler. **And the vocabulary went too, while this spec was being written:**
  `lifecycle.OpFlatten` and the `operations` table it was a kind of are gone in `c81d83d`,
  replaced by a comment naming every kind that left (`internal/lifecycle/lifecycle.go`, above
  `SnapshotState.PredecessorNames` — cited by symbol because that file's line numbers moved by 61 during
  the writing of this spec) and by `schema.sql`'s *"There is no `operations` table"* (`:6-12`).
  So a FLATTEN is now new
  machinery — an operation kind, a row, a term guard, a reconciliation — and not an
  implementation behind a name that already exists. It is a phase, not this increment, and
  §8's "structure" answer should be priced with that in it.
- **No change to `Publish`'s CAS or `PublishSnapshot`'s create-only.** Mutual exclusion at
  publish is its own review zone and is untouched by everything above.
- **No compaction, no segment objects, no per-volume index.** Phase 12, and ADR-0012's amendment
  already says that is where the index belongs.
- **Nothing about the WAL record or segment format.**
- **No decision about deletion.** DELETION-AND-RECLAIM-SPEC owns that, and §3's option 2 is
  where the two touch.

## 8. ~~The question for review~~ — answered 2026-08-07: **structure**

See "DECIDED" at the top of this file, which supersedes §4's recommendation. The original
framing follows, because the alternatives it lays out are what the decision was made against.

**Is `chain_depth` a structure or a label?**

Today it is neither on purpose: the Control Plane increments it (`clone.go:91`), nothing reads
it, no ceiling refuses on it, its metric has no producer, and the flattening makes it describe
a lineage rather than a read path. Every option above is downstream of the answer.

- **Label.** Keep flattening (option 1). `chain_depth` becomes observability or is deleted;
  §20.1's FLATTEN is "already done, eagerly, at every publish"; the ceiling of 5 bounds nothing
  and comes out of the document; §20 gains the sentence about upload cost it has never had.
  The price is paying for a duplicate per link, forever.
- **Structure.** Stop flattening (options 2+3). The ceiling becomes a refusal
  `controlplane.Clone` enforces, and FLATTEN becomes the operation an operator runs to get back
  *under* it. The price is a format change, a chain walk in the attach path, a background
  operation this repository does not have, and a deletion design that must count references
  across prefixes.

**The author's view, offered so the reviewer has something to disagree with:** *label* is right
for V1, and the value of deciding it is not the storage — it is that it is what the code
already does, by accident, with two documents and a metric claiming otherwise. An accident
nobody wrote down is precisely what DEV-0020 has been for two waves. Whichever way it goes,
the decision belongs in a comment at `controlplane.Clone`'s `ChainDepth` line and at
`image.uploadChunks`'s `view.Ranges()` loop — the two places a reader stands when they ask —
and not only here.
