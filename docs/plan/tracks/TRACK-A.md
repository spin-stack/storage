# Track A — the documents

**This file is track A's alone.** It was carved out of `STATUS.md` on 2026-08-04
because five lanes appending to one file is a collision every wave: in wave 3 one lane
committed a stale copy and deleted 121 lines of another's, ten seconds after they
landed, and only that lane looking again restored them. Ownership by *file* is a
control; ownership by *section of a file* is a convention, and a convention is what
`PARALLEL-PLAN.md` says is not a control.

`STATUS.md` remains the single answer to "what is true right now" — its head, its
tables and its DEV entries. This is the running log of one track's increments.

---

## Track A — the documents (open work, appended per increment)

*Only track A appends here* — it owns the architecture document, the head of this file,
`REFERENCE.md`, `RISKS.md` and `INVARIANTS.md`. The head tables are recounted once, at
integration, by this track; no other track edits them.

### A1 — the architecture document stops contradicting itself (2026-08-04)

`98e38b6` (the document), `725b75e` (`REFERENCE.md` + `RISKS.md`).

**Four lines stated a withdrawn contract in the present tense** and are corrected where
they stood: the v5.1 changelog bullet announcing the dual durability mode, §4's two ACK
rows, §8's `durability TEXT NOT NULL DEFAULT 'remote'` (the declared schema dropped that
column, with the reasoning at `internal/schema/schema.sql:96-102`), and §31's criteria
3/3b. **Four sections got a banner instead of a rewrite** — §23, §29, §31, §32 — matching
what §12, §21, §22 and `INVARIANTS.md` already do, and each banner names which parts of
its own section are still true.

**§17 got line fixes rather than the banner the audit suggested, deliberately.** Its
*WRITE normal* and *DISCARD* blocks are exactly what the code does; only the FLUSH
pointer (to §14.4's six steps) and the closing "every FLUSHed write survives host loss"
were false — and that last one contradicted §2 and §14.8 inside the same file. A banner
saying "this section is V2" would have been as wrong as the two lines were.

**What is still stale and was deliberately left**, so the next increment has an inventory
rather than a rediscovery: §5.8 (S3 as recovery authority — INV-08 is boot authority now),
§11 (I/O classes; `internal/ioclass` is deleted), §14.2/§14.3/§14.4/§14.5 (the remote
batch and its rules), §16 (the state machine's `SELF_FENCED`/`RECOVERING` arms), §24 (the
S3 client subsystem), §30's roadmap items 6, 8, 10, 11 and 12. None of them contradicts
§14.8 in the way §17 and §23 did; all of them describe V2 in the present tense.

**Two claims in files this track does not own were checked and are wrong**, both about
tests: `integration/vhost/lifecycle_test.go`'s doc comment still describes checkpoints,
truncation and §21.1 above a function renamed `TestAGuestSurvivesAStopAndComesBackFromItsImage`,
and `integration/vhost/guest_test.go:38` still says a guest's FLUSH is answered by "our
§14.4 ACK path — the object verified and the lease valid at the instant of the ACK", when
`integration/e2e/guest_test.go:75` asserts that same `fsync` publishes **zero** objects.
The bodies of `## ~~DEV-0007~~` and `## ~~DEV-0022~~` in this file's open-work region name
the old test too. Track C owns the first, track B the second, and the owner the third.

**`task ci` was red before and after this change**, at `fmt:check` on
`internal/metadata/pg/pg.go` — another lane's uncommitted gofmt alignment. This increment
changed three Markdown files. What it is accountable to are the four self-checks in
`REFERENCE.md`'s header (dangling `§`, and every `ADR-`/`INV-`/`DEV-` the code cites
resolving), which were run and are all empty; adding the missing `DEV-0022` row is what
made the last one so.

### A2 — the head is recounted, the body stops using the present tense (2026-08-04)

`30ea4b2` (STATUS.md), `ad77035` (README + two spec deletions), `<this commit>` (this
entry).

**Every claim in both head tables was re-derived from the code and cites the file and line
it was checked against.** That is the whole method, and the audit that produced this item is
the argument for it: the phase table listed checkpoints, GC, the drain, promotion and the
remote WAL as integrated, and none of those packages exists. A table edited from another
table is what produced that. The invariant line is now **14 active, 6 withdrawn, 2
pending** — counted from `INVARIANTS.md`'s explicit states, against "21 of 22 active".

**Three rows needed a state the table did not have**, so `withdrawn` was added to the
maturity legend, matching `INVARIANTS.md`. "Model" reads as *written, waiting for a
caller*; a reader who goes looking for `internal/recovery` finds nothing at all, and those
are not the same claim.

**The old BUILD-INVENTORY table (increments 0-8) was deleted rather than banner-ed** — the
only deletion in this increment. It recorded a queue, not a defect or a decision, and
`git log` has it. Everything else got a banner and kept its account: the durability
scheduler's two traps, the e2e lane's "an object in the bucket is not proof of a claim",
§14.8's two halves, DEV-0007's clone-chain findings, DEV-0019's three-level fix, DEV-0012's
two fencing triggers, Gap 1's LIST precondition.

**DEV-0020 was rewritten, not banner-ed, because it is open and it described the wrong
defect.** `materialize.FromSnapshot` is gone; nothing walks a clone chain, and
`image.Publish`/`PublishSnapshot` upload `view.Ranges()`, which flattens the base into the
layer (`internal/cow/ranges.go:24`) — so a depth-2 clone reads a *copy* of its
grandparent's data, not zeros, and pays a full duplicate per link. Nothing in the tree
exercises `chain_depth > 1`, and the entry now says so rather than asserting the inference.
C11 and this are one decision, which is what `PARALLEL-PLAN.md` concluded independently.

**Four counters were recounted with the command beside them**, all four wrong: ADR-0013's
citations (54 non-test lines, not 32 — it has overtaken ADR-0026 as the most-cited),
`internal/obs.Catalog()` (25 declared, 9 with a producer, under a paragraph that said
"six" above a list of nine, one of which had been cut), the architecture document's
citations (846, not 2.106), and §12.3's (22 across 15 files, none in the deleted
`promotion.go`). Two of them moved between two runs an hour apart while other lanes
committed.

**`README.md`'s reachability claim was wrong and is now a loop that answers itself.** Six
`.go` files cite `SHUTDOWN-PUBLISH-SPEC.md`; one cites `VIEW-ADOPTION-SPEC.md`; every other
spec is cited by no code at all. `DURABILITY-SCHEDULER-SPEC.md` and
`RUNTIME-FENCING-SPEC.md` are deleted under that file's own rule — ADR-0026 removed the
scheduler and the lease-gated ACK they reviewed, and the decisions that outlived them are
in `internal/agent/volume.go` where CLAUDE.md says they belong.

**`task ci` exit 0** (fmt, build, lint, race tests, dst) immediately before the first
commit; an earlier run the same afternoon was red at `fmt:check` on
`internal/metadata/metadatatest/contract.go`, another lane's file, and that lane fixed it.
The four self-checks in `REFERENCE.md`'s header were run and are all empty. **Left for
whoever holds the counters:** `CLAUDE.md` says "25 ADRs and 10 spec documents" and the
tree has 22 and 7 — that file is outside this track's ownership.

### A3 — the record catches up, and stops needing to (2026-08-06)

`00bbeee` (`INVARIANTS.md`), `cd0c0a6` (ADR-0025, ADR-0016, `REFERENCE.md`), `60ecc13`
(`STATUS.md`, `RISKS.md`, `README.md`), `<this commit>` (this entry).

**Four lanes changed the tree for a wave while this file did not run, and the item was to
catch it up *and* remove the reason it needed catching up.** Two sections did most of the
rotting and both are now handed to something that computes them.

**"Components with no production caller" is deleted as a list.** It was wrong in three ways
at once: it stated that `published_sequence` "is permanently 0 and nothing reclaims a
segment", which stopped being true when `wal.Log.InstallBase` landed; it still listed
`metadata.Store.ResizeVolume`, which track D deleted (`446b61e`) about an hour before this
increment committed; and it had never grown the ten findings that now sit in
`hack/deadcode-pending.txt`. `task deadcode` moved into `task ci` in the same window
(`64b9087`), with a two-list ratchet — allowlist for "unreachable and correct forever",
pending list for "unreachable and not yet deleted" — and both fail on an entry with no reason
*and* on an entry the tool stops reporting. What is left in `STATUS.md` is only what the tool
cannot see, and each of those is verified by a command rather than asserted:
`deadcode -whylive '…/internal/metadata/pg.(*Store).BumpVolumeEpoch' ./cmd/...` answers
`not found in program`, and `wal.Log.Broken`'s own doc comment says it has no production
caller.

**Four invariant rows had moved, and the one that matters is INV-13.** It was `withdrawn` on
"there is no mid-session truncation, and the rule survives with no production caller".
`Log.InstallBase` — called from `internal/agent/volume.go`'s `fetchBase` — reclaims the
segments the restored image already covers, through `truncateLocalLocked` and
`StrictOrder.AllowTruncate`, which *is* INV-13; it also raises `published`, which ends INV-03's
"permanently 0". INV-04 described §5.7's unflushed bounds, and `agent.Budget.Limits` sets
neither: `Sync` clears the unflushed counter on every guest `fsync`, so the bound that binds
is `MaxLocalBytes`. INV-20 claimed a rebuilt catalog loses "snapshot lineage depth" — it does
not, `volumeFromDescriptor` restores `ChainDepth` and `ParentSnapshotID`; what it loses is
placement and a snapshot's `SourceHostID`.

**ADR-0025 got a banner, not a rewrite**, matching the six amended before it. It decided
twice that a missing artefact *skips* the guest lane, and wave 4 reversed that; the banner
says what overturned it (a skip notice does not survive the sentence "ci:full was green", and
a skipped needed job leaves its dependents free to run) and what survives (the developer path
still skips; `REQUIRE_PROOFS` is what removes it). **ADR-0016 got a second banner** because
its first one said stage 1 "survives and is real" and wave 4 deleted all of it.

**Every count in the head is now the command that recomputes it**, and each one was wrong
when checked: ADR-0013's citations (the file said 54 across 23 non-test files; the ranking
command is what is written now), `obs.Catalog()`'s declared and produced entries, the
invariant tally, and §7's "96 references" in `REFERENCE.md`. Line numbers went the same way —
several cited lines had moved while other lanes committed. **One trap is recorded next to the
metric command rather than left to be rediscovered:** a catalog name that is also a JSON tag
matches its struct field, so `chain_depth` reports a producer it does not have, and the
previous "nine of 25" was produced by that grep without the caveat.

**The wave moved under this increment twice**, which is the argument for the whole approach:
`ResizeVolume` was a "waiting on a human" bullet when it was written and an answered decision
when it was committed, and `task deadcode` went from red-and-outside-the-gate to
green-and-inside it between two runs. Both were caught by re-running the commands, not by
rereading the prose.

**`task ci` exit 0** immediately before the first commit (fmt, build, lint, deadcode, race
tests, dst, workflows:verify). The four self-checks in `REFERENCE.md`'s header were run and
are all empty. **Left for other lanes, all outside this track's files:**
`internal/descriptor`'s package doc still says the layout is autodescriptive "together with
the epoch object, recovery-points, and manifests", two of which ADR-0026 deleted, and
`Descriptor.CurrentEpoch`'s field comment still says "last known; the epoch object is
authoritative" while `controlplane.rebuild` states the opposite and is right. Both are track
D's.
