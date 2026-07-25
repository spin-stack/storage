# PHASE 04 — CoW (64 KiB segments) + local WAL (real extents) + format v2 + WAL property tests

> **Roadmap §30.4.** Depends only on Phase 01 (simulable interfaces + DST harness):
> the WAL/CoW engine is driven directly by the harness, so it does **not** require
> Phase 02 (guest layout) or Phase 03 (vhost-user) to exist first. Integration with
> the guest data path is a later, separate increment.
>
> **⚠️ HUMAN-REVIEW-REQUIRED ZONE.** This phase defines **on-disk / on-S3 formats**
> and touches **durability**. Per PLAN §2 the increment spec is reviewed by a human
> *before* the Harness agent starts, and the diff *before* merge. This file is that
> spec. The format below is transcribed from doc §14.1/§14.2; any encoding decision
> the doc leaves open is called out as an **[OPEN]** item needing an ADR.

**Phase objective (one sentence):** implement the v2 WAL record and object formats
(real guest extents, crypto fields reserved), append-only local WAL with a read-side
interval map, 64 KiB CoW segment granularity with a roaring-bitmap active map, and the
§25.2 serialize/replay property tests — all under the DST harness.

**Invariants this phase activates:** INV-03 (ordered watermarks), INV-04 (unflushed
bounds/backpressure — bound checks; full enforcement Phase 06), INV-05 (WAL
serialize/replay total & safe), INV-18 (S3 not in every WRITE). Reserves the fields for
INV-15 (encryption, Phase 05) and INV-19 (format read-old/write-new, Phase 13).

**Metrics activated (from the §26.2 registry, values start flowing):**
`wal_append_latency_seconds`, `wal_fdatasync_latency_seconds`, `wal_unflushed_bytes`,
`wal_oldest_unflushed_age_seconds`, `wal_{local,durable,published}_sequence`,
`active_map_bytes`, `discarded_bytes_total` (DISCARD lands fully in Phase 05).

---

## Increment 4.1 — WAL record + object format v2  ⚠️ format review

**Objective:** define the exact byte layout of the WAL record header (§14.1) and WAL
object header (§14.2), with a versioned, magic-tagged, CRC-protected encoder/decoder.
Crypto fields (`KeyID`, `AuthTag`) are **present but unused** until Phase 05.

**Scope in:**
- `internal/wal/format`: Go structs + `MarshalBinary`/`UnmarshalBinary` for:
  - `RecordHeader` — 96 bytes fixed, magic `"VW02"`, version 2, `RecordType`
    (0=WRITE, 1=DISCARD, 2=WRITE_ZEROES), `VolumeID[16]`, `Epoch`, `Sequence`,
    `OperationID[16]`, `Offset`, `Length`, `KeyID`, `PayloadCRC32C` (of plaintext),
    `Flags` (bit0=FUA, bit1=part of FLUSH), `AuthTag[16]`, `HeaderCRC32C`. Payload
    follows (ciphertext in Phase 05; plaintext-with-CRC now).
  - `ObjectHeader` — 96 bytes, magic `"WB02"`, version 2, `VolumeID`, `Epoch`,
    `FirstSequence`, `LastSequence`, `RecordCount`, `KeyID`, `PayloadLength`,
    `PayloadSHA256[32]`, `HeaderCRC32C`.
  - The deterministic S3 key builder: `wal/<volume_id>/<epoch>/<first>-<last>-<sha8>.wal`.
- Explicit endianness, alignment, and `Reserved` fields for forward-compat (§27.5).

**[OPEN] decisions to confirm in the review (each → ADR-000N):**
1. **Byte order:** little-endian (x86/arm64 native, avoids per-field swaps). *Proposed.*
2. **CRC:** CRC32C (Castagnoli) via `hash/crc32` — matches doc's `CRC32C`. *Confirmed by doc.*
3. **Nonce derivation** (Phase 05, reserved now): `KDF(volume_id, epoch, sequence)`,
   not stored (§15.2). Field layout must leave room — it does (`AuthTag` present).
4. **Header size hard-check:** encode/decode assert `HeaderLen==96`; mismatch = hard fail.

**Tests first (Harness agent):**
- Golden-bytes test: a known record/object header marshals to an exact byte slice
  (locks the wire format; any accidental change fails CI).
- Round-trip: `Unmarshal(Marshal(x)) == x` for randomized valid headers.
- Corruption: flip any header byte ⇒ `HeaderCRC32C` mismatch ⇒ decode error (never a
  silently-wrong struct).
- Magic/version: wrong magic or version ⇒ typed error (read-old path groundwork §27).

**Gate:** standard gate + **golden-bytes format test** (this is the format lock) +
human review of this spec and the diff. No invariant flips yet (format only).

**Rollback:** format is additive and unreferenced by any running writer until 4.3;
revert branch. The `Version=2`/magic discipline means a bad format never silently
co-mingles with a future v3.

---

## Increment 4.2 — WAL serialize/replay + property tests (§25.2, INV-05)

**Objective:** a WAL segment writer/reader over `simio.disk` that appends records and
replays them to reconstruct state, with the §25.2 property test: any record sequence,
truncated at any byte and bit-corrupted anywhere, either replays to the exact expected
state or is caught by CRC — never silently wrong.

**Scope in:**
- `internal/wal`: `Writer` (append record → disk, track `local_sequence`), `Replayer`
  (scan a WAL file, validate CRCs, stop at the first torn/truncated tail cleanly).
- A pure in-memory reference model (map offset→bytes) to compare replay against.
- Property test (`pgregory.net/rapid`, per ADR-0001): generate `[]Record`
  (write/discard/zeroes, arbitrary aligned extents) → serialize → for every truncation
  offset and a sample of bit flips → replay → assert (exact expected state) XOR
  (detected-corruption error).

**Invariants activated:** INV-05 (WAL serialize/replay), INV-18 (normal WRITE issues no
PUT — asserted at this layer).

**Tests first:** the property test itself, plus unit tests for torn-tail handling
(a half-written trailing record is dropped, not misread).

**Gate:** standard gate + §25.2 property test in the mandatory set + human review
(durability/format zone). Activates INV-05, INV-18.

---

## Increment 4.3 — Local WAL with real extents + read interval map (§13.1–13.2)

**Objective:** the write path logs the **real guest extent** (offset+length, 512 B
aligned), not 64 KiB; reads resolve through an in-memory interval map of not-yet-
objectized extents, then fall through the read chain (§13.2).

**Scope in:**
- `internal/wal`: extent-accurate append; `local_sequence`/`durable_sequence`/
  `published_sequence` watermarks with the ordering invariant.
- `internal/cow` (read side): interval map (per volume) for partial-overlap reads;
  read chain: dirty extents → (active map, 4.4) → zero block.
- Unflushed accounting: `unflushed_bytes`, `oldest_unflushed_age` (bound checks only;
  backpressure enforcement is Phase 06).

**Invariants activated:** INV-03 (ordered watermarks), INV-04 (bounds; enforcement
Phase 06).

**Tests first:** watermark-ordering checker in the harness; interval-map read tests
(overlapping/partial extents, read-after-write within a volume); crash-around-append
DST arm (extends the Phase 01 scenario with real records).

**Gate:** standard gate. Activates INV-03, INV-04(bounds).

---

## Increment 4.4 — CoW 64 KiB segments + roaring-bitmap active map (§13.3)

**Objective:** segment granularity of 64 KiB with an active map that stays within a
memory budget via roaring bitmaps (presence) + a range-located placement table.

**Scope in:**
- `internal/cow`: 64 KiB segment model; active map = `RoaringBitmap` presence +
  location table (not naive hashmaps); `active_map_bytes` metric.
- Read chain completion: dirty → active map → (checkpoint/segment/S3 are Phase 06/10) →
  zero block.
- Property: 1 TiB volume ⇒ 16M possible segments handled without blowing the memory
  budget (§10.1).

**Invariants activated:** none new (supports INV-03/04); memory budget metric wired.

**Tests first:** active-map memory-bound test (large sparse volume stays under budget);
segment mapping round-trips; DISCARD marks segments absent (full DISCARD Phase 05).

**Gate:** standard gate.

---

## Phase 04 exit gate

- [ ] Format v2 (record + object) locked by golden-bytes tests; crypto fields reserved.
- [ ] §25.2 serialize/replay property test green in the mandatory set (INV-05).
- [ ] Watermark ordering (INV-03), unflushed bounds (INV-04), no-PUT-on-WRITE (INV-18)
      active and green.
- [ ] Active map within memory budget; `active_map_bytes` flowing.
- [ ] DEVIATIONS.md clean; ADRs for the [OPEN] format decisions (byte order etc.).
- [ ] Human review of the format spec and diffs (data-loss zone) recorded.

Then Phase 05 (encryption + DISCARD) can flip on the crypto fields, and Phase 06 (remote
WAL) can start on the locked object format.
