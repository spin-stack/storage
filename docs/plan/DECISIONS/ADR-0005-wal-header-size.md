# ADR-0005 — WAL header size is 104 bytes (doc says 96); little-endian; CRC coverage

- **Status:** Accepted (Phase 04 / Increment 4.1) — **pending human format review**
- **Date:** 2026-07-25
- **Deciders:** tech-lead agent
- **Implements/Extends:** §14.1, §14.2. Recorded in DEVIATIONS.md as DEV-0001.

## Context

Doc §14.1 states the WAL record header is "96 bytes" (`HeaderLen uint16 // 96`) and
§14.2 states the WAL object header is "96 bytes" (`HeaderLength uint16 // 96`). However,
the enumerated fields in each sum to **104 bytes** (verified: record and object headers
both total 104). Every listed field is load-bearing, including the v5 crypto additions
(`AuthTag[16]` in the record header; `PayloadSHA256[32]` in the object header). The "96"
cannot be reconciled with the field list without dropping a specified field.

The on-disk format is the highest-stakes irreversible decision in the system, so this is
resolved explicitly rather than silently.

## Decision

1. **Header size = 104 bytes** for both the record header and the object header. All
   fields from §14.1/§14.2 are kept, in the order listed. `HeaderLen` / `HeaderLength`
   is set to the true total, **104**, and decode hard-fails if it does not match.
2. The doc's "96" is treated as an **erratum** (likely a stale figure from before the
   crypto fields were added). A patch to the architecture document is proposed
   (DEV-0001 resolution).
3. **Byte order: little-endian** (native on x86-64/arm64; avoids per-field byte swaps).
4. **CRC: CRC32C (Castagnoli)** via `hash/crc32`, matching the doc's `CRC32C`.
   `HeaderCRC32C` covers **all header bytes preceding the CRC field**:
   - Record header: CRC field is the last field (offset 100); CRC covers bytes [0,100).
   - Object header: CRC field is at offset 96, followed by `Reserved[4]`; CRC covers
     bytes [0,96). The trailing reserved bytes are not CRC-covered (unused; forward-
     compat only).
5. **`magic` + `Version=2`** gate every decode; wrong magic/version → typed error (the
   read-old groundwork for §27). `Reserved` fields exist for extension without a version
   bump (§27.5).
6. **Crypto fields reserved, not active:** `KeyID` and `AuthTag` are present and encoded
   now (zero-valued) so Phase 05 turns on encryption without a format change. In Phase 04
   the payload is plaintext and `PayloadCRC32C` is computed over it directly.

## Consequences

- Payloads begin at offset 104. 104 is 8-byte aligned; fine for our access pattern
  (records are not individually 512 B aligned; the WAL file is append-only).
- A golden-bytes test locks the exact wire layout; any accidental change fails CI.
- If the human review prefers padding to a 16-byte-multiple header (112 or 128) for
  alignment/room, that is a one-line `Reserved` change now (cheap) and impossible after
  the first real data — hence flagged for review before merge.

## Alternatives considered

- **Force 96 bytes by dropping/ō shrinking a field** (e.g., a smaller `OperationID` or
  moving `AuthTag` into the payload): rejected — loses idempotency/tracing or crypto
  integrity the doc requires.
- **Pad to 112/128 for alignment:** deferred to the human format review; not chosen
  unilaterally to stay minimal and faithful to the field list.
