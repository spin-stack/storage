# DEVIATIONS

Doc↔code divergences detected by the Doc-sync agent (or anyone). Each entry has a
**mandatory resolution**: either the code is corrected, or an ADR is written (and, if
warranted, a patch to the architecture document is proposed). **Open entries block the
gate** (PLAN §2).

## Format

```
### DEV-NNNN — <short title>
- Detected: <date> by <role/agent> in <increment>
- Doc section(s): §X.Y
- Divergence: <what the code does vs what the doc says>
- Severity: low | medium | high (data-loss zones = high)
- Resolution: <fix | ADR-XXXX> — <status: open | resolved>
```

## Open

_None._

## Resolved

### DEV-0002 — Drain moves from the durable prefix, not from a snapshot
- Detected: 2026-07-25 by implementer agent in Increment 11.3
- Doc section(s): §28.1 (and §20, §22.1)
- Divergence: §28.1 describes drain as "snapshot + restore cross-host en el MVP". A
  snapshot is taken by the *source Agent* (§19) and so requires the evacuated host to be
  alive and cooperating, plus a CP↔Agent RPC that does not exist yet. The implementation
  evacuates from the volume's **durable prefix in S3** (§5.8/§22.1) instead.
- Severity: high (fencing / durability zone)
- Resolution: **ADR-0008** — the move is bulk-materialize → fence → final materialize →
  recovery point; the same durability guarantee, and it also works when the source host
  is dead or partitioned. The snapshot path remains for cross-host clones.
  **Status: resolved (pending human review of the fencing diff).**

### DEV-0001 — WAL header size: doc says 96 bytes, fields sum to 104
- Detected: 2026-07-25 by tech-lead agent in Increment 4.1
- Doc section(s): §14.1, §14.2
- Divergence: both header structs are documented as "96 bytes" but their enumerated
  fields total 104 bytes each. Every field is load-bearing (incl. crypto `AuthTag[16]`,
  `PayloadSHA256[32]`).
- Severity: high (on-disk format, data-loss zone)
- Resolution: **ADR-0005** — headers are 104 bytes, `HeaderLen`/`HeaderLength`=104, all
  fields kept, little-endian, CRC32C over the pre-CRC bytes. Proposed patch to the arch
  doc to correct the "96" erratum. **Status: resolved (pending human format review of
  the branch).**
