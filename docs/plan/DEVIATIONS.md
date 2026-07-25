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
