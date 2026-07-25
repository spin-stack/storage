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

_None._ (Phase 0 produced no production code.)

## Resolved

_None yet._
