# ADR-0003 — Enforcement of simulable interfaces (the day-1 lint)

Accepted 2026-07-24. Implements §25.1, INV-01. §25.1 requires this be verified by CI, "not
by good intentions"; §29.7 records that it is impossible to retrofit. This ADR fixes *how*.

## Decision

No `time.Now()`, socket, or disk/network/S3 syscall outside `internal/simio` (the
interfaces) and `internal/simio/real` (thin passthroughs); everything else takes them by
injection. Enforcement is **layered**, so a gap in one layer is caught by another:

1. **Package boundary.** Only `simio` and `simio/real` may import the real primitives.
2. **`depguard`.** Forbids importing `time` (except for types), `net`, `os` file
   operations, `syscall` and the concrete S3 SDK packages from anywhere outside those two.
3. **`forbidigo`.** Forbids the call expressions that slip past import rules:
   `time.Now/Since/Sleep/After/Tick`, `net.Dial*`, `os.Open/Create/OpenFile`, and raw disk
   and net `syscall.*`.
4. **The custom `simulable` analyzer** (`hack/analyzers/simulable`, a `go/analysis` pass,
   runnable standalone and as a golangci-lint plugin). It catches what config-based linters
   cannot express cleanly — constructing a real clock, dialer or objectstore outside the
   allowed packages, method-value escapes — and ships `analysistest` fixtures, one violating
   package and one compliant.

`task lint` runs all four and is a required CI job. A planted violation must turn it red;
that demonstration is the checker for INV-01.

Adding a new real primitive (a KMS client, say) means extending `simio` with an interface
and its real/sim pair, never an inline SDK call. That is the rule the lint exists to keep.

## Alternatives rejected

- **Convention plus code review.** Refused by §25.1 in as many words.
- **The custom analyzer alone.** More code to maintain and slower to author; config buys
  most of it, and the analyzer is reserved for what config cannot state.
