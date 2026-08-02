# DEK-ARM-SPEC — BUILD-INVENTORY increment 6

**Human-review zone: keys + format.** This is the increment spec, reviewed before
implementation per CLAUDE.md. It is a *review* — what will be built and where the design
says so — not a set of questions.

## Why this is the increment that makes the slice honest

Today the Agent builds every `wal.Log` with `enc == nil`. Every WAL record, every object
in the bucket, is **plaintext**. §15 opens with the requirement:

> Todo payload de datos de VM (WAL records, segmentos, checkpoints) se cifra con la DEK
> del volumen antes de cualquier PUT. — §15 (line 249)

and the reason it is day-one work rather than later:

> Por qué día 1: re-cifrar petabytes de objetos inmutables después es un proyecto de
> migración. — §15.3

Everything needed already exists and is tested: `crypto.DEK` (AES-256-GCM, nonce derived
from `(volume_id, epoch, sequence)` per §15.2), `crypto.DevKMS` (§15.1's "mínimo
aceptable on-prem: KEK por host en archivo"), `wal.NewEncryption`, `Encryption.Encrypt` /
`Decrypt`, and INV-15's checker. `controlplane.Provisioner` already generates a DEK and
wraps it. **The pieces are not connected**, and one specific link is missing.

## The missing link: nothing remembers the DEK's version

§15.1 requires it:

> `KeyID` en cada record/objeto permite rotación de DEK sin re-cifrar histórico.

`RecordHeader.KeyID` carries it, `wal.NewEncryption` **refuses `KeyID == 0`**
(`ErrUnversionedKey` — 0 means "plaintext record" on the WAL path), and
`Provisioner.Provision` generates the DEK at `KeyID: 1` and returns it in
`ProvisionedVolume`. Then it is dropped: the catalog has `dek_wrapped` and `kek_id` and
no column for the version, `GetVolumeKeysResponse` carries the same two fields, and its
comment already states the consequence in as many words —

> The DEK's own version … is deliberately not here: nothing stores it. … a default of 0
> would be actively wrong. The increment that implements rotation adds the column and
> this field together.

So **the Agent cannot build an encryptor from what the RPC returns**, and it is not a
matter of wiring: there is no honest value to pass. That is why this increment is
"the DEK arm" and not "call EnableEncryption".

The doc's §8 schema sketch has no `dek_key_id` column either. That is not a design
change being made here: §15.1's rotation rule requires the field, the on-disk format
already carries it, and the sketch in §8 already differs from `schema.sql` in other ways
that ADR-0007/ADR-0019 settled (ids are `uuid`, not `TEXT`). Adding the column
*implements* §15.1 rather than extending it, so it is recorded here and in `STATUS.md`
rather than in an ADR.

## What gets built

**1 — the catalog remembers the version.** `volumes.dek_key_id BIGINT NOT NULL`, with a
`CHECK (dek_key_id > 0 AND dek_key_id <= 4294967295)`: `BIGINT` because the format field
is `uint32` and Postgres `INTEGER` is signed 32-bit, and the lower bound because 0 is not
a version — it is the plaintext marker, and a row carrying it would produce a volume the
Agent refuses to open, at attach time, with the DEK already unwrapped. `CreateVolume` and
`GetVolumeKeys` carry it; `metadata.Volume` gains `DEKKeyID uint32`; the `pg` adapter
converts at the boundary as it does for every other id.

**2 — the wire carries it.** `uint32 dek_key_id = 4` on `GetVolumeKeysResponse`, and the
comment quoted above is replaced rather than left contradicting the field beside it.

**3 — the descriptor carries it.** `descriptor.Descriptor` already holds `KEKID` and
`DEKWrapped`; it gains `DEKKeyID`. This is an **on-S3 format change**, so per CLAUDE.md
it lands with a serialize/replay property test over arbitrary truncations and bit
corruptions (§25.2). Formats change in place until the spine ships (DEV-0007) — no v2
beside v1.

**4 — the Agent unwraps and encrypts.** `-kek-file` on `cmd/volume-agent` (§15.1's
file-based KEK), `crypto.NewDevKMS`, a `KMS` field on the Agent's deps, and at attach:
`UnwrapDEK(wrapped, keyID)` → `wal.NewEncryption` → `Log.EnableEncryption` +
`NewBatcher(keyID)`. §15.1 is explicit that this happens **only at attach/recovery**, one
KMS call outside the data path, and the DEK lives in memory only.

**Fail closed.** A volume whose keys the Agent cannot unwrap is not served in plaintext —
it is not served. The alternative is a volume that silently writes cleartext into a bucket
under a name that says it is encrypted, which is the failure §15.3's crypto-shredding
guarantee cannot survive: the objects would be readable after the DEK is destroyed.

Symmetrically, an Agent started **without** `-kek-file` runs as it does today (no KMS, no
encryption) — that is the local/dev mode the DST harness and the QEMU lane use, and it is
honest because nothing claims otherwise. What is refused is the mixed case: a volume that
*has* wrapped keys, on an Agent that *has* a KMS, that fails to unwrap.

## Tests that land with it

- **Format (§25.2):** the descriptor's serialize/replay property test extended to the new
  field — truncate at every byte, flip bits, and get the exact state or a detected error,
  never a descriptor that decodes with a plausible-but-wrong key version.
- **Schema:** `dek_key_id`'s CHECK rejects 0 and rejects the out-of-`uint32` value, on
  Postgres 18 through the integration lane.
- **Round trip:** provision → `GetVolumeKeys` → unwrap → `NewEncryption` → append →
  replay, with the version carried the whole way. It is the assertion that the number
  survives four boundaries, which is where it was lost before.
- **A DST arm, driving the Agent.** INV-15's checker (`NoPlaintextLeavesHostChecker`)
  exists and `scenarioEncryptedWALNoPlaintextLeak` proves it — but that scenario drives
  `wal.Log` directly, so it can only ever prove the *WAL* encrypts. The thing this
  increment changes is the **Agent's** decision to enable encryption at all, and nothing
  simulated watches that. The new arm serves a volume through `VolumeManager` with a KMS
  and asserts the guest's pattern never appears in anything bound for the bucket.
- **Fail-closed:** an Agent with a KMS and a volume whose DEK will not unwrap serves
  nothing, rather than serving it in the clear.
