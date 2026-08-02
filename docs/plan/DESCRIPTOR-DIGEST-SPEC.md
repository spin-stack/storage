# DESCRIPTOR-DIGEST-SPEC — closing DEV-0015

**Human-review zone: on-S3 format.** Reviewed before implementation per CLAUDE.md, which
also requires a serialize/replay property test over arbitrary truncations and bit
corruptions (§25.2) — the descriptor got one with increment 6, and it is what found this.

## The hole

Everything else this system puts in a bucket verifies itself:

- WAL records carry a CRC32C of the **plaintext** plus a GCM tag (§14.1) — two layers, on
  purpose.
- WAL objects are verified after upload before `durable` advances (INV-07).
- Checkpoints carry `RootDigest`, and `DigestMatches` recomputes it (§20, §22.3).
- Key material is an AEAD ciphertext whose version is bound as additional authenticated
  data.

`volumes/<vol>/descriptor.json` is plain JSON with nothing over it. Truncation is caught —
the property test proves it at every byte, because JSON without its closing brace does not
decode — but **a flipped bit inside a number is not**. Change a digit in `size_bytes` and
the object still decodes, into a different and perfectly valid descriptor.

The blast radius is real but narrow, and worth stating precisely so the fix is not
oversold:

- `dek_wrapped` and `dek_key_id` are already **self-detecting**: corrupting either makes
  the unwrap fail (`ErrUnwrap`), asserted by `TestCorruptedKeyMaterialCannotUnwrap`.
- `current_epoch` is not authoritative here; the epoch object is (§12.4).
- What is exposed is **`size_bytes`, `block_size`, `chain_depth`** — and only on the
  rebuild path, since a live volume never reads its own descriptor for those. §22.5's
  `rebuild-metadata` would recreate the volume at the wrong size, and a volume whose
  catalog says it is smaller than the data written into it is not repairable by re-running
  anything.

## What gets built

A digest over the descriptor's own stored bytes, checked on read before anything is
decoded.

**How it is computed — corrected during implementation.** The spec as first written said:
SHA-256 over the JSON encoding of the descriptor *with the `Digest` field itself empty*, a
field inside the object. **That design does not work, and the property test broke it on
its first run.**

Byte 2 of the stored object is the `v` of `"volume_id"`. Flip one bit and it becomes
`"Volume_id"` — and Go's decoder **matches field names case-insensitively**, so the struct
decodes identically, re-marshalling it reproduces the original digest exactly, and the
corruption is invisible. The same hole swallows unknown fields, duplicate keys, whitespace
and numeric spellings: a hash over a *re-encoding* can only ever see what the decoder did
not normalise away.

So the digest is over the **bytes as stored**, and it lives outside them. The object is:

```
<64 hex chars>\n<json>
```

`Read` splits the line off, hashes the remainder, and compares before decoding anything.
The JSON stays readable for anyone inspecting a bucket by hand, which matters for §22.5.

**What a mismatch means.** `ErrCorruptDescriptor`, and the read fails. It is not repaired
and not guessed at: a descriptor whose bytes disagree with their digest is an object
nobody can say the true contents of, and the recovery path's whole job is to be right
about that.

**Formats change in place** (CLAUDE.md, until the spine ships): no v2 beside v1, no
compatibility shim. A descriptor written before this change is bare JSON with no digest
line, and it is **refused with the same error as a corrupt one**. Nothing is deployed;
every descriptor in existence is one a test wrote, so there is nothing to be lenient
*for*, and a lenient branch would leave the hole open permanently for the sake of a volume
that does not exist.

**Scope.** `descriptor.Write` frames it; `descriptor.Read` verifies it. Every producer and
every consumer in the tree goes through those two functions — nothing else names
`descriptor.json` — so there is no path that sees the object without its digest.

## What this does not do

- It does not make the descriptor authoritative for anything it is not. The epoch object
  still owns the epoch, S3 still owns the durable point (§5.8).
- It does not detect a *replaced* descriptor — a valid, correctly-digested object written
  by someone who should not have. That is §12.4's job and it is unchanged.
- It is not a signature. An attacker with write access to the bucket can recompute it. The
  threat this addresses is corruption — a bit flip, a partial write, a mis-copied object —
  which is what every other format here defends against too.

## Tests that land with it

- **The property test's missing half:** flip any bit at any offset of the stored object
  and `Read` must fail, written as the general case rather than for the three fields
  listed above. This is the assertion that rejected the first design.
- **Round trip** stays as it is: the struct that goes in is the struct that comes out.
- **Bare JSON is refused** — the pre-change format, and the reason it is refused rather
  than tolerated.
