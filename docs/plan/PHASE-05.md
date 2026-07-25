# PHASE 05 — Per-volume encryption (DEK/KEK, dev KMS) + DISCARD/WRITE_ZEROES

> **Roadmap §30.5.** Depends on Phase 04 (the format reserved `KeyID`/`AuthTag`;
> Phase 05 turns them on — no format change). **⚠️ Human-review zone** (crypto +
> durability-adjacent). Implements §15, §5.10, §14.6.

**Phase objective:** every VM-data payload leaving the host is AES-256-GCM encrypted
with the volume's DEK before it is written to the WAL file (and later PUT to S3); the
DEK is wrapped by a KEK held in a (dev) KMS; DISCARD/WRITE_ZEROES converge storage to
the working set. Live in-memory reads stay plaintext (that never leaves the host).

**Invariants activated:** INV-15 (nothing leaves the host in clear).

**Metrics activated:** `discarded_bytes_total`.

---

## Increment 5.1 — Crypto core (DEK/KEK, dev KMS, AES-256-GCM, derived nonce)

**Objective:** `internal/crypto` — DEK (AES-256) with a `KeyID`, a KEK/KMS
abstraction with a dev implementation (KEK in memory/file), payload seal/open with a
**deterministic nonce** derived from `(volume_id, epoch, sequence)` (§15.2, no stored
nonce, no reuse within an epoch by sequence monotonicity), and DEK wrap/unwrap.

**Scope in:**
- `DEK.Seal(volumeID, epoch, seq, plaintext) → (ciphertext, tag)` and `Open(...)`.
  Ciphertext length == plaintext length; the 16-byte GCM tag is returned separately
  (stored in `RecordHeader.AuthTag`). AAD binds `(volumeID, epoch, seq)`.
- Nonce = KDF over `(volumeID, epoch, seq)` (deterministic; not stored).
- `KMS` interface + `DevKMS` (KEK in memory) with `WrapDEK`/`UnwrapDEK`. Randomness
  (DEK generation, wrap nonce) is injected via an `io.Reader` so the data path stays
  deterministic under DST (the derived payload nonce is already deterministic).

**Tests first:** seal/open round-trip; tamper (flip a ciphertext or tag bit) ⇒ `Open`
fails; **nonce uniqueness** property (distinct `(vol,epoch,seq)` ⇒ distinct nonce);
wrap/unwrap round-trip; unwrap with the wrong KEK fails.

**Gate:** standard. No invariant flips yet (library only).

---

## Increment 5.2 — Encrypted WAL payloads end-to-end + INV-15 + DISCARD accounting

**Objective:** the write path encrypts payloads before append (WAL file holds
ciphertext + `KeyID` + `AuthTag`, `PayloadCRC32C` over **plaintext** per §14.1); replay
decrypts and verifies; live reads (interval map) remain plaintext in host memory. DISCARD
converges storage and increments `discarded_bytes_total`. Crypto-shredding: destroying
the wrapped DEK renders remnants unreadable (§15.3).

**Scope in:**
- `format.DecodeRecord`: gate the plaintext payload-CRC check on `KeyID == 0`; for
  `KeyID != 0` return the ciphertext unverified (GCM tag + post-decrypt CRC verify it).
- `wal.Log` optional encryption context (DEK + volumeID): `Write` seals the payload,
  sets `KeyID`/`AuthTag`; the WAL file bytes are ciphertext.
- A recovery/replay path that decrypts ciphertext records back to plaintext and checks
  the plaintext CRC + GCM tag.
- DISCARD/WRITE_ZEROES accounting: `discarded_bytes_total`.

**Tests first / DST:**
- INV-15 scenario: write via an encrypted `Log`, read the **raw WAL file bytes**, assert
  a plaintext canary is **absent** (payload is ciphertext); replay+decrypt reproduces the
  plaintext.
- Tamper: corrupt a ciphertext byte on disk ⇒ decrypt (GCM) fails, never silently wrong.
- Crypto-shred: without the DEK, remnants are undecryptable.
- DISCARD converges (interval map / active map) and `discarded_bytes_total` increments.

**Gate:** standard + INV-15 in the DST set + human review (crypto/format-touching zone).
Activates INV-15.

## Phase 05 exit gate
- [ ] All VM-data payloads on the WAL file are ciphertext; metadata is not.
- [ ] Derived-nonce seal/open + tamper detection; DEK wrap/unwrap via dev KMS.
- [ ] INV-15 active and green; replay decrypts correctly.
- [ ] DISCARD/WRITE_ZEROES converge; `discarded_bytes_total` flowing.
- [ ] Human review of crypto + the `DecodeRecord` format touch.
