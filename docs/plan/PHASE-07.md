# PHASE 07 — Control Plane (verified term) + reconciliation + leases + full fencing

> **Roadmap §30.7.** Depends on Phase 06 (epoch objects / durable path). **⚠️⚠️ THE
> data-loss zone.** This is the correctness heart of v5 (§12): the fencing protocol
> that closes the stale-writer window. Human review of THIS spec is required before the
> Harness agent writes tests. Implements §5.1, §7, §8, §12, §14.4-step-5, §18, §23.
>
> **Simulability:** PostgreSQL is reached through a `metadata` interface (like `simio`):
> a real (PG/pgx) implementation and a deterministic in-memory sim so DST can drive
> leases/epochs/promotions under partitions and clock drift. Real PG is verified by
> integration tests later; the *protocol* is proven in sim.

**Phase objective:** a single-active Control Plane whose every write transaction is
guarded by a verified `term`; per-host leases renewed in one grouped heartbeat; the
Agent's monotonic-clock durable-ACK rule (§12.2); and controlled promotion through
`FENCING_WAIT` with an S3 epoch-object CAS — such that **no write ACKed durable by a
fenced writer can be lost or unseen by its successor** (INV-09).

**Invariants activated:** INV-06 (durable-ACK rule, `remote` arm), INV-07 (lease-verify
step completes it), INV-09 (no ACKed-durable write lost across failover), INV-10
(effective single writer), INV-11 (promotion wait), INV-21 (admin `request_id`
idempotency).

**Metrics activated:** `lease_remaining_seconds`, `lease_renewal_failures_total`,
`self_fenced_total`, `fencing_wait_duration_seconds`, `clock_offset_seconds`.

---

## Increment 7.1 — Metadata interface + schema + verified-term CP  ⚠️ review
**Objective:** the `metadata` store interface (real PG + sim) with the §8 tables
(`volumes`, `hosts`, `host_leases`, `snapshots`, `operations`, `control_plane_leader`);
CP leadership via `term` (take-leadership increments term; **every** write carries
`WHERE term = $mine`; a stale/zombie CP affects 0 rows, detects it, and self-terminates,
§7).
**Tests first:** two CPs contend — the one with the stale term makes 0-row writes and
steps down; committed state is never double-applied.
**Gate:** standard + human review. Activates the CP-term half of INV-10.

## Increment 7.2 — Lease manager + grouped heartbeat + durable-ACK rule  ⚠️ review
**Objective:** Agent-side lease on the **monotonic clock**: one grouped heartbeat per
host renews the lease and reports capacity (§12.2, §12.6); `lease_valid() :=
monotonic_now() - t0 < lease_ttl`. **Durable-ACK rule (§12.2, INV-06):** before ACKing
any FLUSH/FUA, if `!lease_valid()` → do not ACK, fail the request, transition
`SELF_FENCED`. Wire this as step 5 of `Log.Flush` (§14.4). The `local` mode (§14.8)
does not gate FLUSH on the lease (its ACK is local) but still gates checkpoint/manifest.
**Tests first / DST:** a PUT that succeeds after the lease expires is **not** ACKed
(the §12.2 case); parametrized by durability mode (§14.8 rule 5).
**Gate:** standard + INV-06 in the DST set + human review. Activates INV-06, completes
INV-07 (lease step).

## Increment 7.3 — Promotion: PRIMARY_SUSPECTED → FENCING_WAIT → epoch CAS  ⚠️ review
**Objective:** CP promotion (§12.3): a vanished heartbeat → `PRIMARY_SUSPECTED`; a
failover decision → `FENCING_WAIT`; wait `last_renewal + lease_ttl + max_clock_skew`
before granting epoch N+1; increment epoch; CAS the `volumes/<vol>/epoch` object in S3
(§12.4, degrade to lease-only if the backend lacks CAS); grant the new lease. States
per §7 (`ACTIVE → PRIMARY_SUSPECTED → FENCING_WAIT → RECOVERY_REQUIRED → RECOVERING`).
**Tests first / DST:** promotion never grants N+1 before `lease_ttl + skew` elapses,
even with injected clock drift beyond the bound (the only effect is waiting longer,
never losing writes) — INV-11; a stale-epoch writer's publish fails epoch CAS + PG term
— INV-10.
**Gate:** standard + INV-10 + INV-11 + human review. Activates INV-10, INV-11.

## Increment 7.4 — Full fencing DST + admin idempotency  ⚠️ review
**Objective:** the end-to-end invariant (INV-09): partition writer W1 (S3 access intact)
→ W1 cannot ACK durable past its lease (INV-06) → promote W2 after `FENCING_WAIT` → W2's
recovered prefix ⊇ everything W1 ACKed durable. W1's late PUTs are inert (§12.5;
recovery-point object written in Phase 08). Admin operations idempotent by `request_id`
in the `operations` table (§18, INV-21 admin arm).
**Tests first / DST (the mandatory fencing set):** writer-partition-with-S3-intact;
clock-drift-beyond-skew during promotion; PG-down degrades `remote` durability in
~`lease_ttl` (SELF_FENCED) but not `local` (§23, §14.8); duplicate admin op → single
effect.
**Gate:** standard + the fencing DST set + human review. Activates INV-09, INV-21(admin).

## Phase 07 exit gate
- [ ] CP verified-term: zombie CP cannot mutate state (0-row writes, self-terminate).
- [ ] Durable-ACK rule: no FLUSH ACK with an expired lease (INV-06), mode-parametrized.
- [ ] Promotion waits `lease_ttl + skew`; drift only lengthens it (INV-11).
- [ ] Effective single writer: stale epoch fails CAS + term (INV-10).
- [ ] No ACKed-durable write lost across a partition+failover (INV-09) — the headline.
- [ ] Admin ops idempotent by request_id (INV-21 admin).
- [ ] `Log.Flush` step 5 (lease verify) wired; §14.8 `local` mode carve-out honored.
- [ ] Human review of the fencing protocol and every diff in this zone.
