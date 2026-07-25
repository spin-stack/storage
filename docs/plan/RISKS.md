# RISKS

Open risks with an owner and a review trigger. Seeded from the doc's residual
weaknesses (§29) plus planning/execution risks introduced by this plan. Each risk is
re-reviewed when its trigger fires; closing a risk requires a note here.

## Format

```
### RISK-NN — <title>
- Source: §X / plan
- Owner: <role>
- Trigger: <event that forces a re-review>
- Mitigation: <planned mitigation and where it lives>
- Status: open | mitigated | accepted
```

## Design-residual risks (from §29)

### RISK-01 — FLUSH/FUA latency coupled to the object store
- Source: §29.1 (the most important residual)
- Owner: Implementer (Track B) + Adversary
- Trigger: `wal_put_latency` p99 > 500 ms in any DST/HW run; fsync-heavy workload test.
- Mitigation: parallel PUT pipeline on FLUSH (§14.4); same-region backend; `local`
  durability mode (§14.8); WAL-object compaction (§21.2); p99 measured from day 1 with
  injected latency. Documented write-back contract to the user (§2).
- Status: open (inherent to the no-host-to-host-replication model).

### RISK-02 — Durability availability coupled to PostgreSQL via leases
- Source: §29.2 (introduced by the fencing fix)
- Owner: Control Plane Implementer (Track C)
- Trigger: PG outage drill; any `self_fenced_total` spike tied to PG unavailability.
- Mitigation: accepted trade-off for correct fencing without self-hosted consensus; in
  v5.1 affects only `remote` volumes — `local` volumes (§14.8) do not depend on the
  lease for FLUSH ACK. PG standby + PITR; degraded mode and its timings in the runbook.
  Future option: S3-CAS-object lease renewal as a secondary path (§14, post-MVP §30.14).
- Status: accepted (documented degraded mode).

### RISK-03 — Promotion depends on a clock-drift bound
- Source: §29.3
- Owner: Control Plane Implementer + Harness agent
- Trigger: `clock_offset_seconds > 0.5` alert; DST drift-injection scenario.
- Mitigation: old-writer safety uses **monotonic** clock only (independent of the bound);
  chrony monitored, alert at 500 ms; hosts over `max_clock_skew` ineligible for
  promotion; DST injects drift beyond the bound and asserts the only effect is waiting
  longer, never losing writes (INV-11).
- Status: open (mitigation lands with Phase 07).

### RISK-04 — Cold cross-host materialization RTO
- Source: §29.4
- Owner: Implementer (Track A/Phase 11) + operators
- Trigger: cross-host clone test; drain drill.
- Mitigation: warm standby for volumes that matter (RTO in minutes, Phase 12); cold RTO
  published per-GiB in the runbook; lazy loading designed and format-compatible
  (post-MVP §22.4).
- Status: open.

### RISK-05 — Dependence on S3-compatible semantics (CAS, versioning, Object Lock)
- Source: §29.5
- Owner: Harness agent (conformance suite) + operators
- Trigger: enabling/upgrading any backend (MinIO/RustFS/S3) version.
- Mitigation: blocking backend conformance suite per version (§6.1); specified graceful
  degradation to lease-only if CAS is absent (§12.4); minimum on-prem durability
  requirement removes the single-node case (§6.1).
- Status: open (suite lands Phase 13; sim exercises the surface from Phase 01).

### RISK-06 — PostgreSQL as a single point of control
- Source: §29.6
- Owner: Control Plane Implementer
- Trigger: PG total-loss drill; `rebuild-metadata` rehearsal.
- Mitigation: VMs keep serving non-durable I/O; reconciliation makes catch-up trivial;
  PITR + `rebuild-metadata` cover blip-to-total-loss; verified CP term removes
  split-brain (INV-10, §7).
- Status: open (rebuild-metadata basic in Phase 08).

### RISK-07 — Added complexity (implementation surface) of v5
- Source: §29.7 (honest self-assessment)
- Owner: Planner + Adversary
- Trigger: any increment growing beyond its 3–10-day size; DST coverage gaps.
- Mitigation: the roadmap sequences pieces so each phase is DST-verifiable before the
  next; day-1 pieces are mostly interfaces + reserved fields (cheap now, impossible
  later); compaction/standby/flatten are flag-gated and optional for first deploy.
- Status: open (managed by the phase structure itself).

### RISK-08 — Objectization / truncation ordering
- Source: §29.8, §21.1, INV-13
- Owner: Implementer (Phase 10) + Harness agent
- Trigger: any WAL-truncation code; checkpoint publication path.
- Mitigation: strict ordering + DST/fault injection at each step + never truncate above
  a verified `published_sequence` + GC-cannot-permanently-delete as the final net
  (INV-13, INV-14).
- Status: open (lands Phase 10, human-review zone).

## Planning / execution risks (from this plan)

### RISK-09 — Hot-zone contention under parallel tracks
- Source: ADR-0002, PLAN §3 stop signals
- Owner: Planner
- Trigger: two in-flight increments scheduled against the same hot zone.
- Mitigation: Planner-owned hot-zone list, single-writer serialization; conflict is a
  stop signal that halts the increment.
- Status: open (managed continuously).

### RISK-10 — QEMU 11.0.2 inflight-shmfd behavior unverified
- Source: §16, §27 note, roadmap §30.3
- Owner: Implementer (Phase 03) + Harness agent
- Trigger: Phase 03 start (vhost-user reconnection + inflight tracking).
- Mitigation: pin QEMU 11.0.2 identically in CI and prod; verify
  `VHOST_USER_PROTOCOL_F_INFLIGHT_SHMFD` behavior against QEMU docs and by execution
  before relying on it; SPDK vhost as the reference model. Marked **Unverified:** until
  Phase 03 executes it.
- Status: open.
