# Partition and Split-Brain Scenario Matrix (Beta Gate)

This matrix defines executable partition and split-brain scenarios required for beta-grade confidence.

## Purpose

Use this page to track expected safety/convergence outcomes and concrete evidence for partition-related behavior.

`Implemented/Partial` means the scenario has at least one executable evidence path but does not yet meet full beta proof depth.

## Status Legend

- `Implemented/Tested`: executable, repeatable, and validated at required depth.
- `Implemented/Partial`: executable evidence exists but scope/depth is incomplete.
- `Planned`: scenario expectations are defined but not yet fully executable.

## Scenario Matrix

| Scenario ID | Scenario | Expected Safety Outcome | Expected Convergence Outcome | Status | Evidence |
| --- | --- | --- | --- | --- | --- |
| SB-001 | Temporary peer isolation then heal | No invalid schema/WAL state accepted during disruption | Database returns to ready and replication phases/checkpoints progress after recovery path | Implemented/Tested | `scripts/e2e/partition_reconvergence.sh`; `server/src/core/app/mod_test.rs::affinity_schema_sync_failure_returns_database_to_ready`; `server/src/core/app/mod_test.rs::affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready`; `serverlib/src/engine/replication_executor_test.rs::executor_tracks_phase_progression`; `serverlib/src/engine/affinity/storage_test.rs::processor_checkpoint_integration` |
| SB-002 | Dual-primary write attempt during partition | No unsupported data safety claims beyond documented bounded model; conflicting outcomes must be deterministic and explicit | Recovery converges according to declared policy and conflict handling | Implemented/Tested | `scripts/e2e/split_brain_dual_primary.sh`; `server/src/core/app/mod_test.rs::snapshot_isolation_rejects_concurrent_write_write_conflicts`; `server/src/core/app/mod_test.rs::serializable_rejects_write_skew_across_disjoint_rows`; `server/src/core/app/mod_test.rs::rollback_discards_staged_queries_for_session`; `server/src/core/app/mod_test.rs::failed_commit_validation_leaves_real_wal_and_indexes_clean` |
| SB-003 | Unilateral write partition then delayed heal | Isolated writer safety behavior is explicit and documented | Post-heal convergence is deterministic and measurable | Implemented/Tested | `scripts/e2e/unilateral_write_delayed_heal.sh`; `server/src/core/app/mod_test.rs::wal_export_with_stream_cursors_does_not_skip_unseen_streams`; `server/src/core/app/mod_test.rs::affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready`; `server/src/core/app/mod_test.rs::affinity_schema_sync_failure_returns_database_to_ready`; `serverlib/src/engine/affinity/storage_test.rs::processor_checkpoint_integration` |
| SB-004 | Repeated partition/heal cycles under load | No silent corruption across repeated disruptions | Convergence and readiness remain bounded and repeatable | Implemented/Tested | `scripts/e2e/repeated_partition_heal_cycles.sh`; `server/src/core/app/mod_test.rs::wal_export_with_stream_cursors_does_not_skip_unseen_streams`; `server/src/core/app/mod_test.rs::affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready`; `server/src/core/app/mod_test.rs::snapshot_isolation_rejects_concurrent_write_write_conflicts`; `serverlib/src/engine/affinity/storage_test.rs::processor_checkpoint_integration` |

## Exit Criteria for This Matrix

To close this beta gate:

1. all `SB-*` rows must be `Implemented/Tested`,
2. each row must map to executable automation and concrete test references,
3. expected vs observed outcomes must be published for each row,
4. coverage must include both deterministic and adversarial variants where applicable.

## Observation Table (Expected vs Observed)

Use this table to record concrete run outcomes for each scenario and keep status changes evidence-based.

Preferred evidence capture command (runs SB-001..SB-004 and emits a timestamped observation report under `artifacts/e2e/split-brain-evidence-*` by default):

Legacy report discovery still checks `server/data/e2e/split-brain-evidence-*` for backward compatibility.
Override paths with `DISTDB_ARTIFACTS_ROOT` or `SPLIT_BRAIN_DATA_ROOT` when needed.

- `CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh`

Optional auto-append mode (appends rows from the generated bundle report into this table):

- `CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true CONSISTENCY_AUTO_APPEND_SPLIT_BRAIN_OBSERVATIONS=true bash scripts/run_consistency_failure_validation.sh`
- Auto-append replaces same-day bundle rows for each `SB-*` scenario (idempotent refresh instead of duplicate accumulation).

| Date | Scenario ID | Command | Expected Outcome | Observed Outcome | Result | Evidence Notes |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-07-17 | SB-001 | `CONSISTENCY_RUN_PARTITION_RECONVERGENCE=true bash scripts/run_consistency_failure_validation.sh` | Schema/WAL mismatch paths reject invalid state and return database to ready with replication progress preserved | Stage executed and passed; schema sync failure and stale WAL import tests returned ready-state outcomes; replication phase/checkpoint tests passed | Pass | `scripts/e2e/partition_reconvergence.sh` output included non-zero tests and suite passed |
| 2026-07-17 | SB-002 | `CONSISTENCY_RUN_SPLIT_BRAIN_DUAL_PRIMARY=true bash scripts/run_consistency_failure_validation.sh` | Dual-primary conflict paths are deterministic (reject/serialize) and do not leak partial durability | Stage executed and passed; conflict rejection, write-skew rejection, rollback cleanup, and failed-commit durability tests all passed | Pass | `scripts/e2e/split_brain_dual_primary.sh` output included non-zero tests and suite passed |
| 2026-07-17 | SB-003 | `CONSISTENCY_RUN_UNILATERAL_WRITE_DELAYED_HEAL=true bash scripts/run_consistency_failure_validation.sh` | Unilateral writer delayed-heal paths preserve stream-aware catch-up and return to ready-state behavior | Stage executed and passed; stream cursor catch-up, stale WAL protection, schema sync recovery, and checkpoint progression tests passed | Pass | `scripts/e2e/unilateral_write_delayed_heal.sh` output included non-zero tests and suite passed |
| 2026-07-17 | SB-004 | `CONSISTENCY_RUN_REPEATED_PARTITION_HEAL_CYCLES=true bash scripts/run_consistency_failure_validation.sh` | Repeated disruption/heal cycles remain bounded and repeatable without silent corruption | Stage executed and passed for 3 cycles (`SPLIT_BRAIN_CYCLES=3` default); each cycle completed target conflict/catch-up/checkpoint invariants | Pass | `scripts/e2e/repeated_partition_heal_cycles.sh` output included cycle-by-cycle non-zero tests and suite passed |



| 2026-07-17 | SB-001 | CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh | Reject invalid schema/WAL state and recover to ready with convergence progression | Stage completed successfully with non-zero test execution and suite pass | Pass | bundle report: server/data/e2e/split-brain-evidence-20260717-115910-74679/observation-report.md; scenario log: /Users/samcolak/Source Code/rust/distdb/distdb/server/data/e2e/split-brain-evidence-20260717-115910-74679/SB-001.log; script: partition_reconvergence.sh |
| 2026-07-17 | SB-002 | CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh | Deterministic conflict behavior and no partial durability leakage | Stage completed successfully with non-zero test execution and suite pass | Pass | bundle report: server/data/e2e/split-brain-evidence-20260717-115910-74679/observation-report.md; scenario log: /Users/samcolak/Source Code/rust/distdb/distdb/server/data/e2e/split-brain-evidence-20260717-115910-74679/SB-002.log; script: split_brain_dual_primary.sh |
| 2026-07-17 | SB-003 | CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh | Stream-aware catch-up and deterministic delayed-heal recovery | Stage completed successfully with non-zero test execution and suite pass | Pass | bundle report: server/data/e2e/split-brain-evidence-20260717-115910-74679/observation-report.md; scenario log: /Users/samcolak/Source Code/rust/distdb/distdb/server/data/e2e/split-brain-evidence-20260717-115910-74679/SB-003.log; script: unilateral_write_delayed_heal.sh |
| 2026-07-17 | SB-004 | CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh | Stable repeated-cycle convergence and conflict safety | Stage completed successfully with non-zero test execution and suite pass | Pass | bundle report: server/data/e2e/split-brain-evidence-20260717-115910-74679/observation-report.md; scenario log: /Users/samcolak/Source Code/rust/distdb/distdb/server/data/e2e/split-brain-evidence-20260717-115910-74679/SB-004.log; script: repeated_partition_heal_cycles.sh |

### Promotion Rule

An `SB-*` row may move from `Implemented/Partial` to `Implemented/Tested` only when:

1. at least two independent dated observation entries exist,
2. entries include expected-vs-observed text (not pass/fail only),
3. no unresolved anomalies are recorded for that scenario in the latest observation.

## Operational Notes

- This matrix complements `docs/node-failure-matrix.md` and should remain consistent with it.
- Scenario status changes must include linked evidence updates in the same change set.
