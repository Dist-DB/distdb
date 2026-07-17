# Node Failure and Recovery Matrix (Alpha)

This document tracks expected behavior for node and network failure scenarios in DistDB Developer Alpha.

## Purpose

The matrix provides a single operational reference for:

- expected safety and availability outcomes,
- current implementation status,
- evidence level and test coverage,
- beta-readiness closure tracking.

## Status Legend

- `Implemented/Tested`: behavior implemented and validated by automated tests
- `Implemented/Partial`: behavior implemented but test depth is incomplete
- `Planned`: desired behavior not yet fully implemented

## Matrix

| Scenario | Expected Safety Outcome | Expected Availability Outcome | Recovery Expectation | Current Status | Evidence |
| --- | --- | --- | --- | --- | --- |
| Single node process crash | Durable committed WAL-backed state remains recoverable | Node unavailable until restart | Replay restores state on startup | Implemented/Partial | `server/src/core/app/mod_test.rs::bootstrap_replays_latest_schema_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_replays_sql_definition_and_metadata_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_replays_security_change_password_from_wal`; `server/src/core/app/mod_test.rs::show_indexes_reports_user_defined_index_after_restart` |
| Crash during grouped write before commit | Uncommitted grouped mutations remain invisible | Request may fail; retry needed | Abort/non-commit path prevents visibility | Implemented/Partial | `serverlib/src/engine/execution/access_test.rs::load_live_rows_ignores_uncommitted_write_group`; `server/src/core/app/mod_test.rs::failed_commit_validation_leaves_real_wal_and_indexes_clean` |
| Crash after commit append | Committed mutation should remain visible after replay | Temporary unavailability | Replay restores committed result | Implemented/Partial | `serverlib/src/engine/execution/access_test.rs::load_live_rows_applies_committed_write_group`; `server/src/core/app/mod_test.rs::commit_shares_one_group_id_across_touched_tables` |
| WAL import interrupted | No partial-corruption acceptance as committed state | Sync delayed | Next sync/replay resumes from valid boundary | Implemented/Partial | `server/src/core/app/mod_test.rs::lightweight_import_commit_failure_clears_transaction_state_for_followup_reads`; `server/src/engine/wal_probe_test.rs::wal_accepts_out_of_order_append_and_keeps_sorted_order` |
| Peer temporarily unavailable | No unauthorized data-plane fallback outside affinity model | Reduced replication freshness | Retry/checkpoint paths resume convergence | Implemented/Partial | `serverlib/src/engine/affinity/storage_test.rs::processor_checkpoint_integration`; `serverlib/src/engine/replication_executor_test.rs::executor_tracks_phase_progression`; `server/src/core/app/mod_test.rs::affinity_schema_sync_failure_returns_database_to_ready`; `scripts/e2e/partition_reconvergence.sh` |
| Network partition (affinity peers split) | No data safety claim beyond documented bounded model without full partition proof | Divergence risk until healing | Requires explicit partition reconvergence validation | Implemented/Tested | `scripts/e2e/partition_reconvergence.sh`; `scripts/e2e/unilateral_write_delayed_heal.sh`; `scripts/e2e/repeated_partition_heal_cycles.sh`; `scripts/e2e/split_brain_evidence_bundle.sh` (deterministic reconvergence + delayed-heal + repeated-cycle invariants + expected-vs-observed bundle); see `docs/partition-split-brain-matrix.md` for SB-001..SB-004 closure tracking |
| Concurrent writers same logical data path | Conflict/isolation controls should prevent invalid visibility outcomes in documented scope | Writes may serialize/reject based on checks | Deterministic result per defined isolation contract | Implemented/Partial | `scripts/e2e/split_brain_dual_primary.sh`; `server/src/core/app/mod_test.rs::snapshot_isolation_rejects_concurrent_write_write_conflicts`; `server/src/core/app/mod_test.rs::snapshot_isolation_keeps_repeatable_reads_within_transaction`; `server/src/core/app/mod_test.rs::snapshot_isolation_transactional_reads_see_own_staged_writes` |
| Concurrent writers across relations/streams | Stream-local durability preserved | Outcomes bounded by current lock/isolation behavior | Requires explicit multi-stream contention validation | Implemented/Partial | `server/src/core/app/mod_test.rs::commit_shares_one_group_id_across_touched_tables` (group-shared commit path validated); full adversarial multi-stream writer matrix still required |
| Rolling restart in multi-node setup | Adjacent-version rolling restart and rollback path preserves bounded service continuity in drill scope | Service remains available from peer that is not under restart | Restarted node returns to ready and resumes local state checks across N->N+1->N transitions | Implemented/Partial | `scripts/e2e/rolling_restart_upgrade_safety.sh`; `.github/workflows/operability-upgrade-safety.yml`; `.github/workflows/nightly-evidence.yml`; `docs/operability-upgrade-safety.md` |

## Required Beta Evidence Additions

1. Complete `docs/partition-split-brain-matrix.md` to `Implemented/Tested` for all `SB-*` scenarios with expected vs observed outcomes.
2. Concurrent-writer stress suite across contention classes.
3. Recovery timing and convergence metrics publication.
4. Cross-version rolling upgrade matrix expansion with explicit backward/forward compatibility window assertions.

## Operator Guidance (Alpha)

- Treat this matrix as the source of truth for what has and has not been proven.
- Do not infer production-grade guarantees from untested scenarios.
- Prefer documented supported paths and validated topologies for alpha deployments.

## Ownership and Update Cadence

- Primary owners: `server` and `serverlib` maintainers.
- Update cadence: at every milestone that changes recovery, isolation, or replication behavior.
- Any status movement from `Planned` to `Implemented/*` requires linked test evidence.
