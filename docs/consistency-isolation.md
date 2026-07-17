# Consistency and Isolation Contract (Alpha)

This document defines DistDB's current consistency and isolation guarantees for the Developer Alpha release.

## Purpose

This contract exists to:

- define what behavior is guaranteed now,
- distinguish bounded or best-effort behavior,
- identify what requires beta-grade proof,
- give test authors a concrete conformance target.

## Scope

This document applies to:

- SQL mutation/read execution through server query paths,
- WAL-backed persistence and replay behavior,
- replication convergence behavior within documented affinity scope.

This document does not claim broad MySQL parity beyond documented support in `sql-compliance.md`.

## Guarantee Levels

DistDB uses three guarantee levels in alpha:

1. Guaranteed (Implemented and expected stable in documented scope)
2. Bounded (Implemented with documented limits, still maturing)
3. Not Yet Guaranteed (Target behavior requiring beta-grade proof)

## Contract: Mutation Ordering and Visibility

### Guaranteed

- WAL records are append-ordered per stream.
- Grouped write visibility is commit-gated by `WriteBegin` / `WriteCommit` / `WriteAbort` semantics.
- Restart replay reconstructs durable state from WAL and catalog paths.
- Unsupported SQL shapes are expected to fail explicitly on unsupported paths.

### Bounded

- Cross-stream global ordering is not claimed; ordering is stream-local.
- Multi-object contention behavior depends on current lock/state and isolation checks.
- Replication convergence depends on phased sync and stream progress handling.

### Not Yet Guaranteed

- Full adversarial behavior proof across partition + concurrent-writer conditions.
- End-to-end deterministic outcomes for all failure permutations under load.

## Contract: Isolation and Conflict Behavior

### Guaranteed

- Transaction handling uses explicit begin/commit/rollback entry points.
- Commit-gated visibility prevents grouped uncommitted mutations from becoming visible.
- Conflict detection/isolation checks are present in current server control/query paths.

### Bounded

- Isolation behavior should be interpreted within currently documented and tested scenarios.
- Behavior outside documented scenarios is alpha-bounded and may evolve.

### Not Yet Guaranteed

- Publication of a frozen isolation/consistency contract with full adversarial test matrix evidence.

## Contract: Replication Consistency

### Guaranteed

- Replication is affinity-scoped rather than swarm-global.
- Sync ordering follows control -> schema -> snapshot -> WAL intent in documented model.
- Catch-up behavior is stream-aware in current design direction.

### Bounded

- Some replication phases are intentionally conservative in current alpha implementation depth.
- Operational confidence depends on current tested scenarios and checkpoint behavior.

### Not Yet Guaranteed

- Full beta-grade partition/failure validation with published expected/observed outcomes.

## Conformance Mapping (Required for Beta Readiness)

Current baseline mapping:

| Contract Item | Guarantee Level | Test IDs | Status | Last Validation | Owner |
| --- | --- | --- | --- | --- | --- |
| Commit-gated write visibility | Guaranteed | `serverlib/src/engine/execution/access_test.rs::load_live_rows_ignores_uncommitted_write_group`; `serverlib/src/engine/execution/access_test.rs::load_live_rows_applies_committed_write_group` | green | 2026-07-17 | serverlib |
| Session commit uses one write group across touched tables | Bounded | deterministic: `server/src/core/app/mod_test.rs::commit_shares_one_group_id_across_touched_tables`; adversarial variant: `server/src/core/app/mod_test.rs::rollback_discards_staged_queries_for_session` | green (deterministic + adversarial baseline) | 2026-07-17 | server |
| Snapshot isolation rejects write/write conflicts | Bounded | deterministic: `server/src/core/app/mod_test.rs::snapshot_isolation_rejects_concurrent_write_write_conflicts`; adversarial variant: `server/src/core/app/mod_test.rs::serializable_rejects_write_skew_across_disjoint_rows` | green (deterministic + adversarial baseline) | 2026-07-17 | server |
| Repeatable reads and own staged write visibility | Bounded | deterministic: `server/src/core/app/mod_test.rs::snapshot_isolation_keeps_repeatable_reads_within_transaction`; `server/src/core/app/mod_test.rs::snapshot_isolation_transactional_reads_see_own_staged_writes`; adversarial variant: `server/src/core/app/mod_test.rs::transaction_commit_rejects_duplicate_insert_during_validation` | green (deterministic + adversarial baseline) | 2026-07-17 | server |
| Failed commit validation leaves real WAL/index state clean | Guaranteed | `server/src/core/app/mod_test.rs::failed_commit_validation_leaves_real_wal_and_indexes_clean` | green | 2026-07-17 | server |
| Stream-aware catch-up does not skip unseen streams | Bounded | deterministic: `server/src/core/app/mod_test.rs::wal_export_with_stream_cursors_does_not_skip_unseen_streams`; `server/src/core/app/mod_test.rs::affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready`; adversarial variant: `server/src/core/app/mod_test.rs::affinity_schema_sync_failure_returns_database_to_ready` | green (deterministic + adversarial baseline) | 2026-07-17 | server |
| Restart replay restores latest schema/metadata/security state | Guaranteed | `server/src/core/app/mod_test.rs::bootstrap_replays_latest_schema_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_replays_sql_definition_and_metadata_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_replays_security_change_password_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_acl_replay_prefers_latest_wal_snapshot_for_user` | green | 2026-07-17 | server |
| Partition + adversarial concurrent-writer proof matrix | Not Yet Guaranteed | (no complete automated matrix yet) | missing | 2026-07-17 | server/serverlib |

## Change Control

Any change to this contract should include:

- corresponding test additions/updates,
- release-note mention of behavioral impact,
- update of `release.md` beta-exit progress where relevant.

## Validation Baseline (Automated)

The current targeted validation subset is executed via:

- `bash scripts/run_consistency_failure_validation.sh`

Optional extended resilience stage:

- `CONSISTENCY_RUN_EXTENDED_E2E=true bash scripts/run_consistency_failure_validation.sh`
- Runs `scripts/e2e/isolation_restart.sh` and `scripts/e2e/stress.sh` in addition to targeted test invariants.

Optional reconvergence stage (peer isolation + heal simulation):

- `CONSISTENCY_RUN_PARTITION_RECONVERGENCE=true bash scripts/run_consistency_failure_validation.sh`
- Runs `scripts/e2e/partition_reconvergence.sh` to validate affinity reconvergence invariants (schema sync rejection/recovery, stale WAL rejection, replication phase/checkpoint progression).

Optional dual-primary safety stage:

- `CONSISTENCY_RUN_SPLIT_BRAIN_DUAL_PRIMARY=true bash scripts/run_consistency_failure_validation.sh`
- Runs `scripts/e2e/split_brain_dual_primary.sh` to validate deterministic conflict rejection and recovery invariants used for split-brain safety modeling.

Optional unilateral-writer delayed-heal stage:

- `CONSISTENCY_RUN_UNILATERAL_WRITE_DELAYED_HEAL=true bash scripts/run_consistency_failure_validation.sh`
- Runs `scripts/e2e/unilateral_write_delayed_heal.sh` to validate stream-aware catch-up and delayed-heal convergence invariants.

Optional repeated partition/heal cycles stage:

- `CONSISTENCY_RUN_REPEATED_PARTITION_HEAL_CYCLES=true bash scripts/run_consistency_failure_validation.sh`
- Runs `scripts/e2e/repeated_partition_heal_cycles.sh` for repeated-cycle convergence and conflict-safety invariants (`SPLIT_BRAIN_CYCLES` defaults to `3`).

CI workflow:

- `.github/workflows/consistency-failure-validation.yml`

This subset is intentionally focused on high-signal consistency/failure invariants and now includes deterministic plus adversarial variants for currently bounded contract items.

Current caveat: full bidirectional split-brain partition automation remains a planned beta-evidence item; the current reconvergence suite validates deterministic affinity recovery invariants.

## Beta-Grade Pass Criteria

To graduate this contract from alpha-bounded to beta-grade confidence:

1. all contract items classified `Guaranteed` must map to at least one deterministic automated test,
2. all contract items classified `Bounded` must map to deterministic tests plus at least one adversarial variant,
3. no `Implemented/Tested` scenario in `node-failure-matrix.md` may lack concrete evidence references,
4. the consistency/failure validation workflow must pass on every PR that changes server/serverlib behavior,
5. partition and concurrent-writer matrix rows currently marked `Planned` must be upgraded with executable evidence before beta declaration.
