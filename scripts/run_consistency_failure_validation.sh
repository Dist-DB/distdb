#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_DIR="$ROOT_DIR/server"
SERVERLIB_DIR="$ROOT_DIR/serverlib"
E2E_DIR="$ROOT_DIR/scripts/e2e"
RUN_EXTENDED_E2E="${CONSISTENCY_RUN_EXTENDED_E2E:-false}"
RUN_PARTITION_RECONVERGENCE="${CONSISTENCY_RUN_PARTITION_RECONVERGENCE:-false}"
RUN_SPLIT_BRAIN_DUAL_PRIMARY="${CONSISTENCY_RUN_SPLIT_BRAIN_DUAL_PRIMARY:-false}"
RUN_UNILATERAL_WRITE_DELAYED_HEAL="${CONSISTENCY_RUN_UNILATERAL_WRITE_DELAYED_HEAL:-false}"
RUN_REPEATED_PARTITION_HEAL_CYCLES="${CONSISTENCY_RUN_REPEATED_PARTITION_HEAL_CYCLES:-false}"
RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE="${CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE:-false}"

run_serverlib_tests() {
  pushd "$SERVERLIB_DIR" >/dev/null
  echo "[consistency-validation] serverlib: committed visibility boundaries"
  cargo test -q load_live_rows_ignores_uncommitted_write_group
  cargo test -q load_live_rows_applies_committed_write_group
  popd >/dev/null
}

run_server_tests() {
  pushd "$SERVER_DIR" >/dev/null

  echo "[consistency-validation] server: isolation and concurrency invariants"
  cargo test -q snapshot_isolation_rejects_concurrent_write_write_conflicts
  cargo test -q snapshot_isolation_keeps_repeatable_reads_within_transaction
  cargo test -q snapshot_isolation_transactional_reads_see_own_staged_writes
  cargo test -q serializable_rejects_write_skew_across_disjoint_rows

  echo "[consistency-validation] server: write-group and failed-commit durability invariants"
  cargo test -q commit_shares_one_group_id_across_touched_tables
  cargo test -q failed_commit_validation_leaves_real_wal_and_indexes_clean
  cargo test -q rollback_discards_staged_queries_for_session

  echo "[consistency-validation] server: replay and stream-cursor convergence invariants"
  cargo test -q bootstrap_replays_latest_schema_from_wal
  cargo test -q bootstrap_replays_sql_definition_and_metadata_from_wal
  cargo test -q wal_export_with_stream_cursors_does_not_skip_unseen_streams
  cargo test -q affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready
  cargo test -q affinity_schema_sync_failure_returns_database_to_ready

  echo "[consistency-validation] server: wal replay probe invariants"
  cargo test -q wal_replay_without_lower_bound_returns_all_records
  cargo test -q wal_replay_with_lower_bound_is_exclusive
  cargo test -q wal_accepts_out_of_order_append_and_keeps_sorted_order

  popd >/dev/null
}

run_extended_e2e() {
  if [[ "$RUN_EXTENDED_E2E" != "1" && "$RUN_EXTENDED_E2E" != "true" && "$RUN_EXTENDED_E2E" != "yes" && "$RUN_EXTENDED_E2E" != "on" ]]; then
    echo "[consistency-validation] extended e2e stage skipped (set CONSISTENCY_RUN_EXTENDED_E2E=true to enable)"
    return 0
  fi

  echo "[consistency-validation] extended e2e: build server + console binaries"
  (cd "$ROOT_DIR/server" && cargo build --quiet)
  (cd "$ROOT_DIR/console" && cargo build --quiet)

  echo "[consistency-validation] extended e2e: isolation restart suite"
  "$E2E_DIR/isolation_restart.sh"

  echo "[consistency-validation] extended e2e: stress suite"
  "$E2E_DIR/stress.sh"
}

run_partition_reconvergence() {
  if [[ "$RUN_PARTITION_RECONVERGENCE" != "1" && "$RUN_PARTITION_RECONVERGENCE" != "true" && "$RUN_PARTITION_RECONVERGENCE" != "yes" && "$RUN_PARTITION_RECONVERGENCE" != "on" ]]; then
    echo "[consistency-validation] partition reconvergence stage skipped (set CONSISTENCY_RUN_PARTITION_RECONVERGENCE=true to enable)"
    return 0
  fi

  echo "[consistency-validation] partition reconvergence e2e suite"
  bash "$E2E_DIR/partition_reconvergence.sh"
}

run_split_brain_dual_primary() {
  if [[ "$RUN_SPLIT_BRAIN_DUAL_PRIMARY" != "1" && "$RUN_SPLIT_BRAIN_DUAL_PRIMARY" != "true" && "$RUN_SPLIT_BRAIN_DUAL_PRIMARY" != "yes" && "$RUN_SPLIT_BRAIN_DUAL_PRIMARY" != "on" ]]; then
    echo "[consistency-validation] split-brain dual-primary stage skipped (set CONSISTENCY_RUN_SPLIT_BRAIN_DUAL_PRIMARY=true to enable)"
    return 0
  fi

  echo "[consistency-validation] split-brain dual-primary e2e suite"
  bash "$E2E_DIR/split_brain_dual_primary.sh"
}

run_unilateral_write_delayed_heal() {
  if [[ "$RUN_UNILATERAL_WRITE_DELAYED_HEAL" != "1" && "$RUN_UNILATERAL_WRITE_DELAYED_HEAL" != "true" && "$RUN_UNILATERAL_WRITE_DELAYED_HEAL" != "yes" && "$RUN_UNILATERAL_WRITE_DELAYED_HEAL" != "on" ]]; then
    echo "[consistency-validation] unilateral-writer delayed-heal stage skipped (set CONSISTENCY_RUN_UNILATERAL_WRITE_DELAYED_HEAL=true to enable)"
    return 0
  fi

  echo "[consistency-validation] unilateral-writer delayed-heal e2e suite"
  bash "$E2E_DIR/unilateral_write_delayed_heal.sh"
}

run_repeated_partition_heal_cycles() {
  if [[ "$RUN_REPEATED_PARTITION_HEAL_CYCLES" != "1" && "$RUN_REPEATED_PARTITION_HEAL_CYCLES" != "true" && "$RUN_REPEATED_PARTITION_HEAL_CYCLES" != "yes" && "$RUN_REPEATED_PARTITION_HEAL_CYCLES" != "on" ]]; then
    echo "[consistency-validation] repeated partition/heal cycles stage skipped (set CONSISTENCY_RUN_REPEATED_PARTITION_HEAL_CYCLES=true to enable)"
    return 0
  fi

  echo "[consistency-validation] repeated partition/heal cycles e2e suite"
  bash "$E2E_DIR/repeated_partition_heal_cycles.sh"
}

run_split_brain_evidence_bundle() {
  if [[ "$RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE" != "1" && "$RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE" != "true" && "$RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE" != "yes" && "$RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE" != "on" ]]; then
    echo "[consistency-validation] split-brain evidence bundle stage skipped (set CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true to enable)"
    return 0
  fi

  echo "[consistency-validation] split-brain evidence bundle"
  bash "$E2E_DIR/split_brain_evidence_bundle.sh"
}

run_serverlib_tests
run_server_tests
run_extended_e2e
run_partition_reconvergence
run_split_brain_dual_primary
run_unilateral_write_delayed_heal
run_repeated_partition_heal_cycles
run_split_brain_evidence_bundle

echo "[consistency-validation][ok] targeted consistency/failure validation suite passed"
