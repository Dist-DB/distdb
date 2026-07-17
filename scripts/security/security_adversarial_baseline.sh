#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_DIR="$ROOT_DIR/server"
SERVERLIB_DIR="$ROOT_DIR/serverlib"
PEERLIB_DIR="$ROOT_DIR/peerlib"
FINDINGS_LOG="$ROOT_DIR/docs/security-findings-log.md"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
OUT_DIR="$ARTIFACTS_ROOT/security/security-baseline-$RUN_ID"
LOG_FILE="$OUT_DIR/run.log"
MANIFEST_FILE="$OUT_DIR/manifest.json"
RUN_STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"

mkdir -p "$OUT_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

write_manifest() {
  local exit_code="$1"
  local status="fail"
  if [[ "$exit_code" -eq 0 ]]; then
    status="pass"
  fi

  cat >"$MANIFEST_FILE" <<JSON
{
  "run_id": "$RUN_ID",
  "kind": "security_adversarial_baseline",
  "status": "$status",
  "exit_code": $exit_code,
  "started_at_utc": "$RUN_STARTED_UTC",
  "finished_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$GIT_SHA",
  "artifacts_dir": "$OUT_DIR",
  "log_file": "$LOG_FILE",
  "findings_log": "$FINDINGS_LOG"
}
JSON
}

on_exit() {
  local exit_code="$?"
  write_manifest "$exit_code"
}

trap on_exit EXIT

echo "[security-baseline] running adversarial security baseline"
echo "[security-baseline] artifacts_dir=$OUT_DIR"

echo "[security-baseline] server: ACL enforcement and privilege boundaries"
(
  cd "$SERVER_DIR"
  cargo test -q query_requires_select_privilege_for_non_root_user
  cargo test -q query_object_acl_allows_only_granted_objects
  cargo test -q query_join_requires_privileges_for_all_referenced_objects
  cargo test -q grant_and_revoke_queries_update_object_acl_access
  cargo test -q schema_grant_preserves_access_after_object_revoke_until_schema_revoke
  cargo test -q object_grant_survives_schema_revoke_and_remains_object_scoped
  cargo test -q qualified_object_grant_uses_explicit_database_not_query_hint
  cargo test -q schema_grant_uses_explicit_schema_not_query_hint_database
  cargo test -q qualified_object_revoke_only_affects_targeted_database_object
  cargo test -q schema_revoke_only_affects_targeted_database_not_other_schema_grants
  cargo test -q mixed_cross_database_revoke_order_keeps_scope_isolated_per_step
  cargo test -q malformed_qualified_acl_target_is_rejected
  cargo test -q schema_grant_with_grant_option_tracks_grant_acl_and_revokes_cleanly
  cargo test -q acl_and_non_acl_batch_is_rejected_without_applying_acl_side_effect
  cargo test -q non_root_acl_batch_is_rejected_without_acl_side_effects
  cargo test -q non_root_delegated_grant_attempt_is_rejected_even_after_access_grant
  cargo test -q root_acl_only_batch_applies_multiple_acl_mutations
  cargo test -q non_root_cross_database_acl_batch_is_rejected_without_side_effects
  cargo test -q non_root_cross_database_delegated_grant_attempt_is_rejected_without_side_effects
  cargo test -q root_acl_batch_with_malformed_statement_has_no_partial_side_effects
  cargo test -q cross_database_grant_option_is_scoped_to_target_database_only
  cargo test -q root_multi_target_acl_batch_with_late_malformed_statement_has_no_partial_side_effects
  cargo test -q cross_database_revoke_cleans_grant_option_only_for_target_database
  cargo test -q acl_batch_recovery_after_malformed_request_applies_next_valid_batch_deterministically
  cargo test -q cross_database_grant_option_transition_chain_remains_scope_isolated
  cargo test -q repeated_malformed_acl_batches_at_different_positions_preserve_acl_state_until_valid_batch
  cargo test -q cross_database_object_acl_transition_chain_remains_target_isolated
  cargo test -q alternating_malformed_grant_revoke_batches_preserve_state_until_valid_reconciliation
  cargo test -q cross_database_schema_grant_with_object_revoke_chain_preserves_scope_precedence
  cargo test -q mixed_schema_object_malformed_batch_rejects_without_partial_side_effects
  cargo test -q cross_database_mixed_schema_object_grant_option_chain_scopes_grant_acl_and_access
  cargo test -q mixed_schema_object_grant_option_malformed_batch_has_no_side_effects_then_recovers
  cargo test -q affinity_join_request_
)

echo "[security-baseline] transport: malformed payload rejection hardening"
(
  cd "$SERVER_DIR"
  cargo test -q decode_service_message_rejects_
)
(
  cd "$SERVERLIB_DIR/../peerlib"
  cargo test -q malformed
)

echo "[security-baseline] fault-injection: malformed transport frame decode failures"
(
  cd "$PEERLIB_DIR"
  cargo test -q connect_active_peer_rejects_malformed_challenge_payload
  cargo test -q request_drops_live_connection_after_malformed_response_payload
  cargo test -q fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response
  cargo test -q fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response
)
(
  cd "$SERVER_DIR"
  cargo test -q apply_bootstrap_password_wal_payload_rejects_non_utf8_payload
  cargo test -q apply_bootstrap_password_wal_payload_rejects_malformed_segments
  cargo test -q apply_bootstrap_password_wal_payload_rejects_invalid_encrypted_password_state
  cargo test -q authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial
  cargo test -q authorization_interleaving_high_contention_multi_session_revokes_stay_effective
  cargo test -q authorization_parallel_reader_writer_contention_preserves_revoke_effectiveness
)

echo "[security-baseline] server: credential and security snapshot durability"
(
  cd "$SERVER_DIR"
  cargo test -q create_user_requires_identified_by_password_clause
  cargo test -q create_user_duplicate_requires_if_not_exists
  cargo test -q create_user_creates_acl_entry_and_wal_snapshot
  cargo test -q bootstrap_replays_security_change_password_from_wal
  cargo test -q bootstrap_acl_replay_prefers_latest_wal_snapshot_for_user
)

echo "[security-baseline] serverlib: privilege model and nonce behavior"
(
  cd "$SERVERLIB_DIR"
  cargo test -q privilege_selector_star_maps_to_all_mysql8_privileges
  cargo test -q usage_token_is_treated_as_no_privileges
  cargo test -q revoking_privilege_also_revokes_grant_option
  cargo test -q revoke_object_privilege_removes_object_access
  cargo test -q password_nonce_does_not_depend_on_username
  cargo test -q password_nonce_changes_with_database_or_seed
)

if [[ ! -f "$FINDINGS_LOG" ]]; then
  echo "[security-baseline][fail] findings log missing: $FINDINGS_LOG"
  exit 1
fi

echo "[security-baseline] log=$LOG_FILE"
echo "[security-baseline] manifest=$MANIFEST_FILE"
echo "[security-baseline][ok] adversarial security baseline passed"
