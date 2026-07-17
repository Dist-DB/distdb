#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_DIR="$ROOT_DIR/server"
SERVERLIB_DIR="$ROOT_DIR/serverlib"
PEERLIB_DIR="$ROOT_DIR/peerlib"
FINDINGS_LOG="$ROOT_DIR/docs/security-findings-log.md"

echo "[security-baseline] running adversarial security baseline"

echo "[security-baseline] server: ACL enforcement and privilege boundaries"
(
  cd "$SERVER_DIR"
  cargo test -q query_requires_select_privilege_for_non_root_user
  cargo test -q query_object_acl_allows_only_granted_objects
  cargo test -q query_join_requires_privileges_for_all_referenced_objects
  cargo test -q grant_and_revoke_queries_update_object_acl_access
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

echo "[security-baseline][ok] adversarial security baseline passed"
