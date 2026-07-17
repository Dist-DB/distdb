# Security Adversarial Matrix (Beta Gate)

This matrix defines the current adversarial security evidence baseline and remaining gaps for beta confidence.

## Purpose

Track security-focused abuse cases, expected outcomes, evidence coverage, and status.

## Status Legend

- `Implemented/Tested`: executable and validated at required depth.
- `Implemented/Partial`: executable evidence exists, but depth/scope is incomplete.
- `Planned`: scenario defined but not yet covered by executable evidence.

## Matrix

| Scenario ID | Scenario | Expected Security Outcome | Status | Evidence |
| --- | --- | --- | --- | --- |
| SEC-001 | Non-root read without `SELECT` privilege | Request is rejected with no data disclosure | Implemented/Tested | `server/src/core/app/mod_test.rs::query_requires_select_privilege_for_non_root_user`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-002 | Object-level ACL boundary violations | Access only granted for explicitly authorized objects | Implemented/Tested | `server/src/core/app/mod_test.rs::query_object_acl_allows_only_granted_objects`; `server/src/core/app/mod_test.rs::query_join_requires_privileges_for_all_referenced_objects`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-003 | Privilege escalation via grant/revoke misuse | ACL changes are explicit and reversible through authorization paths, including mixed schema/object precedence, explicit cross-database target resolution, targeted cross-database revoke boundaries, revoke-order sequencing isolation, malformed ACL target rejection, grant-option persistence/revocation invariants, cross-database grant-option scope isolation and scoped grant-option revoke cleanup, ACL batch authorization boundaries, cross-database ACL batch rejection safeguards, malformed ACL batch no-partial-apply guarantees, multi-target malformed-batch no-partial-apply guarantees, malformed-batch recovery determinism for subsequent valid batches, mixed ACL/non-ACL batch rejection, high-contention multi-session interleavings, and parallel reader/writer contention | Implemented/Tested | `server/src/core/app/mod_test.rs::grant_and_revoke_queries_update_object_acl_access`; `server/src/core/app/mod_test.rs::schema_grant_preserves_access_after_object_revoke_until_schema_revoke`; `server/src/core/app/mod_test.rs::object_grant_survives_schema_revoke_and_remains_object_scoped`; `server/src/core/app/mod_test.rs::qualified_object_grant_uses_explicit_database_not_query_hint`; `server/src/core/app/mod_test.rs::schema_grant_uses_explicit_schema_not_query_hint_database`; `server/src/core/app/mod_test.rs::qualified_object_revoke_only_affects_targeted_database_object`; `server/src/core/app/mod_test.rs::schema_revoke_only_affects_targeted_database_not_other_schema_grants`; `server/src/core/app/mod_test.rs::mixed_cross_database_revoke_order_keeps_scope_isolated_per_step`; `server/src/core/app/mod_test.rs::malformed_qualified_acl_target_is_rejected`; `server/src/core/app/mod_test.rs::schema_grant_with_grant_option_tracks_grant_acl_and_revokes_cleanly`; `server/src/core/app/mod_test.rs::acl_and_non_acl_batch_is_rejected_without_applying_acl_side_effect`; `server/src/core/app/mod_test.rs::non_root_acl_batch_is_rejected_without_acl_side_effects`; `server/src/core/app/mod_test.rs::root_acl_only_batch_applies_multiple_acl_mutations`; `server/src/core/app/mod_test.rs::non_root_cross_database_acl_batch_is_rejected_without_side_effects`; `server/src/core/app/mod_test.rs::root_acl_batch_with_malformed_statement_has_no_partial_side_effects`; `server/src/core/app/mod_test.rs::cross_database_grant_option_is_scoped_to_target_database_only`; `server/src/core/app/mod_test.rs::root_multi_target_acl_batch_with_late_malformed_statement_has_no_partial_side_effects`; `server/src/core/app/mod_test.rs::cross_database_revoke_cleans_grant_option_only_for_target_database`; `server/src/core/app/mod_test.rs::acl_batch_recovery_after_malformed_request_applies_next_valid_batch_deterministically`; `server/src/core/app/mod_test.rs::authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial`; `server/src/core/app/mod_test.rs::authorization_interleaving_high_contention_multi_session_revokes_stay_effective`; `server/src/core/app/mod_test.rs::authorization_parallel_reader_writer_contention_preserves_revoke_effectiveness`; `serverlib/src/engine/security_test.rs::revoking_privilege_also_revokes_grant_option`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-004 | Invalid or duplicate user creation attempts | Invalid credential clauses rejected; duplicate creation requires explicit idempotent mode | Implemented/Tested | `server/src/core/app/mod_test.rs::create_user_requires_identified_by_password_clause`; `server/src/core/app/mod_test.rs::create_user_duplicate_requires_if_not_exists`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-005 | Security WAL replay consistency | Latest security state replays deterministically after restart | Implemented/Tested | `server/src/core/app/mod_test.rs::bootstrap_replays_security_change_password_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_acl_replay_prefers_latest_wal_snapshot_for_user`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-006 | Password nonce predictability abuse | Nonce generation remains independent from username and sensitive to seed/database | Implemented/Tested | `serverlib/src/engine/security_test.rs::password_nonce_does_not_depend_on_username`; `serverlib/src/engine/security_test.rs::password_nonce_changes_with_database_or_seed`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-007 | Unauthorized affinity join attempt with invalid credentials | Join is rejected and no unauthorized affinity state is accepted | Implemented/Tested | `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_invalid_key`; `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_affinity_id_mismatch`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-008 | Malformed transport payload handling | Invalid payloads are rejected without state corruption | Implemented/Tested | `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_missing_magic_prefix`; `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_truncated_bincode_payload`; `peerlib/src/connector/transport_test.rs::connect_active_peer_rejects_malformed_challenge_payload`; `peerlib/src/connector/transport_test.rs::request_drops_live_connection_after_malformed_response_payload`; `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response`; `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response`; `scripts/security/security_adversarial_baseline.sh` |

## Baseline Runner

- `bash scripts/security/security_adversarial_baseline.sh`
- Findings rubric/log: `docs/security-findings-log.md`
- Reproducible security fault-injection stage: `connect_active_peer_rejects_malformed_challenge_payload`, `request_drops_live_connection_after_malformed_response_payload`, `fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response`, `fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response`, `apply_bootstrap_password_wal_payload_rejects_non_utf8_payload`, `apply_bootstrap_password_wal_payload_rejects_malformed_segments`, `apply_bootstrap_password_wal_payload_rejects_invalid_encrypted_password_state`, `authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial`, `authorization_interleaving_high_contention_multi_session_revokes_stay_effective`, `authorization_parallel_reader_writer_contention_preserves_revoke_effectiveness`

Latest SEC-003 expansions included in the baseline:

- `server/src/core/app/mod_test.rs::cross_database_grant_option_transition_chain_remains_scope_isolated`
- `server/src/core/app/mod_test.rs::repeated_malformed_acl_batches_at_different_positions_preserve_acl_state_until_valid_batch`
- `server/src/core/app/mod_test.rs::cross_database_object_acl_transition_chain_remains_target_isolated`
- `server/src/core/app/mod_test.rs::alternating_malformed_grant_revoke_batches_preserve_state_until_valid_reconciliation`
- `server/src/core/app/mod_test.rs::cross_database_schema_grant_with_object_revoke_chain_preserves_scope_precedence`
- `server/src/core/app/mod_test.rs::mixed_schema_object_malformed_batch_rejects_without_partial_side_effects`
- `server/src/core/app/mod_test.rs::cross_database_mixed_schema_object_grant_option_chain_scopes_grant_acl_and_access`
- `server/src/core/app/mod_test.rs::mixed_schema_object_grant_option_malformed_batch_has_no_side_effects_then_recovers`
- `server/src/core/app/mod_test.rs::non_root_delegated_grant_attempt_is_rejected_even_after_access_grant`
- `server/src/core/app/mod_test.rs::non_root_cross_database_delegated_grant_attempt_is_rejected_without_side_effects`

## Maintenance Conditions

1. Keep `SEC-001..SEC-008` at `Implemented/Tested` with executable evidence links.
2. Keep findings hygiene current and leave no unresolved `High`/`Critical` findings in `docs/security-findings-log.md`.
3. Keep `scripts/security/security_adversarial_baseline.sh` in nightly evidence execution and passing.
