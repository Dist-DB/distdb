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
| SEC-001 | Non-root read without `SELECT` privilege | Request is rejected with no data disclosure | Implemented/Partial | `server/src/core/app/mod_test.rs::query_requires_select_privilege_for_non_root_user`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-002 | Object-level ACL boundary violations | Access only granted for explicitly authorized objects | Implemented/Partial | `server/src/core/app/mod_test.rs::query_object_acl_allows_only_granted_objects`; `server/src/core/app/mod_test.rs::query_join_requires_privileges_for_all_referenced_objects`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-003 | Privilege escalation via grant/revoke misuse | ACL changes are explicit and reversible through authorization paths | Implemented/Partial | `server/src/core/app/mod_test.rs::grant_and_revoke_queries_update_object_acl_access`; `serverlib/src/engine/security_test.rs::revoking_privilege_also_revokes_grant_option`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-004 | Invalid or duplicate user creation attempts | Invalid credential clauses rejected; duplicate creation requires explicit idempotent mode | Implemented/Partial | `server/src/core/app/mod_test.rs::create_user_requires_identified_by_password_clause`; `server/src/core/app/mod_test.rs::create_user_duplicate_requires_if_not_exists`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-005 | Security WAL replay consistency | Latest security state replays deterministically after restart | Implemented/Partial | `server/src/core/app/mod_test.rs::bootstrap_replays_security_change_password_from_wal`; `server/src/core/app/mod_test.rs::bootstrap_acl_replay_prefers_latest_wal_snapshot_for_user`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-006 | Password nonce predictability abuse | Nonce generation remains independent from username and sensitive to seed/database | Implemented/Partial | `serverlib/src/engine/security_test.rs::password_nonce_does_not_depend_on_username`; `serverlib/src/engine/security_test.rs::password_nonce_changes_with_database_or_seed`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-007 | Unauthorized affinity join attempt with invalid credentials | Join is rejected and no unauthorized affinity state is accepted | Implemented/Partial | `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_invalid_key`; `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_affinity_id_mismatch`; `scripts/security/security_adversarial_baseline.sh` |
| SEC-008 | Malformed transport payload handling | Invalid payloads are rejected without state corruption | Implemented/Partial | `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_missing_magic_prefix`; `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_truncated_bincode_payload`; `peerlib/src/connector/transport_test.rs::connect_active_peer_rejects_malformed_challenge_payload`; `peerlib/src/connector/transport_test.rs::request_drops_live_connection_after_malformed_response_payload`; `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response`; `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response`; `scripts/security/security_adversarial_baseline.sh` |

## Baseline Runner

- `bash scripts/security/security_adversarial_baseline.sh`
- Findings rubric/log: `docs/security-findings-log.md`
- Reproducible security fault-injection stage: `connect_active_peer_rejects_malformed_challenge_payload`, `request_drops_live_connection_after_malformed_response_payload`, `fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response`, `fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response`, `apply_bootstrap_password_wal_payload_rejects_non_utf8_payload`, `apply_bootstrap_password_wal_payload_rejects_malformed_segments`, `apply_bootstrap_password_wal_payload_rejects_invalid_encrypted_password_state`, `authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial`

## Required Beta Additions

1. Expand fault-injection matrix breadth beyond current malformed frame + trust-bootstrap + replay decode abuse paths (e.g., higher-contention authorization race conditions).
2. Maintain findings log hygiene and ensure no `High`/`Critical` items remain open at beta declaration.
