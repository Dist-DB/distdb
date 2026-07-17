# Security Findings Log

This document tracks security findings, triage decisions, and mitigation status for beta confidence.

## Purpose

- Maintain a durable, auditable findings trail.
- Provide disposition evidence for scorecard security gates.
- Ensure high-severity findings are either fixed or accepted with explicit mitigation and owner sign-off.

## Severity Rubric

Use this rubric for all findings:

| Severity | Definition | Beta Impact | SLA Target |
| --- | --- | --- | --- |
| Critical | Direct unauthenticated compromise, privilege escalation to admin/root, or durable integrity corruption with practical exploitability | Blocks beta immediately | 24h triage, immediate fix/mitigation |
| High | Authenticated or constrained exploit that can bypass policy boundaries, leak sensitive data, or degrade trust guarantees | Blocks beta until resolved or formally accepted with mitigation | 72h triage, fix before beta declaration |
| Medium | Defense-in-depth weakness, limited-scope abuse path, or misconfiguration risk with bounded blast radius | Does not block beta by itself, but must have tracked remediation plan | 7d triage |
| Low | Hardening opportunity, low-impact issue, or informational weakness with no immediate exploit chain | Informational for beta, fix as capacity allows | 14d triage |

## Disposition States

| State | Meaning |
| --- | --- |
| Open | Finding exists and has not yet been resolved or accepted |
| In Progress | Mitigation/fix is in progress |
| Fixed | Code or configuration fix merged and validated |
| Accepted | Risk is explicitly accepted with documented mitigation and owner approval |
| False Positive | Determined non-issue with evidence |

## Findings Table

| ID | Date | Surface | Scenario | Severity | Status | Owner | Evidence | Mitigation / Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SEC-FIND-001 | 2026-07-17 | affinity join control-plane | Unauthorized affinity join accepted due to missing affinity key/id validation | High | Fixed | server/serverlib | `server/src/core/control/connector_handler.rs::validate_affinity_join_request`; `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_invalid_key`; `server/src/core/control/connector_handler_test.rs::affinity_join_request_rejects_affinity_id_mismatch`; `scripts/security/security_adversarial_baseline.sh` | Added strict affinity_id + key validation before join response; baseline now continuously exercises rejection paths. |
| SEC-FIND-002 | 2026-07-17 | connector transport framing | Malformed challenge/response payload decode handling not explicitly regression-tested | Medium | Fixed | peerlib/server | `peerlib/src/connector/transport_test.rs::connect_active_peer_rejects_malformed_challenge_payload`; `peerlib/src/connector/transport_test.rs::request_drops_live_connection_after_malformed_response_payload`; `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_missing_magic_prefix`; `server/src/core/control/p2p_wire_test.rs::decode_service_message_rejects_truncated_bincode_payload`; `scripts/security/security_adversarial_baseline.sh` | Added deterministic malformed-frame tests and enforced connection drop after malformed response payload. |
| SEC-FIND-003 | 2026-07-17 | connector trust bootstrap | Malformed CA bootstrap responses (direct or challenge-prefixed) lacked explicit adversarial regression coverage | Medium | Fixed | peerlib | `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response`; `peerlib/src/connector/transport_test.rs::fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response`; `scripts/security/security_adversarial_baseline.sh` | Added deterministic trust-bootstrap abuse tests to ensure malformed CA frames degrade safely to no-cert result instead of crashing/accepting invalid trust state. |
| SEC-FIND-004 | 2026-07-17 | security WAL replay path | Malformed bootstrap security WAL payload handling lacked explicit replay-edge adversarial regression coverage | Medium | Fixed | server | `server/src/core/control/session_test.rs::apply_bootstrap_password_wal_payload_rejects_non_utf8_payload`; `server/src/core/control/session_test.rs::apply_bootstrap_password_wal_payload_rejects_malformed_segments`; `server/src/core/control/session_test.rs::apply_bootstrap_password_wal_payload_rejects_invalid_encrypted_password_state`; `scripts/security/security_adversarial_baseline.sh` | Added deterministic replay-edge malformed payload tests to ensure invalid security WAL payloads are rejected during bootstrap password replay path. |
| SEC-FIND-005 | 2026-07-17 | authorization mutation/read interleaving | Privilege mutation/read interleaving lacked explicit repeated-cycle regression coverage across active sessions | Medium | Fixed | server | `server/src/core/app/mod_test.rs::authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial`; `scripts/security/security_adversarial_baseline.sh` | Added repeated grant/revoke interleaving cycles with two active user sessions to ensure post-revoke reads are denied consistently across cycles. |

## Update Rules

1. Add a finding entry before or with any security behavior change.
2. `Critical` and `High` findings must not remain `Open` at beta declaration.
3. Any `Accepted` finding must include explicit mitigation text and owner approval reference.
4. Evidence links must point to concrete tests, scripts, or workflow runs.
