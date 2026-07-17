# Non-Functional Findings Log

This log tracks non-functional regressions and disposition state for beta confidence.

## Purpose

- maintain auditable regression triage history,
- capture ownership and mitigation notes for failed threshold runs,
- ensure no unresolved Critical non-functional regressions remain when declaring beta posture.

## Severity Rubric

| Severity | Definition | Beta Impact |
| --- | --- | --- |
| Critical | Material latency/throughput/recovery regression that violates declared baseline and blocks release posture progression | Blocks Domain 2 Green while unresolved |
| High | Significant degradation with bounded blast radius requiring planned remediation | Does not block by itself if disposition is documented and mitigation is active |
| Medium | Variance or environment-sensitive drift requiring tracking and follow-up | Informational for beta posture with owner follow-up |
| Low | Minor drift/hardening opportunity | Informational |

## Disposition States

| State | Meaning |
| --- | --- |
| Open | Finding exists and has not been resolved |
| In Progress | Fix/mitigation is in progress |
| Fixed | Regression fix validated by evidence |
| Accepted | Risk accepted with documented mitigation and owner approval |
| Invalid Run | Non-functional run invalid due to harness/infrastructure issue |

## Findings

| ID | Date | Profile | Severity | Status | Owner | Evidence | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| NF-FIND-001 | 2026-07-17 | Governance baseline initialization | Medium | Fixed | server/serverlib | `scripts/perf/check_nonfunctional_thresholds.sh`; `scripts/check_artifact_evidence_quality.sh`; `artifacts/trends/nonfunctional-trend.json` | Added enforced threshold gate + trend-history gate; baseline now has >=3 passing entries with nightly enforcement. |

## Governance Rule

- Domain 2 cannot be marked `Green` if any `Critical` finding is `Open` or `In Progress`.
- Enforcement path: `DISTDB_REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED=true bash scripts/check_artifact_evidence_quality.sh`.
