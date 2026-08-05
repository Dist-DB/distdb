# Release Candidate Checklist

This checklist defines the minimum evidence required to claim DistDB release-candidate readiness.

## Purpose

Use this page to convert release intent into objective gates across platform correctness, trust boundaries, multi-tenant safety, non-functional behavior, and operational readiness.

RC should be blocked unless all required checklist sections are complete and evidence-linked.

## Decision Model

Checklist result states:

- `Blocked`: one or more required checks are incomplete.
- `Candidate`: all required checks are complete, and no open release blockers exist.
- `Ready`: candidate checks are complete, sign-off owners have approved, and rollout/rollback rehearsals are complete.

Release posture:

- `Not RC Ready`: any required section is blocked.
- `RC Candidate`: all required sections complete, pending final sign-off.
- `RC Ready`: all required sections complete and signed off.

---

## Section 1: Core Correctness and Data Safety

### Objective

Prove deterministic correctness and recovery under expected and failure conditions.

### Required checks

- [ ] WAL replay and restart consistency tests pass in CI and local reproducible runs.
- [ ] Transaction correctness and isolation invariants are green for declared isolation behavior.
- [ ] Schema/index lifecycle operations are interruption-safe and idempotent.
- [ ] No open Sev-1/Sev-2 correctness defects.

### Evidence links

- [ ] `docs/consistency-isolation.md`
- [ ] `docs/node-failure-matrix.md`
- [ ] `docs/partition-split-brain-matrix.md`

---

## Section 2: TLS Trust Boundary and Identity Enforcement

### Objective

Validate split public/private trust with strict fail-closed controls.

### Required checks

- [ ] Public edge traffic uses publicly issued certificates for tenant hostnames.
- [ ] Internal gateway-to-instance traffic is mTLS-only.
- [ ] Gateway validates tenant route identity against backend certificate identity (SAN/workload identity).
- [ ] Private CA issuance/rotation/revocation flow is tested.
- [ ] No plaintext fallback path exists between gateway and managed instance runtime.

### Evidence links

- [ ] `docs/security.md`
- [ ] `docs/installation-advisories.md`
- [ ] `distdb-cloud/docs/tls-trust-model.md`

---

## Section 3: Multi-Tenant Isolation and Routing Safety

### Objective

Demonstrate tenant isolation and correct routing under shared infrastructure operation.

### Required checks

- [ ] Cross-tenant request isolation tests are negative (no bleed-through).
- [ ] Routing policy enforces strict host/SNI/domain mapping.
- [ ] Container co-location does not weaken identity checks or network policy controls.
- [ ] Affinity and replication flows do not bypass tenant boundary enforcement.

### Evidence links

- [ ] `distdb-cloud/docs/architecture.md`
- [ ] `distdb-cloud/docs/instance-lifecycle.md`
- [ ] `distdb-cloud/docs/control-plane-api.md`

---

## Section 4: Non-Functional Performance and Memory Stability

### Objective

Show predictable latency/throughput/recovery with bounded memory behavior.

### Required checks

- [ ] Published baseline targets exist for p50/p95/p99 latency, throughput, and recovery-to-ready.
- [ ] WAL replay and runtime-index bootstrap stay within declared bounds for target datasets.
- [ ] Soak run evidence exists (for example 24h to 72h) with no unbounded memory growth.
- [ ] No open critical non-functional regressions against baseline.

### Evidence links

- [ ] `docs/non-functional-benchmarking.md`
- [ ] `docs/nonfunctional-findings-log.md`
- [ ] `artifacts/trends/nonfunctional-trend.json`

---

## Section 5: Security and Adversarial Validation

### Objective

Prove resilience of authn/authz, transport trust, and message handling under hostile inputs.

### Required checks

- [ ] Adversarial baseline suite passes for authn/authz/transport misuse scenarios.
- [ ] Security fault-injection runs are reproducible and evidence-linked.
- [ ] Findings triage and severity rubric are current.
- [ ] No unresolved High/Critical findings without approved exception.

### Evidence links

- [ ] `docs/security-adversarial-matrix.md`
- [ ] `docs/security-findings-log.md`

---

## Section 6: Operability, Upgrade, and Rollback

### Objective

Ensure operators can safely deploy, observe, recover, and roll back.

### Required checks

- [ ] Provision/start/stop/suspend/resume/delete paths are validated end-to-end.
- [ ] Rolling restart and upgrade compatibility expectations are tested for the declared version window.
- [ ] Backup/restore drills pass declared RPO/RTO criteria.
- [ ] Rollback runbook is documented and rehearsal-proven.

### Evidence links

- [ ] `docs/release.md`
- [ ] `docs/running.md`
- [ ] `docs/using.md`

---

## Section 7: Observability and Incident Readiness

### Objective

Verify the platform can be diagnosed and operated under incident conditions.

### Required checks

- [ ] Required logs/metrics/events for trust, routing, replay, and replication state are present.
- [ ] Alert thresholds for TLS failures, memory growth, replay lag, and error rates are configured.
- [ ] Incident runbooks exist for primary high-risk failure modes.
- [ ] On-call drill evidence is recorded.

### Evidence links

- [ ] `distdb-cloud/docs/observability.md`
- [ ] `docs/node-failure-matrix.md`

---

## Section 8: Release Hygiene and Sign-Off

### Objective

Confirm release hygiene, artifact quality, and owner approval.

### Required checks

- [ ] Dependency and vulnerability review completed to policy threshold.
- [ ] Build artifacts are reproducible and version-identifiable.
- [ ] Release notes and upgrade notes are complete.
- [ ] Engineering sign-off recorded.
- [ ] Security sign-off recorded.
- [ ] Operations sign-off recorded.

---

## RC Exit Criteria (Hard Gate)

- [ ] No open Sev-1/Sev-2 defects.
- [ ] All required checklist items above are complete and evidence-linked.
- [ ] Security High/Critical findings are closed or explicitly accepted with mitigation.
- [ ] Non-functional baseline and memory stability checks are passing.
- [ ] Rollback drill completed successfully in current release window.

## Status Table

Update this table whenever RC posture changes.

| Area | Status | Last Updated | Owner | Blocking Gaps |
| --- | --- | --- | --- | --- |
| Core correctness and data safety | Blocked | 2026-08-05 | server/serverlib | Beta evidence is strong, but RC still lacks explicit Sev-1/Sev-2 closure record and interruption-safety evidence mapping for schema/index lifecycle. |
| TLS trust boundary and identity enforcement | Blocked | 2026-08-05 | gateway/cloud + server/tls | Trust model is documented, but fail-closed gateway enforcement evidence (public edge cert + internal mTLS identity checks) is not yet linked as executable proof. |
| Multi-tenant isolation and routing safety | Blocked | 2026-08-05 | gateway/cloud | Architecture and lifecycle policy are documented, but explicit cross-tenant negative test evidence and routing-identity enforcement proof are missing. |
| Non-functional performance and memory stability | Blocked | 2026-08-05 | server/serverlib | Benchmark baselines and trend governance are documented, but RC-specific soak-run acceptance evidence is not yet linked in this checklist. |
| Security/adversarial validation | Blocked | 2026-08-05 | server/serverlib | Adversarial matrix is green and no open High/Critical findings are listed, but RC checklist needs explicit run references for the target RC cut window. |
| Operability, upgrade, and rollback | Blocked | 2026-08-05 | server + ops | Operability beta evidence exists, but RC requires current-window rollback rehearsal and backup/restore RPO/RTO drill evidence linkage. |
| Observability and incident readiness | Blocked | 2026-08-05 | cloud + ops | Observability requirements are documented, but configured alert thresholds and recorded incident/on-call drills are not yet evidenced. |
| Release hygiene and sign-off | Blocked | 2026-08-05 | release owners | Dependency/vulnerability attestation, artifact provenance, and engineering/security/ops sign-off records are not yet linked. |

## Validation Snapshot (2026-08-05)

Current validation against this checklist based on repository evidence:

1. Core correctness and data safety: `Partially evidenced`.
	- Evidence present: beta scorecard Domain 1 marked `Green` in `docs/checklists/beta-confidence-scorecard.md`.
	- Remaining gap: explicit RC-window Sev-1/Sev-2 defect closure and interruption-safety mapping.
2. TLS trust boundary and identity enforcement: `Policy documented, enforcement evidence pending`.
	- Evidence present: `distdb-cloud/docs/tls-trust-model.md` defines split public/private trust and mTLS internal requirement.
	- Remaining gap: executable proof links for fail-closed gateway identity verification.
3. Multi-tenant isolation and routing safety: `Policy documented, test evidence pending`.
	- Evidence present: `distdb-cloud/docs/architecture.md` and `distdb-cloud/docs/instance-lifecycle.md` define strict gateway/clover boundaries and routing policy intent.
	- Remaining gap: cross-tenant negative tests and route-to-identity proof runs.
4. Non-functional performance and memory stability: `Partially evidenced`.
	- Evidence present: beta scorecard Domain 2 marked `Green`, trend and findings governance documented in `docs/non-functional-benchmarking.md` and `docs/nonfunctional-findings-log.md`.
	- Remaining gap: RC soak-run acceptance evidence for this cut.
5. Security and adversarial validation: `Strongly evidenced, RC linkage pending`.
	- Evidence present: `docs/security-adversarial-matrix.md` scenarios are marked `Implemented/Tested`; no open High/Critical findings in `docs/security-findings-log.md`.
	- Remaining gap: pin the exact RC-window run artifacts in this checklist.
6. Operability, upgrade, and rollback: `Partially evidenced`.
	- Evidence present: beta scorecard Domain 4 marked `Green`.
	- Remaining gap: explicit RC-window rollback and backup/restore drill results.
7. Observability and incident readiness: `Requirements present, operational evidence pending`.
	- Evidence present: `distdb-cloud/docs/observability.md` defines required signals and alert baseline.
	- Remaining gap: evidence of configured thresholds and completed on-call drill records.
8. Release hygiene and sign-off: `Not yet evidenced`.
	- Remaining gap: attestations and owner sign-off records.

Overall posture for this checklist: `Not RC Ready`.

Current trajectory: `Advancing toward RC Candidate`.

## Evidence Tracking Matrix

Use this matrix to track concrete closure evidence for each checklist section.

Legend:

- `Status`: `Missing` | `Scheduled` | `Captured` | `Verified`
- `Evidence Type`: test run, workflow run, artifact bundle, runbook drill, sign-off record

### Section 1: Core Correctness and Data Safety

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-1.1 | WAL replay and restart consistency tests pass | server/serverlib | workflow run | `CONSISTENCY_RUN_PARTITION_RECONVERGENCE=true bash scripts/run_consistency_failure_validation.sh` + `.github/workflows/consistency-failure-validation.yml` | `artifacts/e2e/split-brain-evidence-20260805-140005-1976/` + `docs/consistency-isolation.md` | 2026-08-05 | Captured | Fresh split-brain evidence bundle captured; attach CI run URL at RC cut. |
| RC-1.2 | Isolation invariants are green | server/serverlib | test run | `bash scripts/e2e/isolation_restart.sh` + documented invariants review | `server/data/e2e/isolation-restart-20260805-140030-2750/` + `docs/consistency-isolation.md` | 2026-08-05 | Captured | Isolation+restart suite passed in current RC-window local run. |
| RC-1.3 | Schema/index lifecycle interruption safety | server/serverlib | test run | schema/index lifecycle replay/restart scenarios + interruption drill | `artifacts/e2e/` lifecycle bundle (pending) | TBD | Scheduled | Runner path is known; explicit interruption/idempotency evidence still pending. |
| RC-1.4 | No open Sev-1/Sev-2 correctness defects | server/serverlib | sign-off record | issue tracker query / release board export | RC defect closure snapshot | TBD | Missing | Capture dated export at RC cut. |

### Section 2: TLS Trust Boundary and Identity Enforcement

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-2.1 | Public edge uses publicly issued tenant certs | gateway/cloud | runbook drill | `openssl s_client -servername provision.distdb.com -connect 104.248.250.139:4002` cert probe | `artifacts/security/public-edge-cert-104.248.250.139-4002-20260805.txt` + `artifacts/security/public-edge-cert-provision-distdb-com-20260805.raw.txt` | 2026-08-05 | Captured | SAN coverage includes `provision.distdb.com`, but issuer is `DistDB Platform Issuing CA`; public-CA issuance evidence is still pending. |
| RC-2.2 | Internal gateway-to-instance traffic is mTLS-only | gateway/cloud + server/tls | integration test | `cargo test -q validate_wss_tls_policy_requires_tls_required` + `cargo test -q validate_wss_tls_policy_requires_acceptor` + remote runtime config check | `server/src/core/comms/wss_test.rs` + `artifacts/security/tls-runtime-evidence-20260805.log` | 2026-08-05 | Captured | Server-side fail-closed TLS policy evidence captured (`tls=required`, WSS bound); gateway-to-instance mTLS hop proof is still required. |
| RC-2.3 | Backend identity validation is enforced fail-closed | gateway/cloud | adversarial test | `cargo test -q startup_tls_requires_san` + startup/runtime SAN evidence checks | `server/src/main_test.rs` + `artifacts/security/tls-runtime-evidence-20260805.log` | 2026-08-05 | Captured | Node startup rejects missing SAN and runtime SANs are declared; gateway route-to-backend mismatch negative tests remain pending. |
| RC-2.4 | Private CA rotation/revocation tested | server/tls + cloud | runbook drill | tlsserver rotation + revoke drill | drill report and timeline | TBD | Missing | Include rollback/restore notes. |

### Section 3: Multi-Tenant Isolation and Routing Safety

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-3.1 | Cross-tenant request isolation is negative | gateway/cloud | adversarial test | multi-tenant routing isolation suite | isolation test bundle (pending) | TBD | Scheduled | Test intent and policy are documented; execution evidence is still pending. |
| RC-3.2 | Routing policy enforces host/SNI/domain mapping | gateway/cloud | integration test | SNI/Host mismatch matrix | routing validation report (pending) | TBD | Scheduled | Expected vs observed matrix must be attached for RC. |
| RC-3.3 | Container co-location does not weaken trust controls | gateway/cloud + ops | runbook drill | shared-host trust boundary drill | co-location hardening evidence | TBD | Missing | Show identity checks remain strict intra-host. |
| RC-3.4 | Affinity/replication cannot bypass tenant boundary | server/serverlib + cloud | adversarial test | affinity boundary abuse scenarios + unauthorized join rejection baseline | `docs/security-adversarial-matrix.md` (SEC-007) + `artifacts/security/security-baseline-20260805-135853-97484/` | 2026-08-05 | Captured | Captured as partial proxy evidence in current run; tenant-scoped routing proof still required. |

### Section 4: Non-Functional Performance and Memory Stability

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-4.1 | Baseline targets are published and current | server/serverlib | artifact bundle | `bash scripts/perf/nonfunctional_baseline.sh` + `bash scripts/perf/check_nonfunctional_thresholds.sh` | `docs/non-functional-benchmarking.md` + `artifacts/perf/nonfunctional-baseline-20260805-135943-98167/` + `artifacts/trends/nonfunctional-trend.json` | 2026-08-05 | Captured | Fresh RC-window baseline captured with passing thresholds; freeze RC-cut values when branching. |
| RC-4.2 | Replay/bootstrap within declared bounds | server/serverlib | benchmark run | replay/bootstrap profile runs | timing artifact bundle | TBD | Missing | Include target dataset profile labels. |
| RC-4.3 | Soak run shows bounded memory behavior | server/serverlib + ops | soak run | 24h-72h soak workflow | soak logs + memory trend graphs | TBD | Missing | Include pass thresholds and violations count. |
| RC-4.4 | No open critical non-functional regressions | server/serverlib | sign-off record | `DISTDB_REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED=true bash scripts/check_artifact_evidence_quality.sh` | `docs/nonfunctional-findings-log.md` + `artifacts/trends/nonfunctional-trend.json` + nightly evidence workflow | 2026-08-05 | Captured | Governance gate revalidated in current RC-window evidence-quality pass. |

### Section 5: Security and Adversarial Validation

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-5.1 | Adversarial baseline passes | server/serverlib | test run | `bash scripts/security/security_adversarial_baseline.sh` | `docs/security-adversarial-matrix.md` + `artifacts/security/security-baseline-20260805-135853-97484/` | 2026-08-05 | Captured | Fresh RC-window baseline pass captured with manifest and run log. |
| RC-5.2 | Fault-injection runs are reproducible | server/serverlib | workflow run | `.github/workflows/security-adversarial-baseline.yml` + `.github/workflows/nightly-evidence.yml` | `artifacts/security/security-baseline-20260805-135853-97484/` + `artifacts/trends/security-trend.json` | 2026-08-05 | Captured | Reproducible stage revalidated and trend ledger updated in RC-window. |
| RC-5.3 | Findings rubric and triage are current | server/serverlib | document attestation | findings/rubric review | `docs/security-findings-log.md` | 2026-07-17 | Captured | Rubric and disposition states are documented; add RC approver/date entry. |
| RC-5.4 | No unresolved High/Critical findings | server/serverlib + security | sign-off record | findings log export + closure verification | `docs/security-findings-log.md` (no open High/Critical entries) + `DISTDB_REQUIRE_SECURITY_HIGH_CRITICAL_FINDINGS_CLOSED=true bash scripts/check_artifact_evidence_quality.sh` | 2026-08-05 | Captured | Governance gate revalidated in current RC-window; explicit human security sign-off still required. |

### Section 6: Operability, Upgrade, and Rollback

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-6.1 | Lifecycle operations validated end-to-end | server + ops | runbook drill | provision/start/stop/suspend/resume/delete drill suite + rolling lifecycle rehearsal | `docs/running.md` + `artifacts/e2e/rolling-upgrade-safety-rc-window-local-20260805-140317-9012/` | 2026-08-05 | Scheduled | Current run proves provision/start/stop/restart paths; explicit suspend/resume/delete drill evidence remains pending. |
| RC-6.2 | Rolling restart/upgrade compatibility verified | server/serverlib + ops | workflow run | `bash scripts/e2e/rolling_restart_upgrade_safety.sh` + `.github/workflows/operability-upgrade-safety.yml` | `docs/operability-upgrade-safety.md` + `artifacts/e2e/rolling-upgrade-safety-rc-window-local-20260805-140317-9012/` + `artifacts/e2e/rolling-upgrade-safety-head-1-20260717-161303-49139/` + `artifacts/e2e/rolling-upgrade-safety-head-2-20260717-161306-49257/` + `artifacts/e2e/rolling-upgrade-safety-head-3-20260717-161309-48780/` + `artifacts/trends/operability-trend.json` | 2026-08-05 | Captured | RC-window local drill passed rolling restart and upgrade continuity checks. |
| RC-6.3 | Backup/restore meets RPO/RTO | ops | runbook drill | backup restore drill | restore timing and integrity report | TBD | Missing | Include RPO/RTO actual vs target. |
| RC-6.4 | Rollback runbook is rehearsal-proven | ops + release owners | runbook drill | rollback rehearsal procedure via rolling upgrade safety drill rollback phase | `docs/release.md` + `artifacts/e2e/rolling-upgrade-safety-rc-window-local-20260805-140317-9012/summary.json` + `artifacts/e2e/rolling-upgrade-safety-rc-window-local-20260805-140317-9012/manifest.json` | 2026-08-05 | Captured | Current run reports `cross_version_rollback_survived=true`; add operator go/no-go attestation at release cut. |

### Section 7: Observability and Incident Readiness

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-7.1 | Required logs/metrics/events are present | cloud + ops | observability audit | signal coverage review against trust/routing/replay/replication requirements | `distdb-cloud/docs/observability.md` | TBD | Scheduled | Requirements are documented; source-to-signal mapping evidence still needed. |
| RC-7.2 | Alert thresholds are configured | cloud + ops | config audit | alert policy review and export | `distdb-cloud/docs/observability.md` + alert policy export (pending) | TBD | Scheduled | Threshold config evidence has not yet been linked. |
| RC-7.3 | Incident runbooks exist for high-risk modes | ops | document attestation | runbook completeness review + runbook hash index generation | `docs/operability-runbooks.md` + `docs/node-failure-matrix.md` + `artifacts/ops/runbook-index-20260805.txt` | 2026-08-05 | Captured | Dated runbook index captured; escalation contacts/on-call roster linkage still belongs in drill evidence. |
| RC-7.4 | On-call drill evidence is recorded | ops | drill run | incident simulation drill | drill timeline and postmortem | TBD | Missing | Include corrective actions and due dates. |

### Section 8: Release Hygiene and Sign-Off

| Item ID | Requirement | Owner | Evidence Type | Command / Workflow | Artifact / Link | Last Run | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RC-8.1 | Dependency/vulnerability policy threshold met | release owners + security | scan report | dependency/security scan workflow | scan report artifact (pending) | TBD | Scheduled | Workflow wiring is expected; explicit RC attestation artifact is pending. |
| RC-8.2 | Build artifacts are reproducible | release owners | build attestation | reproducible build workflow | artifact checksum/provenance record (pending) | TBD | Scheduled | Reproducibility record is required for RC sign-off package. |
| RC-8.3 | Release and upgrade notes complete | release owners | document attestation | release notes review | `docs/release.md` + upgrade notes package (pending) | TBD | Scheduled | Base release document exists; RC notes/sign-off linkage is pending. |
| RC-8.4 | Engineering sign-off recorded | engineering owner | sign-off record | RC approval workflow | signed approval record | TBD | Missing | |
| RC-8.5 | Security sign-off recorded | security owner | sign-off record | RC approval workflow | signed approval record | TBD | Missing | |
| RC-8.6 | Operations sign-off recorded | operations owner | sign-off record | RC approval workflow | signed approval record | TBD | Missing | |

## Cadence and Enforcement

1. Update this checklist at every milestone that changes release posture.
2. Block RC declaration unless all hard-gate criteria are complete.
3. Link each check to concrete evidence (tests, workflow runs, artifacts, or incident drill output).
4. Keep this checklist aligned with `docs/checklists/beta-confidence-scorecard.md` and `docs/release.md`.
