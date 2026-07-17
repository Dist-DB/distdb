# Beta Confidence Scorecard

This scorecard defines the minimum evidence required to claim DistDB beta-grade confidence.

## Purpose

Use this page to turn design confidence into objective release gates across the four highest-risk areas:

1. partition and split-brain correctness,
2. non-functional performance/recovery behavior,
3. security/adversarial resilience,
4. operability and upgrade safety.

This scorecard is intentionally strict. A beta claim should be blocked unless the minimum pass criteria are met.

## Rating Model

Each domain is scored as one of:

- `Red`: required baseline evidence is missing.
- `Yellow`: partial evidence exists but one or more beta gates remain open.
- `Green`: all domain beta gates are satisfied with repeatable evidence.

Overall release confidence:

- `Not Beta Ready`: any domain is `Red`.
- `Beta Candidate`: all domains are at least `Yellow`, and at least three domains are `Green`.
- `Beta Ready`: all four domains are `Green`.

## Domain 1: Partition and Split-Brain Correctness

### Objective

Prove deterministic safety and convergence behavior under network disruption and healing.

### Beta gates

1. deterministic reconvergence invariants pass in CI:
   - `scripts/e2e/partition_reconvergence.sh`
2. full split-brain matrix exists with executable scenarios and expected outcomes:
   - dual-primary attempt,
   - unilateral write partition,
   - asymmetric heal,
   - repeated partition/heal cycles.
3. `docs/node-failure-matrix.md` partition rows are backed by concrete evidence references.
4. `docs/partition-split-brain-matrix.md` remains current and evidence-linked.

### Current evidence hooks

- `CONSISTENCY_RUN_PARTITION_RECONVERGENCE=true bash scripts/run_consistency_failure_validation.sh`
- `docs/consistency-isolation.md`
- `docs/node-failure-matrix.md`
- `docs/partition-split-brain-matrix.md`

## Domain 2: Non-Functional Behavior (Latency, Throughput, Recovery)

### Objective

Show predictable runtime behavior under representative load with explicit SLO-style boundaries.

### Beta gates

1. benchmark workload profiles are published:
   - write-heavy,
   - read-heavy,
   - mixed OLTP,
   - replication catch-up.
2. trend history exists for p50/p95/p99 latency, throughput, and recovery-time-to-ready.
3. regression budget is enforced in CI or scheduled runs (nightly/weekly).
4. no unresolved critical regressions against the declared beta baseline.

### Required artifacts

- benchmark spec document,
- raw run outputs and summarized trend report,
- pass/fail thresholds committed in repository docs.

Current baseline spec and runner:

- `docs/non-functional-benchmarking.md`
- `scripts/perf/nonfunctional_baseline.sh`

## Domain 3: Security and Adversarial Validation

### Objective

Demonstrate that authentication, authorization, TLS, and replication trust controls hold under adversarial conditions.

### Beta gates

1. adversarial security test set exists and is automated:
   - invalid credential replay,
   - unauthorized replication join attempts,
   - ACL privilege escalation attempts,
   - malformed/invalid transport payload handling.
2. at least one fault-injection pass for security-sensitive code paths is reproducible.
3. security findings triage process and severity rubric are documented.
4. all open high-severity findings are either fixed or explicitly accepted with documented mitigation.

### Required artifacts

- security test matrix,
- threat-model update for affinity and connector boundaries,
- tracked findings log with disposition.

Current baseline spec and runner:

- `docs/security-adversarial-matrix.md`
- `scripts/security/security_adversarial_baseline.sh`

## Domain 4: Operability and Upgrade Safety

### Objective

Prove that operators can run, observe, recover, and upgrade the system with bounded risk.

### Beta gates

1. rolling restart and rolling upgrade scenarios are documented and validated.
2. backward/forward compatibility expectations for WAL/catalog formats are published.
3. runbook coverage exists for:
   - node crash and restart,
   - replication lag/convergence investigation,
   - degraded peer handling,
   - schema migration rollback strategy.
4. observability minimums are defined and present (logs/metrics/events needed for incident diagnosis).

### Required artifacts

- upgrade compatibility contract,
- runbook set,
- operational drill records with observed outcomes.

## Scorecard Status Table

Update at every milestone that changes consistency, replication, security, performance, or upgrade behavior.

| Domain | Current Score | Last Updated | Owner | Blocking Gaps |
| --- | --- | --- | --- | --- |
| Partition and split-brain correctness | Yellow | 2026-07-17 | server/serverlib | All SB rows are executable but remain `Implemented/Partial`; expected-vs-observed publication and full split-brain depth still required |
| Non-functional behavior | Yellow | 2026-07-17 | server/serverlib | benchmark baselines, threshold checks, trend ledger plumbing, and governance cadence/escalation policy are now in place (`docs/non-functional-benchmarking.md`, `scripts/check_artifact_evidence_quality.sh`, `.github/workflows/nightly-evidence.yml`); remaining blocker is accumulating sufficient trend history depth and maintaining critical-regression closure discipline |
| Security/adversarial validation | Yellow | 2026-07-17 | server/serverlib | baseline matrix and findings governance are in place with transport/trust/replay plus mixed schema/object precedence, cross-database ACL target/revoke boundaries, revoke-order sequencing isolation, malformed ACL target rejection, schema/cross-database grant-option invariants (including scoped revoke cleanup and transition-chain isolation), object-level cross-database transition-chain isolation, mixed schema/object transition-chain precedence checks, mixed schema/object grant-option chain scoping checks, explicit non-root delegated grant escalation denial (single-db and cross-db), ACL batch authorization/rejection safeguards (including malformed-batch no-partial-apply, position invariants, alternating malformed grant/revoke atomicity, malformed mixed schema/object batch atomicity, malformed mixed grant-option batch rollback/recovery, and recovery determinism), and parallel contention coverage; continue expanding multi-database privilege abuse depth and keep high-severity findings closed |
| Operability/upgrade safety | Green | 2026-07-17 | server/serverlib | All Domain 4 beta gates are currently satisfied: rolling restart/upgrade matrix is automated and evidence-linked (`HEAD~1`, `HEAD~2`, `HEAD~3`), frozen WAL/catalog compatibility windows are published, runbook coverage is published, and observability minimums are documented and enforced in evidence quality checks. Operability trend history currently meets minimum depth per declared window in `artifacts/trends/operability-trend.json` and is enforced by `scripts/check_artifact_evidence_quality.sh` in `.github/workflows/nightly-evidence.yml`. Maintain Green by keeping window-history thresholds passing and leaving no unresolved high-severity operability regressions. |

## Cadence and Enforcement

1. update this scorecard before changing release posture,
2. block beta declaration unless all domains are `Green`,
3. link any score change to concrete evidence (test IDs, workflow runs, or published artifacts),
4. keep this scorecard consistent with `docs/release.md`, `docs/consistency-isolation.md`, and `docs/node-failure-matrix.md`.
