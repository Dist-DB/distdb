# Non-Functional Benchmarking Baseline

This document defines the first benchmark baseline for non-functional beta confidence.

## Purpose

Establish repeatable evidence for:

- latency percentiles ($p50$, $p95$, $p99$),
- throughput (operations per second),
- recovery-time-to-ready after restart.

## Baseline Runner

Command:

- `bash scripts/perf/nonfunctional_baseline.sh`

Optional sizing knobs:

- `PERF_WRITE_OPS` (default `60`)
- `PERF_READ_OPS` (default `90`)
- `PERF_MIXED_OPS` (default `80`)
- `PERF_SEED_ROWS` (default `200`)
- `DISTDB_ARTIFACTS_ROOT` (default `artifacts/` under repo root)
- `PERF_DATA_ROOT` (overrides the non-functional artifact subdirectory)

Output location:

- `artifacts/perf/nonfunctional-baseline-<timestamp>-<pid>/summary.json`
- same directory contains per-profile latency CSV artifacts.

Threshold check command:

- `bash scripts/perf/check_nonfunctional_thresholds.sh`

Trend ledger append command:

- `bash scripts/append_artifact_trends.sh`
- writes JSON trend ledgers under `artifacts/trends/`:
	- `security-trend.json`
	- `nonfunctional-trend.json`
	- `split-brain-trend.json`

One-command governance accumulation cycle:

- `bash scripts/perf/run_nonfunctional_governance_cycle.sh`
- runs baseline + threshold checks + trend append, then reports current trend-entry depth and remaining runs needed to satisfy governance minimum history target.

## Profile Definitions

1. write-heavy
- repeated single-row insert operations.

2. read-heavy
- repeated point-read operations.

3. mixed
- alternating read and write operations.

4. recovery
- process restart and time-to-ready measurement.

## Evidence Schema

`summary.json` contains:

- profile operation count,
- `p50_ms`, `p95_ms`, `p99_ms`,
- `total_ms`,
- `throughput_ops_per_sec`,
- `recovery_to_ready_ms`.

## Promotion Guidance

To move Non-functional from `Red` to `Yellow` in the scorecard:

1. run baseline at least once and commit/publish evidence references,
2. define initial acceptable ranges per profile,
3. add periodic execution (scheduled CI) and artifact retention.

To move from `Yellow` to `Green`:

1. maintain trend history,
2. enforce regression budgets,
3. close critical regressions before release declaration.

## Governance Policy

### Ownership and cadence

- primary owners: `server` and `serverlib` maintainers.
- required review cadence: weekly review of latest `nonfunctional-trend.json` entries.
- required review trigger: any threshold failure in CI/nightly requires same-day owner acknowledgement.

### Regression disposition model

Classify each failed non-functional run as one of:

1. `Critical regression`: performance/recovery degradation blocks release progression until fixed or explicitly accepted with mitigation and expiry.
2. `Accepted variance`: bounded environment/noise-related deviation with documented rationale and follow-up window.
3. `Invalid run`: infrastructure/test harness issue; rerun required and original run excluded from posture decisions.

All failed runs must be logged with disposition in release-tracking updates before release posture changes.

### Escalation rules

1. one `Critical regression` in the latest nightly window blocks non-functional promotion and release posture upgrades.
2. two consecutive non-functional threshold failures (any profile) trigger mandatory owner review and remediation plan.
3. unresolved critical non-functional regressions are incompatible with `Beta Ready` posture.

### Minimum trend-history gate

For confidence claims beyond initial baseline:

- require at least `3` ingested non-functional trend entries in `artifacts/trends/nonfunctional-trend.json`.
- enforce this gate in evidence validation for scheduled/nightly runs.

## Initial Regression Budgets

Current default budgets enforced by `check_nonfunctional_thresholds.sh`:

- write-heavy latency: `p95 <= 120ms`, `p99 <= 180ms`
- read-heavy latency: `p95 <= 90ms`, `p99 <= 140ms`
- mixed latency: `p95 <= 130ms`, `p99 <= 200ms`
- recovery: `recovery_to_ready_ms <= 800ms`
- throughput floors:
	- write-heavy `>= 15 ops/s`
	- read-heavy `>= 20 ops/s`
	- mixed `>= 15 ops/s`

All budgets are overrideable via environment variables (`PERF_MAX_*`, `PERF_MIN_*`) for calibration and platform-specific tuning.
