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
