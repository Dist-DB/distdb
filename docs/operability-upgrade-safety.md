# Operability and Upgrade Safety Contract (Alpha)

This document defines the current operability and upgrade-safety contract for DistDB Developer Alpha.

## Purpose

The goal is to make upgrade and recovery posture explicit, testable, and evidence-linked.

This page covers:

- rolling restart guarantees and limits,
- current upgrade compatibility expectations for WAL and catalog data,
- minimum observability signals required for incident diagnosis,
- executable evidence collection for operability drills.

## Current Guarantee Levels

### Guaranteed

- Single-node restart from durable state is supported through WAL and catalog replay paths documented in [node-failure-matrix.md](node-failure-matrix.md).
- Rolling restart drill automation exists and is repeatable:
  - `bash scripts/e2e/rolling_restart_upgrade_safety.sh`

### Bounded

- Multi-node rolling restart evidence validates a configurable cross-version transition path (old binary -> new binary -> old binary) when both binaries are supplied.
- Compatibility confidence remains bounded to the tested version window and evidence cadence.
- Operational confidence for upgrades remains bounded to published artifact runs and explicit compatibility assertions.

### Not Yet Guaranteed

- Frozen semver compatibility commitments for every WAL/catalog payload type.
- Published compatibility window assertions that extend beyond the tested adjacent-version transition.

## WAL and Catalog Compatibility Expectations (Alpha)

Current operator expectations:

1. WAL replay must fail closed on malformed payloads and avoid silent acceptance.
2. Catalog replay must prefer latest valid state snapshots and reject malformed state transitions.
3. Any format-affecting change must include:
   - migration strategy,
   - replay compatibility notes,
   - rollback behavior notes,
   - evidence links in release documentation.

Until beta, treat format compatibility as explicitly versioned-by-evidence rather than globally guaranteed.

## Rolling Restart and Upgrade-Safety Drill

Run command:

- `bash scripts/e2e/rolling_restart_upgrade_safety.sh`

Optional cross-version inputs:

- `DISTDB_SERVER_BIN_OLD=/path/to/server-old`
- `DISTDB_SERVER_BIN_NEW=/path/to/server-new`

If not provided, both values default to the local `server` binary.

Generated evidence (default):

- `artifacts/e2e/rolling-upgrade-safety-<timestamp>-<pid>/summary.json`
- `artifacts/e2e/rolling-upgrade-safety-<timestamp>-<pid>/manifest.json`
- per-node logs and command outputs in the same directory.

CI workflow:

- `.github/workflows/operability-upgrade-safety.yml`
- Workflow builds both current and previous server binaries and runs the drill as an adjacent-version transition (`HEAD~1` -> `HEAD` -> `HEAD~1`).

## Observability Minimums (Alpha)

The following minimum signals are required for operability drills and incident triage:

1. per-node startup/restart logs,
2. per-drill summary artifact with timing and final state checks,
3. manifest artifact with pass/fail status and git revision,
4. retained CI artifact bundle for each scheduled run.

## Beta Closure Requirements

To mark Domain 4 as green in [beta-confidence-scorecard.md](beta-confidence-scorecard.md):

1. expand cross-version rolling upgrade evidence into a maintained matrix (multiple adjacent and declared-supported version windows),
2. publish frozen WAL/catalog compatibility contract with explicit supported version windows,
3. demonstrate runbook drill execution history over multiple scheduled runs,
4. ensure no unresolved high-severity operability regressions remain open.
