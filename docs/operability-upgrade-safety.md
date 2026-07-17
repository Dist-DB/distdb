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

- Multi-node rolling restart evidence validates a maintained cross-version transition matrix (old binary -> new binary -> old binary) for declared windows.
- Compatibility confidence remains bounded to the tested version window and evidence cadence.
- Operational confidence for upgrades remains bounded to published artifact runs and explicit compatibility assertions.

### Not Yet Guaranteed

- Full semver compatibility commitments for every historical WAL/catalog payload version.
- Upgrade guarantees outside the declared supported window matrix.

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

## Frozen Compatibility Window Assertions

The following compatibility windows are currently declared as supported for rolling upgrade and rollback drills, and are enforced by scheduled evidence runs.

| Window Label | Old Ref | New Ref | Upgrade Path | Rollback Path | Status |
| --- | --- | --- | --- | --- | --- |
| head-1 | `HEAD~1` | `HEAD` | Supported | Supported | Active |
| head-2 | `HEAD~2` | `HEAD` | Supported | Supported | Active |
| head-3 | `HEAD~3` | `HEAD` | Supported | Supported | Active |

Assertions:

1. WAL replay compatibility is guaranteed only for the declared windows above.
2. Catalog replay compatibility is guaranteed only for the declared windows above.
3. Upgrade or rollback attempts outside declared windows are unsupported until a window is added and evidenced.
4. Any format-affecting change must either preserve current windows or explicitly revise this table with new evidence links.

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
- Workflow builds the current server binary and a matrix of old refs (`HEAD~1`, `HEAD~2`, `HEAD~3`) and runs the drill per window.

## Observability Minimums (Alpha)

The following minimum signals are required for operability drills and incident triage:

1. per-node startup/restart logs,
2. per-drill summary artifact with timing and final state checks,
3. manifest artifact with pass/fail status and git revision,
4. retained CI artifact bundle for each scheduled run.

## Beta Closure Requirements

To mark Domain 4 as green in [beta-confidence-scorecard.md](beta-confidence-scorecard.md):

1. expand cross-version rolling upgrade evidence into a maintained matrix (multiple adjacent and declared-supported version windows),
2. keep the frozen WAL/catalog compatibility table current with evidence-backed window updates,
3. demonstrate runbook drill execution history over multiple scheduled runs,
4. ensure no unresolved high-severity operability regressions remain open.

## Domain 4 Closure Status (2026-07-17)

Current status against Domain 4 beta gates in `docs/beta-confidence-scorecard.md`:

1. Rolling restart and rolling upgrade scenarios are documented and validated: satisfied.
2. Backward/forward WAL/catalog compatibility expectations are published with declared windows: satisfied.
3. Runbook coverage for crash/restart, lag investigation, degraded peers, and rollback strategy is published: satisfied.
4. Observability minimums and operational drill evidence collection are defined and enforced: satisfied.

Current evidence posture:

- Compatibility-window drill matrix is automated for `head-1`, `head-2`, and `head-3` in `.github/workflows/operability-upgrade-safety.yml` and `.github/workflows/nightly-evidence.yml`.
- Window labels and refs are embedded in drill outputs (`summary.json` and `manifest.json`) by `scripts/e2e/rolling_restart_upgrade_safety.sh`.
- Per-window history depth is tracked in `artifacts/trends/operability-trend.json` and enforced by `scripts/check_artifact_evidence_quality.sh`.

Maintenance condition for keeping Domain 4 Green:

- Keep declared compatibility-window evidence depth at or above enforcement thresholds and keep high-severity operability regressions at zero unresolved.
