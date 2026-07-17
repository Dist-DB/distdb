#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "[partition-reconvergence] validating affinity sync recovery and reconvergence invariants"

echo "[partition-reconvergence] server: schema sync failure returns database to ready"
(
  cd "$ROOT_DIR/server"
  cargo test affinity_schema_sync_failure_returns_database_to_ready -- --nocapture
)

echo "[partition-reconvergence] server: stale wal import ignored and database returns to ready"
(
  cd "$ROOT_DIR/server"
  cargo test affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready -- --nocapture
)

echo "[partition-reconvergence] serverlib: replication executor tracks phase progression"
(
  cd "$ROOT_DIR/serverlib"
  cargo test executor_tracks_phase_progression -- --nocapture
)

echo "[partition-reconvergence] serverlib: checkpoint integration persists recovery position"
(
  cd "$ROOT_DIR/serverlib"
  cargo test processor_checkpoint_integration -- --nocapture
)

echo "[partition-reconvergence] suite passed"
