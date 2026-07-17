#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "[unilateral-write-delayed-heal] validating unilateral writer partition and delayed-heal invariants"

echo "[unilateral-write-delayed-heal] server: stream cursor catch-up does not skip unseen streams"
(
  cd "$ROOT_DIR/server"
  cargo test wal_export_with_stream_cursors_does_not_skip_unseen_streams -- --nocapture
)

echo "[unilateral-write-delayed-heal] server: stale schema revision wal import is rejected and database returns ready"
(
  cd "$ROOT_DIR/server"
  cargo test affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready -- --nocapture
)

echo "[unilateral-write-delayed-heal] server: schema sync failure returns database to ready"
(
  cd "$ROOT_DIR/server"
  cargo test affinity_schema_sync_failure_returns_database_to_ready -- --nocapture
)

echo "[unilateral-write-delayed-heal] serverlib: affinity checkpoint integration preserves convergence progress"
(
  cd "$ROOT_DIR/serverlib"
  cargo test processor_checkpoint_integration -- --nocapture
)

echo "[unilateral-write-delayed-heal] suite passed"
