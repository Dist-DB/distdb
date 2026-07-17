#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CYCLES="${SPLIT_BRAIN_CYCLES:-3}"

if ! [[ "$CYCLES" =~ ^[0-9]+$ ]] || [[ "$CYCLES" -lt 1 ]]; then
  echo "[repeated-partition-heal][fail] SPLIT_BRAIN_CYCLES must be a positive integer"
  exit 1
fi

echo "[repeated-partition-heal] validating repeated partition/heal invariants cycles=$CYCLES"

run_cycle() {
  local cycle="$1"

  echo "[repeated-partition-heal] cycle=$cycle server: stream cursor catch-up remains stable"
  (
    cd "$ROOT_DIR/server"
    cargo test wal_export_with_stream_cursors_does_not_skip_unseen_streams -- --nocapture
  )

  echo "[repeated-partition-heal] cycle=$cycle server: stale schema revision protection remains stable"
  (
    cd "$ROOT_DIR/server"
    cargo test affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready -- --nocapture
  )

  echo "[repeated-partition-heal] cycle=$cycle server: concurrent conflict rejection remains stable"
  (
    cd "$ROOT_DIR/server"
    cargo test snapshot_isolation_rejects_concurrent_write_write_conflicts -- --nocapture
  )

  echo "[repeated-partition-heal] cycle=$cycle serverlib: checkpoint progression remains stable"
  (
    cd "$ROOT_DIR/serverlib"
    cargo test processor_checkpoint_integration -- --nocapture
  )
}

for cycle in $(seq 1 "$CYCLES"); do
  run_cycle "$cycle"
done

echo "[repeated-partition-heal] suite passed"
