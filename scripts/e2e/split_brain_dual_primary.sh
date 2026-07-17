#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "[split-brain-dual-primary] validating dual-primary conflict safety invariants"

echo "[split-brain-dual-primary] server: concurrent write/write conflict rejection"
(
  cd "$ROOT_DIR/server"
  cargo test snapshot_isolation_rejects_concurrent_write_write_conflicts -- --nocapture
)

echo "[split-brain-dual-primary] server: serializable write-skew rejection"
(
  cd "$ROOT_DIR/server"
  cargo test serializable_rejects_write_skew_across_disjoint_rows -- --nocapture
)

echo "[split-brain-dual-primary] server: rollback clears staged state for deterministic recovery"
(
  cd "$ROOT_DIR/server"
  cargo test rollback_discards_staged_queries_for_session -- --nocapture
)

echo "[split-brain-dual-primary] server: failed commit validation does not leak partial durability"
(
  cd "$ROOT_DIR/server"
  cargo test failed_commit_validation_leaves_real_wal_and_indexes_clean -- --nocapture
)

echo "[split-brain-dual-primary] suite passed"
