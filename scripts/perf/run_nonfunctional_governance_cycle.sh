#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
TREND_LEDGER="${DISTDB_NONFUNCTIONAL_TREND_LEDGER:-$ARTIFACTS_ROOT/trends/nonfunctional-trend.json}"
MIN_TREND_ENTRIES="${DISTDB_NONFUNCTIONAL_TREND_MIN_ENTRIES:-3}"

log() {
  echo "[nonfunctional-governance] $*"
}

count_trend_entries() {
  local ledger_path="$1"
  if [[ ! -f "$ledger_path" ]]; then
    echo 0
    return 0
  fi

  local count
  count="$({ grep -c '"run_id"[[:space:]]*:[[:space:]]*"' "$ledger_path"; } || true)"
  if [[ -z "$count" ]]; then
    count=0
  fi
  echo "$count"
}

log "starting non-functional governance cycle"
(
  cd "$ROOT_DIR"
  bash scripts/perf/run_nonfunctional_baseline_with_retry.sh
)

log "checking non-functional thresholds"
(
  cd "$ROOT_DIR"
  bash scripts/perf/check_nonfunctional_thresholds.sh
)

log "appending trend ledgers"
(
  cd "$ROOT_DIR"
  bash scripts/append_artifact_trends.sh
)

entry_count="$(count_trend_entries "$TREND_LEDGER")"
remaining=$((MIN_TREND_ENTRIES - entry_count))
if (( remaining < 0 )); then
  remaining=0
fi

log "trend history status ledger=$TREND_LEDGER entries=$entry_count target=$MIN_TREND_ENTRIES"
if (( remaining == 0 )); then
  log "history target satisfied"
else
  log "history target not yet satisfied; additional successful cycle(s) needed=$remaining"
fi

log "cycle complete"
