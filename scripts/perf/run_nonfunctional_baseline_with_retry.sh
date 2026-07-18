#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAX_ATTEMPTS="${DISTDB_NONFUNCTIONAL_RUN_ATTEMPTS:-2}"

log() {
  echo "[nonfunctional-baseline-retry] $*"
}

run_once() {
  (
    cd "$ROOT_DIR"
    bash scripts/perf/nonfunctional_baseline.sh
  )
}

check_once() {
  (
    cd "$ROOT_DIR"
    bash scripts/perf/check_nonfunctional_thresholds.sh
  )
}

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  log "attempt=$attempt max_attempts=$MAX_ATTEMPTS running baseline"

  if run_once; then
    log "attempt=$attempt checking thresholds"
    if check_once; then
      log "attempt=$attempt succeeded"
      exit 0
    fi

    log "attempt=$attempt threshold check failed"
  else
    log "attempt=$attempt baseline run failed"
  fi

  if [[ "$attempt" -lt "$MAX_ATTEMPTS" ]]; then
    log "retrying with a fresh baseline run"
  fi
done

log "all attempts exhausted"
exit 1
