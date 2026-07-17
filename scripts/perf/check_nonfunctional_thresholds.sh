#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
PERF_DATA_ROOT="${PERF_DATA_ROOT:-$ARTIFACTS_ROOT/perf}"
LEGACY_PERF_DATA_ROOT="$ROOT_DIR/server/data/perf"

resolve_summary_file() {
  if [[ -n "${PERF_SUMMARY_JSON:-}" ]]; then
    printf '%s\n' "$PERF_SUMMARY_JSON"
    return 0
  fi

  local latest_summary
  latest_summary="$(ls -dt "$PERF_DATA_ROOT"/nonfunctional-baseline-*/summary.json 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest_summary" ]]; then
    latest_summary="$(ls -dt "$LEGACY_PERF_DATA_ROOT"/nonfunctional-baseline-*/summary.json 2>/dev/null | head -n 1 || true)"
  fi
  if [[ -z "$latest_summary" ]]; then
    return 1
  fi

  printf '%s\n' "$latest_summary"
}

SUMMARY_JSON="$(resolve_summary_file || true)"
if [[ -z "$SUMMARY_JSON" ]] || [[ ! -f "$SUMMARY_JSON" ]]; then
  echo "[nonfunctional-thresholds][fail] summary json not found"
  exit 1
fi

MAX_WRITE_P95_MS="${PERF_MAX_WRITE_P95_MS:-120}"
MAX_WRITE_P99_MS="${PERF_MAX_WRITE_P99_MS:-180}"
MAX_READ_P95_MS="${PERF_MAX_READ_P95_MS:-90}"
MAX_READ_P99_MS="${PERF_MAX_READ_P99_MS:-140}"
MAX_MIXED_P95_MS="${PERF_MAX_MIXED_P95_MS:-130}"
MAX_MIXED_P99_MS="${PERF_MAX_MIXED_P99_MS:-200}"
MAX_RECOVERY_READY_MS="${PERF_MAX_RECOVERY_READY_MS:-800}"

MIN_WRITE_THROUGHPUT="${PERF_MIN_WRITE_THROUGHPUT:-15}"
MIN_READ_THROUGHPUT="${PERF_MIN_READ_THROUGHPUT:-20}"
MIN_MIXED_THROUGHPUT="${PERF_MIN_MIXED_THROUGHPUT:-15}"

mkdir -p "$PERF_DATA_ROOT"
metrics_tmp="$PERF_DATA_ROOT/.nonfunctional-metrics-$$.tmp"
trap 'rm -f "$metrics_tmp"' EXIT

awk '
  function trim(s) {
    gsub(/^[[:space:]]+/, "", s)
    gsub(/[[:space:]]+$/, "", s)
    return s
  }

  function num_from_line(line, out) {
    out = line
    gsub(/.*: /, "", out)
    gsub(/,/, "", out)
    gsub(/"/, "", out)
    out = trim(out)
    return out
  }

  /"write_heavy": \{/ { section = "write"; next }
  /"read_heavy": \{/ { section = "read"; next }
  /"mixed": \{/ { section = "mixed"; next }

  /"recovery_to_ready_ms":/ {
    print "RECOVERY_READY_MS=" num_from_line($0)
  }

  section == "write" && /"p95_ms":/ { print "WRITE_P95_MS=" num_from_line($0) }
  section == "write" && /"p99_ms":/ { print "WRITE_P99_MS=" num_from_line($0) }
  section == "write" && /"throughput_ops_per_sec":/ { print "WRITE_THROUGHPUT=" num_from_line($0) }

  section == "read" && /"p95_ms":/ { print "READ_P95_MS=" num_from_line($0) }
  section == "read" && /"p99_ms":/ { print "READ_P99_MS=" num_from_line($0) }
  section == "read" && /"throughput_ops_per_sec":/ { print "READ_THROUGHPUT=" num_from_line($0) }

  section == "mixed" && /"p95_ms":/ { print "MIXED_P95_MS=" num_from_line($0) }
  section == "mixed" && /"p99_ms":/ { print "MIXED_P99_MS=" num_from_line($0) }
  section == "mixed" && /"throughput_ops_per_sec":/ { print "MIXED_THROUGHPUT=" num_from_line($0) }

  /\}/ {
    if (section == "write" || section == "read" || section == "mixed") {
      section = ""
    }
  }
' "$SUMMARY_JSON" >"$metrics_tmp"

if [[ ! -s "$metrics_tmp" ]]; then
  echo "[nonfunctional-thresholds][fail] could not parse metrics from $SUMMARY_JSON"
  exit 1
fi

while IFS='=' read -r key value; do
  case "$key" in
    WRITE_P95_MS) WRITE_P95_MS="$value" ;;
    WRITE_P99_MS) WRITE_P99_MS="$value" ;;
    WRITE_THROUGHPUT) WRITE_THROUGHPUT="$value" ;;
    READ_P95_MS) READ_P95_MS="$value" ;;
    READ_P99_MS) READ_P99_MS="$value" ;;
    READ_THROUGHPUT) READ_THROUGHPUT="$value" ;;
    MIXED_P95_MS) MIXED_P95_MS="$value" ;;
    MIXED_P99_MS) MIXED_P99_MS="$value" ;;
    MIXED_THROUGHPUT) MIXED_THROUGHPUT="$value" ;;
    RECOVERY_READY_MS) RECOVERY_READY_MS="$value" ;;
  esac
done <"$metrics_tmp"

required_vars=(
  WRITE_P95_MS WRITE_P99_MS WRITE_THROUGHPUT
  READ_P95_MS READ_P99_MS READ_THROUGHPUT
  MIXED_P95_MS MIXED_P99_MS MIXED_THROUGHPUT
  RECOVERY_READY_MS
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "[nonfunctional-thresholds][fail] missing parsed metric: $var_name"
    exit 1
  fi
done

failures=0

assert_max() {
  local label="$1"
  local value="$2"
  local max_value="$3"

  if ! awk -v v="$value" -v m="$max_value" 'BEGIN { exit !(v <= m) }'; then
    echo "[nonfunctional-thresholds][fail] $label value=$value exceeds max=$max_value"
    failures=$((failures + 1))
  else
    echo "[nonfunctional-thresholds][ok] $label value=$value within max=$max_value"
  fi
}

assert_min() {
  local label="$1"
  local value="$2"
  local min_value="$3"

  if ! awk -v v="$value" -v m="$min_value" 'BEGIN { exit !(v >= m) }'; then
    echo "[nonfunctional-thresholds][fail] $label value=$value below min=$min_value"
    failures=$((failures + 1))
  else
    echo "[nonfunctional-thresholds][ok] $label value=$value above min=$min_value"
  fi
}

assert_max "write_p95_ms" "$WRITE_P95_MS" "$MAX_WRITE_P95_MS"
assert_max "write_p99_ms" "$WRITE_P99_MS" "$MAX_WRITE_P99_MS"
assert_max "read_p95_ms" "$READ_P95_MS" "$MAX_READ_P95_MS"
assert_max "read_p99_ms" "$READ_P99_MS" "$MAX_READ_P99_MS"
assert_max "mixed_p95_ms" "$MIXED_P95_MS" "$MAX_MIXED_P95_MS"
assert_max "mixed_p99_ms" "$MIXED_P99_MS" "$MAX_MIXED_P99_MS"
assert_max "recovery_to_ready_ms" "$RECOVERY_READY_MS" "$MAX_RECOVERY_READY_MS"

assert_min "write_throughput_ops_per_sec" "$WRITE_THROUGHPUT" "$MIN_WRITE_THROUGHPUT"
assert_min "read_throughput_ops_per_sec" "$READ_THROUGHPUT" "$MIN_READ_THROUGHPUT"
assert_min "mixed_throughput_ops_per_sec" "$MIXED_THROUGHPUT" "$MIN_MIXED_THROUGHPUT"

if [[ "$failures" -gt 0 ]]; then
  echo "[nonfunctional-thresholds][fail] threshold violations=$failures summary=$SUMMARY_JSON"
  exit 1
fi

echo "[nonfunctional-thresholds][ok] all thresholds passed summary=$SUMMARY_JSON"
