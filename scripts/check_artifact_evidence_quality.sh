#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
PERF_ROOT="${PERF_DATA_ROOT:-$ARTIFACTS_ROOT/perf}"
E2E_ROOT="${SPLIT_BRAIN_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
SECURITY_ROOT="$ARTIFACTS_ROOT/security"
OPERABILITY_ROOT="${OPERABILITY_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
REQUIRE_OPERABILITY_EVIDENCE="${DISTDB_REQUIRE_OPERABILITY_EVIDENCE:-false}"
REQUIRE_OPERABILITY_WINDOW_MATRIX="${DISTDB_REQUIRE_OPERABILITY_WINDOW_MATRIX:-false}"
REQUIRED_OPERABILITY_WINDOWS="${DISTDB_REQUIRED_OPERABILITY_WINDOWS:-head-1,head-2,head-3}"
REQUIRE_OPERABILITY_TREND_HISTORY="${DISTDB_REQUIRE_OPERABILITY_TREND_HISTORY:-false}"
OPERABILITY_TREND_MIN_ENTRIES="${DISTDB_OPERABILITY_TREND_MIN_ENTRIES:-3}"
REQUIRE_NONFUNCTIONAL_TREND_HISTORY="${DISTDB_REQUIRE_NONFUNCTIONAL_TREND_HISTORY:-false}"
NONFUNCTIONAL_TREND_MIN_ENTRIES="${DISTDB_NONFUNCTIONAL_TREND_MIN_ENTRIES:-3}"
NONFUNCTIONAL_TREND_LEDGER="$ARTIFACTS_ROOT/trends/nonfunctional-trend.json"
REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED="${DISTDB_REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED:-false}"
NONFUNCTIONAL_FINDINGS_LOG="${DISTDB_NONFUNCTIONAL_FINDINGS_LOG:-$ROOT_DIR/docs/nonfunctional-findings-log.md}"
REQUIRE_SPLIT_BRAIN_MATRIX_CLOSURE="${DISTDB_REQUIRE_SPLIT_BRAIN_MATRIX_CLOSURE:-false}"
SPLIT_BRAIN_MATRIX_DOC="${DISTDB_SPLIT_BRAIN_MATRIX_DOC:-$ROOT_DIR/docs/partition-split-brain-matrix.md}"
SPLIT_BRAIN_MATRIX_MIN_OBSERVATIONS="${DISTDB_SPLIT_BRAIN_MATRIX_MIN_OBSERVATIONS:-2}"
REQUIRE_SECURITY_HIGH_CRITICAL_FINDINGS_CLOSED="${DISTDB_REQUIRE_SECURITY_HIGH_CRITICAL_FINDINGS_CLOSED:-false}"
SECURITY_FINDINGS_LOG="${DISTDB_SECURITY_FINDINGS_LOG:-$ROOT_DIR/docs/security-findings-log.md}"
REQUIRE_SECURITY_MATRIX_CLOSURE="${DISTDB_REQUIRE_SECURITY_MATRIX_CLOSURE:-false}"
SECURITY_MATRIX_DOC="${DISTDB_SECURITY_MATRIX_DOC:-$ROOT_DIR/docs/security-adversarial-matrix.md}"
OPERABILITY_TREND_LEDGER="$ARTIFACTS_ROOT/trends/operability-trend.json"

fail() {
  echo "[artifact-evidence][fail] $*" >&2
  exit 1
}

ok() {
  echo "[artifact-evidence][ok] $*"
}

latest_dir_by_pattern() {
  local root_dir="$1"
  local name_pattern="$2"
  find "$root_dir" -maxdepth 1 -type d -name "$name_pattern" -print0 2>/dev/null \
    | xargs -0 ls -dt 2>/dev/null \
    | head -n 1 \
    || true
}

assert_file() {
  local file_path="$1"
  local label="$2"
  [[ -f "$file_path" ]] || fail "$label missing: $file_path"
  [[ -s "$file_path" ]] || fail "$label empty: $file_path"
}

assert_manifest_pass() {
  local manifest="$1"
  assert_file "$manifest" "manifest"
  grep -q '"status": "pass"' "$manifest" || fail "manifest does not indicate pass: $manifest"
}

latest_operability_dir_for_label() {
  local label="$1"
  latest_dir_by_pattern "$OPERABILITY_ROOT" "rolling-upgrade-safety-${label}-*"
}

security_dir="$(latest_dir_by_pattern "$SECURITY_ROOT" "security-baseline-*")"
[[ -n "$security_dir" ]] || fail "no security baseline artifact directory found under $SECURITY_ROOT"
assert_file "$security_dir/run.log" "security run log"
assert_manifest_pass "$security_dir/manifest.json"
ok "security artifact bundle validated: $security_dir"

if [[ "$REQUIRE_SECURITY_HIGH_CRITICAL_FINDINGS_CLOSED" == "true" ]]; then
  assert_file "$SECURITY_FINDINGS_LOG" "security findings log"

  if grep -Eq '^\|[^|]*\|[^|]*\|[^|]*\|[^|]*\|[[:space:]]*(Critical|High)[[:space:]]*\|[[:space:]]*(Open|In Progress)[[:space:]]*\|' "$SECURITY_FINDINGS_LOG"; then
    fail "security findings log contains unresolved High/Critical findings: $SECURITY_FINDINGS_LOG"
  fi

  ok "security findings governance validated: no unresolved High/Critical findings in $SECURITY_FINDINGS_LOG"
else
  ok "security high/critical findings gate disabled (set DISTDB_REQUIRE_SECURITY_HIGH_CRITICAL_FINDINGS_CLOSED=true to enforce)"
fi

if [[ "$REQUIRE_SECURITY_MATRIX_CLOSURE" == "true" ]]; then
  assert_file "$SECURITY_MATRIX_DOC" "security adversarial matrix"

  for scenario in SEC-001 SEC-002 SEC-003 SEC-004 SEC-005 SEC-006 SEC-007 SEC-008; do
    grep -Eq "^\|[[:space:]]*$scenario[[:space:]]*\|.*\|[[:space:]]*Implemented/Tested[[:space:]]*\|" "$SECURITY_MATRIX_DOC" \
      || fail "security matrix row not Implemented/Tested for $scenario in $SECURITY_MATRIX_DOC"
  done

  ok "security matrix closure validated: SEC-001..SEC-008 are Implemented/Tested"
else
  ok "security matrix closure gate disabled (set DISTDB_REQUIRE_SECURITY_MATRIX_CLOSURE=true to enforce)"
fi

perf_dir="$(latest_dir_by_pattern "$PERF_ROOT" "nonfunctional-baseline-*")"
[[ -n "$perf_dir" ]] || fail "no non-functional artifact directory found under $PERF_ROOT"
assert_file "$perf_dir/summary.json" "non-functional summary"
assert_file "$perf_dir/manifest.json" "non-functional manifest"
assert_manifest_pass "$perf_dir/manifest.json"
perf_csv_count="$(find "$perf_dir" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')"
[[ "$perf_csv_count" -ge 3 ]] || fail "expected at least 3 non-functional csv files in $perf_dir"
ok "non-functional artifact bundle validated: $perf_dir"

if [[ -f "$NONFUNCTIONAL_TREND_LEDGER" ]]; then
  trend_entry_count="$({ grep -c '"run_id"[[:space:]]*:[[:space:]]*"' "$NONFUNCTIONAL_TREND_LEDGER"; } || true)"
  if [[ -z "$trend_entry_count" ]]; then
    trend_entry_count=0
  fi

  if [[ "$trend_entry_count" -ge "$NONFUNCTIONAL_TREND_MIN_ENTRIES" ]]; then
    ok "non-functional trend history validated: entries=$trend_entry_count min_required=$NONFUNCTIONAL_TREND_MIN_ENTRIES ledger=$NONFUNCTIONAL_TREND_LEDGER"
  elif [[ "$REQUIRE_NONFUNCTIONAL_TREND_HISTORY" == "true" ]]; then
    fail "non-functional trend history insufficient: entries=$trend_entry_count min_required=$NONFUNCTIONAL_TREND_MIN_ENTRIES ledger=$NONFUNCTIONAL_TREND_LEDGER"
  else
    ok "non-functional trend history below target: entries=$trend_entry_count min_target=$NONFUNCTIONAL_TREND_MIN_ENTRIES (set DISTDB_REQUIRE_NONFUNCTIONAL_TREND_HISTORY=true to enforce)"
  fi
elif [[ "$REQUIRE_NONFUNCTIONAL_TREND_HISTORY" == "true" ]]; then
  fail "non-functional trend history required but ledger missing: $NONFUNCTIONAL_TREND_LEDGER"
else
  ok "non-functional trend ledger not found; skipping history gate (set DISTDB_REQUIRE_NONFUNCTIONAL_TREND_HISTORY=true to enforce)"
fi

if [[ "$REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED" == "true" ]]; then
  assert_file "$NONFUNCTIONAL_FINDINGS_LOG" "non-functional findings log"

  if grep -Eq '^\|[^|]*\|[^|]*\|[^|]*\|[[:space:]]*Critical[[:space:]]*\|[[:space:]]*(Open|In Progress)[[:space:]]*\|' "$NONFUNCTIONAL_FINDINGS_LOG"; then
    fail "non-functional findings log contains unresolved Critical findings: $NONFUNCTIONAL_FINDINGS_LOG"
  fi

  ok "non-functional findings governance validated: no unresolved Critical findings in $NONFUNCTIONAL_FINDINGS_LOG"
else
  ok "non-functional critical findings gate disabled (set DISTDB_REQUIRE_NONFUNCTIONAL_CRITICAL_FINDINGS_CLOSED=true to enforce)"
fi

e2e_dir="$(latest_dir_by_pattern "$E2E_ROOT" "split-brain-evidence-*")"
[[ -n "$e2e_dir" ]] || fail "no split-brain artifact directory found under $E2E_ROOT"
assert_file "$e2e_dir/observation-report.md" "split-brain observation report"
assert_file "$e2e_dir/summary.txt" "split-brain summary"
assert_manifest_pass "$e2e_dir/manifest.json"
grep -q '^PASS$' "$e2e_dir/summary.txt" || fail "split-brain summary does not indicate PASS: $e2e_dir/summary.txt"
for scenario in SB-001 SB-002 SB-003 SB-004; do
  assert_file "$e2e_dir/${scenario}.log" "split-brain scenario log $scenario"
done
ok "split-brain artifact bundle validated: $e2e_dir"

if [[ "$REQUIRE_SPLIT_BRAIN_MATRIX_CLOSURE" == "true" ]]; then
  assert_file "$SPLIT_BRAIN_MATRIX_DOC" "split-brain matrix document"

  for scenario in SB-001 SB-002 SB-003 SB-004; do
    grep -Eq "^\|[[:space:]]*$scenario[[:space:]]*\|.*\|[[:space:]]*Implemented/Tested[[:space:]]*\|" "$SPLIT_BRAIN_MATRIX_DOC" \
      || fail "split-brain matrix row not Implemented/Tested for $scenario in $SPLIT_BRAIN_MATRIX_DOC"

    observation_count="$({ grep -Ec "^\|[[:space:]]*[0-9]{4}-[0-9]{2}-[0-9]{2}[[:space:]]*\|[[:space:]]*$scenario[[:space:]]*\|.*\|[[:space:]]*Pass[[:space:]]*\|" "$SPLIT_BRAIN_MATRIX_DOC"; } || true)"
    if [[ -z "$observation_count" ]]; then
      observation_count=0
    fi

    [[ "$observation_count" -ge "$SPLIT_BRAIN_MATRIX_MIN_OBSERVATIONS" ]] \
      || fail "split-brain matrix observations insufficient for $scenario: entries=$observation_count min_required=$SPLIT_BRAIN_MATRIX_MIN_OBSERVATIONS doc=$SPLIT_BRAIN_MATRIX_DOC"
  done

  ok "split-brain matrix closure validated: all SB rows Implemented/Tested with >=$SPLIT_BRAIN_MATRIX_MIN_OBSERVATIONS passing observations each"
else
  ok "split-brain matrix closure gate disabled (set DISTDB_REQUIRE_SPLIT_BRAIN_MATRIX_CLOSURE=true to enforce)"
fi

if [[ "$REQUIRE_OPERABILITY_WINDOW_MATRIX" == "true" ]]; then
  required_windows_normalized="$(printf '%s' "$REQUIRED_OPERABILITY_WINDOWS" | tr ',' ' ')"
  [[ -n "$required_windows_normalized" ]] || fail "DISTDB_REQUIRED_OPERABILITY_WINDOWS is empty while matrix enforcement is enabled"

  for window_label in $required_windows_normalized; do
    operability_window_dir="$(latest_operability_dir_for_label "$window_label")"
    [[ -n "$operability_window_dir" ]] || fail "operability evidence missing for required window '$window_label' under $OPERABILITY_ROOT"
    assert_file "$operability_window_dir/summary.json" "operability summary ($window_label)"
    assert_file "$operability_window_dir/manifest.json" "operability manifest ($window_label)"
    assert_manifest_pass "$operability_window_dir/manifest.json"
    grep -q "\"window_label\": \"$window_label\"" "$operability_window_dir/summary.json" || fail "operability summary window label mismatch for '$window_label': $operability_window_dir/summary.json"
    grep -q "\"window_label\": \"$window_label\"" "$operability_window_dir/manifest.json" || fail "operability manifest window label mismatch for '$window_label': $operability_window_dir/manifest.json"
    ok "operability matrix window validated: label=$window_label dir=$operability_window_dir"
  done

  if [[ -f "$OPERABILITY_TREND_LEDGER" ]]; then
    for window_label in $required_windows_normalized; do
      window_entry_count="$({ grep -c "\"window_label\"[[:space:]]*:[[:space:]]*\"$window_label\"" "$OPERABILITY_TREND_LEDGER"; } || true)"
      if [[ -z "$window_entry_count" ]]; then
        window_entry_count=0
      fi

      if [[ "$window_entry_count" -ge "$OPERABILITY_TREND_MIN_ENTRIES" ]]; then
        ok "operability trend history validated: window=$window_label entries=$window_entry_count min_required=$OPERABILITY_TREND_MIN_ENTRIES ledger=$OPERABILITY_TREND_LEDGER"
      elif [[ "$REQUIRE_OPERABILITY_TREND_HISTORY" == "true" ]]; then
        fail "operability trend history insufficient: window=$window_label entries=$window_entry_count min_required=$OPERABILITY_TREND_MIN_ENTRIES ledger=$OPERABILITY_TREND_LEDGER"
      else
        ok "operability trend history below target: window=$window_label entries=$window_entry_count min_target=$OPERABILITY_TREND_MIN_ENTRIES (set DISTDB_REQUIRE_OPERABILITY_TREND_HISTORY=true to enforce)"
      fi
    done
  elif [[ "$REQUIRE_OPERABILITY_TREND_HISTORY" == "true" ]]; then
    fail "operability trend history required but ledger missing: $OPERABILITY_TREND_LEDGER"
  else
    ok "operability trend ledger not found; skipping history gate (set DISTDB_REQUIRE_OPERABILITY_TREND_HISTORY=true to enforce)"
  fi
elif [[ "$REQUIRE_OPERABILITY_EVIDENCE" == "true" ]]; then
  operability_dir="$(latest_dir_by_pattern "$OPERABILITY_ROOT" "rolling-upgrade-safety-*")"
  [[ -n "$operability_dir" ]] || fail "operability evidence required but no rolling-upgrade-safety artifact directory found under $OPERABILITY_ROOT"
  assert_file "$operability_dir/summary.json" "operability summary"
  assert_file "$operability_dir/manifest.json" "operability manifest"
  assert_manifest_pass "$operability_dir/manifest.json"
  ok "operability artifact bundle validated: $operability_dir"
else
  ok "operability artifact bundle not found; skipping (set DISTDB_REQUIRE_OPERABILITY_EVIDENCE=true to enforce)"
fi

ok "artifact evidence quality checks passed"
