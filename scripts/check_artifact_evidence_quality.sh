#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
PERF_ROOT="${PERF_DATA_ROOT:-$ARTIFACTS_ROOT/perf}"
E2E_ROOT="${SPLIT_BRAIN_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
SECURITY_ROOT="$ARTIFACTS_ROOT/security"

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

security_dir="$(latest_dir_by_pattern "$SECURITY_ROOT" "security-baseline-*")"
[[ -n "$security_dir" ]] || fail "no security baseline artifact directory found under $SECURITY_ROOT"
assert_file "$security_dir/run.log" "security run log"
assert_manifest_pass "$security_dir/manifest.json"
ok "security artifact bundle validated: $security_dir"

perf_dir="$(latest_dir_by_pattern "$PERF_ROOT" "nonfunctional-baseline-*")"
[[ -n "$perf_dir" ]] || fail "no non-functional artifact directory found under $PERF_ROOT"
assert_file "$perf_dir/summary.json" "non-functional summary"
assert_file "$perf_dir/manifest.json" "non-functional manifest"
assert_manifest_pass "$perf_dir/manifest.json"
perf_csv_count="$(find "$perf_dir" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')"
[[ "$perf_csv_count" -ge 3 ]] || fail "expected at least 3 non-functional csv files in $perf_dir"
ok "non-functional artifact bundle validated: $perf_dir"

e2e_dir="$(latest_dir_by_pattern "$E2E_ROOT" "split-brain-evidence-*")"
[[ -n "$e2e_dir" ]] || fail "no split-brain artifact directory found under $E2E_ROOT"
assert_file "$e2e_dir/observation-report.md" "split-brain observation report"
assert_file "$e2e_dir/summary.txt" "split-brain summary"
assert_manifest_pass "$e2e_dir/manifest.json"
for scenario in SB-001 SB-002 SB-003 SB-004; do
  assert_file "$e2e_dir/${scenario}.log" "split-brain scenario log $scenario"
done
ok "split-brain artifact bundle validated: $e2e_dir"

ok "artifact evidence quality checks passed"
