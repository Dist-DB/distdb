#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
DATA_ROOT="${SPLIT_BRAIN_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
RUN_STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"

mkdir -p "$DATA_ROOT"

ts="$(date +%Y%m%d-%H%M%S)"
run_dir="$DATA_ROOT/split-brain-evidence-$ts-$$"
mkdir -p "$run_dir"

report_file="$run_dir/observation-report.md"
summary_file="$run_dir/summary.txt"
manifest_file="$run_dir/manifest.json"
AUTO_APPEND_OBSERVATIONS="${CONSISTENCY_AUTO_APPEND_SPLIT_BRAIN_OBSERVATIONS:-false}"

cat >"$report_file" <<'MD'
# Split-Brain Evidence Observation Report

| Scenario ID | Script | Expected Outcome | Observed Outcome | Result | Log |
| --- | --- | --- | --- | --- | --- |
MD

overall_result=0

write_manifest() {
  local exit_code="$1"
  local status="fail"
  if [[ "$exit_code" -eq 0 ]]; then
    status="pass"
  fi

  cat >"$manifest_file" <<JSON
{
  "run_id": "$(basename "$run_dir")",
  "kind": "split_brain_evidence_bundle",
  "status": "$status",
  "exit_code": $exit_code,
  "started_at_utc": "$RUN_STARTED_UTC",
  "finished_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$GIT_SHA",
  "artifacts_dir": "$run_dir",
  "report_file": "$report_file",
  "summary_file": "$summary_file",
  "scenarios": ["SB-001", "SB-002", "SB-003", "SB-004"]
}
JSON
}

run_stage() {
  local scenario_id="$1"
  local script_name="$2"
  local expected="$3"

  local script_path="$ROOT_DIR/scripts/e2e/$script_name"
  local log_file="$run_dir/${scenario_id}.log"
  local observed
  local result

  if bash "$script_path" >"$log_file" 2>&1; then
    observed="Stage completed successfully with non-zero test execution and suite pass"
    result="Pass"
  else
    observed="Stage failed; inspect log for failure details"
    result="Fail"
    overall_result=1
  fi

  printf '| %s | %s | %s | %s | %s | %s |\n' \
    "$scenario_id" \
    "$script_name" \
    "$expected" \
    "$observed" \
    "$result" \
    "$log_file" \
    >>"$report_file"
}

echo "[split-brain-evidence] run_dir=$run_dir"

run_stage "SB-001" "partition_reconvergence.sh" "Reject invalid schema/WAL state and recover to ready with convergence progression"
run_stage "SB-002" "split_brain_dual_primary.sh" "Deterministic conflict behavior and no partial durability leakage"
run_stage "SB-003" "unilateral_write_delayed_heal.sh" "Stream-aware catch-up and deterministic delayed-heal recovery"
run_stage "SB-004" "repeated_partition_heal_cycles.sh" "Stable repeated-cycle convergence and conflict safety"

if [[ "$overall_result" -eq 0 ]]; then
  echo "PASS" >"$summary_file"
  echo "[split-brain-evidence] bundle passed"

  if [[ "$AUTO_APPEND_OBSERVATIONS" == "1" || "$AUTO_APPEND_OBSERVATIONS" == "true" || "$AUTO_APPEND_OBSERVATIONS" == "yes" || "$AUTO_APPEND_OBSERVATIONS" == "on" ]]; then
    echo "[split-brain-evidence] appending observations to matrix"
    SPLIT_BRAIN_REPORT_FILE="$report_file" bash "$ROOT_DIR/scripts/e2e/append_split_brain_observations.sh"
  fi
else
  echo "FAIL" >"$summary_file"
  echo "[split-brain-evidence] bundle failed"
fi

echo "[split-brain-evidence] report=$report_file"
echo "[split-brain-evidence] summary=$summary_file"
write_manifest "$overall_result"
echo "[split-brain-evidence] manifest=$manifest_file"

cat "$report_file"

exit "$overall_result"
