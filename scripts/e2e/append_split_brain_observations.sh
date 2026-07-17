#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MATRIX_FILE="$ROOT_DIR/docs/partition-split-brain-matrix.md"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
PRIMARY_DATA_ROOT="${SPLIT_BRAIN_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
LEGACY_DATA_ROOT="$ROOT_DIR/server/data/e2e"

resolve_report_file() {
  if [[ -n "${SPLIT_BRAIN_REPORT_FILE:-}" ]]; then
    printf '%s\n' "$SPLIT_BRAIN_REPORT_FILE"
    return 0
  fi

  local latest_dir
  latest_dir="$(ls -dt "$PRIMARY_DATA_ROOT"/split-brain-evidence-* 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest_dir" ]]; then
    latest_dir="$(ls -dt "$LEGACY_DATA_ROOT"/split-brain-evidence-* 2>/dev/null | head -n 1 || true)"
  fi
  if [[ -z "$latest_dir" ]]; then
    return 1
  fi

  printf '%s\n' "$latest_dir/observation-report.md"
}

report_file="$(resolve_report_file || true)"
if [[ -z "$report_file" ]] || [[ ! -f "$report_file" ]]; then
  echo "[append-split-brain-observations][fail] report file not found"
  exit 1
fi

if [[ ! -f "$MATRIX_FILE" ]]; then
  echo "[append-split-brain-observations][fail] matrix file not found at $MATRIX_FILE"
  exit 1
fi

obs_date="$(date +%Y-%m-%d)"
command_text="CONSISTENCY_RUN_SPLIT_BRAIN_EVIDENCE_BUNDLE=true bash scripts/run_consistency_failure_validation.sh"

mkdir -p "$PRIMARY_DATA_ROOT"
rows_tmp="$PRIMARY_DATA_ROOT/.split-brain-rows-$$.tmp"
filtered_tmp="$PRIMARY_DATA_ROOT/.split-brain-filtered-$$.tmp"
out_tmp="$PRIMARY_DATA_ROOT/.split-brain-matrix-$$.tmp"
trap 'rm -f "$rows_tmp" "$filtered_tmp" "$out_tmp"' EXIT

awk -F'|' '
  function trim(s) {
    gsub(/^[[:space:]]+/, "", s)
    gsub(/[[:space:]]+$/, "", s)
    return s
  }

  /^\| SB-[0-9]+ / {
    scenario = trim($2)
    script = trim($3)
    expected = trim($4)
    observed = trim($5)
    result = trim($6)
    log_file = trim($7)

    if (scenario == "" || script == "" || expected == "" || observed == "" || result == "") {
      next
    }

    notes = "bundle report: " report_file "; scenario log: " log_file "; script: " script
    printf "| %s | %s | %s | %s | %s | %s | %s |\n", obs_date, scenario, command_text, expected, observed, result, notes
  }
' obs_date="$obs_date" command_text="$command_text" report_file="$report_file" "$report_file" >"$rows_tmp"

if [[ ! -s "$rows_tmp" ]]; then
  echo "[append-split-brain-observations][fail] no SB rows found in report $report_file"
  exit 1
fi

# Deduplicate same-day bundle-generated rows for SB scenarios before inserting fresh rows.
awk -F'|' '
  function trim(s) {
    gsub(/^[[:space:]]+/, "", s)
    gsub(/[[:space:]]+$/, "", s)
    return s
  }

  {
    if ($0 ~ /^\|/) {
      row_date = trim($2)
      row_scenario = trim($3)
      row_command = trim($4)

      if (row_date == obs_date && row_command == command_text && row_scenario ~ /^SB-[0-9]+$/) {
        next
      }
    }

    print
  }
' obs_date="$obs_date" command_text="$command_text" "$MATRIX_FILE" >"$filtered_tmp"

awk '
  BEGIN { inserted = 0 }
  {
    if (!inserted && $0 ~ /^### Promotion Rule$/) {
      while ((getline line < rows_file) > 0) {
        print line
      }
      close(rows_file)
      print ""
      inserted = 1
    }
    print
  }
' rows_file="$rows_tmp" "$filtered_tmp" >"$out_tmp"

mv "$out_tmp" "$MATRIX_FILE"

echo "[append-split-brain-observations] updated observation rows from $report_file"
