#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=../e2e/lib.sh
source "$ROOT_DIR/scripts/e2e/lib.sh"

require_binaries

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
PERF_DATA_ROOT="${PERF_DATA_ROOT:-$ARTIFACTS_ROOT/perf}"
OUT_DIR="$PERF_DATA_ROOT/nonfunctional-baseline-$RUN_ID"
mkdir -p "$OUT_DIR"
MANIFEST_FILE="$OUT_DIR/manifest.json"
RUN_STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"

NODE_ID="perf-baseline-node"
PORT="19401"
LOG_FILE="$OUT_DIR/server.log"
SETUP_SQL="$OUT_DIR/setup.sql"
RECOVERY_SQL="$OUT_DIR/recovery.sql"

WRITE_OPS="${PERF_WRITE_OPS:-60}"
READ_OPS="${PERF_READ_OPS:-90}"
MIXED_OPS="${PERF_MIXED_OPS:-80}"
SEED_ROWS="${PERF_SEED_ROWS:-200}"

WRITE_CSV="$OUT_DIR/write-heavy.csv"
READ_CSV="$OUT_DIR/read-heavy.csv"
MIXED_CSV="$OUT_DIR/mixed.csv"
SUMMARY_JSON="$OUT_DIR/summary.json"
ENVIRONMENT_SNAPSHOT="$OUT_DIR/environment.txt"

write_manifest() {
  local exit_code="$1"
  local status="fail"
  if [[ "$exit_code" -eq 0 ]]; then
    status="pass"
  fi

  cat >"$MANIFEST_FILE" <<JSON
{
  "run_id": "$RUN_ID",
  "kind": "nonfunctional_baseline",
  "status": "$status",
  "exit_code": $exit_code,
  "started_at_utc": "$RUN_STARTED_UTC",
  "finished_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$GIT_SHA",
  "artifacts_dir": "$OUT_DIR",
  "summary_json": "$SUMMARY_JSON",
  "profiles": ["write_heavy", "read_heavy", "mixed", "recovery"],
  "config": {
    "write_ops": $WRITE_OPS,
    "read_ops": $READ_OPS,
    "mixed_ops": $MIXED_OPS,
    "seed_rows": $SEED_ROWS
  }
}
JSON
}

on_exit() {
  local exit_code="$?"
  stop_server || true
  write_manifest "$exit_code"
}

trap on_exit EXIT

now_ms() {
  perl -MTime::HiRes=time -e 'printf("%.0f\n", time()*1000)'
}

capture_environment_snapshot() {
  {
    echo "run_id=$RUN_ID"
    echo "captured_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "hostname=$(hostname 2>/dev/null || echo unknown)"
    echo "uname=$(uname -a 2>/dev/null || echo unknown)"

    if command -v getconf >/dev/null 2>&1; then
      echo "cpu_count=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)"
    elif command -v nproc >/dev/null 2>&1; then
      echo "cpu_count=$(nproc 2>/dev/null || echo unknown)"
    elif command -v sysctl >/dev/null 2>&1; then
      echo "cpu_count=$(sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
    else
      echo "cpu_count=unknown"
    fi

    if [[ -r /proc/loadavg ]]; then
      echo "loadavg=$(tr -d '\n' </proc/loadavg)"
    elif command -v sysctl >/dev/null 2>&1; then
      echo "loadavg=$(sysctl -n vm.loadavg 2>/dev/null || echo unknown)"
    else
      echo "loadavg=unknown"
    fi

    if [[ -r /proc/meminfo ]]; then
      awk '
        /^MemTotal:|^MemFree:|^MemAvailable:|^SwapTotal:|^SwapFree:/ {
          print tolower($1) "=" $2 " " $3
        }
      ' /proc/meminfo
    elif command -v vm_stat >/dev/null 2>&1; then
      vm_stat 2>/dev/null || true
    else
      echo "memory=unknown"
    fi

    if command -v df >/dev/null 2>&1; then
      echo "disk_usage=$(df -h "$OUT_DIR" 2>/dev/null | tail -n +2 | tr '\n' ' ' || true)"
    fi
  } >"$ENVIRONMENT_SNAPSHOT"
}

calc_percentile_from_csv() {
  local csv_file="$1"
  local percentile="$2"
  local total
  local rank
  local sorted_file

  total="$(wc -l <"$csv_file" | tr -d ' ')"

  if [[ "$total" -eq 0 ]]; then
    echo "0"
    return 0
  fi

  rank=$(( (percentile * total + 99) / 100 ))
  if [[ "$rank" -lt 1 ]]; then
    rank=1
  fi

  sorted_file="$OUT_DIR/.sorted-$(basename "$csv_file")-$$.tmp"
  sort -n "$csv_file" >"$sorted_file"
  sed -n "${rank}p" "$sorted_file"
  rm -f "$sorted_file"
}

calc_throughput_ops_per_sec() {
  local ops="$1"
  local total_ms="$2"

  awk -v ops="$ops" -v total_ms="$total_ms" '
    BEGIN {
      if (total_ms <= 0) {
        print "0.00"
      } else {
        printf "%.2f", (ops * 1000.0) / total_ms
      }
    }
  '
}

run_sql_inline() {
  local sql_payload="$1"
  local out_file="$2"
  local sql_file="$OUT_DIR/tmp.sql"

  cat >"$sql_file" <<SQL
password root;
use perfdb;
$sql_payload
quit;
SQL

  run_console_sql_file "$PORT" "$NODE_ID" "$sql_file" "$out_file"
}

seed_database() {
  cat >"$SETUP_SQL" <<'SQL'
password root;
create database perfdb;
use perfdb;
create table events (id uint64 primary key, payload text);
SQL

  for i in $(seq 1 "$SEED_ROWS"); do
    printf "insert into events (id, payload) values (%d, 'seed:%d');\n" "$i" "$i" >>"$SETUP_SQL"
  done

  printf "quit;\n" >>"$SETUP_SQL"

  run_console_sql_file "$PORT" "$NODE_ID" "$SETUP_SQL" "$OUT_DIR/setup.out"
}

run_write_heavy() {
  : >"$WRITE_CSV"
  local op_id start end duration out_file

  for i in $(seq 1 "$WRITE_OPS"); do
    op_id=$((SEED_ROWS + i))
    out_file="$OUT_DIR/write-$i.out"
    start="$(now_ms)"
    run_sql_inline "insert into events (id, payload) values ($op_id, 'write:$i');" "$out_file"
    end="$(now_ms)"
    duration=$((end - start))
    echo "$duration" >>"$WRITE_CSV"
  done
}

run_read_heavy() {
  : >"$READ_CSV"
  local lookup_id start end duration out_file

  for i in $(seq 1 "$READ_OPS"); do
    lookup_id=$((((i - 1) % SEED_ROWS) + 1))
    out_file="$OUT_DIR/read-$i.out"
    start="$(now_ms)"
    run_sql_inline "select payload from events where id = $lookup_id;" "$out_file"
    end="$(now_ms)"
    duration=$((end - start))
    echo "$duration" >>"$READ_CSV"
  done
}

run_mixed() {
  : >"$MIXED_CSV"
  local start end duration out_file op_id lookup_id

  for i in $(seq 1 "$MIXED_OPS"); do
    out_file="$OUT_DIR/mixed-$i.out"

    if (( i % 2 == 0 )); then
      op_id=$((SEED_ROWS + WRITE_OPS + i))
      start="$(now_ms)"
      run_sql_inline "insert into events (id, payload) values ($op_id, 'mixed-write:$i');" "$out_file"
      end="$(now_ms)"
    else
      lookup_id=$((((i - 1) % SEED_ROWS) + 1))
      start="$(now_ms)"
      run_sql_inline "select count(*) as c from events where id <= $lookup_id;" "$out_file"
      end="$(now_ms)"
    fi

    duration=$((end - start))
    echo "$duration" >>"$MIXED_CSV"
  done
}

measure_recovery_ready_ms() {
  stop_server
  local start end

  start="$(now_ms)"
  start_server "$NODE_ID" "$OUT_DIR" "$PORT" "$OUT_DIR/server-recovery.log"
  wait_for_server "$PORT" "$NODE_ID"
  end="$(now_ms)"

  echo $((end - start))
}

profile_json() {
  local name="$1"
  local csv_file="$2"
  local ops="$3"

  local p50 p95 p99 total_ms throughput
  p50="$(calc_percentile_from_csv "$csv_file" 50)"
  p95="$(calc_percentile_from_csv "$csv_file" 95)"
  p99="$(calc_percentile_from_csv "$csv_file" 99)"
  total_ms="$(awk '{sum += $1} END {print sum+0}' "$csv_file")"
  throughput="$(calc_throughput_ops_per_sec "$ops" "$total_ms")"

  cat <<JSON
    "$name": {
      "operations": $ops,
      "p50_ms": $p50,
      "p95_ms": $p95,
      "p99_ms": $p99,
      "total_ms": $total_ms,
      "throughput_ops_per_sec": $throughput
    }
JSON
}

echo "[nonfunctional-baseline] run_id=$RUN_ID out_dir=$OUT_DIR"

capture_environment_snapshot

start_server "$NODE_ID" "$OUT_DIR" "$PORT" "$LOG_FILE"
wait_for_server "$PORT" "$NODE_ID"

seed_database
run_write_heavy
run_read_heavy
run_mixed

recovery_ready_ms="$(measure_recovery_ready_ms)"

write_profile="$(profile_json "write_heavy" "$WRITE_CSV" "$WRITE_OPS")"
read_profile="$(profile_json "read_heavy" "$READ_CSV" "$READ_OPS")"
mixed_profile="$(profile_json "mixed" "$MIXED_CSV" "$MIXED_OPS")"

cat >"$SUMMARY_JSON" <<JSON
{
  "run_id": "$RUN_ID",
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "config": {
    "write_ops": $WRITE_OPS,
    "read_ops": $READ_OPS,
    "mixed_ops": $MIXED_OPS,
    "seed_rows": $SEED_ROWS,
    "port": $PORT
  },
  "profiles": {
$write_profile,
$read_profile,
$mixed_profile
  },
  "recovery": {
    "recovery_to_ready_ms": $recovery_ready_ms
  },
  "artifacts": {
    "write_csv": "$WRITE_CSV",
    "read_csv": "$READ_CSV",
    "mixed_csv": "$MIXED_CSV",
    "server_log": "$LOG_FILE",
    "environment_snapshot": "$ENVIRONMENT_SNAPSHOT"
  }
}
JSON

echo "[nonfunctional-baseline] summary=$SUMMARY_JSON"
echo "[nonfunctional-baseline] manifest=$MANIFEST_FILE"
cat "$SUMMARY_JSON"
