#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=lib.sh
source "$ROOT_DIR/scripts/e2e/lib.sh"

require_binaries

RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ARTIFACTS_ROOT="${DISTDB_ARTIFACTS_ROOT:-$ROOT_DIR/artifacts}"
OPERABILITY_DATA_ROOT="${OPERABILITY_DATA_ROOT:-$ARTIFACTS_ROOT/e2e}"
OUT_DIR="$OPERABILITY_DATA_ROOT/rolling-upgrade-safety-$RUN_ID"
mkdir -p "$OUT_DIR"

MANIFEST_FILE="$OUT_DIR/manifest.json"
SUMMARY_JSON="$OUT_DIR/summary.json"
RUN_STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
GIT_SHA="$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
OLD_SERVER_BIN="${DISTDB_SERVER_BIN_OLD:-$SERVER_BIN}"
NEW_SERVER_BIN="${DISTDB_SERVER_BIN_NEW:-$SERVER_BIN}"

if [[ ! -x "$OLD_SERVER_BIN" ]]; then
  echo "[operability][fail] old server binary not executable: $OLD_SERVER_BIN"
  exit 1
fi

if [[ ! -x "$NEW_SERVER_BIN" ]]; then
  echo "[operability][fail] new server binary not executable: $NEW_SERVER_BIN"
  exit 1
fi

OLD_SERVER_SHA256="$(shasum -a 256 "$OLD_SERVER_BIN" | awk '{print $1}')"
NEW_SERVER_SHA256="$(shasum -a 256 "$NEW_SERVER_BIN" | awk '{print $1}')"

NODE_A_ID="op-node-a"
NODE_B_ID="op-node-b"
NODE_A_PORT="19501"
NODE_B_PORT="19502"
NODE_A_DIR="$OUT_DIR/node-a"
NODE_B_DIR="$OUT_DIR/node-b"
NODE_A_LOG="$OUT_DIR/node-a.log"
NODE_B_LOG="$OUT_DIR/node-b.log"

mkdir -p "$NODE_A_DIR" "$NODE_B_DIR"

NODE_A_PID=""
NODE_B_PID=""

rolling_restart_a_ms=0
rolling_restart_b_ms=0
upgrade_restart_a_ms=0
upgrade_restart_b_ms=0
rollback_restart_a_ms=0
rollback_restart_b_ms=0
node_a_row_count=0
node_b_row_count=0

now_ms() {
  perl -MTime::HiRes=time -e 'printf("%.0f\n", time()*1000)'
}

start_node_a() {
  local server_bin="$1"
  "$server_bin" \
    "node_id=$NODE_A_ID" \
    "datadir=$NODE_A_DIR" \
    "port=$NODE_A_PORT" \
    "listen_addr=127.0.0.1" \
    "tls=off" \
    >"$NODE_A_LOG" 2>&1 &
  NODE_A_PID=$!
}

start_node_b() {
  local server_bin="$1"
  "$server_bin" \
    "node_id=$NODE_B_ID" \
    "datadir=$NODE_B_DIR" \
    "port=$NODE_B_PORT" \
    "listen_addr=127.0.0.1" \
    "tls=off" \
    >"$NODE_B_LOG" 2>&1 &
  NODE_B_PID=$!
}

stop_node_a() {
  if [[ -n "$NODE_A_PID" ]] && kill -0 "$NODE_A_PID" >/dev/null 2>&1; then
    kill "$NODE_A_PID" >/dev/null 2>&1 || true
    wait "$NODE_A_PID" >/dev/null 2>&1 || true
  fi
  NODE_A_PID=""
}

stop_node_b() {
  if [[ -n "$NODE_B_PID" ]] && kill -0 "$NODE_B_PID" >/dev/null 2>&1; then
    kill "$NODE_B_PID" >/dev/null 2>&1 || true
    wait "$NODE_B_PID" >/dev/null 2>&1 || true
  fi
  NODE_B_PID=""
}

wait_node_a() {
  wait_for_server "$NODE_A_PORT" "$NODE_A_ID"
}

wait_node_b() {
  wait_for_server "$NODE_B_PORT" "$NODE_B_ID"
}

run_sql_inline() {
  local node_id="$1"
  local port="$2"
  local sql_payload="$3"
  local out_file="$4"
  local sql_file="$OUT_DIR/tmp-$node_id-$port.sql"

  cat >"$sql_file" <<SQL
password root;
$sql_payload
quit;
SQL

  run_console_sql_file "$port" "$node_id" "$sql_file" "$out_file"
}

provision_node_schema() {
  local node_id="$1"
  local port="$2"
  local out_file="$3"
  local sql_payload

  sql_payload=$'create database opsdb;\nuse opsdb;\ncreate table heartbeat (id uint64 primary key, origin text);\ninsert into heartbeat (id, origin) values (1, '\''seed'\'');'
  run_sql_inline "$node_id" "$port" "$sql_payload" "$out_file"
}

insert_heartbeat() {
  local node_id="$1"
  local port="$2"
  local row_id="$3"
  local marker="$4"
  local out_file="$5"
  local sql_payload

  printf -v sql_payload "use opsdb;\ninsert into heartbeat (id, origin) values (%s, '%s');" "$row_id" "$marker"
  run_sql_inline "$node_id" "$port" "$sql_payload" "$out_file"
}

capture_row_count() {
  local node_id="$1"
  local port="$2"
  local out_file="$3"
  local sql_payload

  sql_payload=$'use opsdb;\nselect count(*) as c from heartbeat;'
  run_sql_inline "$node_id" "$port" "$sql_payload" "$out_file"
  extract_count "$out_file" "c"
}

write_manifest() {
  local exit_code="$1"
  local status="fail"
  if [[ "$exit_code" -eq 0 ]]; then
    status="pass"
  fi

  cat >"$MANIFEST_FILE" <<JSON
{
  "run_id": "$RUN_ID",
  "kind": "operability_rolling_upgrade_safety",
  "status": "$status",
  "exit_code": $exit_code,
  "started_at_utc": "$RUN_STARTED_UTC",
  "finished_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_sha": "$GIT_SHA",
  "server_sha256_old": "$OLD_SERVER_SHA256",
  "server_sha256_new": "$NEW_SERVER_SHA256",
  "server_bin_old": "$OLD_SERVER_BIN",
  "server_bin_new": "$NEW_SERVER_BIN",
  "artifacts_dir": "$OUT_DIR",
  "summary_json": "$SUMMARY_JSON"
}
JSON
}

cleanup() {
  local exit_code="$?"
  stop_node_a || true
  stop_node_b || true
  write_manifest "$exit_code"
}

trap cleanup EXIT

echo "[operability] run_id=$RUN_ID out_dir=$OUT_DIR"

echo "[operability] bootstrapping both nodes using old binary"
start_node_a "$OLD_SERVER_BIN"
wait_node_a
start_node_b "$OLD_SERVER_BIN"
wait_node_b

provision_node_schema "$NODE_A_ID" "$NODE_A_PORT" "$OUT_DIR/node-a-provision.out"
provision_node_schema "$NODE_B_ID" "$NODE_B_PORT" "$OUT_DIR/node-b-provision.out"

node_a_row_count="$(capture_row_count "$NODE_A_ID" "$NODE_A_PORT" "$OUT_DIR/node-a-count-initial.out")"
node_b_row_count="$(capture_row_count "$NODE_B_ID" "$NODE_B_PORT" "$OUT_DIR/node-b-count-initial.out")"

if [[ "$node_a_row_count" != "1" || "$node_b_row_count" != "1" ]]; then
  echo "[operability][fail] initial row counts are unexpected: node_a=$node_a_row_count node_b=$node_b_row_count"
  exit 1
fi

echo "[operability] rolling restart phase"
start_ms="$(now_ms)"
stop_node_a
insert_heartbeat "$NODE_B_ID" "$NODE_B_PORT" "2" "rolling-a-down" "$OUT_DIR/node-b-insert-rolling-a-down.out"
start_node_a "$OLD_SERVER_BIN"
wait_node_a
end_ms="$(now_ms)"
rolling_restart_a_ms=$((end_ms - start_ms))

start_ms="$(now_ms)"
stop_node_b
insert_heartbeat "$NODE_A_ID" "$NODE_A_PORT" "2" "rolling-b-down" "$OUT_DIR/node-a-insert-rolling-b-down.out"
start_node_b "$OLD_SERVER_BIN"
wait_node_b
end_ms="$(now_ms)"
rolling_restart_b_ms=$((end_ms - start_ms))

echo "[operability] cross-version upgrade phase (N -> N+1)"
start_ms="$(now_ms)"
stop_node_a
start_node_a "$NEW_SERVER_BIN"
wait_node_a
insert_heartbeat "$NODE_B_ID" "$NODE_B_PORT" "3" "upgrade-a-node-old-peer-active" "$OUT_DIR/node-b-insert-upgrade-a.out"
end_ms="$(now_ms)"
upgrade_restart_a_ms=$((end_ms - start_ms))

start_ms="$(now_ms)"
stop_node_b
start_node_b "$NEW_SERVER_BIN"
wait_node_b
insert_heartbeat "$NODE_A_ID" "$NODE_A_PORT" "3" "upgrade-b-node-new-peer-active" "$OUT_DIR/node-a-insert-upgrade-b.out"
end_ms="$(now_ms)"
upgrade_restart_b_ms=$((end_ms - start_ms))

echo "[operability] rollback phase (N+1 -> N)"
start_ms="$(now_ms)"
stop_node_a
start_node_a "$OLD_SERVER_BIN"
wait_node_a
insert_heartbeat "$NODE_B_ID" "$NODE_B_PORT" "4" "rollback-a-node-new-peer-active" "$OUT_DIR/node-b-insert-rollback-a.out"
end_ms="$(now_ms)"
rollback_restart_a_ms=$((end_ms - start_ms))

start_ms="$(now_ms)"
stop_node_b
start_node_b "$OLD_SERVER_BIN"
wait_node_b
insert_heartbeat "$NODE_A_ID" "$NODE_A_PORT" "4" "rollback-b-node-old-peer-active" "$OUT_DIR/node-a-insert-rollback-b.out"
end_ms="$(now_ms)"
rollback_restart_b_ms=$((end_ms - start_ms))

node_a_row_count="$(capture_row_count "$NODE_A_ID" "$NODE_A_PORT" "$OUT_DIR/node-a-count-final.out")"
node_b_row_count="$(capture_row_count "$NODE_B_ID" "$NODE_B_PORT" "$OUT_DIR/node-b-count-final.out")"

if [[ "$node_a_row_count" != "4" || "$node_b_row_count" != "4" ]]; then
  echo "[operability][fail] final row counts are unexpected: node_a=$node_a_row_count node_b=$node_b_row_count"
  exit 1
fi

cat >"$SUMMARY_JSON" <<JSON
{
  "run_id": "$RUN_ID",
  "generated_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "server": {
    "old_binary": "$OLD_SERVER_BIN",
    "new_binary": "$NEW_SERVER_BIN",
    "old_sha256": "$OLD_SERVER_SHA256",
    "new_sha256": "$NEW_SERVER_SHA256"
  },
  "nodes": {
    "node_a": {
      "id": "$NODE_A_ID",
      "port": $NODE_A_PORT,
      "final_row_count": $node_a_row_count,
      "log_file": "$NODE_A_LOG"
    },
    "node_b": {
      "id": "$NODE_B_ID",
      "port": $NODE_B_PORT,
      "final_row_count": $node_b_row_count,
      "log_file": "$NODE_B_LOG"
    }
  },
  "timings_ms": {
    "rolling_restart_a": $rolling_restart_a_ms,
    "rolling_restart_b": $rolling_restart_b_ms,
    "upgrade_restart_a": $upgrade_restart_a_ms,
    "upgrade_restart_b": $upgrade_restart_b_ms,
    "rollback_restart_a": $rollback_restart_a_ms,
    "rollback_restart_b": $rollback_restart_b_ms
  },
  "checks": {
    "rolling_restart_survived": true,
    "cross_version_upgrade_survived": true,
    "cross_version_rollback_survived": true
  }
}
JSON

echo "[operability] summary=$SUMMARY_JSON"
echo "[operability] manifest=$MANIFEST_FILE"
cat "$SUMMARY_JSON"
