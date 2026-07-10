#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "$SCRIPT_DIR/lib.sh"

require_binaries

SUITE="stress"
NODE_ID="e2e-stress-node"
PORT="19323"
RUN_DIR="$(new_run_dir "$SUITE")"
LOG_FILE="$RUN_DIR/server.log"
SEED_SQL="$RUN_DIR/seed.sql"
CHECK_SQL="$RUN_DIR/check.sql"
OUT_FILE="$RUN_DIR/check.out"
FAIL_FILE="$RUN_DIR/failures.log"

WORKERS=4
ITERATIONS=25
EXPECTED=$((WORKERS * ITERATIONS))

trap stop_server EXIT

log "starting stress suite run_dir=$RUN_DIR"
start_server "$NODE_ID" "$RUN_DIR" "$PORT" "$LOG_FILE"
wait_for_server "$PORT" "$NODE_ID"

cat >"$SEED_SQL" <<'SQL'
password root;
create database alpha;
create database beta;
use alpha;
create table events (id uint64 primary key, payload text);
use beta;
create table events (id uint64 primary key, payload text);
quit;
SQL

run_console_sql_file "$PORT" "$NODE_ID" "$SEED_SQL" "$RUN_DIR/seed.out"

worker() {
  local w="$1"
  local fail_file="$2"

  for i in $(seq 1 "$ITERATIONS"); do
    local aid bid sql out
    aid=$((100000 + w * 1000 + i))
    bid=$((200000 + w * 1000 + i))
    sql="$RUN_DIR/w${w}_${i}.sql"
    out="$RUN_DIR/w${w}_${i}.out"

    cat >"$sql" <<SQL
  password root;
use alpha;
insert into events (id, payload) values ($aid, 'A:$w:$i');
use beta;
insert into events (id, payload) values ($bid, 'B:$w:$i');
quit;
SQL

    if ! run_console_sql_file "$PORT" "$NODE_ID" "$sql" "$out"; then
      echo "worker=$w iteration=$i console_failed" >>"$fail_file"
      continue
    fi

    if grep -qiE 'command failed|rejected|error:' "$out"; then
      echo "worker=$w iteration=$i query_failed" >>"$fail_file"
    fi
  done
}

rm -f "$FAIL_FILE"
WORKER_PIDS=()
for w in $(seq 1 "$WORKERS"); do
  worker "$w" "$FAIL_FILE" &
  WORKER_PIDS+=("$!")
done
for pid in "${WORKER_PIDS[@]}"; do
  wait "$pid"
done

if [[ -f "$FAIL_FILE" ]] && [[ -s "$FAIL_FILE" ]]; then
  fail "stress insert failures detected; see $FAIL_FILE"
fi

cat >"$CHECK_SQL" <<'SQL'
password root;
use alpha;
select count(*) as c_all from events;
select count(*) as c_like from events where payload like 'A:%';
use beta;
select count(*) as c_all from events;
select count(*) as c_like from events where payload like 'B:%';
quit;
SQL

run_console_sql_file "$PORT" "$NODE_ID" "$CHECK_SQL" "$OUT_FILE"
assert_count "$OUT_FILE" "c_all" "$EXPECTED" 1
assert_count "$OUT_FILE" "c_all" "$EXPECTED" 2
assert_count "$OUT_FILE" "c_like" "$EXPECTED" 1
assert_count "$OUT_FILE" "c_like" "$EXPECTED" 2

log "stress suite passed"
