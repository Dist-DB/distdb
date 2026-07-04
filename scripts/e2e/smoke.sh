#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "$SCRIPT_DIR/lib.sh"

require_binaries

SUITE="smoke"
NODE_ID="e2e-smoke-node"
PORT="19321"
RUN_DIR="$(new_run_dir "$SUITE")"
LOG_FILE="$RUN_DIR/server.log"
SQL_FILE="$RUN_DIR/smoke.sql"
OUT_FILE="$RUN_DIR/smoke.out"

trap stop_server EXIT

log "starting smoke suite run_dir=$RUN_DIR"
start_server "$NODE_ID" "$RUN_DIR" "$PORT" "$LOG_FILE"
wait_for_server "$PORT" "$NODE_ID"

cat >"$SQL_FILE" <<'SQL'
password password;
create database alpha;
use alpha;
create table users (id uint64 primary key, email text);
insert into users (id, email) values (1, 'sam@example.com');
insert into users (id, email) values (2, 'alex@example.com');
select count(*) as c_all from users;
select count(*) as c_like from users where email like '%@example.com';
quit;
SQL

run_console_sql_file "$PORT" "$NODE_ID" "$SQL_FILE" "$OUT_FILE"
assert_count "$OUT_FILE" "c_all" "2" 1
assert_count "$OUT_FILE" "c_like" "2" 1

log "smoke suite passed"
