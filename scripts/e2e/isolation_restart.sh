#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "$SCRIPT_DIR/lib.sh"

require_binaries

SUITE="isolation-restart"
NODE_ID="e2e-isolation-node"
PORT="19322"
RUN_DIR="$(new_run_dir "$SUITE")"
LOG_PRE="$RUN_DIR/server-pre.log"
LOG_POST="$RUN_DIR/server-post.log"
SEED_SQL="$RUN_DIR/seed.sql"
CHECK_SQL="$RUN_DIR/check.sql"
OUT_PRE="$RUN_DIR/check-pre.out"
OUT_POST="$RUN_DIR/check-post.out"

EXPECTED=40

trap stop_server EXIT

cat >"$SEED_SQL" <<SQL
password password;
create database alpha;
create database beta;
use alpha;
create table events (id uint64 primary key, payload text);
use beta;
create table events (id uint64 primary key, payload text);
SQL

for i in $(seq 1 "$EXPECTED"); do
  printf "use alpha;\ninsert into events (id, payload) values (%d, 'A:%d');\n" "$i" "$i" >>"$SEED_SQL"
  printf "use beta;\ninsert into events (id, payload) values (%d, 'B:%d');\n" "$i" "$i" >>"$SEED_SQL"
done
printf "quit;\n" >>"$SEED_SQL"

cat >"$CHECK_SQL" <<'SQL'
password password;
use alpha;
select count(*) as c_all from events;
select count(*) as c_like_all from events where payload like '%';
select count(*) as c_like_alpha from events where payload like 'A:%';
use beta;
select count(*) as c_all from events;
select count(*) as c_like_all from events where payload like '%';
select count(*) as c_like_beta from events where payload like 'B:%';
quit;
SQL

log "starting isolation+restart suite run_dir=$RUN_DIR"
start_server "$NODE_ID" "$RUN_DIR" "$PORT" "$LOG_PRE"
wait_for_server "$PORT" "$NODE_ID"

run_console_sql_file "$PORT" "$NODE_ID" "$SEED_SQL" "$RUN_DIR/seed.out"
run_console_sql_file "$PORT" "$NODE_ID" "$CHECK_SQL" "$OUT_PRE"

assert_count "$OUT_PRE" "c_all" "$EXPECTED" 1
assert_count "$OUT_PRE" "c_all" "$EXPECTED" 2
assert_count "$OUT_PRE" "c_like_all" "$EXPECTED" 1
assert_count "$OUT_PRE" "c_like_all" "$EXPECTED" 2
assert_count "$OUT_PRE" "c_like_alpha" "$EXPECTED" 1
assert_count "$OUT_PRE" "c_like_beta" "$EXPECTED" 1

log "pre-restart checks passed"
stop_server

start_server "$NODE_ID" "$RUN_DIR" "$PORT" "$LOG_POST"
wait_for_server "$PORT" "$NODE_ID"
run_console_sql_file "$PORT" "$NODE_ID" "$CHECK_SQL" "$OUT_POST"

assert_count "$OUT_POST" "c_all" "$EXPECTED" 1
assert_count "$OUT_POST" "c_all" "$EXPECTED" 2
assert_count "$OUT_POST" "c_like_all" "$EXPECTED" 1
assert_count "$OUT_POST" "c_like_all" "$EXPECTED" 2
assert_count "$OUT_POST" "c_like_alpha" "$EXPECTED" 1
assert_count "$OUT_POST" "c_like_beta" "$EXPECTED" 1

log "isolation+restart suite passed"
