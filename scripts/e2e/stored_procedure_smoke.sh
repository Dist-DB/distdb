#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./lib.sh
source "$SCRIPT_DIR/lib.sh"

require_binaries

SUITE="stored-procedure-smoke"
NODE_ID="e2e-proc-smoke-node"
PORT="19322"
RUN_DIR="$(new_run_dir "$SUITE")"
LOG_FILE="$RUN_DIR/server.log"
SQL_FILE="$RUN_DIR/stored_procedure_smoke.sql"
OUT_FILE="$RUN_DIR/stored_procedure_smoke.out"

trap stop_server EXIT

log "starting stored procedure smoke suite run_dir=$RUN_DIR"
start_server "$NODE_ID" "$RUN_DIR" "$PORT" "$LOG_FILE"
wait_for_server "$PORT" "$NODE_ID"

cat >"$SQL_FILE" <<'SQL'
password password;
create database alpha;
use alpha;

create table users (id uint64 primary key, email text);
insert into users (id, email) values (1, 'sam@example.com');
insert into users (id, email) values (2, 'alex@example.com');

delimiter //
create procedure p_arg_route(p_mode uint64) as begin if p_mode = 1 then select count(*) as c_arg_route from users where email = 'sam@example.com'; else select count(*) as c_arg_route from users where email = 'nobody@example.com'; end if; end//

create procedure p_temp_scope(p_flag uint64) as begin if p_flag = 1 then create temporary table users (id uint64 primary key, email text); insert into users (id, email) values (900, 'temp@example.com'); select count(*) as c_proc_tmp from users; else select 0 as c_proc_tmp; end if; end//

create procedure p_delim_route(p_mode uint64) as begin if p_mode = 1 then select count(*) as c_delim_route from users where email like '%@example.com'; else select count(*) as c_delim_route from users where email = 'none@example.com'; end if; end//

create procedure p_loop_leave(p_mode uint64) as begin if p_mode = 1 then loop leave; end loop; select abs(1) as c_loop_leave; else select abs(0) as c_loop_leave; end if; end//

create procedure p_iterate_while(p_mode uint64) as begin if p_mode = 1 then while p_mode = 1 do set p_mode = 0; iterate; end while; select abs(1) as c_iterate_while; else select abs(0) as c_iterate_while; end if; end//

create procedure p_handler_continue(p_mode uint64) as begin if p_mode = 1 then declare continue handler for sqlexception select abs(2) as c_handler_continue; drop table missing_handler_table; else select abs(0) as c_handler_continue; end if; end//

create procedure p_handler_exit(p_mode uint64) as begin if p_mode = 1 then declare exit handler for sqlexception select abs(7) as c_handler_exit; drop table missing_handler_table; select abs(9) as c_handler_exit; else select abs(0) as c_handler_exit; end if; end//
delimiter ;

call p_arg_route(1);
call p_arg_route(2);
call p_temp_scope(1);
call p_delim_route(1);
call p_delim_route(2);
call p_loop_leave(1);
call p_iterate_while(1);
call p_handler_continue(1);
call p_handler_exit(1);

select count(*) as c_users_after from users;
quit;
SQL

run_console_sql_file "$PORT" "$NODE_ID" "$SQL_FILE" "$OUT_FILE"
assert_count "$OUT_FILE" "c_arg_route" "1" 1
assert_count "$OUT_FILE" "c_arg_route" "0" 2
assert_count "$OUT_FILE" "c_proc_tmp" "1" 1
assert_count "$OUT_FILE" "c_delim_route" "2" 1
assert_count "$OUT_FILE" "c_delim_route" "0" 2
assert_count "$OUT_FILE" "c_loop_leave" "1" 1
assert_count "$OUT_FILE" "c_iterate_while" "1" 1
assert_count "$OUT_FILE" "c_handler_continue" "2" 1
assert_count "$OUT_FILE" "c_handler_exit" "7" 1
assert_count "$OUT_FILE" "c_users_after" "2" 1

log "stored procedure smoke suite passed"
