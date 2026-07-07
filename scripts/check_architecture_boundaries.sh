#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

has_pattern() {
  local pattern="$1"
  local target="$2"

  if command -v rg >/dev/null 2>&1; then
    rg -n --fixed-strings "$pattern" "$target" >/dev/null
    return $?
  fi

  if [[ -d "$target" ]]; then
    grep -R -n -F -- "$pattern" "$target" >/dev/null
    return $?
  fi

  grep -n -F -- "$pattern" "$target" >/dev/null
}

fail() {
  printf '[architecture][fail] %s\n' "$*" >&2
  exit 1
}

pass() {
  printf '[architecture][ok] %s\n' "$*"
}

ensure_absent_path() {
  local path="$1"
  if [[ -e "$ROOT_DIR/$path" ]]; then
    fail "forbidden path exists: $path"
  fi
}

ensure_no_pattern_in_server_src() {
  local pattern="$1"
  local description="$2"
  if has_pattern "$pattern" "$ROOT_DIR/server/src"; then
    fail "forbidden server functional implementation detected: $description"
  fi
}

ensure_pattern_present() {
  local pattern="$1"
  local path="$2"
  local description="$3"
  if ! has_pattern "$pattern" "$ROOT_DIR/$path"; then
    fail "required architecture usage missing: $description"
  fi
}

ensure_no_pattern_in_server_file() {
  local pattern="$1"
  local path="$2"
  local description="$3"
  if has_pattern "$pattern" "$ROOT_DIR/$path"; then
    fail "forbidden server ownership pattern detected: $description"
  fi
}

# Keep functional SQL loop control-flow logic out of the server namespace.
ensure_absent_path "server/src/core/mappings/query/core/control_flow"
ensure_no_pattern_in_server_src "fn execute_local_while_block" "execute_local_while_block definition"
ensure_no_pattern_in_server_src "fn execute_local_repeat_block" "execute_local_repeat_block definition"
ensure_no_pattern_in_server_src "fn execute_local_loop_block" "execute_local_loop_block definition"
ensure_no_pattern_in_server_src "fn parse_local_while_block" "parse_local_while_block definition"
ensure_no_pattern_in_server_src "fn parse_local_repeat_block" "parse_local_repeat_block definition"
ensure_no_pattern_in_server_src "fn parse_local_loop_block" "parse_local_loop_block definition"

# Ensure server orchestration still routes loop handling through serverlib.
ensure_pattern_present "serverlib::execute_local_while_block" "server/src/core/mappings/query/core/dispatch_ops.rs" "dispatch uses serverlib::execute_local_while_block"
ensure_pattern_present "serverlib::execute_local_repeat_block" "server/src/core/mappings/query/core/dispatch_ops.rs" "dispatch uses serverlib::execute_local_repeat_block"
ensure_pattern_present "serverlib::execute_local_loop_block" "server/src/core/mappings/query/core/dispatch_ops.rs" "dispatch uses serverlib::execute_local_loop_block"

# Ensure serverlib remains owner of loop functional implementations.
ensure_pattern_present "fn execute_local_while_block" "serverlib/src/engine/execution/commands/control_flow/while_block.rs" "serverlib defines execute_local_while_block"
ensure_pattern_present "fn execute_local_repeat_block" "serverlib/src/engine/execution/commands/control_flow/repeat_block.rs" "serverlib defines execute_local_repeat_block"
ensure_pattern_present "fn execute_local_loop_block" "serverlib/src/engine/execution/commands/control_flow/loop_block.rs" "serverlib defines execute_local_loop_block"
ensure_pattern_present "pub use repeat_block::execute_local_repeat_block;" "serverlib/src/engine/execution/commands/control_flow/mod.rs" "serverlib control_flow exports repeat executor"
ensure_pattern_present "pub use while_block::execute_local_while_block;" "serverlib/src/engine/execution/commands/control_flow/mod.rs" "serverlib control_flow exports while executor"
ensure_pattern_present "pub use loop_block" "serverlib/src/engine/execution/commands/control_flow/mod.rs" "serverlib control_flow exports loop executor"

# Keep SQL parser/dialect engine details out of server implementation code.
ensure_no_pattern_in_server_src "sqlparser::" "direct sqlparser usage in server source"

# Enforce parser/planner ownership: server orchestrates through serverlib APIs.
ensure_pattern_present "serverlib::parse_mysql8_sql_requests" "server/src/core/mappings/query/core/mod.rs" "query core parse entry uses serverlib"
ensure_pattern_present "serverlib::parse_select_read_plan_from_statement" "server/src/core/mappings/query/core/select_ops.rs" "select read planning uses serverlib"
ensure_pattern_present "serverlib::parse_alter_table_change_plan_from_statement" "server/src/core/mappings/query/core/ddl_ops.rs" "alter table planning uses serverlib"
ensure_pattern_present "serverlib::create_table_plan_from_statement" "server/src/core/mappings/query/core/ddl_ops.rs" "create table planning uses serverlib"

# Prevent accidental local parser helper ownership drift in core query dispatch.
ensure_no_pattern_in_server_file "fn parse_local_while_block" "server/src/core/mappings/query/core/dispatch_ops.rs" "dispatch defines local WHILE parser"
ensure_no_pattern_in_server_file "fn parse_local_repeat_block" "server/src/core/mappings/query/core/dispatch_ops.rs" "dispatch defines local REPEAT parser"

pass "architecture boundary checks passed"
