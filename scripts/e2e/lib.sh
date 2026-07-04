#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_BIN="$ROOT_DIR/server/target/debug/server"
CONSOLE_BIN="$ROOT_DIR/console/target/debug/console"
DATA_ROOT="$ROOT_DIR/server/data/e2e"

mkdir -p "$DATA_ROOT"

log() {
  printf '[e2e] %s\n' "$*"
}

fail() {
  printf '[e2e][fail] %s\n' "$*" >&2
  exit 1
}

require_binaries() {
  [[ -x "$SERVER_BIN" ]] || fail "server binary missing at $SERVER_BIN"
  [[ -x "$CONSOLE_BIN" ]] || fail "console binary missing at $CONSOLE_BIN"
}

new_run_dir() {
  local suite="$1"
  local ts
  ts="$(date +%Y%m%d-%H%M%S)"
  local dir="$DATA_ROOT/${suite}-${ts}-$$"
  mkdir -p "$dir"
  printf '%s\n' "$dir"
}

start_server() {
  local node_id="$1"
  local datadir_root="$2"
  local port="$3"
  local logfile="$4"

  "$SERVER_BIN" \
    "node_id=$node_id" \
    "datadir=$datadir_root" \
    "port=$port" \
    "listen_addr=127.0.0.1" \
    "tls=off" \
    >"$logfile" 2>&1 &

  SERVER_PID=$!
  export SERVER_PID
}

stop_server() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  unset SERVER_PID
}

wait_for_server() {
  local port="$1"
  local node_id="$2"

  for _ in {1..50}; do
    if "$CONSOLE_BIN" "127.0.0.1:$port" tls=off "user=root@$node_id" <<'SQL' >/dev/null 2>&1
password password;
quit;
SQL
    then
      return 0
    fi
    sleep 0.2
  done

  fail "server did not become ready on port $port"
}

run_console_sql_file() {
  local port="$1"
  local node_id="$2"
  local sql_file="$3"
  local out_file="$4"

  "$CONSOLE_BIN" "127.0.0.1:$port" tls=off "user=root@$node_id" <"$sql_file" >"$out_file" 2>&1
}

extract_count() {
  local out_file="$1"
  local column="$2"
  local occurrence="${3:-1}"

  awk -v col="$column" -v target="$occurrence" '
    BEGIN { seen = 0; want = 0 }
    $0 ~ "\\| " col " \\|" { seen++; want = (seen == target); next }
    want == 1 && $0 ~ /^\|/ {
      line = $0
      gsub(/\|/, "", line)
      gsub(/ /, "", line)
      if (line ~ /^[0-9]+$/) {
        print line
        exit
      }
    }
  ' "$out_file"
}

assert_count() {
  local out_file="$1"
  local column="$2"
  local expected="$3"
  local occurrence="${4:-1}"

  local actual
  actual="$(extract_count "$out_file" "$column" "$occurrence")"

  if [[ -z "$actual" ]]; then
    fail "missing count for column '$column' occurrence $occurrence in $out_file"
  fi

  if [[ "$actual" != "$expected" ]]; then
    fail "count mismatch for '$column' occurrence $occurrence: expected=$expected actual=$actual"
  fi
}
