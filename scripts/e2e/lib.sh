#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVER_BIN="$ROOT_DIR/server/target/debug/server"
CONSOLE_BIN="$ROOT_DIR/console/target/debug/console"
TLSSERVER_BIN="$ROOT_DIR/tlsserver/target/debug/tlsserver"

DATA_ROOT="$ROOT_DIR/server/data/e2e"
TLSSERVER_PORT="${DISTDB_E2E_TLSSERVER_PORT:-19443}"
TLSSERVER_ADDR="127.0.0.1:${TLSSERVER_PORT}"
TLSSERVER_DATA_DIR="$DATA_ROOT/tlsserver"
TLSSERVER_LOG="$DATA_ROOT/tlsserver.log"
TLSSERVER_CA_PATH="$TLSSERVER_DATA_DIR/p2p-tls/ca-cert.pem"

mkdir -p "$DATA_ROOT"

log() {
  printf '[e2e] %s\n' "$*"
}

fail() {
  printf '[e2e][fail] %s\n' "$*" >&2
  exit 1
}

require_binaries() {
  if [[ ! -x "$SERVER_BIN" ]]; then
    log "server binary missing; building server crate"
    (cd "$ROOT_DIR/server" && cargo build --quiet)
  fi

  if [[ ! -x "$CONSOLE_BIN" ]]; then
    log "console binary missing; building console crate"
    (cd "$ROOT_DIR/console" && cargo build --quiet)
  fi

  if [[ ! -x "$TLSSERVER_BIN" ]]; then
    log "tlsserver binary missing; building tlsserver crate"
    (cd "$ROOT_DIR/tlsserver" && cargo build --quiet)
  fi

  [[ -x "$SERVER_BIN" ]] || fail "server binary missing at $SERVER_BIN"
  [[ -x "$CONSOLE_BIN" ]] || fail "console binary missing at $CONSOLE_BIN"
  [[ -x "$TLSSERVER_BIN" ]] || fail "tlsserver binary missing at $TLSSERVER_BIN"
}

ensure_tlsserver() {
  if [[ -n "${TLSSERVER_PID:-}" ]] && kill -0 "$TLSSERVER_PID" >/dev/null 2>&1; then
    return 0
  fi

  mkdir -p "$TLSSERVER_DATA_DIR"

  "$TLSSERVER_BIN" \
    "datadir=$TLSSERVER_DATA_DIR" \
    "listen_addr=127.0.0.1" \
    "port=$TLSSERVER_PORT" \
    >"$TLSSERVER_LOG" 2>&1 &

  TLSSERVER_PID=$!
  export TLSSERVER_PID

  for _ in {1..100}; do
    if [[ -f "$TLSSERVER_CA_PATH" ]] && lsof -nP -iTCP:"$TLSSERVER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
      return 0
    fi

    if ! kill -0 "$TLSSERVER_PID" >/dev/null 2>&1; then
      fail "tlsserver exited unexpectedly; see $TLSSERVER_LOG"
    fi

    sleep 0.1
  done

  fail "tlsserver did not become ready at $TLSSERVER_ADDR"
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

  ensure_tlsserver

  "$SERVER_BIN" \
    "node_id=$node_id" \
    "datadir=$datadir_root" \
    "port=$port" \
    "listen_addr=127.0.0.1" \
    "advertise_addr=127.0.0.1" \
    "tls_san=localhost,127.0.0.1" \
    "tls_server=$TLSSERVER_ADDR" \
    "tls_ca=$TLSSERVER_CA_PATH" \
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

  if [[ -n "${TLSSERVER_PID:-}" ]] && kill -0 "$TLSSERVER_PID" >/dev/null 2>&1; then
    kill "$TLSSERVER_PID" >/dev/null 2>&1 || true
    wait "$TLSSERVER_PID" >/dev/null 2>&1 || true
  fi
  unset TLSSERVER_PID
}

wait_for_server() {
  local port="$1"
  local _node_id="$2"
  local logfile="${3:-}"
  local probe_out="${DATA_ROOT}/wait-${port}.out"

  for _ in {1..50}; do
    if [[ -n "$logfile" ]] && [[ -f "$logfile" ]]; then
      if ! rg -q "connector bootstrap gate opened" "$logfile"; then
        sleep 0.2
        continue
      fi
    fi

    if "$CONSOLE_BIN" "127.0.0.1:$port" "tls_ca=$TLSSERVER_CA_PATH" <<'SQL' >"$probe_out" 2>&1
  show peers;
password root;
quit;
SQL
    then
      if rg -q "no active peer connection|transport reconnect failed|Connection refused|server is bootstrapping" "$probe_out"; then
        sleep 0.2
        continue
      fi
      return 0
    fi
    sleep 0.2
  done

  fail "server did not become ready on port $port"
}

run_console_sql_file() {
  local port="$1"
  local _node_id="$2"
  local sql_file="$3"
  local out_file="$4"

  local staged_file
  staged_file="${out_file}.input"

  {
    printf '%s\n' "show peers;"
    cat "$sql_file"
  } >"$staged_file"

  "$CONSOLE_BIN" "127.0.0.1:$port" "tls_ca=$TLSSERVER_CA_PATH" <"$staged_file" >"$out_file" 2>&1
}

extract_count() {
  local out_file="$1"
  local column="$2"
  local occurrence="${3:-1}"

  awk -v col="$column" -v target="$occurrence" '
    function trim(s) {
      gsub(/^[[:space:]]+/, "", s)
      gsub(/[[:space:]]+$/, "", s)
      return s
    }

    function first_cell(line, parts, n, cell) {
      gsub(/│/, "|", line)
      n = split(line, parts, "|")
      if (n < 3) {
        return ""
      }
      cell = parts[2]
      return trim(cell)
    }

    BEGIN { seen = 0; want = 0 }

    /^[\|│]/ {
      cell = first_cell($0)

      if (cell == col || index(cell, col ":") == 1) {
        seen++
        want = (seen == target)
        next
      }

      if (want == 1 && cell ~ /^[0-9]+$/) {
        print cell
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
