#!/usr/bin/env bash
set -euo pipefail

print_usage() {
  cat <<'USAGE'
Usage:
  bash scripts/import_large_dataset.sh \
    --server <host:port> \
    --database <db_name> \
    --file <absolute_or_relative_sql_file> \
    [--user <username@peer-id>] \
    [--password <password>] \
    [--tls off|optional|required] \
    [--tls-ca <path_to_ca_pem>] \
    [--chunk-tuples <n>] \
    [--chunk-bytes <n>] \
    [--tx-batch-size <n>] \
    [--tx-batch-max-age-ms <n>]

Notes:
  - This script launches the DistDB console and executes:
      use <database>;
      import <file>;
      exit;
  - Defaults are tuned for large imports and can be overridden via flags.
USAGE
}

require_arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" ]]; then
    echo "error: missing value for $flag" >&2
    print_usage
    exit 1
  fi
}

SERVER=""
DATABASE=""
DATA_FILE=""
USER_ARG=""
PASSWORD_ARG=""
TLS_MODE=""
TLS_CA=""

CHUNK_TUPLES="1024"
CHUNK_BYTES="524288"
TX_BATCH_SIZE="1200"
TX_BATCH_MAX_AGE_MS="1500"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server)
      require_arg_value "$1" "${2:-}"
      SERVER="$2"
      shift 2
      ;;
    --database)
      require_arg_value "$1" "${2:-}"
      DATABASE="$2"
      shift 2
      ;;
    --file)
      require_arg_value "$1" "${2:-}"
      DATA_FILE="$2"
      shift 2
      ;;
    --user)
      require_arg_value "$1" "${2:-}"
      USER_ARG="$2"
      shift 2
      ;;
    --password)
      require_arg_value "$1" "${2:-}"
      PASSWORD_ARG="$2"
      shift 2
      ;;
    --tls)
      require_arg_value "$1" "${2:-}"
      TLS_MODE="$2"
      shift 2
      ;;
    --tls-ca)
      require_arg_value "$1" "${2:-}"
      TLS_CA="$2"
      shift 2
      ;;
    --chunk-tuples)
      require_arg_value "$1" "${2:-}"
      CHUNK_TUPLES="$2"
      shift 2
      ;;
    --chunk-bytes)
      require_arg_value "$1" "${2:-}"
      CHUNK_BYTES="$2"
      shift 2
      ;;
    --tx-batch-size)
      require_arg_value "$1" "${2:-}"
      TX_BATCH_SIZE="$2"
      shift 2
      ;;
    --tx-batch-max-age-ms)
      require_arg_value "$1" "${2:-}"
      TX_BATCH_MAX_AGE_MS="$2"
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "error: unknown argument '$1'" >&2
      print_usage
      exit 1
      ;;
  esac
done

if [[ -z "$SERVER" || -z "$DATABASE" || -z "$DATA_FILE" ]]; then
  echo "error: --server, --database, and --file are required" >&2
  print_usage
  exit 1
fi

if [[ ! -f "$DATA_FILE" ]]; then
  echo "error: import file not found: $DATA_FILE" >&2
  exit 1
fi

if [[ -n "$PASSWORD_ARG" && -z "$USER_ARG" ]]; then
  echo "error: --password requires --user <username@peer-id>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONSOLE_DIR="$ROOT_DIR/console"

if [[ ! -d "$CONSOLE_DIR" ]]; then
  echo "error: console crate not found at $CONSOLE_DIR" >&2
  exit 1
fi

CONSOLE_ARGS=("$SERVER")

if [[ -n "$TLS_MODE" ]]; then
  CONSOLE_ARGS+=("tls=$TLS_MODE")
fi

if [[ -n "$TLS_CA" ]]; then
  CONSOLE_ARGS+=("tls_ca=$TLS_CA")
fi

if [[ -n "$USER_ARG" ]]; then
  CONSOLE_ARGS+=("user=$USER_ARG")
fi

if [[ -n "$PASSWORD_ARG" ]]; then
  CONSOLE_ARGS+=("password=$PASSWORD_ARG")
fi

export IMPORT_INSERT_CHUNK_MAX_TUPLES="$CHUNK_TUPLES"
export IMPORT_INSERT_CHUNK_BYTES="$CHUNK_BYTES"
export IMPORT_TX_BATCH_SIZE="$TX_BATCH_SIZE"
export IMPORT_TX_BATCH_MAX_AGE_MS="$TX_BATCH_MAX_AGE_MS"

echo "Import configuration:"
echo "  server=$SERVER"
echo "  database=$DATABASE"
echo "  file=$DATA_FILE"
echo "  IMPORT_INSERT_CHUNK_MAX_TUPLES=$IMPORT_INSERT_CHUNK_MAX_TUPLES"
echo "  IMPORT_INSERT_CHUNK_BYTES=$IMPORT_INSERT_CHUNK_BYTES"
echo "  IMPORT_TX_BATCH_SIZE=$IMPORT_TX_BATCH_SIZE"
echo "  IMPORT_TX_BATCH_MAX_AGE_MS=$IMPORT_TX_BATCH_MAX_AGE_MS"

cd "$CONSOLE_DIR"

cargo run --quiet -- "${CONSOLE_ARGS[@]}" <<EOF
use $DATABASE;
import $DATA_FILE;
exit;
EOF
