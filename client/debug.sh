#!/usr/bin/env bash
set -euo pipefail

SERVER_ADDR="${SERVER_ADDR:-127.0.0.1:4001}"
QUERY_SQL="${QUERY_SQL:-show databases}"
USER_NAME="${USER_NAME:-root}"
PASSWORD="${PASSWORD:-root}"
DATABASE_NAME="${DATABASE_NAME:-main}"

# Keep demo defaults simple: rely on bootstrap address discovery and do not force
# an explicit peer id unless it is known to match connector-discovered identity.
RUST_LOG="${RUST_LOG:-info,client=info,clientlib=info,connector=warn,peerlib=warn}" \
RUST_BACKTRACE=1 \
cargo run -- \
	"${SERVER_ADDR}" \
	"servers=${SERVER_ADDR}" \
	"query=${QUERY_SQL}" \
	"user=${USER_NAME}" \
	"password=${PASSWORD}" \
	"database=${DATABASE_NAME}"