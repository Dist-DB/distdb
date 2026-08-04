#!/usr/bin/env bash
set -euo pipefail

SERVER_ADDR="${SERVER_ADDR:-localhost:4001}"

RUST_LOG="info,serverlib::p2p=debug,console=debug,peerlib::connector::transport=debug" \
RUST_BACKTRACE=1 \
cargo run localhost
