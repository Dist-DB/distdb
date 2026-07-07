#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVERLIB_DIR="$ROOT_DIR/serverlib"
SERVER_DIR="$ROOT_DIR/server"
COMMON_DIR="$ROOT_DIR/common"
CONNECTOR_DIR="$ROOT_DIR/connector"
PEERLIB_DIR="$ROOT_DIR/peerlib"

OUT_DIR="$ROOT_DIR/coverage"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "cargo-llvm-cov is not installed. Install with: cargo install cargo-llvm-cov"
  exit 1
fi

if [[ -z "${LLVM_COV:-}" ]] && command -v xcrun >/dev/null 2>&1; then
  LLVM_COV="$(xcrun -f llvm-cov 2>/dev/null || true)"
fi

if [[ -z "${LLVM_PROFDATA:-}" ]] && command -v xcrun >/dev/null 2>&1; then
  LLVM_PROFDATA="$(xcrun -f llvm-profdata 2>/dev/null || true)"
fi

if [[ -n "${LLVM_COV:-}" && -n "${LLVM_PROFDATA:-}" ]]; then
  export LLVM_COV LLVM_PROFDATA
  echo "Using LLVM tools:"
  echo "  LLVM_COV=$LLVM_COV"
  echo "  LLVM_PROFDATA=$LLVM_PROFDATA"
fi

MIN_LINES="${COVERAGE_MIN_LINES:-50}"
echo "Using coverage line threshold: ${MIN_LINES}%"

mkdir -p "$OUT_DIR"

run_cov() {
  local crate_dir="$1"
  local crate_name="$2"

  echo
  echo "==> Running coverage for ${crate_name} (${crate_dir})"
  pushd "$crate_dir" >/dev/null

  cargo llvm-cov clean --workspace
  cargo llvm-cov \
    --workspace \
    --summary-only \
    --fail-under-lines "$MIN_LINES"

  cargo llvm-cov \
    --workspace \
    --lcov \
    --output-path "$OUT_DIR/${crate_name}.lcov"

  popd >/dev/null
}

run_cov "$SERVERLIB_DIR" "serverlib"
run_cov "$SERVER_DIR" "server"
run_cov "$CONNECTOR_DIR" "connector"
run_cov "$COMMON_DIR" "common"
run_cov "$PEERLIB_DIR" "peerlib"

echo
echo "Coverage reports generated:"
echo "  $OUT_DIR/serverlib.lcov"
echo "  $OUT_DIR/server.lcov"
echo "  $OUT_DIR/connector.lcov"
echo "  $OUT_DIR/common.lcov"
echo "  $OUT_DIR/peerlib.lcov"