#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVERLIB_DIR="$ROOT_DIR/serverlib"
CLIENTLIB_DIR="$ROOT_DIR/clientlib"
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
MIN_FUNCTIONS="${COVERAGE_MIN_FUNCTIONS:-0}"
MIN_REGIONS="${COVERAGE_MIN_REGIONS:-0}"
MIN_FILE_LINES="${COVERAGE_MIN_FILE_LINES:-0}"
ENABLE_BRANCH_COVERAGE="${COVERAGE_ENABLE_BRANCH:-false}"

echo "Using coverage thresholds:"
echo "  lines:      ${MIN_LINES}%"
echo "  functions:  ${MIN_FUNCTIONS}%"
echo "  regions:    ${MIN_REGIONS}%"
echo "  file lines: ${MIN_FILE_LINES}%"
echo "  branch mode: ${ENABLE_BRANCH_COVERAGE}"

mkdir -p "$OUT_DIR"

run_cov() {
  local crate_dir="$1"
  local crate_name="$2"
  local -a threshold_args=()
  local -a mode_args=()

  echo
  echo "==> Running coverage for ${crate_name} (${crate_dir})"
  pushd "$crate_dir" >/dev/null

  threshold_args+=(--fail-under-lines "$MIN_LINES")
  if [[ "$MIN_FUNCTIONS" != "0" ]]; then
    threshold_args+=(--fail-under-functions "$MIN_FUNCTIONS")
  fi
  if [[ "$MIN_REGIONS" != "0" ]]; then
    threshold_args+=(--fail-under-regions "$MIN_REGIONS")
  fi
  if [[ "$MIN_FILE_LINES" != "0" ]]; then
    threshold_args+=(--fail-under-file-lines "$MIN_FILE_LINES")
  fi

  if [[ "$ENABLE_BRANCH_COVERAGE" == "1" || "$ENABLE_BRANCH_COVERAGE" == "true" || "$ENABLE_BRANCH_COVERAGE" == "yes" || "$ENABLE_BRANCH_COVERAGE" == "on" ]]; then
    # Branch coverage is supported by cargo-llvm-cov behind an unstable flag.
    mode_args+=(--branch)
  fi

  cargo llvm-cov clean --workspace

  # Run tests once with gates enabled; emit reports in follow-up no-run steps.
  cargo llvm-cov \
    --workspace \
    --no-report \
    "${threshold_args[@]}" \
    "${mode_args[@]}"

  cargo llvm-cov \
    --workspace \
    --no-run \
    --json \
    --summary-only \
    --output-path "$OUT_DIR/${crate_name}-summary.json" \
    "${mode_args[@]}"

  cargo llvm-cov \
    --workspace \
    --no-run \
    --lcov \
    --output-path "$OUT_DIR/${crate_name}.lcov" \
    "${mode_args[@]}"

  popd >/dev/null
}

run_cov "$SERVERLIB_DIR" "serverlib"
run_cov "$SERVER_DIR" "server"
run_cov "$CONNECTOR_DIR" "connector"
run_cov "$COMMON_DIR" "common"
run_cov "$PEERLIB_DIR" "peerlib"
run_cov "$CLIENTLIB_DIR" "clientlib"

echo
echo "Coverage reports generated:"
echo "  $OUT_DIR/serverlib-summary.json"
echo "  $OUT_DIR/serverlib.lcov"
echo "  $OUT_DIR/server-summary.json"
echo "  $OUT_DIR/server.lcov"
echo "  $OUT_DIR/connector-summary.json"
echo "  $OUT_DIR/connector.lcov"
echo "  $OUT_DIR/common-summary.json"
echo "  $OUT_DIR/common.lcov"
echo "  $OUT_DIR/peerlib-summary.json"
echo "  $OUT_DIR/peerlib.lcov"
echo "  $OUT_DIR/clientlib-summary.json"
echo "  $OUT_DIR/clientlib.lcov"