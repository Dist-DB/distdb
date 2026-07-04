#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

printf '[e2e] building server + console binaries\n'
(cd "$ROOT_DIR/server" && cargo build --quiet)
(cd "$ROOT_DIR/console" && cargo build --quiet)

for suite in smoke.sh isolation_restart.sh stress.sh; do
  printf '[e2e] running %s\n' "$suite"
  "$SCRIPT_DIR/$suite"
done

printf '[e2e] ALL E2E SUITES PASSED\n'
