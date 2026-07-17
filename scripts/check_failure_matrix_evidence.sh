#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MATRIX_FILE="$ROOT_DIR/docs/node-failure-matrix.md"

fail() {
  printf '[failure-matrix][fail] %s\n' "$*" >&2
  exit 1
}

pass() {
  printf '[failure-matrix][ok] %s\n' "$*"
}

[[ -f "$MATRIX_FILE" ]] || fail "missing matrix file: $MATRIX_FILE"

violations="$({
  awk '
    function trim(s) {
      gsub(/^[ \t\r\n]+/, "", s)
      gsub(/[ \t\r\n]+$/, "", s)
      return s
    }

    /^## Matrix$/ { in_matrix=1; next }
    /^## / && in_matrix { in_matrix=0 }

    in_matrix {
      if ($0 ~ /^\|/) {
        # Skip table header and separator lines.
        if ($0 ~ /^\|[[:space:]]*Scenario[[:space:]]*\|/) next
        if ($0 ~ /^\|[[:space:]-]+\|/) next

        line = $0
        n = split(line, c, "|")
        # Expected: leading pipe, 6 columns, trailing pipe => at least 8 parts.
        if (n < 8) next

        scenario = trim(c[2])
        status = trim(c[6])
        evidence = trim(c[7])

        if (status == "Implemented/Tested") {
          # Require explicit evidence text beyond placeholders.
          if (evidence == "" || evidence == "TODO" || evidence == "TBD" || evidence == "Required beta gate") {
            printf("line %d: scenario '%s' is Implemented/Tested but evidence is missing or placeholder\n", NR, scenario)
          } else if (evidence !~ /`/) {
            printf("line %d: scenario '%s' is Implemented/Tested but evidence should reference concrete test identifiers (use backtick-wrapped paths::tests)\n", NR, scenario)
          }
        }
      }
    }
  ' "$MATRIX_FILE"
} || true)"

if [[ -n "$violations" ]]; then
  printf '%s\n' "$violations" >&2
  fail "node failure matrix evidence validation failed"
fi

pass "node failure matrix evidence validation passed"
