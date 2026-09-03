#!/usr/bin/env bash
set -euo pipefail

# Deterministic CLI robustness battery. Invalid/adversarial input may fail, but
# it must fail promptly without signals, hangs, or accidental protocol fallback.
BINARY="${1:?usage: security-fuzz.sh <binary-path>}"
[[ -x "$BINARY" ]] || { echo "FAIL: binary not executable: $BINARY" >&2; exit 2; }
TMP=$(mktemp -d "${TMPDIR:-/tmp}/cbm-cli-fuzz.XXXXXX")
trap 'rm -rf "$TMP"' EXIT INT TERM
export HOME="$TMP/home" XDG_CACHE_HOME="$TMP/cache"
mkdir -p "$HOME" "$XDG_CACHE_HOME" "$TMP/repo"
printf 'int main(void){return 0;}\n' > "$TMP/repo/main.c"
"$BINARY" index "$TMP/repo" --json >/dev/null

run_case() {
  local name=$1; shift
  local ec=0
  if command -v timeout >/dev/null 2>&1; then
    timeout 10 "$@" >"$TMP/out" 2>"$TMP/err" || ec=$?
  else
    perl -e 'alarm(10); exec @ARGV' -- "$@" >"$TMP/out" 2>"$TMP/err" || ec=$?
    [[ $ec -ne 142 ]] || ec=124
  fi
  case "$ec" in
    124) echo "FAIL: $name hung" >&2; exit 1 ;;
    134|136|137|139) echo "FAIL: $name crashed (exit $ec)" >&2; exit 1 ;;
  esac
  printf 'PASS: %s (exit %d)\n' "$name" "$ec"
}

HUGE=$(python3 - <<'PY'
print('A' * 1048576)
PY
)
run_case removed-mcp "$BINARY" mcp
run_case unknown-command "$BINARY" definitely-not-a-command
run_case missing-search-query "$BINARY" search
run_case huge-search-query "$BINARY" search "$HUGE" --json
run_case traversal-snippet "$BINARY" snippet ../../../../etc/passwd --json
run_case cypher-sql-injection "$BINARY" query 'MATCH (n) RETURN n; DROP TABLE nodes; --' --json
run_case cypher-attach "$BINARY" query "ATTACH DATABASE '/tmp/evil.db' AS evil" --json
run_case source-shell-metacharacters "$BINARY" source-search '$(touch /tmp/cbm-should-not-exist);|;&' --json
run_case changes-shell-metacharacters "$BINARY" changes "main; touch /tmp/cbm-should-not-exist" --json
run_case nonexistent-index "$BINARY" index /nonexistent/path/abc123 --json

[[ ! -e /tmp/cbm-should-not-exist ]] || { rm -f /tmp/cbm-should-not-exist; echo 'FAIL: shell metacharacters executed' >&2; exit 1; }
echo 'PASS: deterministic CLI robustness battery'
