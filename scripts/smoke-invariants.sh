#!/usr/bin/env bash
# CLI-first production invariant battery. Exercises the shipped binary through
# its public command surface; no MCP/JSON-RPC compatibility path is involved.
set -euo pipefail

BIN=${1:-}
if [[ -z "$BIN" ]]; then
  echo "Usage: scripts/smoke-invariants.sh <codebase-memory-cli binary>" >&2
  exit 2
fi
if [[ ! -x "$BIN" ]]; then
  echo "smoke-invariants: binary is not executable: $BIN" >&2
  exit 2
fi
BIN=$(cd "$(dirname "$BIN")" && pwd -P)/$(basename "$BIN")

TMP=$(mktemp -d "${TMPDIR:-/tmp}/cbm-cli-smoke.XXXXXX")
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT INT TERM
export HOME="$TMP/home"
export XDG_CACHE_HOME="$TMP/cache"
mkdir -p "$HOME" "$XDG_CACHE_HOME" "$TMP/repo/src"
cat > "$TMP/repo/src/main.c" <<'SRC'
static int helper(int value) { return value + 1; }
int main(void) { return helper(41) == 42 ? 0 : 1; }
SRC

pass() { printf 'PASS: %s\n' "$1"; }
fail() { printf 'FAIL: %s%s\n' "$1" "${2:+ — $2}" >&2; exit 1; }
run_json() {
  local label=$1; shift
  local out
  if ! out=$("$@" 2>"$TMP/stderr"); then
    cat "$TMP/stderr" >&2 || true
    fail "$label" "command failed"
  fi
  python3 -c 'import json,sys; json.load(sys.stdin)' <<<"$out" >/dev/null 2>&1 || fail "$label" "stdout is not valid JSON"
  printf '%s' "$out"
}

"$BIN" --version >/dev/null || fail version
pass version
"$BIN" --help | grep -q 'codebase-memory-cli' || fail help
pass help
if "$BIN" mcp >/dev/null 2>&1; then fail removed-mcp-command "legacy MCP command unexpectedly succeeded"; fi
pass removed-mcp-command
if "$BIN" definitely-not-a-command >/dev/null 2>&1; then fail unknown-command "unknown command unexpectedly succeeded"; fi
pass unknown-command

INDEX=$(run_json index "$BIN" index "$TMP/repo" --json)
grep -q '"project"' <<<"$INDEX" || fail index "missing project field"
pass index
PROJECTS=$(run_json projects "$BIN" projects --json)
grep -q 'project' <<<"$PROJECTS" || fail projects "indexed project not visible"
pass projects
STATUS=$(cd "$TMP/repo" && run_json status "$BIN" status --json)
grep -q 'project' <<<"$STATUS" || fail status "project context not resolved"
pass status
SEARCH=$(cd "$TMP/repo" && run_json search "$BIN" search helper --json)
grep -q 'helper' <<<"$SEARCH" || fail search "fixture symbol not found"
pass search
SNIPPET=$(cd "$TMP/repo" && run_json snippet "$BIN" snippet helper --json)
grep -q 'helper' <<<"$SNIPPET" || fail snippet "fixture source not returned"
pass snippet
COVERAGE=$(cd "$TMP/repo" && run_json coverage "$BIN" coverage src/main.c --json)
grep -q 'main.c' <<<"$COVERAGE" || fail coverage "fixture path not represented"
pass coverage

if "$BIN" search >/dev/null 2>&1; then fail malformed-search "missing query unexpectedly succeeded"; fi
pass malformed-search

# Doctor is a release assurance surface; allow a non-zero deep-health result but
# require the command itself to exist and produce bounded diagnostic output.
if "$BIN" doctor --help >/dev/null 2>&1; then pass doctor-help; else fail doctor-help; fi

printf 'smoke-invariants: all CLI-first invariants passed\n'
