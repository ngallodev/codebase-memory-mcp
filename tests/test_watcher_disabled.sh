#!/usr/bin/env bash
# test_watcher_disabled.sh — process-level regression for watcher_enabled.
# Uses only supported CLI/daemon surfaces; no retired protocol lifecycle.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${CBM_TEST_BINARY:-${ROOT}/build/c/codebase-memory-cli}"
[ -x "${BINARY}" ] || { echo "missing test binary: ${BINARY}" >&2; exit 2; }
command -v git >/dev/null 2>&1 || { echo "git required for fixture" >&2; exit 2; }

work="$(mktemp -d)"
cleanup() {
  local cache
  for cache in "${work}"/cache-*; do
    [[ -d "${cache}" ]] || continue
    CBM_CACHE_DIR="${cache}" "${BINARY}" daemon stop >/dev/null 2>&1 || true
  done
  rm -rf "${work}"
}
trap cleanup EXIT

repo="${work}/repo"
mkdir -p "${repo}"
printf '%s\n' 'int add(int a, int b) { return a + b; }' >"${repo}/sample.c"
git -C "${repo}" init -q
git -C "${repo}" -c user.email=t@example.com -c user.name=t add -A
git -C "${repo}" -c user.email=t@example.com -c user.name=t commit -q -m init

fail() {
  echo "FAIL: $*" >&2
  for log in "${work}"/cache-*/logs/cbm-daemon.log; do
    [[ -f "${log}" ]] || continue
    grep -E 'msg=(watcher|daemon)\.' "${log}" >&2 || true
  done
  exit 1
}
wait_for() {
  local file="$1" pattern="$2" label="$3" attempts=200
  while [ "$attempts" -gt 0 ]; do
    [ -f "$file" ] && grep -qE "$pattern" "$file" && return 0
    attempts=$((attempts - 1)); sleep 0.1
  done
  fail "timed out waiting for ${label}"
}

c_off="${work}/cache-off"
CBM_CACHE_DIR="$c_off" "$BINARY" config set watcher_enabled false >/dev/null
CBM_CACHE_DIR="$c_off" "$BINARY" config set auto_index false >/dev/null
CBM_CACHE_DIR="$c_off" "$BINARY" daemon start >/dev/null
log_off="$c_off/logs/cbm-daemon.log"
wait_for "$log_off" 'msg=watcher\.disabled .*reason=config' 'watcher.disabled'
! grep -qE 'msg=watcher\.start( |$)' "$log_off" || fail 'watcher started while disabled'
CBM_CACHE_DIR="$c_off" "$BINARY" index "$repo" --mode fast --json >"$work/index.json"
python3 - "$work/index.json" <<'PY'
import json, sys
obj=json.load(open(sys.argv[1], encoding='utf-8'))
if int(obj.get('nodes',0)) <= 0: raise SystemExit('index returned no nodes')
PY
CBM_CACHE_DIR="$c_off" "$BINARY" daemon stop >/dev/null 2>&1 || true
echo 'ok: disabled gate suppresses watcher without suppressing manual index'

c_on="${work}/cache-on"
CBM_CACHE_DIR="$c_on" "$BINARY" config set watcher_enabled true >/dev/null
CBM_CACHE_DIR="$c_on" "$BINARY" daemon start >/dev/null
log_on="$c_on/logs/cbm-daemon.log"
wait_for "$log_on" 'msg=watcher\.start( |$)' 'watcher.start positive control'
CBM_CACHE_DIR="$c_on" "$BINARY" daemon stop >/dev/null 2>&1 || true
echo 'ok: enabled positive control starts watcher'
