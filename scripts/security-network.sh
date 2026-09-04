#!/usr/bin/env bash
set -euo pipefail

# Network egress test for the CLI-first product. Runs local index/search/status
# activity under strace and rejects unexpected outbound connections.
BINARY="${1:?usage: security-network.sh <binary-path>}"
[[ -x "$BINARY" ]] || { echo "FAIL: binary not executable: $BINARY" >&2; exit 2; }
if ! command -v strace >/dev/null 2>&1; then
    echo "SKIP: strace is unavailable"
    exit 0
fi

# shellcheck source=test-runtime.sh
source "$(dirname "${BASH_SOURCE[0]}")/test-runtime.sh"
cbm_test_runtime_init
ROOT="$CBM_TEST_RUNTIME_ROOT"
trap 'cbm_test_runtime_cleanup "$BINARY"' EXIT
export HOME="$ROOT/home" XDG_CACHE_HOME="$CBM_CACHE_DIR"
mkdir -p "$HOME" "$ROOT/repo"
printf 'int helper(void){return 42;}\nint main(void){return helper()!=42;}\n' > "$ROOT/repo/main.c"
TRACE="$ROOT/strace.log"

# No updater should be required to exercise local code intelligence. Keep the
# workload deliberately local and machine-readable.
strace -f -e trace=network -o "$TRACE" "$BINARY" index "$ROOT/repo" --json >/dev/null 2>&1
(
  cd "$ROOT/repo"
  strace -f -e trace=network -o "$ROOT/search.strace" "$BINARY" search helper --json >/dev/null 2>&1
  strace -f -e trace=network -o "$ROOT/status.strace" "$BINARY" status --json >/dev/null 2>&1
)
cat "$ROOT/search.strace" "$ROOT/status.strace" >> "$TRACE"

# AF_UNIX coordination is expected. Any AF_INET/AF_INET6 connect is outbound
# network activity and is forbidden for these local operations.
if grep -E 'connect\([^,]+, \{sa_family=AF_INET6?' "$TRACE" >/dev/null 2>&1; then
    echo "FAIL: local CLI workflow attempted an IP network connection" >&2
    grep -E 'connect\([^,]+, \{sa_family=AF_INET6?' "$TRACE" >&2 || true
    exit 1
fi

echo "PASS: local CLI index/search/status made no IP network connections"
