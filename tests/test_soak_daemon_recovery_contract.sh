#!/usr/bin/env bash
# Static contract for the daemon crash/restart assertions in the soak harness.
# Source-contract patterns intentionally retain shell variables literally.
# shellcheck disable=SC2016

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
soak="$ROOT/scripts/soak-test.sh"

for required in \
    'start_soak_daemon()' \
    'CBM_DIAGNOSTICS=1' \
    '"$BINARY" daemon start' \
    'diagnostics_start_count()' \
    'DAEMON_PID=$(diagnostics_json_value pid)' \
    'Idle daemon CPU:' \
    'SOAK_PROJECT_VALUE="$SOAK_PROJECT"' \
    'SOAK_PROJECT_VALUE=$(cygpath -m "$SOAK_PROJECT")' \
    'cli_call()' \
    'index_project() { cli_call index "$@"; }' \
    'PROJ_NAME=$(index_response_project)' \
    'wait_for_diagnostics_snapshot "$DIAGNOSTICS_START_COUNT" "$DIAG_FILE_BEFORE_CRASH"' \
    '"$BINARY" index "$SOAK_PROJECT_VALUE" --mode fast --json' \
    'kill -9 "$DAEMON_PID"' \
    'start_soak_daemon append' \
    'index_project "$SOAK_PROJECT_VALUE" --mode fast || PASS=false' \
    '"$BINARY" projects --json' \
    '"$BINARY" daemon stop'; do
    if ! grep -Fq "$required" "$soak"; then
        echo "FAIL: daemon soak recovery contract missing: $required" >&2
        exit 1
    fi
done

if grep -Fq 'WARN: soak DACL stamp failed' "$soak"; then
    echo "FAIL: native-Windows soak must fail closed when its trusted-root DACL cannot be set" >&2
    exit 1
fi

if grep -Fq "json.load(open('\$DIAG_FILE'))" "$soak" ||
    grep -Fq 'with open(sys.argv[1]' "$soak"; then
    echo "FAIL: native Windows Python must consume diagnostics through stdin, not an MSYS path" >&2
    exit 1
fi

if grep -Fq 'ps -o %cpu= -p "$SERVER_PID"' "$soak"; then
    echo "FAIL: soak idle CPU must measure the daemon diagnostics PID, not a retired frontend PID" >&2
    exit 1
fi

if [ "$(grep -c '^PASS=true$' "$soak")" -ne 1 ]; then
    echo "FAIL: soak result state must be initialized exactly once" >&2
    exit 1
fi
if ! grep -Fq 'tests/test_soak_daemon_recovery_contract.sh' "$ROOT/scripts/test.sh"; then
    echo "FAIL: daemon soak recovery contract is not wired into the test suite" >&2
    exit 1
fi

echo "Daemon soak recovery contract passed"
