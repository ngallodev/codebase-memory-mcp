#!/usr/bin/env bash
# analyze-memory.sh — focused static and dynamic memory analysis.
#
# Usage: scripts/analyze-memory.sh [suite ...]
# Defaults to the suites most relevant to daemon/session lifetime leaks.

set -u -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '2,7p' "$0"
    exit 0
fi

if [[ "$#" -gt 0 ]]; then
    SUITES=("$@")
else
    SUITES=(daemon_application daemon_ipc)
fi

RESULTS_DIR="${CBM_MEMORY_RESULTS_DIR:-$ROOT/memlab-memory-analysis}"
RUN_ID="$(date +%Y%m%d-%H%M%S)-$$"
ANALYSIS_BUILD="$(mktemp -d "$ROOT/build/memory-analysis.XXXXXX")"
TEST_BIN="$ANALYSIS_BUILD/test-runner"
RUN_RESULTS="$RESULTS_DIR/$RUN_ID"
SCAN_RESULTS="$RUN_RESULTS/scan-build"
mkdir -p "$RUN_RESULTS"
trap 'rm -rf "$ANALYSIS_BUILD"' EXIT

for tool in scan-build clang gcc g++ heaptrack valgrind; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "missing required tool: $tool" >&2
        exit 1
    }
done

printf '=== memory analysis: suites=%s ===\n' "${SUITES[*]}"
printf '=== memory analysis: results=%s ===\n' "$RUN_RESULTS"

echo '=== 1/4 scan-build ==='
mkdir -p "$SCAN_RESULTS"
SCAN_STATUS=0
for source in \
    src/operations/session_state.c \
    src/operations/store_host.c \
    src/operations/mutation.c \
    src/daemon/application.c; do
    if ! CBM_NO_CCACHE=1 scan-build --status-bugs -analyze-headers \
        -o "$SCAN_RESULTS" \
        clang -std=c11 -D_DEFAULT_SOURCE -D_GNU_SOURCE \
        -Isrc -Ivendored -Ivendored/sqlite3 -Ivendored/mimalloc/include \
        -Iinternal/cbm -Iinternal/cbm/vendored/ts_runtime/include \
        -Ibuild/c/generated -DCBM_ENABLE_TEST_SEAMS=1 -g -O1 \
        -c "$source" -o /dev/null; then
        SCAN_STATUS=1
        echo "scan-build: finding or failure in $source" >&2
    fi
done
if [[ "$SCAN_STATUS" -ne 0 ]]; then
    echo 'scan-build: findings or failure (see scan-build/)' >&2
else
    echo 'scan-build: no findings in targeted sources'
fi

echo '=== 2/4 plain debug build ==='
if ! CBM_NO_CCACHE=1 make -j"${NPROC:-$(nproc 2>/dev/null || echo 4)}" \
    -f Makefile.cbm BUILD_DIR="$ANALYSIS_BUILD" CC=gcc CXX=g++ SANITIZE= \
    CFLAGS_TEST_EXTRA=-Wno-error=free-nonheap-object \
    "$ANALYSIS_BUILD/test-runner";
then
    echo 'plain debug build failed; dynamic analysis not run' >&2
    exit 1
fi

echo '=== 3/4 heaptrack ==='
heaptrack --output "$RUN_RESULTS/heaptrack" "$TEST_BIN" "${SUITES[@]}" \
    >"$RUN_RESULTS/heaptrack.stdout" 2>"$RUN_RESULTS/heaptrack.stderr";
HEAPTRACK_STATUS=$?
HEAPTRACK_LEAKS=unknown
if [[ "$HEAPTRACK_STATUS" -eq 0 && -f "$RUN_RESULTS/heaptrack.zst" ]]; then
    heaptrack --analyze "$RUN_RESULTS/heaptrack.zst" \
        >"$RUN_RESULTS/heaptrack.analysis" 2>"$RUN_RESULTS/heaptrack.analysis.stderr"
    HEAPTRACK_ANALYZE_STATUS=$?
    HEAPTRACK_LEAKS="$(awk '/leaked allocations:/ {print $3; exit}' \
        "$RUN_RESULTS/heaptrack.stderr")"
    if [[ "$HEAPTRACK_ANALYZE_STATUS" -ne 0 ]]; then
        HEAPTRACK_STATUS=1
        echo "heaptrack: analyzer failed (see $RUN_RESULTS)" >&2
    elif [[ "${HEAPTRACK_LEAKS:-0}" =~ ^[1-9][0-9]*$ ]]; then
        # Heaptrack's raw retained-allocation counter is diagnostic, not a
        # release leak verdict. This suite intentionally keeps process-lifetime
        # lock-registry identity tombstones reachable, and fork-only test
        # children inherit parent allocations before _exit(). Memcheck below
        # classifies the top-level process by leak kind and remains the gate.
        echo "heaptrack: retained allocations=${HEAPTRACK_LEAKS} (diagnostic; Memcheck classifies leak ownership)" >&2
    fi
fi
if [[ "$HEAPTRACK_STATUS" -ne 0 ]]; then
    echo "heaptrack: test/analyzer exit $HEAPTRACK_STATUS (see $RUN_RESULTS)" >&2
fi

echo '=== 4/4 valgrind memcheck ==='
VALGRIND_STATUS=0
valgrind \
    --tool=memcheck \
    --leak-check=full \
    --show-leak-kinds=all \
    --errors-for-leak-kinds=none \
    --track-origins=yes \
    --num-callers=30 \
    --error-exitcode=99 \
    --log-file="$RUN_RESULTS/valgrind.log" \
    "$TEST_BIN" "${SUITES[@]}" \
    >"$RUN_RESULTS/valgrind.stdout" 2>"$RUN_RESULTS/valgrind.stderr";
VALGRIND_RUN_STATUS=$?
VALGRIND_STATUS=0

# Leak kinds are not allowed to rewrite fork-child exit codes: these tests
# intentionally fork with inherited parent state and verify the child's own
# status. Instead, retain --error-exitcode for actual Memcheck memory errors,
# aggregate every process's ERROR SUMMARY, and gate definite/indirect losses
# from the final (top-level test-runner) leak summary. The runner waits for its
# fork children, so its summary is the final summary in the shared log.
VALGRIND_ERROR_CONTEXTS="$(awk '/ERROR SUMMARY:/ {sum += $4} END {print sum + 0}' \
    "$RUN_RESULTS/valgrind.log")"
VALGRIND_DEFINITE_LOST="$(awk '/definitely lost:/ {value=$4} END {gsub(/,/, "", value); print value + 0}' \
    "$RUN_RESULTS/valgrind.log")"
VALGRIND_INDIRECT_LOST="$(awk '/indirectly lost:/ {value=$4} END {gsub(/,/, "", value); print value + 0}' \
    "$RUN_RESULTS/valgrind.log")"
VALGRIND_POSSIBLY_LOST="$(awk '/possibly lost:/ {value=$4} END {gsub(/,/, "", value); print value + 0}' \
    "$RUN_RESULTS/valgrind.log")"
VALGRIND_REACHABLE="$(awk '/still reachable:/ {value=$4} END {gsub(/,/, "", value); print value + 0}' \
    "$RUN_RESULTS/valgrind.log")"

if [[ "$VALGRIND_RUN_STATUS" -ne 0 || "${VALGRIND_ERROR_CONTEXTS:-0}" -ne 0 ||
      "${VALGRIND_DEFINITE_LOST:-0}" -ne 0 || "${VALGRIND_INDIRECT_LOST:-0}" -ne 0 ]]; then
    VALGRIND_STATUS=1
    echo "valgrind: runner/errors/leaks non-green (runner_exit=${VALGRIND_RUN_STATUS} errors=${VALGRIND_ERROR_CONTEXTS:-unknown} definite=${VALGRIND_DEFINITE_LOST:-unknown} indirect=${VALGRIND_INDIRECT_LOST:-unknown}; see $RUN_RESULTS)" >&2
else
    echo "valgrind: top-level lost=0/0 bytes (definite/indirect), errors=0; possibly=${VALGRIND_POSSIBLY_LOST:-unknown} reachable=${VALGRIND_REACHABLE:-unknown}"
fi

printf '\n=== memory analysis summary ===\n'
printf 'scan-build=%s heaptrack=%s valgrind=%s\n' "$SCAN_STATUS" "$HEAPTRACK_STATUS" "$VALGRIND_STATUS"
printf 'results=%s\n' "$RUN_RESULTS"

if [[ "$SCAN_STATUS" -ne 0 || "$HEAPTRACK_STATUS" -ne 0 || "$VALGRIND_STATUS" -ne 0 ]]; then
    exit 1
fi
