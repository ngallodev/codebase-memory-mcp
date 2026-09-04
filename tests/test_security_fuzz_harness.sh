#!/usr/bin/env bash
set -euo pipefail

# Regression test for scripts/security-fuzz.sh itself. The harness must execute
# the CLI workload inside a private runtime/cache/home rather than inheriting
# caller state, and it must reject a target that crashes under a fuzz case.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

FAKE="$WORKDIR/fake-cli"
cat > "$FAKE" <<'EOF_FIXTURE'
#!/usr/bin/env bash
printf '%s\t%s\t%s\n' "${HOME-}" "${CBM_CACHE_DIR-}" "${CBM_RUNTIME_DIR-}" >> "$CBM_FUZZ_ENV_PROBE"
if [[ "${1-}" == "definitely-not-a-command" && "${CBM_FUZZ_FORCE_CRASH-}" == "1" ]]; then
    kill -SEGV $$
fi
exit 0
EOF_FIXTURE
chmod +x "$FAKE"

CALLER_HOME="$WORKDIR/caller-home"
CALLER_CACHE="$WORKDIR/caller-cache"
CALLER_RUNTIME="$WORKDIR/caller-runtime"
ENV_LOG="$WORKDIR/environment.log"
mkdir -p "$CALLER_HOME" "$CALLER_CACHE" "$CALLER_RUNTIME"

HOME="$CALLER_HOME" \
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_FUZZ_ENV_PROBE="$ENV_LOG" \
    "$ROOT/scripts/security-fuzz.sh" "$FAKE" > "$WORKDIR/fuzz.out" 2>&1

[[ -s "$ENV_LOG" ]] || { echo "FAIL: security-fuzz did not execute the fixture"; exit 1; }
while IFS=$'\t' read -r child_home child_cache child_runtime; do
    [[ -n "$child_home" && "$child_home" != "$CALLER_HOME" ]] || { echo "FAIL: caller HOME leaked"; exit 1; }
    [[ -n "$child_cache" && "$child_cache" != "$CALLER_CACHE" ]] || { echo "FAIL: caller cache leaked"; exit 1; }
    [[ -n "$child_runtime" && "$child_runtime" != "$CALLER_RUNTIME" ]] || { echo "FAIL: caller runtime leaked"; exit 1; }
done < "$ENV_LOG"

set +e
HOME="$CALLER_HOME" \
CBM_CACHE_DIR="$CALLER_CACHE" \
CBM_RUNTIME_DIR="$CALLER_RUNTIME" \
CBM_FUZZ_ENV_PROBE="$ENV_LOG" \
CBM_FUZZ_FORCE_CRASH=1 \
    "$ROOT/scripts/security-fuzz.sh" "$FAKE" > "$WORKDIR/crash.out" 2>&1
rc=$?
set -e
[[ $rc -ne 0 ]] || { echo "FAIL: security-fuzz accepted a crashing target"; exit 1; }

echo "PASS: security fuzz harness isolates runtime state and rejects crashes"
