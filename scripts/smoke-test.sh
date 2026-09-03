#!/usr/bin/env bash
set -euo pipefail

# Canonical CLI-first smoke entry. Product behavior is exercised through the
# public CLI invariant battery. When a local release fixture URL is supplied,
# also exercise the real installer and the installed executable.
BINARY="${1:?usage: smoke-test.sh <binary-path> [--agent-config-only]}"
MODE="${2:-}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[[ -x "$BINARY" ]] || { echo "smoke-test: binary not executable: $BINARY" >&2; exit 2; }

if [[ "$MODE" == "--agent-config-only" ]]; then
    # Agent configuration is now CLI/skill based. A dry-run install is the
    # supported contract; legacy MCP-registration fixture matrices are gone.
    "$BINARY" install --dry-run --yes >/dev/null
    echo "PASS: agent integration dry-run"
    exit 0
elif [[ -n "$MODE" ]]; then
    echo "smoke-test: unknown option: $MODE" >&2
    exit 2
fi

"$ROOT/scripts/smoke-invariants.sh" "$BINARY"

DOWNLOAD_URL="${SMOKE_DOWNLOAD_URL:-${CBM_DOWNLOAD_URL:-}}"
if [[ -n "$DOWNLOAD_URL" ]]; then
    INSTALL_ROOT="${SMOKE_TEMP_ROOT:-${TMPDIR:-/tmp}}/cbm-install-smoke"
    rm -rf "$INSTALL_ROOT"
    mkdir -p "$INSTALL_ROOT"
    CBM_DOWNLOAD_URL="$DOWNLOAD_URL" \
        bash "$ROOT/install.sh" --dir "$INSTALL_ROOT" --skip-config >/dev/null
    INSTALLED="$INSTALL_ROOT/codebase-memory-cli"
    [[ -x "$INSTALLED" ]] || { echo "FAIL: installer did not create $INSTALLED" >&2; exit 1; }
    "$INSTALLED" --version >/dev/null
    "$ROOT/scripts/smoke-invariants.sh" "$INSTALLED"
    echo "PASS: release installer round-trip"
fi

echo "smoke-test: CLI-first smoke passed"
