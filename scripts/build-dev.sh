#!/usr/bin/env bash
# build-dev.sh — incremental local production build.
# CI/release callers should use build.sh, which deliberately cleans first.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: scripts/build-dev.sh [VAR=VAL ...]

Incrementally builds build/c/codebase-memory-cli. Make dependency files and
the compiler cache rebuild only changed or stale translation units.
Use scripts/build.sh for a clean CI/release build.
EOF
    exit 0
fi

# shellcheck source=env.sh
source "$ROOT/scripts/env.sh"
# shellcheck source=path-safety.sh
source "$ROOT/scripts/path-safety.sh"

print_env "build-dev.sh"
verify_compiler "$CC"
make -j"$NPROC" -f Makefile.cbm cbm "$@"
