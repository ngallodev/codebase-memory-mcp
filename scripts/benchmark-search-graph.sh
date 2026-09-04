#!/usr/bin/env bash
# benchmark-search-graph.sh — Time canonical CLI graph-search cases against a
# codebase-memory-cli binary to measure regex / BM25 search performance.
#
# Usage:
#   scripts/benchmark-search-graph.sh <binary-path> <project-name>

set -euo pipefail

BINARY="${1:?Usage: $0 <binary-path> <project-name>}"
PROJECT="${2:?Usage: $0 <binary-path> <project-name>}"

echo "Binary:  $BINARY"
echo "Project: $PROJECT"
echo ""

run_case() {
    local label="$1"; shift
    local start end elapsed_ms result count

    start=$(date +%s%3N)
    result=$("$BINARY" search --project "$PROJECT" --json "$@" 2>/dev/null || true)
    end=$(date +%s%3N)
    elapsed_ms=$(( end - start ))

    count=$(printf '%s' "$result" | python3 -c '
import json, sys
try:
    obj = json.load(sys.stdin)
    print(obj.get("total", obj.get("count", "?")))
except Exception:
    print("?")
' 2>/dev/null || echo "?")

    printf "  %-55s %5dms  (total=%s)\n" "$label" "$elapsed_ms" "$count"
}

echo "=== search name-pattern benchmarks ==="
run_case "name_pattern=.*Controller.*"         --name-pattern '.*Controller.*' --limit 20
run_case "name_pattern=.*Service.*"            --name-pattern '.*Service.*' --limit 20
run_case "name_pattern=.*Repository.*"         --name-pattern '.*Repository.*' --limit 20
run_case "name_pattern=specificFunctionName"   --name-pattern 'specificFunctionName' --limit 20
run_case "label=Method + name_pattern=.*get.*" --label Method --name-pattern '.*get.*' --limit 20

echo ""
echo "=== search query benchmarks (BM25 path) ==="
run_case "query=controller service handler"                  --query 'controller service handler' --limit 20
run_case "query=user authentication permission role"         --query 'user authentication permission role' --limit 20
run_case "query=create update delete manage list view admin" --query 'create update delete manage list view admin' --limit 20
