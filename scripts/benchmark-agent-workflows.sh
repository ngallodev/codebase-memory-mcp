#!/usr/bin/env bash
set -euo pipefail

# Reproducible Linux baseline for agent-relevant Codebase Memory CLI workflows.
# Usage: benchmark-agent-workflows.sh <binary> <repo> <results-dir> [secondary-repo]
# Optional environment: CBM_BENCH_QUERY, CBM_BENCH_SYMBOL, CBM_BENCH_FILE,
# CBM_BENCH_BASE_BRANCH, CBM_BENCH_REPEATS (default 5).
# Each invocation is one independent trial and must use a new/empty results directory.

BINARY="${1:?Usage: $0 <binary> <repo> <results-dir> [secondary-repo]}"
REPO="${2:?}"
RESULTS_DIR="${3:?}"
SECONDARY_REPO="${4:-}"
REPEATS="${CBM_BENCH_REPEATS:-5}"
QUERY="${CBM_BENCH_QUERY:-daemon}"
SYMBOL="${CBM_BENCH_SYMBOL:-main}"
FILE_PATH="${CBM_BENCH_FILE:-src/main.c}"
BASE_BRANCH="${CBM_BENCH_BASE_BRANCH:-main}"

REPO=$(cd "$REPO" && pwd -P)
if [[ -e "$RESULTS_DIR/timings.tsv" || -e "$RESULTS_DIR/cache" ]]; then
  echo "error: results directory already contains benchmark state; use a new empty directory: $RESULTS_DIR" >&2
  exit 2
fi
mkdir -p "$RESULTS_DIR"
RESULTS_DIR=$(cd "$RESULTS_DIR" && pwd -P)
export CBM_CACHE_DIR="$RESULTS_DIR/cache"
mkdir -p "$CBM_CACHE_DIR"

if [[ ! -x "$BINARY" ]]; then
  echo "error: binary is not executable: $BINARY" >&2
  exit 2
fi

printf 'case\trun\telapsed_ms\texit_code\n' > "$RESULTS_DIR/timings.tsv"

now_ms() { python3 -c 'import time; print(time.monotonic_ns() // 1000000)'; }

run_once() {
  local label="$1" run="$2"; shift 2
  local out="$RESULTS_DIR/${label}.${run}.json"
  local err="$RESULTS_DIR/${label}.${run}.stderr"
  local start end rc
  start=$(now_ms)
  set +e
  "$@" >"$out" 2>"$err"
  rc=$?
  set -e
  end=$(now_ms)
  printf '%s\t%s\t%s\t%s\n' "$label" "$run" "$((end-start))" "$rc" >> "$RESULTS_DIR/timings.tsv"
  return "$rc"
}

repeat_case() {
  local label="$1"; shift
  # One unrecorded warm-up keeps daemon startup/cache priming out of steady-state read timings.
  "$@" >/dev/null 2>/dev/null || true
  for ((i=1; i<=REPEATS; i++)); do
    run_once "$label" "$i" "$@" || true
  done
}

# Keep daemon startup out of steady-state read timings.
"$BINARY" daemon stop >/dev/null 2>&1 || true
"$BINARY" daemon start >/dev/null

run_once index 1 "$BINARY" index "$REPO" --mode full --json
PROJECT=$(python3 - "$RESULTS_DIR/index.1.json" <<'PY'
import json,sys
try:
    obj=json.load(open(sys.argv[1], encoding='utf-8'))
    print(obj.get('project') or '')
except Exception:
    print('')
PY
)
if [[ -z "$PROJECT" ]]; then
  echo "error: index did not return a project name" >&2
  exit 3
fi

repeat_case search "$BINARY" search --project "$PROJECT" --query "$QUERY" --limit 20 --json
repeat_case architecture "$BINARY" architecture --project "$PROJECT" --json
repeat_case snippet "$BINARY" snippet --project "$PROJECT" --qualified-name "$SYMBOL" --json
repeat_case outline "$BINARY" outline --project "$PROJECT" --file-path "$FILE_PATH" --limit 100 --json
repeat_case changes "$BINARY" changes --project "$PROJECT" --scope files --base-branch "$BASE_BRANCH" --json
repeat_case status "$BINARY" status --project "$PROJECT" --json

if [[ -n "$SECONDARY_REPO" ]]; then
  SECONDARY_REPO=$(cd "$SECONDARY_REPO" && pwd -P)
  run_once secondary_index 1 "$BINARY" index "$SECONDARY_REPO" --mode full --json
  SECONDARY_PROJECT=$(python3 - "$RESULTS_DIR/secondary_index.1.json" <<'PY'
import json,sys
try:
    obj=json.load(open(sys.argv[1], encoding='utf-8'))
    print(obj.get('project') or '')
except Exception:
    print('')
PY
)
  if [[ -n "$SECONDARY_PROJECT" ]]; then
    run_once cross_repo 1 "$BINARY" index "$REPO" --mode cross-repo-intelligence \
      --target-projects "$SECONDARY_PROJECT" --json || true
  fi
fi

python3 - "$RESULTS_DIR/timings.tsv" > "$RESULTS_DIR/summary.tsv" <<'PY'
import csv, statistics, sys
from collections import defaultdict
rows=defaultdict(list)
with open(sys.argv[1], newline='', encoding='utf-8') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        if r['exit_code']=='0': rows[r['case']].append(int(r['elapsed_ms']))
print('case\truns\tmin_ms\tmedian_ms\tmax_ms')
for case in sorted(rows):
    vals=rows[case]
    print(f"{case}\t{len(vals)}\t{min(vals)}\t{statistics.median(vals):g}\t{max(vals)}")
PY

{
  echo "captured_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "binary=$BINARY"
  "$BINARY" --version 2>/dev/null | sed 's/^/version=/' || true
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$BINARY" | awk '{print "binary_sha256=" $1}'
    sha256sum "$0" | awk '{print "harness_sha256=" $1}'
  fi
  echo "repo=$REPO"
  if git -C "$REPO" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$REPO" rev-parse HEAD | sed 's/^/repo_commit=/'
    git -C "$REPO" status --porcelain=v1 | python3 -c 'import sys; print("repo_dirty=" + ("yes" if sys.stdin.read().strip() else "no"))'
    git -C "$REPO" remote get-url origin 2>/dev/null | sed 's/^/repo_origin=/' || true
  fi
  echo "project=$PROJECT"
  echo "repeats=$REPEATS"
  echo "query=$QUERY"
  echo "symbol=$SYMBOL"
  echo "file=$FILE_PATH"
  echo "base_branch=$BASE_BRANCH"
  [[ -n "$SECONDARY_REPO" ]] && echo "secondary_repo=$SECONDARY_REPO"
  uname -a | sed 's/^/uname=/'
  if command -v lscpu >/dev/null 2>&1; then
    lscpu | sed 's/^/lscpu=/'
  fi
  if [[ -r /proc/meminfo ]]; then
    grep -E '^(MemTotal|SwapTotal):' /proc/meminfo | sed 's/^/meminfo=/'
  fi
  df -T "$REPO" "$RESULTS_DIR" 2>/dev/null | sed 's/^/filesystem=/' || true
  env | grep '^CBM_' | sort | sed 's/^/env=/' || true
} > "$RESULTS_DIR/environment.txt"

"$BINARY" daemon stop >/dev/null 2>&1 || true
cat "$RESULTS_DIR/summary.tsv"
