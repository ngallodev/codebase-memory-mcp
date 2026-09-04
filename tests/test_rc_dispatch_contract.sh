#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

test -f scripts/ci/prepare-rc-dispatch.py
test -f docs/agents/GITHUB_RC_DISPATCH_AGENT.md
python3 -m py_compile scripts/ci/prepare-rc-dispatch.py

grep -q 'hold_for_external_qualification=true' docs/agents/GITHUB_RC_DISPATCH_AGENT.md
grep -q 'luigi.home.arpa' docs/agents/GITHUB_RC_DISPATCH_AGENT.md
grep -q 'v0.11.0-rc.1' docs/agents/GITHUB_RC_DISPATCH_AGENT.md
grep -q 'repository must be clean' scripts/ci/prepare-rc-dispatch.py
grep -q 'expected_release_state_after_ci' scripts/ci/prepare-rc-dispatch.py

if python3 scripts/ci/prepare-rc-dispatch.py --version v0.11.0 --baseline-zero >/dev/null 2>&1; then
  echo 'non-RC version unexpectedly accepted' >&2
  exit 1
fi

printf '%s\n' 'RC dispatch contract passed'

if python3 scripts/ci/prepare-rc-dispatch.py --version v0.11.0-rc.1 >/dev/null 2>&1; then
  echo 'missing benchmark baseline decision unexpectedly accepted' >&2
  exit 1
fi

grep -q 'baseline-zero' docs/agents/GITHUB_RC_DISPATCH_AGENT.md
grep -q 'BASELINE_ZERO' docs/WINDOWS_BENCHMARK_PROTOCOL.md
