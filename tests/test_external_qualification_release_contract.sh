#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
grep -q 'hold_for_external_qualification:' .github/workflows/release.yml
grep -q 'default: true' .github/workflows/release.yml
grep -q 'qualification-hold:' .github/workflows/release.yml
grep -q '!inputs.hold_for_external_qualification' .github/workflows/release.yml
test -f .github/workflows/promote-qualified-release.yml
grep -q 'verify-external-qualification.py' .github/workflows/promote-qualified-release.yml
grep -q 'qualification-manifest.json' .github/workflows/promote-qualified-release.yml
grep -q 'qualification-summary.md' .github/workflows/promote-qualified-release.yml
python3 -m py_compile scripts/ci/verify-external-qualification.py
printf '%s\n' 'External qualification release contract passed'
