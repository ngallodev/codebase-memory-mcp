#!/usr/bin/env bash
# Regression guard: shell entrypoints must remain LF in Windows checkouts so
# they can run directly from WSL and MSYS without a `bash\r` shebang failure.
#
# Distilled from PR #1272 by @xumian520, who both hit the breakage under
# core.autocrlf=true and wrote this contract so it stays fixed. The .sh rule
# itself landed via #1314; the extensionless git hooks need their own
# entries, which this guard also covers.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

failures=0
checked=0
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    while IFS= read -r -d '' path &&
        IFS= read -r -d '' attribute &&
        IFS= read -r -d '' eol; do
        checked=$((checked + 1))
        if [[ "$eol" != "lf" ]]; then
            echo "FAIL: $path must declare eol=lf (got ${eol:-unset})" >&2
            failures=$((failures + 1))
        fi
    done < <(
        git ls-files -z '*.sh' 'scripts/git-hooks/*' 'scripts/hooks/*' |
            git check-attr -z --stdin eol
    )
else
    # Source/release archives have no .git metadata. Validate the retained
    # checkout policy directly and inspect the shipped shell entrypoint bytes.
    grep -Fqx '*.sh text eol=lf' .gitattributes || {
        echo "FAIL: .gitattributes must retain '*.sh text eol=lf'" >&2
        exit 1
    }
    grep -Eq '^scripts/git-hooks/commit-msg[[:space:]]+text eol=lf$' .gitattributes || {
        echo "FAIL: commit-msg hook must retain eol=lf" >&2
        exit 1
    }
    grep -Eq '^scripts/hooks/pre-commit[[:space:]]+text eol=lf$' .gitattributes || {
        echo "FAIL: pre-commit hook must retain eol=lf" >&2
        exit 1
    }
    while IFS= read -r -d '' path; do
        checked=$((checked + 1))
        if LC_ALL=C grep -q $'\r' "$path"; then
            echo "FAIL: $path contains CR bytes in source archive" >&2
            failures=$((failures + 1))
        fi
    done < <(find . -type f \( -name '*.sh' -o -path './scripts/git-hooks/commit-msg' -o -path './scripts/hooks/pre-commit' \) -print0)
fi

if ((checked == 0)); then
    echo "FAIL: the line-ending contract matched no files — the glob set is broken" >&2
    exit 1
fi

if ((failures > 0)); then
    echo "FAIL: $failures shell entrypoint(s) lack an LF checkout contract" >&2
    exit 1
fi

echo "Shell line-ending contract passed ($checked files)"
