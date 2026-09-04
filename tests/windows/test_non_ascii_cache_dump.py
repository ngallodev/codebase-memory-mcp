"""Regression guard for issue #996 — dump phase must write the graph DB into a
NON-ASCII cache directory on Windows.

The existing test_non_ascii_path.py exercises non-ASCII REPO paths against an
ASCII cache, so it never covers the dump->cache write. #996's reporter (a
non-ASCII %USERPROFILE%, e.g. C:\\Users\\Kovács János) saw extract/resolve
succeed and `pipeline.err phase=dump`: cbm_writer_open used a raw ANSI-CP
fopen for the hand-rolled SQLite writer (internal/cbm/sqlite_writer.c), the
one file-creating call on the dump chain without UTF-8→wide conversion.
Fixed by routing it through cbm_fopen (same pattern as #700/#973).

This guard indexes an ASCII repo into a NON-ASCII cache dir (CBM_CACHE_DIR is
read before any USERPROFILE derivation, so it isolates the writer cleanly).
GREEN is non-vacuous: the index must succeed AND a query_graph readback must
count Function nodes > 0 — proving the DB was written to and reopened from
the non-ASCII cache, not merely that no error surfaced.

Passes on Linux/macOS either way (byte-transparent UTF-8 filesystems).

Exit code: 0 == invariant holds, 1 == regression, 2 == setup error.

Usage:
    python test_non_ascii_cache_dump.py <path-to-codebase-memory-cli[.exe]>
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile


MATH_TS = (
    "export function add(a: number, b: number): number { return a + b; }\n"
    "export function mul(a: number, b: number): number { return add(a, a); }\n"
    "export class Calc {\n"
    "  total: number = 0;\n"
    "  push(x: number): void { this.total = add(this.total, x); }\n"
    "}\n"
)

# Mixed scripts in ONE segment — one shot covers the classes the sibling
# test exercises separately (the writer either converts wide or it doesn't).
NON_ASCII_CACHE_SEGMENT = "cache_café_Ωμέγα_日本語"


def run_cli(binary, cache, args, timeout=180):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout, env=env)


def output_text(result):
    return (result.stdout or b"").decode("utf-8", "replace")


def graph_function_count(binary, cache, project):
    result = run_cli(
        binary, cache,
        ["query", "MATCH (n:Function) RETURN count(n) AS c", "--project", project],
        timeout=60,
    )
    if result.returncode != 0:
        return 0
    text = output_text(result)
    for line in text.splitlines():
        stripped = line.strip().strip('"')
        if stripped.isdigit():
            return int(stripped)
    return 0


def main():
    if len(sys.argv) != 2:
        print("usage: test_non_ascii_cache_dump.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print(f"SETUP: binary not found: {binary}")
        return 2

    work = tempfile.mkdtemp(prefix="cbm_996_")
    try:
        repo = os.path.join(work, "ascii_repo")
        os.makedirs(repo)
        with open(os.path.join(repo, "math.ts"), "w", encoding="utf-8") as f:
            f.write(MATH_TS)

        cache = os.path.join(work, NON_ASCII_CACHE_SEGMENT)
        os.makedirs(cache)

        indexed = run_cli(binary, cache, ["index", repo, "--json"])
        text = output_text(indexed)
        if indexed.returncode != 0:
            diagnostic = ((indexed.stdout or b"") + (indexed.stderr or b"")).decode(
                "utf-8", "replace")
            print(f"FAIL: index into non-ASCII cache errored: {diagnostic[:500]}")
            return 1
        if "phase" in text and "dump" in text and "error" in text.lower():
            print(f"FAIL: dump phase error: {text[:300]}")
            return 1

        # Non-vacuous readback: the DB must exist under the non-ASCII cache
        # and the canonical query command must reopen and query it.
        try:
            project = json.loads(text).get("project")
        except (ValueError, AttributeError):
            project = None
        if not project:
            print("FAIL: canonical index output did not identify the project")
            return 1

        count = graph_function_count(binary, cache, project)
        if count < 1:
            print(f"FAIL: readback from non-ASCII cache found {count} Function nodes")
            return 1

        print(f"OK: dump wrote and reopened graph DB under non-ASCII cache ({count} functions)")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
