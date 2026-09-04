#!/usr/bin/env python3
"""Prepare deterministic GitHub RC dispatch metadata and commands.

This does not call GitHub. It validates operator inputs and emits an immutable
handoff record plus the exact `gh workflow run` command to dispatch release.yml
with external qualification held on by default.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

SEMVER_RC = re.compile(r"^v\d+\.\d+\.\d+-rc\.\d+$")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def git_output(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="RC tag, e.g. v0.11.0-rc.1")
    ap.add_argument("--repo", default="DeusData/codebase-memory-mcp")
    ap.add_argument("--root", default=".")
    ap.add_argument("--output", default="qualification/rc-dispatch.json")
    ap.add_argument("--soak-level", choices=("full", "quick"), default="full")
    ap.add_argument("--skip-perf", action="store_true")
    ap.add_argument("--skip-tests", action="store_true")
    baseline = ap.add_mutually_exclusive_group(required=True)
    baseline.add_argument("--baseline-zero", action="store_true", help="establish the first qualified CLI baseline; no comparative baseline artifact")
    baseline.add_argument("--baseline-tag", help="immutable public CLI-first baseline tag for comparative qualification")
    ns = ap.parse_args()

    if not SEMVER_RC.fullmatch(ns.version):
        ap.error("--version must be an explicit vX.Y.Z-rc.N tag")

    root = Path(ns.root).resolve()
    workflow = root / ".github" / "workflows" / "release.yml"
    corpus = root / "docs" / "qualification" / "BENCHMARK_CORPUS.json"
    if not workflow.is_file() or not corpus.is_file():
        ap.error("--root does not look like the qualification-ready repository")

    commit = git_output(root, "rev-parse", "HEAD")
    dirty = bool(git_output(root, "status", "--porcelain"))
    if dirty:
        ap.error("repository must be clean before preparing an RC dispatch")

    record = {
        "schema_version": 1,
        "repository": ns.repo,
        "source_commit": commit,
        "version": ns.version,
        "workflow": ".github/workflows/release.yml",
        "workflow_sha256": sha256_file(workflow),
        "corpus_generation": json.loads(corpus.read_text(encoding="utf-8"))["generation"],
        "corpus_manifest_sha256": sha256_file(corpus),
        "dispatch": {
            "soak_level": ns.soak_level,
            "skip_perf": bool(ns.skip_perf),
            "skip_tests": bool(ns.skip_tests),
            "hold_for_external_qualification": True,
        },
        "qualification_host": "luigi.home.arpa",
        "benchmark_baseline": ({"mode": "BASELINE_ZERO"} if ns.baseline_zero else {"mode": "COMPARE", "tag": ns.baseline_tag}),
        "expected_release_state_after_ci": "draft",
    }

    out = (root / ns.output).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def tf(value: bool) -> str:
        return "true" if value else "false"

    cmd = [
        "gh", "workflow", "run", "release.yml",
        "--repo", ns.repo,
        "--ref", commit,
        "-f", f"version={ns.version}",
        "-f", f"soak_level={ns.soak_level}",
        "-f", f"skip_perf={tf(ns.skip_perf)}",
        "-f", f"skip_tests={tf(ns.skip_tests)}",
        "-f", "hold_for_external_qualification=true",
    ]
    print("RC dispatch record:", out)
    print("Source commit:", commit)
    print("Dispatch command:")
    print(" ".join(cmd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
