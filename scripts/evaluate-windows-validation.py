#!/usr/bin/env python3
"""Native Windows validation for the CLI-first Codebase Memory build.

This runner intentionally exercises Windows-only release risks that Linux cannot
prove: paths containing spaces/Unicode, process-group cancellation, daemon and
project recovery, and the process-level concurrency evaluation harness.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any


def run(binary: str, args: list[str], cwd: Path, env: dict[str, str], timeout: int = 60) -> dict[str, Any]:
    started = time.monotonic_ns()
    proc = subprocess.run(
        [binary, *args], cwd=cwd, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
    )
    return {
        "args": args,
        "returncode": proc.returncode,
        "elapsed_ms": (time.monotonic_ns() - started) // 1_000_000,
        "stdout": proc.stdout[-12000:],
        "stderr": proc.stderr[-12000:],
    }


def write_repo(repo: Path, files: int = 160) -> None:
    src = repo / "src"
    src.mkdir(parents=True, exist_ok=True)
    (repo / "README.md").write_text("# Windows validation fixture\n", encoding="utf-8")
    for i in range(files):
        (src / f"module_{i:04d}.c").write_text(
            f"int windows_target_{i:04d}(int x) {{ return x + {i}; }}\n",
            encoding="utf-8",
        )


def parse_json_output(result: dict[str, Any]) -> Any:
    try:
        return json.loads(result.get("stdout") or "null")
    except json.JSONDecodeError:
        return None


def cancellation_probe(binary: str, repo: Path, env: dict[str, str]) -> dict[str, Any]:
    # Make the mutation substantive enough that the process group can receive a
    # real console cancellation instead of racing an already-completed command.
    src = repo / "src"
    for i in range(160, 700):
        (src / f"cancel_{i:04d}.c").write_text(
            f"int cancellation_target_{i:04d}(int x) {{ return x ^ {i}; }}\n",
            encoding="utf-8",
        )

    flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    started = time.monotonic_ns()
    proc = subprocess.Popen(
        [binary, "index", str(repo), "--json"], cwd=repo, env=env,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=flags,
    )
    time.sleep(0.35)
    if proc.poll() is not None:
        stdout, stderr = proc.communicate(timeout=5)
        return {
            "verdict": "inconclusive",
            "reason": "index completed before CTRL_BREAK_EVENT could be delivered",
            "returncode": proc.returncode,
            "elapsed_ms": (time.monotonic_ns() - started) // 1_000_000,
            "stdout": stdout[-12000:], "stderr": stderr[-12000:],
        }

    try:
        proc.send_signal(signal.CTRL_BREAK_EVENT)
        stdout, stderr = proc.communicate(timeout=20)
        delivered = True
    except (AttributeError, OSError) as exc:
        proc.kill()
        stdout, stderr = proc.communicate(timeout=10)
        return {
            "verdict": "fail", "reason": f"CTRL_BREAK_EVENT delivery failed: {exc}",
            "returncode": proc.returncode, "stdout": stdout[-12000:], "stderr": stderr[-12000:],
        }
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate(timeout=10)
        return {
            "verdict": "fail", "reason": "index did not stop after CTRL_BREAK_EVENT",
            "returncode": proc.returncode, "stdout": stdout[-12000:], "stderr": stderr[-12000:],
        }

    recovery_index = run(binary, ["index", str(repo), "--json"], repo, env, timeout=180)
    recovery_doctor = run(binary, ["doctor", "--deep", "--json"], repo, env, timeout=60)
    recovery_search = run(binary, ["search", "cancellation_target_", "--json"], repo, env, timeout=60)
    ok = (
        delivered
        and proc.returncode not in (None, 0)
        and recovery_index["returncode"] == 0
        and recovery_doctor["returncode"] == 0
        and recovery_search["returncode"] == 0
    )
    return {
        "verdict": "pass" if ok else "fail",
        "returncode": proc.returncode,
        "elapsed_ms": (time.monotonic_ns() - started) // 1_000_000,
        "stdout": stdout[-12000:], "stderr": stderr[-12000:],
        "recovery_index": recovery_index,
        "recovery_doctor": recovery_doctor,
        "recovery_search": recovery_search,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Native Windows CLI validation")
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--concurrency-output", required=True)
    parser.add_argument("--fixture-files", type=int, default=180)
    args = parser.parse_args()

    binary = str(Path(args.binary).resolve())
    output = Path(args.output).resolve()
    concurrency_output = Path(args.concurrency_output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    concurrency_output.parent.mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "schema": "codebase-memory-cli/windows-validation/v1",
        "platform": sys.platform,
        "binary": binary,
        "started_unix": int(time.time()),
        "checks": [],
    }
    if os.name != "nt":
        report["status"] = "fail"
        report["reason"] = "this validation must execute natively on Windows"
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        return 2

    with tempfile.TemporaryDirectory(prefix="cbm-win-validation-") as td:
        # Explicit space + non-ASCII components are part of the acceptance case.
        root = Path(td) / "Codebase Memory Validation Space" / "unicodé-測試"
        repo = root / "repository with spaces"
        cache = root / "cache with spaces"
        write_repo(repo, args.fixture_files)
        cache.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["CBM_CACHE_DIR"] = str(cache)
        env["CBM_NO_UPDATE_CHECK"] = "1"

        commands = [
            ("version", ["--version"]),
            ("help", ["--help"]),
            ("index", ["index", str(repo), "--json"]),
            ("projects", ["projects", "--json"]),
            ("status", ["status", "--json"]),
            ("search", ["search", "windows_target_", "--json"]),
            ("doctor", ["doctor", "--json"]),
            ("doctor_deep", ["doctor", "--deep", "--json"]),
        ]
        for name, cmd in commands:
            result = run(binary, cmd, repo, env, timeout=180 if name == "index" else 60)
            report["checks"].append({"name": name, **result})

        report["path_case"] = {
            "root": str(root), "repo": str(repo), "cache": str(cache),
            "has_space": " " in str(repo) and " " in str(cache),
            "has_unicode": any(ord(ch) > 127 for ch in str(root)),
        }
        report["cancellation"] = cancellation_probe(binary, repo, env)

        harness = Path(__file__).with_name("evaluate-concurrency.py")
        concurrency = subprocess.run(
            [sys.executable, str(harness), "--binary", binary,
             "--output", str(concurrency_output), "--fixture-files", str(args.fixture_files)],
            cwd=Path(__file__).resolve().parents[1], env=env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=900,
        )
        report["concurrency"] = {
            "returncode": concurrency.returncode,
            "stdout": concurrency.stdout[-12000:], "stderr": concurrency.stderr[-12000:],
            "output": str(concurrency_output),
        }

    required_ok = all(item["returncode"] == 0 for item in report["checks"])
    cancellation_ok = report["cancellation"].get("verdict") in {"pass", "inconclusive"}
    concurrency_ok = report["concurrency"]["returncode"] == 0
    report["status"] = "pass" if required_ok and cancellation_ok and concurrency_ok else "fail"
    report["finished_unix"] = int(time.time())
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
