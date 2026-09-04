#!/usr/bin/env python3
"""Real-binary smoke test for the CLI-first coordination daemon.

This product-level guard intentionally exercises only supported CLI/daemon
behavior. Detailed request cancellation, session isolation, rendezvous ABI,
version-conflict, and committed-client stop-refusal semantics are covered by
``tests/test_daemon_runtime.c`` and are not duplicated here through a retired
frontend protocol.

Covered here:
* cold one-shot operation and startup-tax guidance;
* permanent daemon start/status and stable PID across concurrent CLI clients;
* daemon-backed indexing and persisted project visibility;
* daemon crash recovery and fresh generation startup;
* clean/idempotent daemon stop.
"""

import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time


START_TIMEOUT = 45
OP_TIMEOUT = 120
STOP_TIMEOUT = 45


class SmokeFailure(RuntimeError):
    pass


def check(condition, message):
    if not condition:
        raise SmokeFailure(message)


def output_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode(
        "utf-8", "replace"
    )


def run_cli(binary, cache, args, timeout=OP_TIMEOUT):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = str(cache)
    return subprocess.run(
        [str(binary)] + list(args),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env=env,
        check=False,
    )


def daemon_pid_from(text):
    match = re.search(r"pid[: ]+(\d+)", text)
    return int(match.group(1)) if match else 0


def process_alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def force_kill(pid):
    if not pid or not process_alive(pid):
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def wait_not_running(binary, cache, timeout=STOP_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = run_cli(binary, cache, ["daemon", "status"], timeout=20)
        if result.returncode != 0 and "not running" in output_text(result):
            return True
        time.sleep(0.2)
    return False


def create_repo(root):
    root.mkdir(parents=True)
    (root / "alpha.py").write_text(
        "def alpha(value):\n    return value + 1\n", encoding="utf-8"
    )
    (root / "beta.py").write_text(
        "from alpha import alpha\n\ndef beta(value):\n    return alpha(value) * 2\n",
        encoding="utf-8",
    )
    if shutil.which("git"):
        subprocess.run(["git", "-C", str(root), "init", "-q"], check=False)
        subprocess.run(["git", "-C", str(root), "add", "-A"], check=False)
        subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "user.email=daemon-smoke@example.invalid",
                "-c",
                "user.name=daemon-smoke",
                "commit",
                "-q",
                "-m",
                "init",
            ],
            check=False,
        )


def require_json(result, label):
    check(result.returncode == 0, label + " failed: " + output_text(result)[:800])
    try:
        return json.loads((result.stdout or b"").decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SmokeFailure(label + " returned invalid JSON: " + str(exc)) from exc


def main():
    if platform.system() not in ("Linux", "Darwin"):
        if os.environ.get("CBM_DAEMON_SMOKE_REQUIRE_RUN") == "1":
            raise SmokeFailure(
                "required daemon smoke is POSIX-only; native Windows behavior is "
                "covered by tests/windows daemon guards"
            )
        print("SKIP: daemon smoke currently validates POSIX process lifecycle only")
        return 0

    if len(sys.argv) != 2:
        raise SmokeFailure("usage: test_daemon_smoke.py <codebase-memory-cli>")

    binary = Path(sys.argv[1]).resolve()
    check(binary.is_file(), "binary not found: " + str(binary))

    daemon_pid = 0
    recovered_pid = 0
    with tempfile.TemporaryDirectory(prefix="cbm-daemon-smoke-") as raw_tmpdir:
        tmpdir = Path(raw_tmpdir)
        cache = tmpdir / "cache"
        repo = tmpdir / "repo"
        cache.mkdir()
        create_repo(repo)

        try:
            absent = run_cli(binary, cache, ["daemon", "status"])
            check(
                absent.returncode != 0 and "not running" in output_text(absent),
                "daemon status did not report an absent daemon",
            )

            cold = run_cli(binary, cache, ["projects", "--json"])
            require_json(cold, "cold projects")
            check(
                "daemon start" in output_text(cold),
                "cold one-shot did not provide daemon-start guidance",
            )

            start = run_cli(binary, cache, ["daemon", "start"], timeout=START_TIMEOUT)
            start_text = output_text(start)
            daemon_pid = daemon_pid_from(start_text)
            check(start.returncode == 0, "daemon start failed: " + start_text[:800])
            check(daemon_pid > 1, "daemon start did not report a pid: " + start_text[:800])
            check("permanent" in start_text, "daemon start did not report permanent mode")

            status = run_cli(binary, cache, ["daemon", "status"])
            status_text = output_text(status)
            check(status.returncode == 0, "daemon status failed: " + status_text[:800])
            check(
                daemon_pid_from(status_text) == daemon_pid,
                "daemon status reported a different pid",
            )

            # Multiple supported CLI clients must reuse the same permanent daemon
            # rather than starting frontend/session-owned generations.
            results = [None, None, None, None]

            def projects_call(index):
                results[index] = run_cli(binary, cache, ["projects", "--json"])

            threads = [threading.Thread(target=projects_call, args=(i,)) for i in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(OP_TIMEOUT)
                check(not thread.is_alive(), "parallel CLI client timed out")
            for index, result in enumerate(results):
                check(result is not None, "parallel CLI result missing")
                require_json(result, "parallel projects {}".format(index))
                check(
                    "daemon start" not in output_text(result),
                    "warm CLI unexpectedly behaved as a cold start",
                )

            after_parallel = run_cli(binary, cache, ["daemon", "status"])
            check(
                after_parallel.returncode == 0
                and daemon_pid_from(output_text(after_parallel)) == daemon_pid,
                "parallel CLI clients restarted the permanent daemon",
            )

            indexed = run_cli(
                binary,
                cache,
                ["index", str(repo), "--mode", "fast", "--json"],
                timeout=OP_TIMEOUT,
            )
            index_payload = require_json(indexed, "daemon-backed index")
            check(isinstance(index_payload, dict), "index response is not a JSON object")

            projects = require_json(
                run_cli(binary, cache, ["projects", "--json"]),
                "projects after index",
            )
            project_rows = projects.get("projects", projects) if isinstance(projects, dict) else projects
            check(isinstance(project_rows, list), "projects response does not contain a list")
            check(project_rows, "daemon-backed index did not persist a project")

            # Crash the actual coordination daemon and require a fresh generation,
            # rather than crashing a retired stdio/frontend process.
            force_kill(daemon_pid)
            check(
                wait_not_running(binary, cache),
                "stale daemon state did not clear after SIGKILL",
            )

            post_crash = run_cli(binary, cache, ["projects", "--json"])
            require_json(post_crash, "post-crash cold projects")
            check(
                "daemon start" in output_text(post_crash),
                "post-crash cold one-shot did not recover normally",
            )

            restart = run_cli(binary, cache, ["daemon", "start"], timeout=START_TIMEOUT)
            restart_text = output_text(restart)
            recovered_pid = daemon_pid_from(restart_text)
            check(restart.returncode == 0, "daemon restart failed: " + restart_text[:800])
            check(recovered_pid > 1, "daemon restart did not report a pid")
            check(recovered_pid != daemon_pid, "daemon crash recovery reused the old pid")
            daemon_pid = 0

            stop = run_cli(binary, cache, ["daemon", "stop"])
            check(stop.returncode == 0, "daemon stop failed: " + output_text(stop)[:800])
            check(wait_not_running(binary, cache), "daemon did not stop cleanly")
            recovered_pid = 0

            stop_again = run_cli(binary, cache, ["daemon", "stop"])
            check(stop_again.returncode == 0, "second daemon stop was not idempotent")

            print(
                "PASS: CLI-first daemon smoke: cold/warm clients, stable permanent "
                "generation, daemon-backed index, crash recovery, and clean stop"
            )
            return 0
        finally:
            if recovered_pid:
                force_kill(recovered_pid)
            if daemon_pid:
                force_kill(daemon_pid)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SmokeFailure, subprocess.TimeoutExpired) as exc:
        print("FAIL: " + str(exc), file=sys.stderr)
        raise SystemExit(1)
