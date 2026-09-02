#!/usr/bin/env python3
"""Process-level concurrency evaluation for codebase-memory-cli.

This is an evaluation harness, not a unit test. It exercises the shipped CLI
against deterministic repositories and records timing + runtime-assurance
evidence as JSON. Scenarios that cannot create meaningful overlap are reported
as inconclusive rather than converted into false confidence.

Usage:
  scripts/evaluate-concurrency.py --binary build/c/codebase-memory-cli
  scripts/evaluate-concurrency.py --binary ./codebase-memory-cli --output result.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


def now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def write_fixture(root: Path, files: int, salt: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(f"# concurrency fixture {salt}\n", encoding="utf-8")
    src = root / "src"
    src.mkdir(exist_ok=True)
    for i in range(files):
        # Enough semantic structure to exercise parsing/graph publication while
        # staying deterministic and cheap to generate on every platform.
        (src / f"module_{i:04d}.py").write_text(
            "\n".join(
                [
                    f"def helper_{i}(value):",
                    f"    return value + {i}",
                    "",
                    f"def target_{i}(value):",
                    f"    return helper_{i}(value) * 2",
                    "",
                    f"class Fixture{i}:",
                    "    def run(self, value):",
                    f"        return target_{i}(value)",
                    "",
                ]
            ),
            encoding="utf-8",
        )


def parse_json(stdout: str) -> Any | None:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return None


def run_command(binary: str, args: list[str], cwd: Path, env: dict[str, str], timeout: int = 180) -> dict[str, Any]:
    started = now_ms()
    try:
        completed = subprocess.run(
            [binary, *args],
            cwd=str(cwd),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        ended = now_ms()
        return {
            "args": args,
            "returncode": completed.returncode,
            "started_ms": started,
            "ended_ms": ended,
            "elapsed_ms": ended - started,
            "stdout_json": parse_json(completed.stdout),
            "stdout": completed.stdout[-4096:],
            "stderr": completed.stderr[-4096:],
        }
    except subprocess.TimeoutExpired as exc:
        ended = now_ms()
        return {
            "args": args,
            "returncode": None,
            "timed_out": True,
            "started_ms": started,
            "ended_ms": ended,
            "elapsed_ms": ended - started,
            "stdout": (exc.stdout or "")[-4096:] if isinstance(exc.stdout, str) else "",
            "stderr": (exc.stderr or "")[-4096:] if isinstance(exc.stderr, str) else "",
        }



def start_command(binary: str, args: list[str], cwd: Path, env: dict[str, str]) -> tuple[subprocess.Popen[str], int]:
    started = now_ms()
    process = subprocess.Popen(
        [binary, *args], cwd=str(cwd), env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return process, started


def finish_started(process: subprocess.Popen[str], args: list[str], started: int, timeout: int = 30) -> dict[str, Any]:
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        hard_kill(process.pid)
        stdout, stderr = process.communicate(timeout=10)
        timed_out = True
    ended = now_ms()
    return {
        "args": args, "returncode": process.returncode, "timed_out": timed_out,
        "started_ms": started, "ended_ms": ended, "elapsed_ms": ended - started,
        "stdout_json": parse_json(stdout), "stdout": stdout[-4096:], "stderr": stderr[-4096:],
    }


def hard_kill(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def wait_for_daemon_pid(binary: str, cwd: Path, env: dict[str, str], timeout_s: float = 10.0) -> int | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = run_command(binary, ["daemon", "status"], cwd, env, timeout=5)
        if result.get("returncode") == 0:
            for line in str(result.get("stdout", "")).splitlines():
                stripped = line.strip()
                if stripped.startswith("pid:"):
                    try:
                        return int(stripped.split(":", 1)[1].strip())
                    except ValueError:
                        pass
        time.sleep(0.05)
    return None


def mutate_fixture(repo: Path, marker: str) -> None:
    for path in (repo / "src").glob("*.py"):
        with path.open("a", encoding="utf-8") as sink:
            sink.write(f"\n# {marker}\n")


def run_parallel(binary: str, invocations: list[tuple[list[str], Path]], env: dict[str, str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any] | None] = [None] * len(invocations)

    def worker(index: int, args: list[str], cwd: Path) -> None:
        results[index] = run_command(binary, args, cwd, env)

    threads = [
        threading.Thread(target=worker, args=(i, args, cwd), daemon=True)
        for i, (args, cwd) in enumerate(invocations)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return [item for item in results if item is not None]


def event_counts(binary: str, cwd: Path, env: dict[str, str]) -> dict[str, int]:
    result = run_command(binary, ["doctor", "--json"], cwd, env, timeout=30)
    payload = result.get("stdout_json")
    if not isinstance(payload, dict):
        return {}
    events = payload.get("reliability_events")
    if not isinstance(events, dict):
        return {}
    counts = events.get("counts")
    if not isinstance(counts, dict):
        return {}
    return {str(key): int(value) for key, value in counts.items() if isinstance(value, int)}


def count_delta(before: dict[str, int], after: dict[str, int], name: str) -> int:
    return max(0, after.get(name, 0) - before.get(name, 0))


def find_project_db(cache: Path, repo: Path) -> Path | None:
    wanted = str(repo.resolve())
    for db_path in sorted(cache.glob("*.db")):
        if db_path.name.startswith("_"):
            continue
        try:
            connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=1.0)
            try:
                row = connection.execute(
                    "SELECT 1 FROM projects WHERE root_path = ? LIMIT 1", (wanted,)
                ).fetchone()
                if row:
                    return db_path
            finally:
                connection.close()
        except sqlite3.Error:
            continue
    return None


def generation_from_connection(connection: sqlite3.Connection) -> str | None:
    try:
        row = connection.execute(
            "SELECT (SELECT v FROM store_meta WHERE k='db_uid'), "
            "       (SELECT v FROM store_meta WHERE k='mutation_gen')"
        ).fetchone()
    except sqlite3.Error:
        return None
    if not row or row[0] is None or row[1] is None:
        return "legacy"
    return f"u{row[0]}g{row[1]}"


def generation_from_path(db_path: Path) -> str | None:
    try:
        connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=1.0)
        try:
            return generation_from_connection(connection)
        finally:
            connection.close()
    except sqlite3.Error:
        return None


def wal_size(db_path: Path) -> int:
    wal = Path(str(db_path) + "-wal")
    try:
        return wal.stat().st_size
    except FileNotFoundError:
        return 0


def scenario_reader_reader(binary: str, repo: Path, env: dict[str, str], readers: int) -> dict[str, Any]:
    runs = run_parallel(binary, [(["search", "target_", "--json"], repo)] * readers, env)
    ok = len(runs) == readers and all(run.get("returncode") == 0 for run in runs)
    starts = [int(run["started_ms"]) for run in runs]
    ends = [int(run["ended_ms"]) for run in runs]
    overlap = max(0, min(ends) - max(starts)) if runs else 0
    return {
        "name": "reader_reader",
        "verdict": "pass" if ok else "fail",
        "expected": "concurrent reads succeed without write serialization",
        "overlap_ms": overlap,
        "runs": runs,
    }


def scenario_reader_writer(binary: str, repo: Path, env: dict[str, str]) -> dict[str, Any]:
    # Force a substantive reindex by changing every fixture file.
    for path in (repo / "src").glob("*.py"):
        with path.open("a", encoding="utf-8") as sink:
            sink.write("\n# changed for reader/writer evaluation\n")

    writer_box: list[dict[str, Any]] = []

    def writer() -> None:
        writer_box.append(run_command(binary, ["index", str(repo), "--json"], repo, env))

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()
    reads: list[dict[str, Any]] = []
    deadline = time.monotonic() + 30
    while thread.is_alive() and time.monotonic() < deadline:
        reads.append(run_command(binary, ["search", "target_", "--json"], repo, env, timeout=15))
        time.sleep(0.025)
    thread.join(timeout=150)
    writer_result = writer_box[0] if writer_box else {"returncode": None, "timed_out": True}
    overlapping_reads = [
        read
        for read in reads
        if int(read["started_ms"]) < int(writer_result.get("ended_ms", 0))
        and int(read["ended_ms"]) > int(writer_result.get("started_ms", 0))
    ]
    if writer_result.get("returncode") != 0 or any(read.get("returncode") != 0 for read in overlapping_reads):
        verdict = "fail"
    elif not overlapping_reads:
        verdict = "inconclusive"
    else:
        verdict = "pass"
    return {
        "name": "reader_writer",
        "verdict": verdict,
        "expected": "reads remain available during index mutation where WAL semantics permit",
        "overlapping_reads": len(overlapping_reads),
        "writer": writer_result,
        "reads": reads,
    }


def scenario_writer_writer_same(binary: str, repo: Path, env: dict[str, str]) -> dict[str, Any]:
    before = event_counts(binary, repo, env)
    runs = run_parallel(binary, [(["index", str(repo), "--json"], repo)] * 2, env)
    after = event_counts(binary, repo, env)
    coalesced = count_delta(before, after, "mutation.coalesced")
    conflicts = count_delta(before, after, "mutation.conflict")
    successful = len(runs) == 2 and all(run.get("returncode") == 0 for run in runs)
    verdict = "pass" if successful and coalesced > 0 and conflicts == 0 else "fail"
    return {
        "name": "writer_writer_same_project",
        "verdict": verdict,
        "expected": "identical same-project mutations share one authoritative job",
        "coalesced_events": coalesced,
        "conflict_events": conflicts,
        "runs": runs,
    }


def scenario_writer_writer_different(binary: str, repo_a: Path, repo_b: Path, env: dict[str, str]) -> dict[str, Any]:
    runs = run_parallel(
        binary,
        [(["index", str(repo_a), "--json"], repo_a), (["index", str(repo_b), "--json"], repo_b)],
        env,
    )
    successful = len(runs) == 2 and all(run.get("returncode") == 0 for run in runs)
    overlap = 0
    if len(runs) == 2:
        overlap = max(0, min(int(r["ended_ms"]) for r in runs) - max(int(r["started_ms"]) for r in runs))
    verdict = "pass" if successful and overlap > 0 else ("inconclusive" if successful else "fail")
    return {
        "name": "writer_writer_different_projects",
        "verdict": verdict,
        "expected": "independent projects may mutate concurrently subject to global limits",
        "overlap_ms": overlap,
        "runs": runs,
    }



def scenario_writer_crash(binary: str, repo: Path, env: dict[str, str]) -> dict[str, Any]:
    mutate_fixture(repo, "changed for writer crash evaluation")
    before = event_counts(binary, repo, env)
    args = ["index", str(repo), "--json"]
    process, started = start_command(binary, args, repo, env)
    # Give the request a bounded opportunity to enter daemon-side mutation ownership.
    time.sleep(0.15)
    was_running = process.poll() is None
    if was_running:
        hard_kill(process.pid)
    crashed = finish_started(process, args, started, timeout=10)
    # Recovery is the product contract: a fresh request must complete and deep
    # verification must not report contention as corruption.
    recovery = run_command(binary, ["index", str(repo), "--json"], repo, env, timeout=180)
    doctor = run_command(binary, ["doctor", "--deep", "--json"], repo, env, timeout=60)
    after = event_counts(binary, repo, env)
    rebuild_delta = count_delta(before, after, "index.rebuild.requested")
    worker_crashes = count_delta(before, after, "index.worker.crash")
    doctor_payload = doctor.get("stdout_json")
    doctor_status = doctor_payload.get("status") if isinstance(doctor_payload, dict) else None
    if not was_running:
        verdict = "inconclusive"
    elif recovery.get("returncode") != 0 or doctor.get("returncode") != 0 or doctor_status == "corrupt":
        verdict = "fail"
    else:
        verdict = "pass"
    return {
        "name": "writer_crash", "verdict": verdict,
        "expected": "abandoned writer request is recoverable without partial publication or spurious corruption",
        "request_was_running_when_killed": was_running, "killed_request": crashed,
        "recovery_index": recovery, "deep_doctor": doctor,
        "rebuild_requested_events": rebuild_delta, "worker_crash_events": worker_crashes,
    }


def scenario_wal_pressure_long_reader(binary: str, repo: Path, cache: Path, env: dict[str, str]) -> dict[str, Any]:
    db_path = find_project_db(cache, repo)
    if db_path is None:
        return {
            "name": "wal_pressure_long_reader", "verdict": "fail",
            "expected": "long reader retains a coherent generation while a new generation publishes",
            "reason": "project database could not be resolved",
        }

    before_generation = generation_from_path(db_path)
    before_events = event_counts(binary, repo, env)
    peak_wal_bytes = wal_size(db_path)
    samples: list[dict[str, Any]] = []
    reader: sqlite3.Connection | None = None
    writer: subprocess.Popen[str] | None = None
    writer_started = 0
    writer_result: dict[str, Any] | None = None
    held_generation: str | None = None
    try:
        reader = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=1.0)
        reader.execute("PRAGMA query_only=ON")
        reader.execute("BEGIN")
        # Establish the snapshot before the writer starts. Reading store_meta now
        # also gives us a direct identity for the generation pinned by this reader.
        held_generation = generation_from_connection(reader)
        reader.execute("SELECT count(*) FROM sqlite_master").fetchone()

        mutate_fixture(repo, "changed for WAL pressure / publication evaluation")
        args = ["index", str(repo), "--json"]
        writer, writer_started = start_command(binary, args, repo, env)
        deadline = time.monotonic() + 60
        while writer.poll() is None and time.monotonic() < deadline:
            current_wal = wal_size(db_path)
            peak_wal_bytes = max(peak_wal_bytes, current_wal)
            samples.append({
                "at_ms": now_ms(),
                "wal_bytes": current_wal,
                "active_generation": generation_from_path(db_path),
            })
            time.sleep(0.025)
        writer_result = finish_started(writer, args, writer_started, timeout=150)

        # The pinned reader must still observe its original snapshot even after
        # publication, while a fresh open should observe the new generation.
        held_after_publish = generation_from_connection(reader)
        active_after_publish = generation_from_path(db_path)
        peak_wal_bytes = max(peak_wal_bytes, wal_size(db_path))
    except sqlite3.Error as exc:
        if writer is not None and writer.poll() is None:
            hard_kill(writer.pid)
            writer_result = finish_started(writer, ["index", str(repo), "--json"], writer_started, timeout=10)
        return {
            "name": "wal_pressure_long_reader", "verdict": "fail",
            "expected": "long reader retains a coherent generation while a new generation publishes",
            "database": str(db_path), "sqlite_error": str(exc), "writer": writer_result,
        }
    finally:
        if reader is not None:
            try:
                reader.rollback()
            except sqlite3.Error:
                pass
            reader.close()

    post_release_read = run_command(binary, ["search", "target_", "--json"], repo, env, timeout=30)
    deep_doctor = run_command(binary, ["doctor", "--deep", "--json"], repo, env, timeout=60)
    after_events = event_counts(binary, repo, env)
    wal_starving = count_delta(before_events, after_events, "store.wal.starving")
    checkpoints = count_delta(before_events, after_events, "store.checkpoint")
    rebuilds = count_delta(before_events, after_events, "store.rebuild.requested")
    corrupt = count_delta(before_events, after_events, "store.integrity.corrupt")
    active_after_release = generation_from_path(db_path)

    writer_ok = writer_result is not None and writer_result.get("returncode") == 0
    generation_advanced = bool(
        before_generation and active_after_publish and before_generation != active_after_publish
    )
    reader_pinned = held_generation == before_generation and held_after_publish == held_generation
    healthy = (
        writer_ok and generation_advanced and reader_pinned
        and post_release_read.get("returncode") == 0
        and deep_doctor.get("returncode") == 0
        and rebuilds == 0 and corrupt == 0
    )
    pressure_observed = peak_wal_bytes > 0 or wal_starving > 0
    verdict = "pass" if healthy and pressure_observed else ("inconclusive" if healthy else "fail")
    return {
        "name": "wal_pressure_long_reader", "verdict": verdict,
        "expected": "long reader retains its generation while publication advances atomically; WAL pressure is measured without unsafe repair",
        "database": str(db_path),
        "before_generation": before_generation,
        "held_generation": held_generation,
        "held_generation_after_publish": held_after_publish,
        "active_generation_after_publish": active_after_publish,
        "active_generation_after_release": active_after_release,
        "generation_advanced": generation_advanced,
        "reader_snapshot_pinned": reader_pinned,
        "peak_wal_bytes": peak_wal_bytes,
        "wal_pressure_observed": pressure_observed,
        "wal_starvation_events": wal_starving,
        "checkpoint_events": checkpoints,
        "rebuild_requested_events": rebuilds,
        "integrity_corrupt_events": corrupt,
        "samples": samples[-256:],
        "writer": writer_result,
        "post_release_read": post_release_read,
        "deep_doctor": deep_doctor,
    }


def scenario_daemon_crash_restart(binary: str, repo: Path, env: dict[str, str]) -> dict[str, Any]:
    start = run_command(binary, ["daemon", "start"], repo, env, timeout=30)
    pid = wait_for_daemon_pid(binary, repo, env)
    if pid is None:
        return {
            "name": "daemon_crash_restart", "verdict": "fail",
            "expected": "hard daemon termination leaves recoverable coordination state",
            "daemon_start": start, "reason": "daemon pid could not be observed",
        }
    hard_kill(pid)
    # Wait until the killed process can no longer answer before testing recovery.
    deadline = time.monotonic() + 10
    post_crash_status: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        post_crash_status = run_command(binary, ["daemon", "status"], repo, env, timeout=5)
        if post_crash_status.get("returncode") != 0:
            break
        time.sleep(0.05)
    restart = run_command(binary, ["daemon", "start"], repo, env, timeout=30)
    recovered_pid = wait_for_daemon_pid(binary, repo, env)
    read = run_command(binary, ["search", "target_", "--json"], repo, env, timeout=30)
    doctor = run_command(binary, ["doctor", "--deep", "--json"], repo, env, timeout=60)
    healthy = (restart.get("returncode") == 0 and recovered_pid is not None and recovered_pid != pid
               and read.get("returncode") == 0 and doctor.get("returncode") == 0)
    return {
        "name": "daemon_crash_restart", "verdict": "pass" if healthy else "fail",
        "expected": "hard daemon termination leaves valid store and restartable process-safe coordination",
        "killed_daemon_pid": pid, "post_crash_status": post_crash_status,
        "restart": restart, "recovered_daemon_pid": recovered_pid,
        "post_restart_read": read, "deep_doctor": doctor,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, help="path to codebase-memory-cli executable")
    parser.add_argument("--output", help="write structured report to this path")
    parser.add_argument("--fixture-files", type=int, default=800, help="files per generated repository")
    parser.add_argument("--readers", type=int, default=4, help="concurrent readers in reader/reader scenario")
    args = parser.parse_args()

    binary = str(Path(args.binary).resolve())
    if not Path(binary).is_file():
        parser.error(f"binary does not exist: {binary}")

    work = Path(tempfile.mkdtemp(prefix="cbm-concurrency-eval-"))
    cache = work / "cache"
    repo_a = work / "fixture-a"
    repo_b = work / "fixture-b"
    env = os.environ.copy()
    env["CBM_CACHE_DIR"] = str(cache)
    env["CBM_LOG_LEVEL"] = "warn"

    report: dict[str, Any] = {
        "schema": "codebase-memory-cli/concurrency-evaluation/v1",
        "binary": binary,
        "fixture_files": args.fixture_files,
        "scenarios": [],
    }
    try:
        write_fixture(repo_a, args.fixture_files, "a")
        write_fixture(repo_b, args.fixture_files, "b")
        seed = run_command(binary, ["index", str(repo_a), "--json"], repo_a, env)
        report["seed_index"] = seed
        if seed.get("returncode") != 0:
            report["status"] = "fail"
            report["reason"] = "seed index failed"
        else:
            report["scenarios"].append(scenario_reader_reader(binary, repo_a, env, args.readers))
            report["scenarios"].append(scenario_reader_writer(binary, repo_a, env))
            report["scenarios"].append(scenario_writer_writer_same(binary, repo_a, env))
            report["scenarios"].append(scenario_writer_writer_different(binary, repo_a, repo_b, env))
            report["scenarios"].append(scenario_writer_crash(binary, repo_a, env))
            report["scenarios"].append(scenario_wal_pressure_long_reader(binary, repo_a, cache, env))
            report["scenarios"].append(scenario_daemon_crash_restart(binary, repo_a, env))
            verdicts = [scenario["verdict"] for scenario in report["scenarios"]]
            report["status"] = "fail" if "fail" in verdicts else ("inconclusive" if "inconclusive" in verdicts else "pass")
        report["doctor"] = run_command(binary, ["doctor", "--json"], repo_a, env, timeout=30)
    finally:
        # Explicit daemon shutdown is important on native Windows: deleting a
        # sandbox while a daemon still owns handles would turn harness cleanup
        # into an OS-locking artifact rather than product evidence.
        run_command(binary, ["daemon", "stop"], repo_a, env, timeout=15)

    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
    print(encoded)
    status = report.get("status")
    shutil.rmtree(work, ignore_errors=True)
    return 1 if status == "fail" else 0


if __name__ == "__main__":
    sys.exit(main())
