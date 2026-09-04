#!/usr/bin/env python3
"""Drive a deterministic sequence of canonical CLI operations.

The long-lived process under observation is the coordination daemon. Each CLI
request connects through the supported neutral operation protocol, so this
harness continues to expose per-request daemon growth without retaining the
retired MCP stdio/JSON-RPC frontend.
"""
import argparse
import json
import subprocess
import sys
import time


def run(binary, argv, *, cwd=None):
    return subprocess.run(
        [binary, *argv], cwd=cwd, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )


def operation_args(tool):
    return {
        "search": ["search", "--name-pattern", ".*Widget.*", "--limit", "10", "--json"],
        "projects": ["projects", "--json"],
        "schema": ["schema", "--json"],
        "source-search": ["source-search", "Widget", "--json"],
    }[tool]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("corpus")
    parser.add_argument("requests", type=int)
    parser.add_argument("--stderr", help="append CLI/daemon diagnostics here")
    parser.add_argument("--tool", default="search",
                        choices=("search", "projects", "schema", "source-search"),
                        help="canonical CLI operation to repeat")
    parser.add_argument("--idle-seconds", type=float, default=0.0)
    parser.add_argument("--skip-index", action="store_true")
    args = parser.parse_args()

    log = open(args.stderr, "a", encoding="utf-8") if args.stderr else None
    served = failures = 0
    try:
        started = run(args.binary, ["daemon", "start"])
        if log:
            log.write(started.stderr or "")
        if started.returncode != 0:
            print("daemon start failed", file=sys.stderr)
            return 3

        if not args.skip_index:
            indexed = run(args.binary, ["index", args.corpus, "--mode", "fast", "--json"])
            if log:
                log.write(indexed.stderr or "")
            if indexed.returncode != 0:
                print(f"index failed: {indexed.stderr.strip()}", file=sys.stderr)
                return 3

        halfway = args.requests // 2
        argv = operation_args(args.tool)
        for i in range(args.requests):
            if args.idle_seconds > 0 and i == halfway:
                print(f"idle-start request={i + 1}", flush=True)
                time.sleep(args.idle_seconds)
                print(f"idle-end request={i + 1}", flush=True)
            reply = run(args.binary, argv, cwd=args.corpus)
            if log:
                log.write(reply.stderr or "")
            served += 1
            if reply.returncode != 0:
                failures += 1
            elif reply.stdout:
                try:
                    json.loads(reply.stdout)
                except json.JSONDecodeError:
                    failures += 1
    finally:
        stopped = run(args.binary, ["daemon", "stop"])
        if log:
            log.write(stopped.stderr or "")
            log.close()

    print(f"served={served} failed={failures}")
    return 0 if served == args.requests and failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
