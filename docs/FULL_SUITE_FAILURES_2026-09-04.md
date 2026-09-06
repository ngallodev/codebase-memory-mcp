# Full Suite Failure Details

Date: 2026-09-04  
Branch: `release-tooling`  
Baseline: `4f0d7b9`

## Status

The full sanitized suite is not green. The production incremental build and
sanitized test-runner build completed successfully, but the direct full-run
execution ended with 12 assertion failures and an AddressSanitizer heap-buffer-overflow.

The focused memory suite remains separate: 52/52 tests passed with
`ASAN_OPTIONS=detect_leaks=1`.

## Canonical runner preflight failures

`scripts/test.sh` did not reach the suite execution because two preflight
contracts failed:

1. The build-directory safety contract failed.
2. The Windows VM worktree manifest contract reported stale expectations for
   the maintained driver and the old `codebase-memory-mcp.exe`.

The full result below therefore came from the direct sanitized runner, not a
successful canonical `scripts/test.sh` gate.

## Assertion failures

| Location | Test | Assertion |
|---|---|---|
| `tests/test_subprocess.c:551` | `subprocess_quiet_timeout_kills_ignoring_tree` | `ASSERT(ready)` |
| `tests/test_daemon_runtime.c:3813` | daemon runtime test | `ASSERT(second_usable_before)` |
| `tests/test_daemon_application.c:707` | `daemon_application_requires_immutable_explicit_context` | `ASSERT(operation_response)` |
| `tests/test_daemon_application.c:1887` | `initialize_coalesces_auto_index_for_full_sessions` | `ASSERT(restricted_started_nothing)` |
| `tests/test_daemon_application.c:2026` | `sensitive_root_blocks_auto_index` | `ASSERT(sensitive_blocked)` |
| `tests/test_daemon_application.c:2149` | `sensitive_root_blocks_watch` | `ASSERT(ordinary_watched)` |
| `tests/test_daemon_application.c:2228` | `auto_index_honors_tracked_file_limit` | `ASSERT(limit_prevented_admission)` |
| `tests/test_daemon_application.c:2383` | `auto_index_retries_transient_busy_admission` | `ASSERT(initially_deferred)` |
| `tests/test_daemon_application.c:2461` | `update_generation_notifies_initial_and_late_sessions_once` | `ASSERT(initial_notified)` |
| `tests/test_daemon_application.c:2506` | `update_generation_retries_worker_start_failure` | `ASSERT(failed_generation_started)` |
| `tests/test_daemon_application.c:2570` | `update_generation_retries_cancelled_check` | `ASSERT(first_started)` |
| `tests/test_daemon_application.c:2644` | `final_disconnect_cancels_and_joins_update_generation` | `ASSERT(generation_started)` |

## Sanitizer failure

After the assertion failures, AddressSanitizer reported a heap-buffer-overflow
while executing the daemon application tests:

- Reported read: `strstr()` read 58 bytes.
- Test location: `tests/test_daemon_application.c:2744`.
- Allocated buffer: 57 bytes.
- Allocation site: `src/daemon/application.c:2545`.
- Relevant dispatch path: `application_operation_request()` through daemon
  application request dispatch.

This is an active correctness and memory-safety failure. The report does not
yet establish whether the length mismatch is in the producer, the response
buffer contract, or the test's string handling; reproduce it in the smallest
daemon-application invocation before changing ownership or bounds logic.

## Follow-up

- Reproduce the ASan overflow with only the daemon-application suite.
- Classify the ten daemon-application assertions as setup, timing, protocol,
  or implementation failures; several concern auto-index/update-generation
  admission and retry behavior.
- Re-run `subprocess_quiet_timeout_kills_ignoring_tree` independently to check
  whether its readiness failure is flaky.
- Repair or update the two stale `scripts/test.sh` preflight contracts before
  treating that script as the full-suite gate.
- Re-run the canonical full suite, focused foundation runner, and LeakSanitizer
  memory suite after remediation.

No assertion or sanitizer failure is marked resolved by this artifact.
