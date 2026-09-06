# Memlab Analysis and Build/Test Run Report

Date: 2026-09-05  
Repository: `codebase-memory-cli`  
Branch: `release-tooling`  
Source state: CP72 overlay applied on the working tree; repository baseline
reported by the overlay is `4f0d7b9`.

## Executive result

The clean production build passed. The canonical full test leg did not reach
test execution because it hung in the venue-parity preflight. Memory analysis
completed its static scan and debug build, but the dynamic run was non-green:
Heaptrack reported 85 leaked allocations and the analyzed test runner had
three daemon IPC failures. Valgrind reported no lost memory, with only
still-reachable allocations.

## Build run

Command:

```text
scripts/build.sh
```

Result: PASS (exit 0).

The clean production binary was built at:

```text
build/c/codebase-memory-cli
```

The build used GCC on Linux x86_64 and completed the full production object
and link step after CP72 changes.

## Canonical full test run

Command:

```text
scripts/test.sh
```

Result: BLOCKED before suite execution.

Passed preflight checks included build-directory safety, Windows VM worktree
manifest, UI proxy security, daemon soak recovery, Windows bundle, tree-sitter
dependencies, security fuzz self-test, smoke fixture, parallel scheduler, and
venue parity checks.

The run then stalled in the venue-parity check while invoking:

```text
scripts/ci/generate-sbom.py --help
```

The child process stopped at `import datetime`, consistent with the existing
shell-versus-Python invocation defect. The stalled process was terminated;
the canonical sanitizer build and full suite were therefore not reached.

## Memory analysis run

Command:

```text
scripts/analyze-memory.sh
```

Suites:

```text
daemon_application daemon_ipc
```

Results directory:

```text
memlab-memory-analysis/20260905-170357-91427
```

### scan-build

Result: PASS. No findings were reported in the targeted sources:

- `src/operations/session_state.c`
- `src/operations/store_host.c`
- `src/operations/mutation.c`
- `src/daemon/application.c`

### Plain debug build

Result: PASS. The dedicated test runner linked successfully in the temporary
memory-analysis build directory.

### Heaptrack

Result: FAIL under the script's leak policy.

- Allocations: 35,173
- Leaked allocations: 85
- Temporary allocations: 1,174
- Total memory leaked in the Heaptrack analysis: 8.53 KiB

The complete capture and analysis are included in `heaptrack.zst`,
`heaptrack.analysis`, and the corresponding stdout/stderr files.

### Valgrind Memcheck

Result: FAIL as a test-runner result, not as a lost-memory result.

The runner reported 84 passed and 3 failed tests. The failures were:

- `daemon_ipc_posix_startup_lock_is_cross_process`
- `daemon_ipc_posix_lifetime_reservation_rejects_fork_inheritance`
- `daemon_ipc_posix_child_participant_handoff_retains_legacy_bridge`

Leak summary:

- Definitely lost: 0 bytes
- Indirectly lost: 0 bytes
- Possibly lost: 0 bytes
- Still reachable: 2,816 bytes in 16 blocks
- Valgrind error contexts: 0

The complete Memcheck log is `valgrind.log`.

## Qualification state

This evidence does not establish a green full-suite or leak-free release
state. The next useful work is to fix the venue-parity Python invocation,
classify the three daemon IPC failures, and attribute Heaptrack's 85 retained
allocations before adding teardown code.

## Included files

This report is packaged with the complete run directory, including:

- Heaptrack compressed capture and analysis;
- Heaptrack stdout/stderr;
- Valgrind log and stdout/stderr.
