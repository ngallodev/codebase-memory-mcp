# Backlog

## Resolved

- [x] Resolve application-teardown ownership omission for neutral session
  state. `cbm_daemon_application_free()` now calls
  `cbm_operation_session_state_free()` before freeing each session. Valgrind
  identified the omission through `test_daemon_application_free_releases_live_watch_once`
  as 80 direct + 107 indirect lost bytes.

## Open

- [ ] Retire or migrate legacy MCP-dependent test sources. The production
  Makefile now leaves `MCP_SRCS` empty, but the monolithic test runner still
  compiles MCP tests and repro harnesses; focused daemon tests therefore stop
  at link time on unresolved `cbm_mcp_*` symbols. Keep these tests out of CLI
  validation until they have neutral owners, then remove their source lists.

- [ ] Classify residual Heaptrack allocations in the neutral daemon test path.
  - Baseline evidence: Heaptrack reported 91 leaked allocations across the
    daemon application and IPC suites. Valgrind reported 538 bytes from
    `cbm_daemon_ipc_endpoint_new()` in the forked crash-simulation child of
    `test_daemon_ipc_posix_current_generation_crash_cleanup_requires_startup_lock`
    (`src/daemon/ipc.c:889`, test at `tests/test_daemon_ipc.c:3735`).
  - Remediation applied: the child now frees its inherited endpoint object
    before `_exit()`; it still bypasses `listener_close`, so kernel-released
    descriptors/locks and crash artifacts remain part of the test.
  - Current evidence: fresh `scripts/analyze-memory.sh` reports no static
    findings and Valgrind reports 0 definite, 0 indirect, and 0 possible lost
    bytes. Heaptrack reports 87 leaked allocations, which require attribution
    as still-reachable allocator/process-lifetime state before any further
    cleanup is attempted.
  - Next path: inspect the saved Heaptrack allocation call trees; only add
    teardown code for allocations proven to be owned by the neutral daemon
    rather than allocator or test-process lifetime state.
