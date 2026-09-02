# Codebase Memory CLI Migration — Plan and Status

**Updated:** 2026-09-02  
**Current implementation target:** CP39 consolidated read-extraction checkpoint  
**Checkpoint note:** the attempted CP38 download was not usable and is not relied upon. Its `detect_changes` implementation work is incorporated into this consolidated state.  
**Direction:** CLI-first code intelligence with a protocol-neutral operation API and coordination daemon; MCP is transitional compatibility only and must ultimately disappear.

Status markers: **COMPLETE**, **PARTIAL**, **REMAINING**, **BLOCKED/EXTERNAL**.

## 1. Product / CLI surface

- **COMPLETE** Product executable and user-facing direction are `codebase-memory-cli` / `codebase-memory-cli.exe`.
- **COMPLETE** No-argument invocation is CLI help; unknown commands fail as CLI errors rather than starting MCP stdio.
- **COMPLETE** Canonical exploration loop exists for `index`, `projects`, `status`, `search`, `snippet`, `trace`, and `coverage`.
- **COMPLETE** Neutral graph/source commands now also include `schema`, `query`, `architecture`, `changes`, `source-search`, and `outline`.
- **COMPLETE** Existing cache/index/database naming and persisted compatibility-sensitive internals remain unchanged.
- **PARTIAL** Legacy installer/release cleanup remains where MCP-era ownership/removal compatibility is still required.

## 2. Neutral operation API / MCP extraction

### Neutralized reads

- **COMPLETE** `list_projects` -> `projects`
- **COMPLETE** `index_status` -> `status`
- **COMPLETE** `check_index_coverage` -> `coverage`
- **COMPLETE** `search_graph` -> `search`
- **COMPLETE** `get_code_snippet` -> `snippet`
- **COMPLETE** `trace_path` -> `trace`
- **COMPLETE (CP36)** `get_graph_schema` -> `schema`
- **COMPLETE (CP36)** `query_graph` -> `query`
- **COMPLETE (CP37)** `get_architecture` -> `architecture`
- **COMPLETE (consolidated CP39 implementation)** `detect_changes` -> `changes`
- **COMPLETE (consolidated CP39 implementation)** `search_code` -> `source-search`
- **COMPLETE (consolidated CP39 implementation)** `get_file_outline` -> `outline`

For migrated operations, the authoritative implementation lives under `src/operations/`; MCP routes through a compatibility adapter and must not own a duplicate handler body.

### Protocol-neutral execution support

- **COMPLETE (consolidated CP39 implementation)** bounded shell-command/process-tree execution used by source search and change detection moved out of MCP into `src/operations/command_runner.*`.
- **COMPLETE (consolidated CP39 implementation)** operation runtime carries cancellation and bounded-command test/limit overrides without an MCP server dependency.
- **COMPLETE (consolidated CP39 implementation)** daemon `REQUEST_OPERATION` supplies neutral cancellation context to long-running operations.
- **COMPLETE (consolidated CP39 implementation)** MCP compatibility maps its request cancellation/test seams into the neutral operation runtime.

### Remaining read-only audit

- **REMAINING — next:** classify and, if appropriate, neutralize `compare_graphs`.
- **REMAINING:** inspect any additional read-only business logic still authoritative in `src/mcp/` before declaring the read migration closed.

### Administrative / mutating operations

- **REMAINING:** project deletion.
- **REMAINING:** ADR management.
- **REMAINING:** trace ingestion.
- **REMAINING:** cross-repository/index mutations and cross-repository mode handling.
- **REMAINING:** indexing behavior still coupled to MCP/application state.

Rule during extraction: preserve behavior first, route all consumers to the neutral implementation, verify parity, delete the legacy body, and only then simplify.

## 3. Daemon semantics and write coordination

- **COMPLETE (CP29):** distinct neutral `REQUEST_OPERATION` path exists.
- **PARTIAL:** legacy `REQUEST_MCP` / `REQUEST_TOOL` compatibility paths remain and must shrink as operations migrate.
- **COMPLETE/PRESERVED:** existing SQLite WAL, busy handling, project mutation leases/locks, worker supervision, staging/atomic publication, cancellation cleanup, and index-job coalescing remain in place.
- **COMPLETE (consolidated CP39 implementation):** long-running neutral reads receive daemon request cancellation without routing through MCP.
- **PARTIAL:** the intended single authoritative same-project mutation path is substantially represented by existing coordination mechanisms, but all remaining mutating commands still need to be audited as MCP is removed.
- **REMAINING:** retire MCP/tool request vocabulary when no valuable behavior depends on it.
- **REMAINING:** remove MCP session concepts from daemon/application state after dependent UI/hooks/runtime paths are neutralized.

## 4. Runtime Assurance

- **COMPLETE (CP30):** read-only `doctor` foundation.
- **COMPLETE (CP30):** `doctor --deep` separates expensive integrity verification from routine checks.
- **COMPLETE/PRESERVED:** SQLite `BUSY` / `LOCKED` are treated as transient/busy states, not corruption.
- **COMPLETE (CP31):** stable reliability-event vocabulary and bounded persistent reliability history.
- **COMPLETE (CP31):** `doctor` can aggregate recent reliability evidence.
- **PARTIAL:** error-code taxonomy and rebuild-reason coverage should continue to be tightened as remaining mutation paths migrate.
- **REMAINING:** benchmark command/baseline comparison surface described in the handoff has not yet been completed.

## 5. Concurrency / recovery evaluation

- **COMPLETE (CP32):** process-level reader + reader scenario.
- **COMPLETE (CP32):** reader + writer scenario.
- **COMPLETE (CP32):** same-project writer + writer scenario with coalescing/ownership evidence.
- **COMPLETE (CP32):** different-project writer + writer scenario with overlap evidence.
- **COMPLETE (CP33):** controlled writer-client crash/recovery scenario.
- **COMPLETE (CP33):** daemon hard-crash/restart scenario.
- **COMPLETE (CP34):** WAL pressure / long-reader evaluation scaffolding and explicit generation/publication evidence using database identity/generation metadata.
- **PARTIAL:** harness exists, but platform-specific evidence depends on executing a real native Windows binary and representative repositories.

## 6. Windows 10/11

- **COMPLETE:** native MSYS2/Clang Windows build paths exist; Windows x64 is a release requirement and ARM64 support is present where configured.
- **COMPLETE (CP35):** dedicated Windows validation workflow builds the native `codebase-memory-cli.exe` and retains evidence artifacts.
- **COMPLETE (CP35):** validation runner includes paths with spaces, Unicode paths, CLI/doctor smoke, concurrency/recovery scenarios, and Windows console cancellation/recovery.
- **BLOCKED/EXTERNAL:** actual Windows validation results must come from the Windows runner/environment; Linux execution is not accepted as Windows proof.
- **REMAINING:** inspect/fix any failures from the real Windows validation run before public release.

## 7. MCP subsystem retirement

- **PARTIAL, materially advanced:** the principal read-heavy handlers identified in the handoff have moved out of MCP and their legacy handler bodies have been removed.
- **PARTIAL:** MCP still owns compare/admin/mutation business logic and compatibility/runtime surfaces.
- **REMAINING:** classify/migrate or deliberately retire `compare_graphs`.
- **REMAINING:** move indexing, deletion, ADR, trace-ingestion, and cross-repository behavior to explicit neutral/admin/mutation boundaries.
- **REMAINING:** remove MCP tool registry/schema ownership after all useful operations are neutral.
- **REMAINING:** remove JSON-RPC framing and `tools/list` / `tools/call`.
- **REMAINING:** remove MCP prompts and stdio frontend.
- **REMAINING:** neutralize UI/session dependencies that still rely on MCP structures.
- **REMAINING:** remove MCP-only installer/release surfaces once legacy owned-config cleanup is no longer required.
- **REMAINING:** delete `src/mcp/` only after it owns no application business logic.

Current authoritative handler bodies still present in `src/mcp/mcp.c` after this read-extraction slice are: `compare_graphs`, project deletion, cross-repository mode, indexing, ADR management, and trace ingestion.

## 8. Release readiness

### Internal Windows validation build

- **READY TO RUN:** CP35 provides the dedicated native-Windows validation workflow. It should be run while operation extraction continues elsewhere.

### Public CLI-first release

- **NOT READY YET.** Primary blockers:
  1. Remaining MCP-owned compare/admin/mutation behavior must be classified and migrated or deliberately retired.
  2. Mutation authority must be audited after indexing/delete/ADR/trace/cross-repository extraction.
  3. Legacy MCP release/install publishing surfaces still require cleanup or explicit quarantine as legacy removal support.
  4. Native Windows validation evidence must be reviewed.
  5. Final high-value end-to-end/concurrency/recovery/release verification remains.

## 9. Checkpoint history

- **CP29 — COMPLETE:** daemon `REQUEST_OPERATION` semantics and assurance-event vocabulary.
- **CP30 — COMPLETE:** `doctor` foundation.
- **CP31 — COMPLETE:** bounded reliability-event history / doctor aggregation.
- **CP32 — COMPLETE:** initial multi-process concurrency evaluation harness.
- **CP33 — COMPLETE:** writer and daemon crash/recovery evaluation.
- **CP34 — COMPLETE:** WAL/long-reader and generation/publication evaluation.
- **CP35 — COMPLETE:** native Windows validation workflow and Windows-specific evaluation runner.
- **CP36 — COMPLETE:** graph schema and graph query moved to neutral operations; generic compact output moved out of MCP.
- **CP37 — COMPLETE:** architecture analysis moved to the neutral operation layer; MCP is an adapter for `get_architecture`.
- **CP38 — NOT RELIED UPON AS AN ARTIFACT:** attempted delivery was unavailable; its `detect_changes` implementation work is incorporated into the current consolidated state.
- **CP39 — CURRENT IMPLEMENTATION TARGET:** consolidate `detect_changes`, source search, neutral command execution/cancellation, and file outline into one valid cumulative checkpoint.

## 10. Immediate planned sequence

1. Finish and validate the **CP39 consolidated read-extraction** state; do not depend on the failed CP38 archive.
2. Audit `compare_graphs` and any remaining read-only MCP logic; neutralize where it remains product behavior worth keeping.
3. Reconcile results from native Windows validation; fix platform-specific locking/cancellation/publication defects before release.
4. Formalize/audit the mutation boundary and extract indexing, delete, ADR, trace-ingestion, and cross-repository behavior without weakening existing coordination guarantees.
5. Shrink daemon compatibility paths and remove MCP session/tool semantics.
6. Clean installer/release surfaces and delete MCP only when it owns no application logic.
7. Complete benchmark/baseline tooling and run the high-value end-to-end/concurrency/recovery/release verification appropriate for the milestone.

## 11. Drift assessment

**On target, with intentional sequencing skew and one corrected seam.** Runtime Assurance and concurrency work was front-loaded before the second read-heavy extraction group. The current extraction exposed subprocess cancellation/supervision as an MCP-owned implementation dependency; instead of duplicating or weakening it, that bounded-command runtime was moved to the neutral operation layer. This is consistent with the target architecture and reduces hidden MCP ownership. The priority should now shift from read extraction to the remaining compare/admin/mutation boundary rather than broadening assurance further, except where real Windows validation exposes defects.
