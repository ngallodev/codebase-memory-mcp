# Codebase Memory CLI Migration — Plan and Status

**Updated:** 2026-09-02  
**Current authoritative baseline:** latest complete consolidated source attached to the project sources page
**Checkpoint policy:** prior overlay/checkpoint archives are historical only and are not replayed or used to reconstruct repository state.
**Direction:** CLI-first code intelligence with a protocol-neutral operation API and coordination daemon; MCP is transitional compatibility only and must ultimately disappear.

Status markers: **COMPLETE**, **PARTIAL**, **REMAINING**, **BLOCKED/EXTERNAL**.

## 1. Product / CLI surface

- **COMPLETE** Product executable and user-facing direction are `codebase-memory-cli` / `codebase-memory-cli.exe`.
- **COMPLETE** No-argument invocation is CLI help; unknown commands fail as CLI errors rather than starting MCP stdio.
- **COMPLETE** Canonical exploration loop exists for `index`, `projects`, `status`, `search`, `snippet`, `trace`, and `coverage`.
- **COMPLETE** Neutral graph/source commands now also include `schema`, `query`, `architecture`, `changes`, `source-search`, `outline`, and `compare`.
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
- **COMPLETE** `get_file_outline` -> `outline`
- **COMPLETE** `compare_graphs` -> `compare`

For migrated operations, the authoritative implementation lives under `src/operations/`; MCP routes through a compatibility adapter and must not own a duplicate handler body.

### Protocol-neutral execution support

- **COMPLETE (consolidated CP39 implementation)** bounded shell-command/process-tree execution used by source search and change detection moved out of MCP into `src/operations/command_runner.*`.
- **COMPLETE (consolidated CP39 implementation)** operation runtime carries cancellation and bounded-command test/limit overrides without an MCP server dependency.
- **COMPLETE (consolidated CP39 implementation)** daemon `REQUEST_OPERATION` supplies neutral cancellation context to long-running operations.
- **COMPLETE (consolidated CP39 implementation)** MCP compatibility maps its request cancellation/test seams into the neutral operation runtime.

### Remaining read-only audit

- **COMPLETE:** the authoritative MCP-handler audit found no remaining ordinary read-analysis handler after `compare_graphs` moved to the neutral layer.
- **REMAINING:** continue auditing incidental read helpers only as dependent mutation/admin handlers move; do not create duplicate neutral implementations.

### Administrative / mutating operations

- **COMPLETE:** project deletion -> `delete-project`; authoritative behavior now lives under `src/operations/mutation.*`, preserves path/alias/tail project resolution through the shared neutral resolver, and refuses uncoordinated execution.
- **REMAINING:** ADR management.
- **COMPLETE:** trace ingestion -> `ingest-traces`; current behavior is explicitly non-mutating (counts/accepts supplied observations and reports that runtime edge creation is not yet implemented), so it now lives in the neutral read/administrative operation layer rather than MCP.
- **REMAINING:** cross-repository/index mutations and cross-repository mode handling.
- **COMPLETE:** ordinary repository indexing now executes through the neutral `index` operation, including path/project resolution, workspace authorization, pipeline execution, artifact bootstrap, coverage/skip reporting, dump verification, and canonical response construction. Daemon admission/coalescing and supervised-worker containment remain preserved. The special `cross-repo-intelligence` mode is intentionally still a separate transitional callback until cross-repository mutation is extracted.

Rule during extraction: preserve behavior first, route all consumers to the neutral implementation, verify parity, delete the legacy body, and only then simplify.

## 3. Daemon semantics and write coordination

- **COMPLETE (CP29):** distinct neutral `REQUEST_OPERATION` path exists.
- **PARTIAL:** legacy `REQUEST_MCP` / `REQUEST_TOOL` compatibility paths remain and must shrink as operations migrate.
- **COMPLETE/PRESERVED:** existing SQLite WAL, busy handling, project mutation leases/locks, worker supervision, staging/atomic publication, cancellation cleanup, and index-job coalescing remain in place.
- **COMPLETE (consolidated CP39 implementation):** long-running neutral reads receive daemon request cancellation without routing through MCP.
- **PARTIAL, materially advanced:** neutral mutation operations now require explicit runtime authority. Daemon-backed deletion maps that authority to the existing cancellable logical reservation plus native per-project lease; indexing/ADR/trace/cross-repo still require migration.
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

- **COMPLETE for read-analysis business logic:** all ordinary read-heavy handlers identified in the handoff, plus file outline and graph comparison, now live under `src/operations/`; MCP is only a compatibility adapter for them.
- **PARTIAL:** MCP still owns cross-repository mode, ADR management, auto-index/session compatibility lifecycle, and transport/runtime compatibility surfaces. Project deletion and ordinary repository indexing are neutralized, and the index worker supervisor is no longer under `src/mcp/`.
- **REMAINING:** move ADR and cross-repository behavior to explicit neutral/admin/mutation boundaries; later retire MCP-owned auto-index/session compatibility lifecycle with the broader session cleanup.
- **REMAINING:** remove MCP tool registry/schema ownership after all useful operations are neutral.
- **REMAINING:** remove JSON-RPC framing and `tools/list` / `tools/call`.
- **REMAINING:** remove MCP prompts and stdio frontend.
- **REMAINING:** neutralize UI/session dependencies that still rely on MCP structures.
- **REMAINING:** remove MCP-only installer/release surfaces once legacy owned-config cleanup is no longer required.
- **REMAINING:** delete `src/mcp/` only after it owns no application business logic.

Current authoritative application handler bodies still present in `src/mcp/mcp.c` are: cross-repository mode and ADR management. MCP still contains compatibility/session auto-index orchestration, but ordinary `index_repository` pipeline/response ownership has moved to `src/operations/index.*`.

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

## 9. Consolidated-source policy

- **COMPLETE:** the latest complete project-source archive is the sole implementation baseline.
- **COMPLETE:** prior CP/overlay artifacts are disposable history and must not be replayed to reconstruct source state.
- **COMPLETE:** this status document is reconciled to the actual consolidated tree rather than inferred from old checkpoint labels.

## 10. Immediate planned sequence

1. **COMPLETE:** neutral mutation-authority contract mapped to the daemon's existing logical reservation + native project lease machinery.
2. **COMPLETE:** project deletion migrated as the first bounded administrative mutation, including legacy project argument/path/tail compatibility.
3. **COMPLETE:** ordinary indexing extraction: neutral ingress, physical pipeline/response implementation, and worker supervisor are outside MCP while daemon coalescing, worker containment, cancellation, staging/publication, and rebuild classification remain preserved.
4. Reconcile results from native Windows validation; fix platform-specific locking/cancellation/publication defects before release.
5. Extract ADR and cross-repository behavior without weakening existing coordination guarantees. Trace ingestion is already neutralized at its current non-mutating behavior.
6. Shrink daemon compatibility paths and remove MCP session/tool semantics.
7. Clean installer/release surfaces and delete MCP only when it owns no application logic.
8. Complete benchmark/baseline tooling and run the high-value end-to-end/concurrency/recovery/release verification appropriate for the milestone.

## 11. Drift assessment

**On target, with intentional sequencing skew and one corrected seam.** Runtime Assurance and concurrency work was front-loaded before the second read-heavy extraction group. The current extraction exposed subprocess cancellation/supervision as an MCP-owned implementation dependency; instead of duplicating or weakening it, that bounded-command runtime was moved to the neutral operation layer. This is consistent with the target architecture and reduces hidden MCP ownership. The read-analysis extraction is now closed. The priority shifts to making mutation authority explicit at the neutral operation boundary and migrating administrative writes without weakening the daemon/project-lock guarantees, except where real Windows validation exposes defects.
