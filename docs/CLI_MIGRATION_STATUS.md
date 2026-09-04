# Codebase Memory CLI Migration — Plan and Status

**Updated:** 2026-09-03  
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
- **PARTIAL** Legacy installer/update/uninstall cleanup remains only for ownership-aware removal of MCP-era state; active install paths are structurally CLI-first and no longer carry an MCP-enable transition flag.

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

The authoritative application implementation lives under `src/operations/`; there is no production MCP compatibility adapter or duplicate MCP handler body.

### Protocol-neutral execution support

- **COMPLETE (consolidated CP39 implementation)** bounded shell-command/process-tree execution used by source search and change detection moved out of MCP into `src/operations/command_runner.*`.
- **COMPLETE (consolidated CP39 implementation)** operation runtime carries cancellation and bounded-command test/limit overrides without an MCP server dependency.
- **COMPLETE (consolidated CP39 implementation)** daemon `REQUEST_OPERATION` supplies neutral cancellation context to long-running operations.

### Remaining read-only audit

- **COMPLETE:** the authoritative MCP-handler audit found no remaining ordinary read-analysis handler after `compare_graphs` moved to the neutral layer.
- **REMAINING:** continue auditing incidental read helpers only as dependent mutation/admin handlers move; do not create duplicate neutral implementations.

### Administrative / mutating operations

- **COMPLETE:** project deletion -> `delete-project`; authoritative behavior now lives under `src/operations/mutation.*`, preserves path/alias/tail project resolution through the shared neutral resolver, and refuses uncoordinated execution.
- **COMPLETE:** ADR management -> `manage-adr`; ADR get/sections/update/set-sections semantics now live under `src/operations/adr.*`, including legacy-file migration and coordinated writes. The generation-aware store resolver is still supplied through a transitional neutral runtime host seam until store recovery itself leaves MCP.
- **COMPLETE:** trace ingestion -> `ingest-traces`; current behavior is explicitly non-mutating (counts/accepts supplied observations and reports that runtime edge creation is not yet implemented), so it now lives in the neutral read/administrative operation layer rather than MCP.
- **COMPLETE:** cross-repository intelligence mode now lives in `src/operations/cross_repo.*`; ordinary `index --mode cross-repo-intelligence` invokes it directly through the neutral index operation while preserving ordered multi-project leases, wildcard/target validation, cancellation, partial-result semantics, and edge counters.
- **COMPLETE:** ordinary repository indexing now executes through the neutral `index` operation, including path/project resolution, workspace authorization, pipeline execution, artifact bootstrap, coverage/skip reporting, dump verification, and canonical response construction. Daemon admission/coalescing and supervised-worker containment remain preserved. `cross-repo-intelligence` now invokes the neutral cross-repository operation directly.

Rule during extraction: preserve behavior first, route all consumers to the neutral implementation, verify parity, delete the legacy body, and only then simplify.

## 3. Daemon semantics and write coordination

- **COMPLETE (CP29):** distinct neutral `REQUEST_OPERATION` path exists.
- **COMPLETE:** transitional daemon `REQUEST_TOOL` vocabulary is retired; all application commands cross IPC through `REQUEST_OPERATION`.
- **COMPLETE:** explicit daemon `REQUEST_MCP` vocabulary and the orphaned MCP stdio frontend are removed from production.
- **COMPLETE/PRESERVED:** existing SQLite WAL, busy handling, project mutation leases/locks, worker supervision, staging/atomic publication, cancellation cleanup, and index-job coalescing remain in place.
- **COMPLETE (consolidated CP39 implementation):** long-running neutral reads receive daemon request cancellation without routing through MCP.
- **COMPLETE for application mutations:** neutral mutation operations require explicit runtime authority. Daemon-backed deletion, ADR, indexing, and cross-repository mutation map to the existing cancellable logical reservation plus native per-project lease machinery. Trace ingestion is neutral at its current non-mutating semantics.
- **COMPLETE:** generic tool request vocabulary is retired; neutral operation responses preserve their error bit across IPC without an MCP envelope.
- **COMPLETE:** the generic maintenance observer formerly co-located with the MCP stdio frontend now lives in `src/daemon/maintenance_monitor.*`; the frontend itself is deleted.
- **COMPLETE:** normal daemon operation/hook sessions use neutral context/cancellation and no daemon application request can instantiate an MCP session object.

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

- **COMPLETE (CP40/CP41):** `src/mcp/`, protocol-only MCP tests, MCP stdio fuzz/introspection tooling, Glama MCP-directory packaging, MCP registry metadata, and MCPB bundle publication machinery are removed from active product/release infrastructure.
- **COMPLETE (CP41):** stale test-runner/TSan references to deleted MCP suites are removed, restoring a coherent neutral test graph.
- **COMPLETE for read-analysis business logic:** all ordinary read-heavy handlers identified in the handoff, plus file outline and graph comparison, now live under `src/operations/`; MCP is only a compatibility adapter for them.
- **COMPLETE:** the generation-aware store recovery/cache host now lives in `src/operations/store_host.*`, including integrity classification, confirmed-corruption quarantine, legacy renamed-database fallback, request/idle invalidation, and no-store diagnostics.
- **COMPLETE:** daemon `REQUEST_OPERATION` sessions own/use the neutral store host directly; neutral operation store resolution no longer reaches through `cbm_mcp_server_t` or public MCP store-adapter APIs.
- **COMPLETE for production execution:** normal CLI, daemon, supervised worker, and UI paths no longer construct or call `cbm_mcp_server_t`. The UI `/rpc` compatibility endpoint maps its two allowlisted calls directly to neutral operations and a neutral store host.
- **COMPLETE:** supervised worker -> daemon coordinator result transport now uses a neutral operation-result wire (`payload` + `is_error`) rather than an MCP tool-result envelope.
- **COMPLETE:** reusable JSON argument parsing, supervised-result classification, auto-index file-count admission, tool-profile vocabulary, and CLI/client tool metadata/schema ownership have neutral homes outside `src/mcp/`.
- **COMPLETE:** the physical `src/mcp/` subsystem and protocol-only `tests/test_mcp.c` suite are removed.
- **COMPLETE:** behavioral indexing/grammar/incremental/integration test harnesses no longer construct an MCP server; retained suites use a neutral test operation host over `cbm_operation_execute`.
- **COMPLETE:** the empty `MCP_SRCS` Makefile category is removed; production and test source lists no longer require MCP sources.
- **COMPLETE:** reusable JSON argument parsing used by test/daemon surfaces has a neutral `src/operations/json_args.*` home.
- **COMPLETE for active invariant/security harnesses:** `scripts/smoke-invariants.sh` now exercises the CLI-first workflow rather than MCP initialize/tools-list protocol behavior, and `scripts/security-audit.sh` no longer expects `src/mcp/mcp.c`.
- **COMPLETE (through CP47):** active release/package/smoke execution and distribution identity use `codebase-memory-cli`; remaining old repository URLs refer to the historical GitHub slug, and negative smoke/security checks intentionally reject retired MCP artifacts.
- **PARTIAL:** MCP-shaped tier-profile renderers and ownership markers remain only where update/uninstall needs byte-accurate recognition of previously released files. They are legacy removal compatibility, not active install output.
- **COMPLETE (post-CP47 audit):** active installer metadata no longer advertises MCP as a client capability; legacy cleanup is modeled explicitly as a removal obligation rather than a product feature.

No production or retained C behavioral-test source includes `mcp/mcp.h` or calls `cbm_mcp_server_*` / `cbm_mcp_handle_tool`. The obsolete protocol implementation is physically absent from the tree.

## 8. Release readiness

### Internal Windows validation build

- **READY TO RUN:** CP35 provides the dedicated native-Windows validation workflow. It should be run while operation extraction continues elsewhere.

### Public CLI-first release

- **NOT READY YET.** Primary blockers:
  1. Native Windows validation evidence must be reviewed.
  2. Final high-value end-to-end/concurrency/recovery/release verification remains.
  3. Post-MCP simplification and compatibility classification must finish without weakening ownership-aware legacy cleanup.

## 9. Consolidated-source policy

- **COMPLETE:** the latest complete project-source archive is the sole implementation baseline.
- **COMPLETE:** prior CP/overlay artifacts are disposable history and must not be replayed to reconstruct source state.
- **COMPLETE:** this status document is reconciled to the actual consolidated tree rather than inferred from old checkpoint labels.

## 10. Immediate planned sequence

1. **COMPLETE:** neutral mutation-authority contract mapped to the daemon's existing logical reservation + native project lease machinery.
2. **COMPLETE:** project deletion migrated as the first bounded administrative mutation, including legacy project argument/path/tail compatibility.
3. **COMPLETE:** ordinary indexing extraction: neutral ingress, physical pipeline/response implementation, and worker supervisor are outside MCP while daemon coalescing, worker containment, cancellation, staging/publication, and rebuild classification remain preserved.
4. Reconcile results from native Windows validation; fix platform-specific locking/cancellation/publication defects before release.
5. **COMPLETE:** neutralize the generic store recovery/cache host and make daemon operations use it without an MCP session dependency.
6. **COMPLETE:** extract neutral session context/cancellation state and make normal daemon operation/hook sessions avoid MCP allocation.
7. **COMPLETE:** retire transitional `REQUEST_TOOL`; CLI/daemon application commands now use only the neutral operation request and preserve operation error status without MCP envelopes.
8. **COMPLETE:** split the generic maintenance observer into `src/daemon/maintenance_monitor.*` and delete the orphaned `REQUEST_MCP`/JSON-RPC stdio frontend.
9. **COMPLETE for production:** remove direct UI MCP dispatch and neutralize production utility/schema/profile ownership; unlink `src/mcp/mcp.c` from the product build.
10. **COMPLETE for runtime/test architecture:** migrate/delete MCP-only tests and physically delete `src/mcp/`.
11. **COMPLETE through CP47:** active installer/release/package/smoke execution is CLI-first; retain only ownership-aware legacy removal state, persisted compatibility paths, negative guards, and historical repository URLs.
12. **IN PROGRESS:** post-MCP architectural simplification audit: transition-only wrappers/vocabulary and the Windows MCP-protocol guard dependency are removed; continue reconciling the remaining soak/repro protocol-shaped reliability harnesses while preserving correctness and legacy removal safety.
13. Complete benchmark/baseline tooling and run the high-value end-to-end/concurrency/recovery/release verification appropriate for the milestone.

## 11. Drift assessment

**On target.** Runtime Assurance and concurrency work was front-loaded before the second read-heavy extraction group. Subprocess cancellation/supervision, indexing, ADR, deletion, trace-ingest, and cross-repository behavior have crossed the neutral operation boundary without duplicate authoritative MCP handlers. CP47 is already merged into the authoritative source. The current phase is therefore post-MCP simplification: remove transition-only vocabulary, dead creation helpers, and stale active product identity while preserving legacy owned-state cleanup, persisted locations, and coordination/recovery semantics. An explicit final Linux production link is being re-established from this authoritative post-CP47 baseline before this document claims build verification; native Windows validation remains separate evidence.

## 12. Post-CP47 simplification audit

- **COMPLETE:** authoritative baseline confirmed to have `src/mcp/` physically absent and no production `cbm_mcp_server_*`, `cbm_mcp_handle_tool`, `REQUEST_MCP`, or `REQUEST_TOOL` references.
- **COMPLETE:** removed three dormant MCP creation/preflight helpers that had no callers and caused the post-CP47 production build to fail its own `-Werror=unused-function` policy.
- **COMPLETE:** renamed the stale generic daemon `MCP_CLIENT` process role to `BOOTSTRAP_CLIENT`; default/unknown public invocations remain stateless and the retained bootstrap path is protocol-neutral.
- **COMPLETE:** removed dead JSON-RPC/MCP bootstrap formatting/watchdog residue from the generic product entrypoint.
- **COMPLETE:** agent-client registry metadata no longer models MCP as an active capability or carries a one-implementation removal callback; profiles record only whether legacy MCP cleanup applies and call the shared removal routine directly.
- **COMPLETE:** removed no-op tier-profile install wrappers/calls. Ownership-aware tier-profile renderers remain reachable only from update/uninstall cleanup so previously released MCP-bound files can be recognized conservatively.
- **COMPLETE:** corrected stale active product identity in workspace guidance, update-manager suggestions, UI process monitoring, graph UI messaging, generated artifact comments, and the native-Windows guard wrapper.
- **PRESERVED:** legacy `codebase-memory-mcp` cache/config locations, old owned-entry names/markers, cleanup matchers, negative security/release guards, and historical GitHub repository URLs remain where changing them would break persisted compatibility, safe removal, or repository addressing.
- **COMPLETE:** explicit Linux production link succeeds as `build/c/codebase-memory-cli`; changed production C surfaces compile under the project's `-Wall -Wextra -Werror` policy.
- **COMPLETE:** removed the obsolete MCP-stdio parent-watchdog harness from the active test graph; retained the supervised worker parent-death watchdog because it protects a real CLI worker containment property.
- **COMPLETE:** active runtime/regression scripts that build or execute the product now default to `build/c/codebase-memory-cli`; canonical benchmark parsing no longer carries an MCP content-envelope fallback.
- **COMPLETE:** graph-UI missed-coverage guidance now instructs agents to use the canonical CLI status command rather than an MCP tool call.
- **COMPLETE (CP49):** native-Windows product guards no longer depend on the retired MCP stdio/JSON-RPC client. Unicode repo/cache indexing now exercises canonical `index`/`query`; daemon churn uses canonical `projects`; update handoff verifies that a live permanent daemon retains the same pid; the protocol-only `tests/windows/mcp_stdio.py` helper is deleted.
- **COMPLETE (CP50):** active watcher, memory-lab, and search-performance harnesses no longer construct MCP/JSON-RPC traffic. The watcher kill-switch is exercised through `config`/`daemon`/`index`; memlab repeatedly drives canonical operations against the long-lived coordination daemon; the search benchmark invokes canonical `search` directly.
- **PRESERVED:** daemon stop refusal while committed clients exist remains covered directly by the neutral daemon runtime suite. The obsolete Windows product-surface setup that created such a client by opening an MCP stdio session was removed rather than replaced with a fake frontend.
- **BLOCKED/EXTERNAL:** native Windows execution/validation evidence.

- **COMPLETE (CP51):** `scripts/soak-test.sh` and `tests/repro/issue832_rss.py` no longer drive a retired MCP stdio/JSON-RPC frontend. Soak now keeps the supported permanent coordination daemon under diagnostics, exercises canonical CLI read/index operations, and kills the daemon itself during an active index to validate recovery. The #832 RSS repro samples the permanent daemon across canonical `index` cycles for in-process versus supervised isolation.
- **REMAINING:** `tests/test_daemon_smoke.py` still contains a large mixed layer of valuable daemon lifecycle/cancellation/version-conflict assertions and retired MCP frontend-session semantics. Reconcile it as a dedicated bounded slice rather than mechanically translating protocol behavior.
