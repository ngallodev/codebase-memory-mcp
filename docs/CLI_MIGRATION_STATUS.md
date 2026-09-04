# Codebase Memory CLI Migration — Plan and Status
> **Planning re-baseline (CP61):** the CLI-first/MCP-removal architecture phase is closed unless qualification evidence exposes a concrete defect. Remaining work is release qualification: Linux consolidation, immutable GitHub RC artifacts, native Windows validation on `luigi.home.arpa`, frozen-corpus comparative benchmarking, agent usability, production skill, and release decision. See `docs/RELEASE_QUALIFICATION_PLAN.md`.

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

- **COMPLETE (CP53):** runtime-isolation wiring for CLI security fuzz/network harnesses uses the shared private runtime/cache helper; the stale MCP-fixture fuzz harness is replaced by a CLI fixture that proves isolation and crash rejection.
- **COMPLETE (CP53):** stale transition vocabulary was removed from active worker/hook/pipeline comments and the phantom `mcp` suite label was removed from the parallel test scheduler.
- **COMPLETE (CP53):** obsolete `tests/DEPRECATED_MCP_TESTS.md` was deleted; its inventory no longer described the retained neutral test graph.

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
- **COMPLETE (CP52):** `tests/test_daemon_smoke.py` is now a CLI-first real-binary guard. It covers cold/warm CLI clients, permanent-daemon PID stability, daemon-backed indexing, crash recovery, and clean stop. Detailed cancellation, session isolation, rendezvous/version-conflict, and committed-client stop-refusal semantics remain in the neutral `tests/test_daemon_runtime.c` suite rather than being duplicated through a retired frontend protocol.

### CP54 — Public documentation and release-contract reconciliation

- **COMPLETE:** Active homepage and `llms.txt` product identity now describe `codebase-memory-cli` as a local CLI-first code-intelligence application rather than an MCP server. Historical repository/homepage URLs remain unchanged because the repository slug is compatibility/history, not runtime architecture.
- **COMPLETE:** `docs/CONFIGURATION.md` distinguishes compatibility-sensitive persisted `codebase-memory-mcp` paths from the active CLI product and removes stale MCP-only request-path wording.
- **COMPLETE:** version- and language-count contract surfaces now point at the renamed CLI package files; the vacuous npm language-count surface was removed from the contract because it no longer publishes a count.
- **COMPLETE:** active test/workflow comments and usage examples touched by this slice no longer describe neutral indexing/runtime behavior as MCP behavior.

### CP55 — Internal profile/observability and UI protocol cleanup

- **COMPLETE:** removed the dead `cbm_log_mcp_request()` observability helper and its self-only unit coverage; production had no callers after physical MCP removal.
- **COMPLETE:** removed the daemon `ALL/ANALYSIS/SCOUT` tool-profile field and `src/operations/tool_profile.h`. Every production context caller already used the unrestricted value, so the profile byte was transition-only state rather than a live product choice.
- **COMPLETE:** simplified daemon SET_CONTEXT wire state by removing the unused profile byte while preserving root, allowed-root, hook event/dialect, cancellation, watcher, auto-index, update, and UI coordination behavior.
- **COMPLETE:** migrated retained daemon-application lifecycle tests from retired numeric request kind 2 / JSON-RPC pings to neutral operation requests or direct context establishment; deleted tests whose only contract was MCP notifications/restricted MCP surfaces.
- **COMPLETE:** removed the UI `POST /rpc` JSON-RPC compatibility endpoint. The current embedded UI has no consumer for it; supported UI behavior remains on neutral `/api/...` routes and direct neutral operations/store infrastructure.
- **COMPLETE:** migrated retained HTTP security/watchdog tests to supported `/api/...` endpoints and deleted the RPC allowlist test that protected only the retired protocol envelope.
- **PRESERVED:** tier-profile renderers and exact `--tool-profile=analysis|scout` strings remain only in ownership-aware uninstall/update recognition of files produced by historical releases. Clean install does not expose a daemon profile mode.
- **PRESERVED:** historical repository URLs, persisted `codebase-memory-mcp` cache/config paths, owned-entry markers, and negative guards remain compatibility/history rather than active architecture.

### CP56 — Legacy profile compatibility boundary and doctor configuration visibility

- **COMPLETE:** renamed the historical tier/profile renderer source and test surfaces to `legacy_agent_profiles.*`, making their ownership explicit: they reproduce bytes from prior releases solely so update/uninstall can remove exact Codebase Memory-owned documents without deleting modified or foreign files.
- **PRESERVED:** historical `--tool-profile=analysis|scout`, MCP server names, and old agent dialect payloads remain inside that legacy renderer because byte-accurate recognition is a safety mechanism, not an active install capability.
- **COMPLETE:** new OpenClaw compaction augmentation now writes `Codebase Knowledge Graph (codebase-memory-cli)`. Cleanup recognizes and removes both the current CLI label and the historical MCP label.
- **COMPLETE:** the durable agent-instructions contract test now validates the actual CLI-first instructions and rejects `codebase-memory-mcp`/`search_graph` leakage from new instructions.
- **COMPLETE:** `doctor` now reports effective watcher state plus UI enabled/port configuration in both human and JSON output, reusing existing configuration sources rather than introducing duplicate health infrastructure.

## CP57 — creation-era test and dead result-helper cleanup

- **COMPLETE:** removed the unused `cbm_cli_mcp_result_is_error()` API. It had no production caller and survived only through a self-test for the retired MCP result envelope.
- **COMPLETE:** removed creation-era CLI tests that expected fresh tiered MCP subagents, per-agent MCP server blocks, or `--tool-profile` registrations. Current install behavior is CLI-first; byte-accurate historical renderers remain tested separately for ownership-aware update/uninstall cleanup.
- **PRESERVED:** durable CLI instructions, skills, hooks, exact legacy cleanup recognition, foreign-file protection, and tier-profile uninstall/migration coverage remain in the active test graph.

## CP58 — recovery observability and concurrency evidence audit

- **COMPLETE:** audited the retained recovery/concurrency harnesses and confirmed substantive coverage already exists for cross-process lock ordering/fairness, cancellation rollback, WAL crash recovery, mutation serialization, index coalescing, worker containment, daemon stop/refusal, and corruption quarantine/recovery. No duplicate concurrency framework or broad low-value unit-test expansion was added.
- **COMPLETE:** the neutral store host now records reliability events when a previously suspect persisted generation rechecks as transient, corrupt, successfully quarantined, or requiring rebuild. A successful coordinated recheck is also recorded as integrity-ok.
- **COMPLETE:** human `doctor` output now surfaces observed bounded reliability-event counts in addition to the existing JSON summary; unobserved/reserved event identifiers remain absent rather than being presented as synthetic zero evidence.
- **PRESERVED:** BUSY/LOCKED classification remains distinct from corruption; the new event wiring records only verdicts actually produced by the existing recovery boundary and does not infer SQLite error codes it does not own.
- **REMAINING:** establish real workflow performance baselines and run the phase-end Linux reliability pass; native Windows validation remains external evidence.

## CP59 — Agent-workflow performance harness

**PARTIAL** — The performance-baseline harness is now CLI-first and reproducible. The canonical
workflow runner covers index, search, architecture, snippet, outline, changes, status, and optional
cross-repository intelligence using an isolated cache. `scripts/benchmark-index.sh` no longer uses
the deprecated raw `cli index_repository` compatibility interface.

Numeric baseline capture remains **REMAINING** for this checkpoint because the production `-O2`
build did not complete within the available uninterrupted compiler window; no substitute
unoptimized/older binary was used for measurements. See `docs/PERFORMANCE_BASELINE.md`.

## CP60 — Benchmark reproducibility contract

- **COMPLETE:** the workflow benchmark now defines a reproducibility contract rather than treating raw timings as self-validating evidence. Comparative runs must pin executable identity/hash, benchmark-repository commit and dirty state, harness revision/parameters, cache-state procedure, host metadata, and relevant `CBM_*` environment.
- **COMPLETE:** each benchmark invocation is one independent trial and refuses a reused result/cache directory. Repeated read cases receive one unrecorded warm-up before recorded timings so reference/candidate comparisons use the same steady-state semantics.
- **COMPLETE:** `environment.txt` records binary/harness SHA-256, repository revision/origin/dirty state, timestamp, CPU, memory, filesystem, workload parameters, and benchmark-relevant environment where available.
- **COMPLETE:** the documented release comparison procedure calls for at least three independent fresh-cache trials per binary, median-based read comparison, exact input/harness parity, correctness/graph-shape checks, and explicit invalidation conditions for mismatched runs.
- **REMAINING:** capture numeric reference/candidate baselines with a successfully linked production-optimized binary. The methodology is now explicit enough for those results to be independently repeated; numeric evidence is still not claimed by this checkpoint.

## CP61 — Release qualification planning re-baseline

- **COMPLETE:** migration-era sequencing is superseded by `docs/RELEASE_QUALIFICATION_PLAN.md`; further architecture changes require evidence from qualification rather than generic cleanup.
- **COMPLETE:** native Windows release validation is specified as an external procedure against immutable GitHub-published bytes on `luigi.home.arpa`; portable and installed forms are both required.
- **COMPLETE:** comparative Windows benchmarking is specified around a frozen repository corpus, exact artifact identity, fresh-cache alternating trials, correctness/shape parity, and retained raw evidence.
- **COMPLETE:** a standalone Windows qualification-agent runbook defines acquisition, identity verification, frozen-corpus checks, functional/recovery testing, install/update/uninstall validation, benchmarking, failure classification, and evidence summary requirements.
- **COMPLETE:** benchmark corpus revisions are immutable within a corpus generation. Any repository revision change creates a new generation and requires a new baseline capture rather than comparison against older results.
- **REMAINING:** finalize the exact `agent-workflow` remote and select/pin the fourth larger open-source repository already available on `r6d12`/the qualification environment.
- **REMAINING:** complete Linux consolidation, publish an immutable GitHub release candidate, execute native Windows qualification and comparative baselines, then run agent usability and produce the final product skill.

## CP62 — Frozen benchmark corpus finalization

**COMPLETE (planning):** `windows-corpus-1` now has fixed repository identities: codebase-memory-cli, specgen-aw, agent-workflow, agent-workflow-spec-contracts, and herdr. Repository remotes are explicit in `docs/qualification/BENCHMARK_CORPUS.json`. Exact commit SHAs remain deliberately unset until the one-time initialization reads them from the actual clean frozen checkouts used on `luigi.home.arpa`; after pinning, any commit change creates a new corpus generation and requires new baseline capture.



## CP63 — Linux qualification consolidation

Status: **IN PROGRESS / QUALIFICATION**

The project is now executing the release-qualification plan rather than further migration cleanup. The fast sanitized foundation gate has been repaired to use a true foundation-only incremental runner and currently passes 312 tests with two explicit Windows-only skips. Release/runtime/security/smoke contracts have been reconciled with the supported CLI-first release topology and pass. See `docs/LINUX_RELEASE_QUALIFICATION.md` for evidence and remaining Linux phase-end work.

The current optimized production build has not yet reached final link within this execution environment's bounded compiler windows; no production compiler error has been observed. Final-link evidence remains required for the current candidate and will also be obtained from the immutable GitHub RC build.

## CP64 — external qualification release hold

**COMPLETE:** GitHub release preparation now defaults to a draft external-qualification hold. A separate promotion workflow validates `luigi.home.arpa` qualification evidence against the exact Windows release archive/executable hashes before registry publication or public un-drafting. Linux broad sanitized qualification remains in progress due build duration; no source failure has surfaced in the incremental build.

### CP65 — RC dispatch reproducibility (release qualification)

Status: **COMPLETE**

- Added a deterministic RC-dispatch preparation tool that refuses dirty trees and non-`vX.Y.Z-rc.N` tags.
- Added a standalone GitHub RC dispatch agent runbook.
- RC dispatch evidence now binds source commit, workflow hash, corpus manifest hash, qualification host, and held-draft release inputs.
- The first CLI-first RC is recommended as `v0.11.0-rc.1` after the current public `v0.10.8`, but version selection remains an explicit operator decision.
- Broad Linux ASan/UBSan runner compilation remains in progress locally; no compiler/source failure has surfaced in the preserved incremental build.

### CP66 — explicit first-CLI baseline decision

Status: **COMPLETE (release qualification contract)**

- RC dispatch now refuses to proceed without an explicit benchmark-baseline decision: either `--baseline-zero` or an immutable `--baseline-tag`.
- The first CLI-first candidate (`v0.11.0-rc.1` under the current plan) must explicitly use `BASELINE_ZERO`; `v0.10.8` is not treated as a like-for-like CLI workflow performance baseline.
- `BASELINE_ZERO` is never inferred automatically. After the first qualified CLI release, later candidates must name the exact prior qualified CLI release tag for comparative benchmarking.
- The Windows qualification runbook consumes the RC dispatch decision and must not substitute an incompatible baseline artifact.
- Local broad sanitized compilation remains useful supporting evidence, but the held GitHub RC workflow is the authoritative final build/test source for immutable release bytes.
