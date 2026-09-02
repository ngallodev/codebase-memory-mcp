# Codebase Memory CLI-First Migration — First Vertical Slice Implementation Plan

Status: implementation plan and pre-implementation critical review  
Date: 2026-09-01  
Authoritative source: `codebase-memory-mcp-cli-only-handoff-17786374.tar.zst`

## 1. Objective

Deliver the first usable CLI-first vertical slice without rewriting the graph engine.

At the end of this slice:

- normal product invocation is CLI-only;
- running the binary with no command does **not** start JSON-RPC/MCP or a daemon-backed MCP session;
- the primary exploration loop is available as ordinary named commands;
- machine-readable output remains available;
- skills teach the named CLI, not MCP tools;
- supported hooks continue to provide useful discovery/coverage context while pointing agents at the CLI;
- installers stop creating new MCP server registrations for the integrations touched by the slice;
- legacy `cli <tool>` remains only as a temporary compatibility/parity surface;
- the internal MCP dispatcher may remain temporarily as a private execution kernel for unmigrated business logic, but there is no supported MCP server surface.

This slice intentionally separates **external MCP disablement** from the larger follow-on job of extracting every tool handler out of `src/mcp/mcp.c`.

## 2. Why this boundary

The current code couples three different concerns:

1. JSON-RPC/MCP transport and session behavior;
2. tool metadata/schema/argument parsing;
3. graph/index business logic.

The CLI, hook augmenter, daemon application, generated client adapters, and installer all depend on pieces currently named `mcp`.
Attempting to physically delete `src/mcp` in the first slice would therefore require a broad horizontal rewrite before the user receives any useful CLI product.

The first slice instead makes the **product contract** CLI-first now, then leaves a clearly documented private compatibility kernel to extract in subsequent slices.

## 3. User-visible vertical slice

### 3.1 Canonical commands

The first slice exposes:

```text
codebase-memory-cli index [PATH] [options]
codebase-memory-cli status [options]
codebase-memory-cli search [QUERY] [options]
codebase-memory-cli trace <SYMBOL> [options]
codebase-memory-cli snippet <SYMBOL> [options]
codebase-memory-cli coverage [PATH] [options]
codebase-memory-cli projects [options]
```

The binary name remains `codebase-memory-mcp` during this slice to avoid coupling functional migration to packaging/rename work. A later phase renames it to `codebase-memory` with `cbm` as an optional alias.

### 3.2 Compatibility commands

The existing form remains temporarily:

```text
codebase-memory-cli cli <tool_name> ...
```

It is not shown as the recommended interface. It exists for parity, old scripts, generated adapters not yet migrated, and characterization during extraction.

### 3.3 Command mapping

| CLI | Existing operation |
|---|---|
| `index` | `index_repository` |
| `status` | `index_status` |
| `search` | `search_graph` |
| `trace` | `trace_path` |
| `snippet` | `get_code_snippet` |
| `coverage` | `check_index_coverage` |
| `projects` | `list_projects` |

Aliases are declared centrally rather than implemented as separate business logic.

## 4. Agent and human interface contract

### 4.1 Humans

Interactive/default output stays readable. Help shows examples and maps ordinary nouns/verbs to behavior. Errors go to stderr and return non-zero.

### 4.2 Agents

Every canonical command accepts `--json`. JSON stdout must contain no progress prose. Progress/diagnostics remain on stderr.

Agents should be able to discover the product with:

```text
codebase-memory-cli --help
codebase-memory-cli search --help
codebase-memory-cli projects --json
```

### 4.3 Positional convenience

Named commands add only thin positional sugar:

- `index PATH` -> `--repo-path PATH`
- `search QUERY` -> a safe name-pattern query suitable for ordinary discovery
- `trace SYMBOL` -> `--function SYMBOL`
- `snippet SYMBOL` -> `--qualified-name SYMBOL`
- `coverage PATH` -> `--path PATH`

Explicit flags remain available and authoritative. Positional sugar must not create a second schema.

## 5. Phase plan through Vertical Slice 1

## Phase 0 — Baseline and migration guardrails

### Purpose

Record what is being changed and prevent accidental engine/security simplification.

### Work

1. Preserve the archive untouched as the source baseline.
2. Add this implementation plan and decision ledger.
3. Inventory every path that can currently start MCP:
   - default process role;
   - daemon MCP frontend;
   - installer-created MCP configs;
   - generated client adapters;
   - docs/help/examples.
4. Record mechanisms that must survive:
   - project mutation locks;
   - version-cohort admission;
   - secure runtime/cache validation;
   - indexing worker supervision;
   - cancellation/maintenance observation;
   - allowed-root human authorization;
   - hook fail-open behavior;
   - existing graph/index format compatibility.
5. Do not change the graph parser, store, Cypher engine, extraction pipeline, or index format.

### Exit

The plan and guardrails exist in-tree and all first-slice product entry points are identified.

## Phase 1 — Make process startup CLI-first

### Purpose

Remove the externally reachable/default MCP server behavior before adding convenience commands.

### Work

1. Change process-role classification so an invocation with no product command is not an MCP client.
2. Make no-argument invocation print top-level CLI help and exit successfully.
3. Ensure unknown top-level commands fail with an actionable CLI error instead of falling into MCP startup.
4. Keep hidden daemon/worker roles only for implementation mechanisms still required by the first slice.
5. Keep `daemon` as an internal/compatibility lifecycle mechanism only if existing named CLI execution still needs it; do not advertise it as a primary product workflow.
6. Update help so MCP server startup, `--tool-profile`, prompts, and MCP-client language are not presented as product features.

### Exit

There is no supported argv shape where “normal usage” enters the MCP JSON-RPC frontend.

## Phase 2 — Add the canonical named CLI surface

### Purpose

Make the product useful without requiring agents/humans to know MCP tool identifiers or JSON argument objects.

### Work

1. Add a small command descriptor table containing:
   - canonical command;
   - underlying compatibility operation ID;
   - short description;
   - positional argument mapping;
   - whether command mutates/indexes;
   - examples.
2. Route the seven first-slice commands to the existing one-shot execution path.
3. Reuse current schema-derived flag parsing/help to avoid a parallel parser.
4. Teach `run_cli` whether it is operating under:
   - legacy `cli <tool>` mode; or
   - canonical named-command mode.
5. Preserve `--json`, `--progress`, `--args-file`, and schema-driven flags.
6. Improve command-specific usage without changing underlying operation semantics.
7. Keep raw JSON accepted only on the legacy compatibility form.

### Exit

All seven canonical commands work through ordinary argv and expose useful `--help`.

## Phase 3 — Reframe skills around the CLI

### Purpose

Make agents use Codebase Memory as a shell capability instead of an MCP capability.

### Work

1. Update the canonical generated `SKILL.md` content:
   - replace MCP/tool-call wording with shell command examples;
   - preserve Scout / Verify / Auditor tiers;
   - preserve graph-first discovery;
   - preserve exact source verification;
   - preserve trace direction discipline;
   - preserve pagination discipline;
   - preserve coverage verification and direct-source fallback.
2. Rewrite tiered agent/subagent profile text to reference named commands.
3. Keep the skill focused on *when and how* to use Codebase Memory; do not turn it into a CLI manual.
4. Ensure examples use `--json` when machine parsing matters and ordinary text when an agent simply needs concise evidence.

### Exit

A newly generated skill contains no instruction telling the agent to call MCP tools or open an MCP session.

## Phase 4 — Preserve useful hooks, remove MCP assumptions from their user contract

### Purpose

Keep the agent ergonomics that add value without treating hooks as an MCP transport.

### Work

1. Preserve search augmentation and post-read coverage warnings.
2. Preserve fail-open and time-budget behavior.
3. Update lifecycle/reminder text to point at canonical CLI commands.
4. Remove notices that tell users to “open an MCP session”.
5. Do **not** broadly rewrite `hook_augment.c` in this slice: it currently calls the private compatibility dispatcher directly, and extracting it is part of the next internal-kernel slice.
6. Ensure hook installation can stand on its own as a CLI/skill integration.

### Exit

Hooks remain useful and no longer teach or require an externally reachable MCP product surface.

## Phase 5 — Stop creating new MCP registrations

### Purpose

Prevent a CLI-first installation from re-enabling the old product contract in coding-agent configuration.

### Work

1. Change default agent configuration so it installs only CLI-oriented assets that are safe/useful:
   - skills/instructions;
   - supported hooks;
   - tiered agent profiles where they add value.
2. Do not write new `mcpServers`, `mcp_servers`, or equivalent registrations.
3. Preserve MCP removal code temporarily so uninstall/migration can clean entries owned by older Codebase Memory installations.
4. Generated adapters that exist solely to create MCP-like tool registrations are not installed by default in this slice.
5. Installation plan output should describe skill/hook/CLI integration rather than MCP registration.

### Exit

A clean first-slice install creates no new MCP server configuration.

## Phase 6 — First-slice docs and executable acceptance

### Purpose

Close the loop and prove the new public contract.

### Work

1. Update README quick start to CLI-first.
2. Mark MCP as disabled/legacy migration internals where references must temporarily remain.
3. Add/adjust focused CLI acceptance checks for:
   - no-arg help;
   - unknown command error;
   - command help;
   - canonical-to-operation mapping;
   - `--json` stdout cleanliness;
   - no default MCP startup;
   - installed skill wording;
   - no new MCP registration in a clean install plan/configuration path.
4. Run the normal build plus a narrow high-value E2E path only after implementation is assembled.

### Vertical Slice 1 user story

From a repository:

```text
codebase-memory-cli index .
codebase-memory-cli status
codebase-memory-cli search ClaimValidator
codebase-memory-cli snippet ClaimValidator.validate
codebase-memory-cli trace ClaimValidator.validate --direction both
codebase-memory-cli coverage src/Claims/ClaimValidator.cs
```

An agent can learn the same flow from the installed skill, and useful hooks can augment searches/reads without asking the user to start MCP.

## 6. Critical review before implementation

The following review changes the earlier, more ambitious proposal.

### 6.1 Risk: confusing “MCP disabled” with “MCP code deleted”

The first proposal made “no migrated command calls `cbm_mcp_handle_tool()`” an exit criterion for the first vertical slice. In this source tree that turns the first slice into a broad internal rewrite because search, trace, snippet, coverage, hooks, schema/help, worker supervision, and daemon request state all share MCP-named structures.

**Decision:** Vertical Slice 1 disables the MCP *product surface* and establishes the canonical CLI. Physical extraction of the private dispatcher is Vertical Slice 2. The temporary dependency is documented, fenced, and not presented as a finished architecture.

### 6.2 Risk: deleting daemon/coordination too early

The archive's initial CLI-only note proposes removing the daemon. Current source shows the daemon family also owns exact-build coordination, index-worker budgeting/coalescing, watcher lifecycle, and safe mutation behavior.

**Decision:** Do not delete daemon/coordination in Slice 1. Remove its role as the public MCP product mechanism first. Later distinguish protocol-neutral runtime coordination from optional long-lived services.

### 6.3 Risk: hooks become slower if they shell out recursively

Rewriting `hook-augment` to spawn the CLI for every graph query would add process startup, identity hashing, coordination, and potentially recursion to a latency-sensitive fail-open path.

**Decision:** Keep in-process private dispatch inside hooks for this slice. Change the hook's *contract and guidance* now; extract its execution dependency only after a protocol-neutral operation API exists.

### 6.4 Risk: duplicate CLI schemas

A hand-built getopt implementation for seven commands would immediately drift from the existing registry and tool schemas.

**Decision:** Named commands remain thin aliases over the existing schema parser during Slice 1. The operation registry replaces MCP metadata in Slice 2/3.

### 6.5 Risk: search positional sugar changes semantics

`search_graph` accepts many filters and a regex-like `name_pattern`; blindly inserting raw user input into that field can change regex semantics or produce unexpected results.

**Decision:** positional `search QUERY` must escape literal input into the existing pattern contract, while advanced callers may still provide explicit flags. If safe escaping cannot be implemented without changing semantics, require `--name-pattern` in the first checkpoint and add literal positional search in a subsequent checkpoint.

### 6.6 Risk: installer cleanup destroys user-owned configuration

Stopping new MCP installs is safe; deleting all old MCP config on update is not. Config files may contain user changes or foreign server entries.

**Decision:** Slice 1 stops adding registrations. Existing ownership-aware removal functions stay for uninstall/migration and continue preserving modified/foreign entries.

### 6.7 Risk: renaming the binary multiplies the migration

Renaming `codebase-memory-mcp` affects installer targets, updater, hooks, skills, scripts, release metadata, process identity, and potentially external package managers.

**Decision:** do not rename the executable in Slice 1. Functional CLI-first behavior lands first. Rename is a later release phase.

### 6.8 Risk: claiming protocol neutrality too early

A new `operations/` wrapper that simply calls `cbm_mcp_handle_tool()` would make dependency diagrams look cleaner without changing the dependency.

**Decision:** do not add a fake operation layer in Slice 1. Keep the temporary compatibility call explicit so it is easy to find and remove. The real operation layer begins only when handlers/results stop depending on MCP types/envelopes.

## 7. Implementation checkpoints

Work is checkpointed cumulatively from the authoritative archive. Each overlay must be:

- self-applying;
- deletion-aware;
- repository-context validated;
- path-safe (no absolute paths or `..` deletion entries);
- cumulative so the latest checkpoint can be applied directly to the authoritative archive.

Expected checkpoints:

1. **CP1 — CLI startup + named command skeleton**
   - no default MCP startup;
   - named command dispatch table;
   - top-level help revised.
2. **CP2 — complete first-slice named command ergonomics**
   - positional mappings where safe;
   - machine/human output behavior retained;
   - command-specific help/examples.
3. **CP3 — skills and hook guidance**
   - generated skill/profile text rewritten;
   - lifecycle notices rewritten;
   - useful hook behavior retained.
4. **CP4 — install surface disables new MCP registration**
   - default config path installs CLI-oriented integrations only;
   - ownership-aware legacy removal preserved.
5. **CP5 — docs + focused acceptance fixes**
   - README/docs aligned;
   - build and high-value E2E acceptance completed;
   - final first-slice overlay.

Checkpoint numbering may compress or expand based on what the source reveals, but no implementation interval should exceed the requested checkpoint cadence without emitting an overlay first.

### Checkpoint status

- CP1 complete: CLI startup + named command skeleton.
- CP2 complete: canonical help + MCP-envelope-free canonical JSON output.
- CP3 complete: CLI-first skill, agent guidance, and lifecycle hook messaging.
- CP4 complete: new install paths are gated against MCP config/profile/adapter creation while ownership-aware legacy cleanup remains compiled.
- CP5 partially complete: primary docs/setup surfaces are CLI-first and executable acceptance exposed a canonical read-path hang.
- CP6 complete: local-read/product-rename boundary established.
- CP7 complete: canonical stdin deadlock resolved and canonical read loop verified.
- CP8 complete: product/distribution rename and Windows build contract migrated.
- CP9 complete: active release containers are CLI-only; MCPB production/publication removed.
- CP10 complete: Windows source-build path is native MSYS2 Clang and produces `codebase-memory-cli.exe`.
- CP11 complete: final product-surface audit and focused compile/link/install checks passed.
- CP12 complete: fresh cold-repository E2E and generated Claude/Gemini/Aider integration fixtures passed; Vertical Slice 1 is closed.

## 8. Deferred immediately after Vertical Slice 1

The next vertical slice begins the architectural extraction:

```text
CLI / hooks / runtime
        |
        v
protocol-neutral operation registry + context + result
        |
        v
engine
```

Its first targets should be `list_projects`, `index_status`, `check_index_coverage`, then `search_graph`, because those are heavily used by both the CLI and hooks and create a real seam quickly.

Only after that seam exists should we:

- move index supervision out of `src/mcp`;
- convert daemon `REQUEST_MCP` / `REQUEST_TOOL` into `REQUEST_OPERATION`;
- delete `daemon/frontend.*`;
- remove JSON-RPC/MCP schemas/prompts/tool profiles;
- delete `src/mcp`;
- retire legacy `cli <tool>`;
- finish distribution/package migration after the product binary has been renamed in Vertical Slice 1.

## 9. Definition of done for Vertical Slice 1

The slice is complete when all are true:

1. No-argument invocation prints CLI help and does not start MCP.
2. Unknown top-level commands fail as CLI errors and do not fall into MCP.
3. `index`, `status`, `search`, `trace`, `snippet`, `coverage`, and `projects` are canonical named commands.
4. `--json` remains script/agent usable and stdout-clean.
5. Legacy `cli <tool>` still works only as a compatibility surface.
6. Generated skill text teaches the CLI and preserves Scout/Verify/Auditor evidence discipline.
7. Hook lifecycle/search/coverage guidance no longer tells users to start or use MCP.
8. Clean installation paths touched by this slice do not create new MCP registrations.
9. Existing ownership-aware uninstall/migration code remains capable of removing old Codebase Memory MCP entries without deleting foreign/modified entries.
10. Build succeeds and the focused end-to-end CLI story succeeds.
11. No graph/index schema rewrite is required and existing indexes remain readable.
12. The remaining internal `cbm_mcp_handle_tool()` dependency is explicitly documented as temporary private compatibility debt for the next slice, not hidden behind a fake abstraction.
13. The canonical executable/build artifact is named `codebase-memory-cli` (`codebase-memory-cli.exe` on Windows).
14. Windows 10/11 x86-64 remains a supported native executable target; Windows build/release configuration must produce the renamed `.exe`, not depend on WSL.

## 10. Caveats added during implementation — 2026-09-01

Two requirements were promoted into Vertical Slice 1 acceptance criteria during implementation:

1. **Product rename now:** the user-facing application and executable are `codebase-memory-cli`. Internal `cbm_*` C symbols, `CBM_*` environment variables, cache/database locations, and persisted index formats stay unchanged in this slice so the rename does not become a storage migration.
2. **Windows is a first-class build target:** the application must compile as a native Windows 10/11 executable. The existing MinGW/Clang compatibility code, Windows UTF-8 argv handling, process/lock implementation, and Windows CI architecture are preserved. Renamed Windows release targets must emit `codebase-memory-cli.exe`.

Implementation consequence: the current read-path hang is being fixed without POSIX-only shortcuts. Canonical one-shot reads use the existing cross-platform coordination/runtime primitives; indexing retains the supervised worker path. The Windows requirement will be checked at both source/build-configuration level and, where the local environment lacks a Windows cross-compiler, through the repository's Windows CI build recipe rather than being treated as a packaging afterthought.

### CP6 scope in progress

- defer creation of the legacy in-memory MCP store for canonical one-shot CLI reads;
- infer the current repository project when canonical read commands omit `--project`, while preserving explicit project selectors;
- rename the runtime/help identity and primary build target to `codebase-memory-cli`;
- validate the local-read hang fix before extending the rename across installer/release/Windows packaging surfaces.

## 11. CP7 acceptance finding — canonical stdin deadlock resolved

Executable acceptance identified the apparent read-path "daemon hang" as an implicit-stdin deadlock in the shared CLI parser. In non-interactive environments, stdin can be an open pipe with no writer/EOF. Canonical commands whose legacy tool schema had optional properties (`projects`, `status`, and similar forms) therefore entered `cli_slurp_stream()` and waited indefinitely before dispatch.

Resolution:

- canonical named commands do **not** read stdin implicitly;
- canonical machine callers use flags or `--args-file`;
- legacy `cli <tool>` retains the historical piped-JSON channel during migration;
- canonical one-shot reads use deferred store initialization and infer the current-repository project when no explicit project selector is supplied.

Verified with the renamed acceptance executable against an isolated indexed C repository:

```text
codebase-memory-cli projects --json                       PASS
codebase-memory-cli status --json                         PASS
codebase-memory-cli search helper --json                  PASS
codebase-memory-cli snippet helper --json                 PASS
codebase-memory-cli trace helper --direction both --json  PASS
codebase-memory-cli coverage main.c --json                PASS
```

The next checkpoint carries the product rename through install/setup/release artifacts and the native Windows build configuration.

## 12. CP8 checkpoint — product/distribution rename and Windows build contract

The first distribution pass now treats `codebase-memory-cli` as the product artifact while retaining the current repository URL and legacy MCP/storage identifiers where they are needed for compatibility or cleanup.

Changed in this checkpoint:

- the primary Makefile target remains `build/c/codebase-memory-cli`;
- Unix and Windows setup/install scripts install `codebase-memory-cli` / `codebase-memory-cli.exe` and request release archives named `codebase-memory-cli-<platform>...`;
- Nix installs and exposes `codebase-memory-cli` as the main program;
- Windows x86-64 (`CLANG64` on `windows-latest`) and Windows ARM64 (`CLANGARM64` on `windows-11-arm`) build jobs now stage `build/c/codebase-memory-cli.exe`;
- release-candidate selection/staging expects the renamed executable;
- Windows product-surface tests point at `build/c/codebase-memory-cli.exe`;
- the legacy monolithic skill cleanup path remains `codebase-memory-mcp` intentionally so upgrades can remove historical owned state.

Compatibility boundaries deliberately *not* renamed here:

- GitHub repository coordinates such as `DeusData/codebase-memory-mcp`;
- `CBM_*` environment variables and internal `cbm_*` symbols;
- existing cache/database locations such as `~/.cache/codebase-memory-mcp`;
- MCP-era configuration keys/ownership markers needed to recognize and remove legacy registrations.

Validation at this checkpoint:

- modified shell installer/build scripts pass `bash -n`;
- the Makefile retains native `_WIN32` detection and Windows system-link dependencies (`ws2_32`, `psapi`, `shell32`, `advapi32`, `bcrypt`) with a static Windows link path;
- the CI build matrix continues to use native MSYS2 Clang toolchains for Windows x86-64 and ARM64, now targeting the renamed executable.

Remaining rename work before Vertical Slice 1 closes: release-container/archive naming and smoke/soak workflows still contain MCP-era artifact names, and MCPB release production must be explicitly disabled rather than silently carried forward into a CLI-only release.

## 13. CP9 checkpoint — CLI-only release containers

The active release path no longer produces or publishes MCP bundles. Canonical release containers are now eight CLI archives named `codebase-memory-cli-<target>.tar.gz` or `.zip`, each containing exactly the renamed executable plus LICENSE, the platform installer, and third-party notices.

Changes include:

- `scripts/package-release.sh` emits only CLI archives and has no MCPB builder;
- the release workflow no longer includes `*.mcpb` in checksums, signing, upload, download, or verification;
- the MCP Registry publication job is removed from the active release workflow;
- release verification expects eight archives/eight selected executables/24 non-executable runtime members rather than the former archive+MCPB matrix;
- canonical smoke/soak/PR/test workflows now reference `build/c/codebase-memory-cli[.exe]` and the new archive names;
- the all-tests driver no longer invokes the obsolete MCPB packaging/registry contract tests.

Focused packaging acceptance used the linked CLI acceptance executable to build a Linux amd64 release archive. The archive inventory was exactly:

```text
codebase-memory-cli
LICENSE
install.sh
THIRD_PARTY_NOTICES.md
```

The executable SHA-256 inside the archive matched the selected input byte-for-byte, and no `.mcpb` file was produced.

Legacy MCP cleanup identifiers and historical test/helper files may still exist in-tree during the migration; they are no longer part of the active install or release product surface. Deleting those remaining implementation artifacts belongs to the protocol-extraction/deletion phase rather than this first vertical slice.

## 14. CP10 checkpoint — native Windows source-build path

The Windows source installer previously violated the Windows caveat: `setup-windows.ps1 -FromSource` built a Linux executable inside WSL. That path has been removed.

A new `scripts/build-windows.ps1` provides an explicit native build entry point:

- Windows x64 selects MSYS2 `CLANG64`;
- Windows ARM64 selects MSYS2 `CLANGARM64`;
- the script requires the matching native Clang and zlib libraries plus GNU make;
- it invokes `Makefile.cbm` with `SANITIZE=` and the native Windows compiler;
- it requires and executes `build\\c\\codebase-memory-cli.exe --version` before reporting success.

`setup-windows.ps1 -FromSource` now clones/updates the source on the Windows filesystem, invokes this native build script, and installs the resulting `.exe`. It no longer presents WSL as a Windows source-build implementation.

The current Linux execution environment does not contain a Windows SDK/MinGW sysroot, and network package bootstrap was unavailable within the bounded checkpoint interval, so this checkpoint does not falsely claim an on-box Windows link. Native proof remains represented by the repository's `windows-latest` CLANG64 and `windows-11-arm` CLANGARM64 jobs; the next native Windows runner must execute the renamed build and Windows product guards. The important product correction is complete: the supported source-build path itself now produces a Windows PE executable rather than a WSL ELF binary.

## 15. CP11 checkpoint — final product-surface audit

A fresh product-surface audit found and corrected two remaining usability leaks:

- the active Aider instructions now use canonical `codebase-memory-cli status/search/trace/snippet/coverage/index/projects` commands rather than the legacy generic `cli <tool>` form;
- current Gemini hook/session payloads now teach the same CLI-first workflow. Separate arrays containing previously released MCP hook command strings remain unchanged solely so upgrades/uninstalls can identify and remove historical owned hooks safely.

Additional acceptance in this checkpoint:

- all modified C translation units (`main`, CLI/profile/hook, daemon bootstrap/application, MCP compatibility dispatcher) compile under the production `-Wall -Wextra -Werror` gate;
- the acceptance executable links successfully with the cached production vendored objects and reports `codebase-memory-cli dev`;
- no-argument invocation displays CLI help;
- explicit `mcp`, `--mcp-profile`, and `--tool-profile` top-level probes fail as unknown CLI commands rather than starting an MCP server;
- the canonical `projects -> status -> search -> snippet -> trace -> coverage` loop still passes against the indexed fixture without an explicit `--project` and without MCP content envelopes;
- `install --dry-run` in an isolated HOME produces no MCP-registration language and performs no filesystem mutation;
- `install --help` is now a real human-facing help surface and explicitly states that new installs do not create MCP registrations;
- all edited GitHub workflow YAML parses successfully;
- active build/smoke/soak/workflow references no longer expect `build/c/codebase-memory-mcp[.exe]` or MCP-era canonical platform archive names.

At this point the remaining first-slice work is closeout: run a completely fresh cold-repository index loop with the latest linked executable, audit generated skill/hook output using detected-client fixtures rather than prose/source inspection alone, and reconcile the definition-of-done/remaining-debt sections. Native Windows execution remains delegated to the repository's native Windows runner because this Linux environment has no usable Windows sysroot; the source-build and CI paths now both target the renamed native `.exe`.

## 16. CP12 closeout — Vertical Slice 1 complete

The final closeout reran the first-slice story from a genuinely cold repository with a private isolated HOME/cache and the latest linked `codebase-memory-cli` executable. The complete loop passed without an explicit project selector:

```text
codebase-memory-cli index . --json                         PASS
codebase-memory-cli projects --json                        PASS
codebase-memory-cli status --json                          PASS
codebase-memory-cli search helper --json                   PASS
codebase-memory-cli snippet helper --json                  PASS
codebase-memory-cli trace helper --direction both --json   PASS
codebase-memory-cli coverage main.c --json                 PASS
```

The index was created from scratch in this run. `projects` resolved the new project, subsequent commands automatically bound the current repository, search returned the indexed `helper` function, snippet returned its exact source, trace returned the `main -> helper` caller relation, and coverage returned current metadata for `main.c`. Canonical JSON remained free of the MCP `content` envelope.

Generated integration output was then exercised rather than inferred from source text. In an isolated HOME with detected Claude Code, Gemini CLI, and Aider fixtures, `install -y --force --skip-binary --clients=claude,gemini,aider` produced:

- Claude CLI-first skill plus fail-open search/read/session/subagent hooks that invoke `codebase-memory-cli`;
- Gemini CLI-first `GEMINI.md` plus BeforeTool, AfterTool coverage, and SessionStart hooks using the renamed executable;
- Aider CLI-first `CONVENTIONS.md` plus loader configuration;
- no `mcpServers`, `tools/list`, `tools/call`, MCP profile, or tool-profile registration.

The historical `codebase-memory-mcp:start/end` comment markers remain in some owned instruction blocks intentionally. They are non-executable ownership markers used to recognize/remove prior Codebase Memory-managed content safely during upgrades and uninstall; renaming them in this slice would weaken cleanup compatibility.

### Definition-of-done reconciliation

All Vertical Slice 1 criteria are satisfied at the source/product-contract level:

1. no-argument startup is CLI help;
2. unknown/MCP-only top-level probes fail as CLI commands;
3. `index`, `status`, `search`, `trace`, `snippet`, `coverage`, and `projects` are canonical commands;
4. canonical machine output is stdout-clean JSON without MCP envelopes;
5. legacy generic dispatch remains private/compatibility debt rather than the advertised interface;
6. generated skills preserve Scout/Verify/Auditor evidence discipline while teaching CLI use;
7. generated hook guidance/invocation is CLI-first and fail-open;
8. clean installs create no MCP registrations;
9. ownership-aware legacy cleanup remains intact;
10. modified C units pass the production warning gate, the acceptance executable links, and the fresh E2E loop passes;
11. existing index/store compatibility is preserved;
12. `cbm_mcp_handle_tool()` remains explicitly documented temporary internal debt for Vertical Slice 2;
13. the canonical executable and release artifact are `codebase-memory-cli` / `codebase-memory-cli.exe`;
14. Windows 10/11 is represented by a native MSYS2 Clang build path and native Windows CI/release jobs rather than WSL.

The only proof not executable in this Linux environment is the final native PE compile/run itself because no Windows SDK/MinGW sysroot is installed. That is not being represented as locally verified. The repository's Windows build path, workflow targets, product guards, installer, and release staging all now require `codebase-memory-cli.exe`; the first native Windows runner is the authoritative execution proof.

### Remaining architectural debt — next slice

Vertical Slice 1 intentionally disables MCP as a product surface before deleting its internal compatibility kernel. The next slice should now extract protocol-neutral operations in this order:

1. `list_projects`;
2. `index_status`;
3. `check_index_coverage`;
4. `search_graph`;
5. `get_code_snippet`;
6. `trace_path`;
7. remaining operations, then indexing supervision.

The target remains:

```text
CLI / hooks / daemon
        |
        v
protocol-neutral operations
        |
        v
engine
```

Only after consumers use that seam should `src/mcp`, daemon MCP request kinds/frontends, JSON-RPC framing, MCP schemas/prompts/profiles, and legacy generic `cli <tool>` dispatch be deleted.


## Runtime Assurance / Concurrency Safety Track

Added during Vertical Slice 2 in response to the concurrency risks of a CLI-first process model. The authoritative detailed plan lives in `docs/RUNTIME_ASSURANCE_PLAN.md`.

Architectural decision: the daemon is retained as the authoritative coordinator for substantive writes. Canonical local execution is restricted by the neutral operation registry to operations marked `read_only=true`; mutating operations must route through daemon/project-lock coordination. SQLite WAL/transactions remain the final integrity layer, and BUSY/LOCKED outcomes are contention, never corruption evidence.

Planned product surfaces are `codebase-memory-cli doctor` (fast and deep integrity/coordination verification) and `codebase-memory-cli benchmark` plus a real-process concurrency eval runner. The release gate will include same-project concurrent index requests, reader-during-write, different-project parallel writes, worker/daemon crash injection, WAL-starvation observation, and explicit proof that forced BUSY/LOCKED conditions cannot trigger quarantine/rebuild. Windows 10/11 process/file-lock behavior is part of the required matrix.
