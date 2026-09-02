# Consolidated Source Reconciliation — 2026-09-01

This note records the pre-edit reconciliation of the authoritative consolidated archive against the CLI-first engineering handoff.

## Confirmed

- The neutral operation registry contains the six expected reads: `projects`, `status`, `coverage`, `search`, `snippet`, and `trace`.
- Canonical CLI reads execute through `cbm_operation_execute()` locally rather than through MCP.
- Hook augmentation calls neutral operations directly.
- The daemon's legacy tool request path checks the operation registry before falling back to the MCP dispatcher.
- MCP's six migrated read entry points are compatibility adapters to neutral operations; the former authoritative handler bodies are absent.
- The product/build/install surfaces use `codebase-memory-cli`; Windows workflows explicitly validate `codebase-memory-cli.exe`.
- Native Windows x64 uses MSYS2/CLANG64; an experimental Windows ARM64 path is also present.
- `docs/RUNTIME_ASSURANCE_PLAN.md` already records the daemon-authoritative mutation invariant, BUSY/LOCKED-not-corruption rule, doctor/benchmark direction, stable event names, and real-process concurrency scenarios.

## Discrepancies / remaining migration debt

- The daemon wire vocabulary still exposed `REQUEST_MCP` and `REQUEST_TOOL`; neutral operations were reached only as a branch inside `REQUEST_TOOL` and their daemon response was re-wrapped as an MCP text result.
- `main_local_cli_daemon_execute()` is still a legacy-tool path for commands whose behavior has not yet been extracted. This is legitimate transition debt and must not become the neutral operation API.
- Daemon session/context and bootstrap naming still contains MCP concepts (`cbm_mcp_server_t`, MCP tool profiles, MCP client role). These are broader extraction tasks; removing them in the operation-wire checkpoint would mix protocol cleanup with behavior migration.
- Index worker execution still intentionally uses the historical MCP dispatcher until `index_repository` is extracted.

## Checkpoint decision

Introduce a distinct `REQUEST_OPERATION` wire request with canonical operation payloads and no MCP envelope. Keep `REQUEST_MCP` and a separately numbered transitional `REQUEST_TOOL` only for unextracted compatibility behavior. New/extracted daemon callers must use the operation request; legacy tool fallback must not be reachable from it.

The runtime-assurance event vocabulary is also promoted into source as stable identifiers so subsequent coordinator/store instrumentation does not invent incompatible names ad hoc.
