# CLI-Only Migration Plan

## Goal

Turn `codebase-memory-mcp` into a local command-line application and stop
exposing or starting an MCP server.

## Keep

- The indexing engine and knowledge-graph storage.
- Existing graph operations and their schemas.
- The current one-shot `cli` execution path as the foundation.
- JSON output for scripting, with normal CLI exit codes.
- Local configuration, locking, and temporary indexing workers where they are
  still needed by commands.

## Remove or disable

- Default stdin/stdout MCP JSON-RPC server mode.
- Long-lived daemon lifecycle and daemon IPC.
- HTTP graph UI and its embedded assets.
- MCP transport-specific code and server startup paths.
- MCP configuration generation and cleanup for coding-agent clients.
- MCP-only hooks, profiles, and documentation.

The internal `cbm_mcp_server` implementation may be retained temporarily if it
is the existing dispatcher used by CLI commands. It must not remain an
externally reachable MCP server; rename it later only if that improves clarity
without duplicating the dispatch layer.

## Proposed command shape

```text
codebase-memory-cli index --repo-path .
codebase-memory-cli search-graph --name-pattern '.*Handler.*'
codebase-memory-cli projects
codebase-memory-cli trace --function MyFunction
```

Keep a compatibility `cli <tool> ...` form during migration if existing
scripts depend on it. Deprecate it only after equivalent named commands and
documentation exist.

## Implementation order

1. Inventory the current `main.c` dispatch, CLI handlers, tool registry,
   daemon, UI, installer, and tests.
2. Make CLI dispatch the only normal entry point; reject MCP/JSON-RPC startup
   with a clear error during the transition.
3. Move or reuse tool dispatch behind ordinary CLI commands and define stable
   stdout, stderr, and exit-code behavior.
4. Remove daemon, UI, and MCP transport build targets and dead includes once
   no CLI path depends on them.
5. Remove MCP client configuration and hook installation from the installer.
6. Update README, configuration docs, packaging metadata, examples, and help
   text to describe the CLI product.
7. Replace MCP handshake/server smoke tests with CLI command, JSON output,
   mutation-lock, indexing, and end-to-end executable tests.
8. Run the normal build, test, packaging, and artifact-integrity gates.

## Acceptance criteria

- Running the binary without CLI arguments never starts a server or daemon.
- No process listens on a network port after any supported CLI command.
- No MCP configuration is installed or modified.
- Core indexing and graph queries work from a clean installation.
- Commands return nonzero status for invalid input and operational failures.
- `--json` output is machine-readable and stdout contains no progress noise.
- Existing user data and graph indexes remain readable, or a documented
  migration is provided.
- Source and release artifacts contain no enabled MCP server entry point.

## Scope boundary

Do not rewrite the graph engine or add a new command framework. The first
version should reuse the existing C CLI parser and tool registry. A separate
daemon or web UI can be restored later only as an explicitly supported product
surface.
