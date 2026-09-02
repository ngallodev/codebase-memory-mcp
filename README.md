# Codebase Memory

Codebase Memory builds a persistent structural knowledge graph of a source repository and exposes it through a local command-line interface designed for coding agents and humans.

The current binary is still named `codebase-memory-mcp` for release compatibility. The product surface is CLI-first: running the binary with no command shows CLI help and does **not** start an MCP stdio server.

> Migration status: the first CLI-first vertical slice intentionally retains some MCP-named internal dispatcher/schema code as private compatibility debt. New usage, documentation, skills, hooks, and installation should use the CLI. See [`docs/CLI_FIRST_VERTICAL_SLICE_IMPLEMENTATION_PLAN.md`](docs/CLI_FIRST_VERTICAL_SLICE_IMPLEMENTATION_PLAN.md).

## What it does

Codebase Memory parses source with tree-sitter and optional language-server enrichment, then stores functions, classes, methods, call relationships, imports, routes, cross-service links, and other structural facts in a local graph-backed index.

Use it when you need to answer questions such as:

- Where is this concept implemented?
- Who calls this function?
- What does this function call?
- What code is likely affected by a change?
- What is the exact source for a graph result?
- Is the relevant code actually covered by the current index?
- What projects have already been indexed?

The graph is a discovery and structural-evidence layer, not a substitute for source verification. For material claims, especially negative or exhaustive claims, verify exact source and index coverage.

## Quick start

Build or install the binary, then from a repository:

```sh
codebase-memory-cli index .
codebase-memory-cli status
codebase-memory-cli search "ClaimValidator"
codebase-memory-cli snippet ClaimValidator.validate
codebase-memory-cli trace ClaimValidator.validate --direction both
codebase-memory-cli coverage src/Claims/ClaimValidator.cs
```

For scripts and coding agents, add `--json`:

```sh
codebase-memory-cli search "ClaimValidator" --json
codebase-memory-cli trace ClaimValidator.validate --direction inbound --json
```

Canonical machine output does not expose the historical MCP `content:[{type:"text"}]` envelope. Operational failures return a non-zero exit status.

## Core commands

| Command | Purpose |
|---|---|
| `index [PATH]` | Build or refresh an index. Defaults naturally to the current repository when possible. |
| `status` | Show index/runtime status for the current or selected project. |
| `search [QUERY]` | Find graph-backed symbols and structural candidates. |
| `trace SYMBOL` | Trace inbound, outbound, or bidirectional call relationships. |
| `snippet SYMBOL` | Retrieve exact source for a qualified symbol. |
| `coverage [PATH]` | Check whether graph evidence covers a file/scope before relying on absence or completeness. |
| `projects` | List indexed projects. |

Use command-specific help for the complete schema-derived flags:

```sh
codebase-memory-cli search --help
codebase-memory-cli trace --help
codebase-memory-cli coverage --help
```

When the working directory does not identify the intended index unambiguously, pass `--project NAME`.

### Compatibility interface

The historical generic form remains temporarily available as a parity/migration surface:

```sh
codebase-memory-cli cli search_graph --project my-project --name-pattern '.*Handler.*'
```

It is not the canonical interface and will be retired after the protocol-neutral operation layer replaces the remaining private dispatcher dependency.

## Agent workflow

The shipped Codebase Memory skill teaches a graph-first evidence loop rather than a tool-name inventory:

1. `projects` / `status` — establish project and freshness.
2. `search` — discover candidate symbols.
3. `snippet` — establish exact source truth.
4. `trace` — establish callers, callees, and likely impact.
5. `coverage` — establish where graph evidence is trustworthy.
6. Fall back to direct source reads/grep for literals, configs, non-code files, or every reported coverage gap.

The skill preserves three evidence levels:

- **Scout** — fast positive/provisional discovery; no negative or exhaustive claims.
- **Verify** — default task-directed evidence with source and coverage verification.
- **Auditor** — bounded exhaustive review with complete relevant pagination, coverage fallback, and explicit limitations.

### Skills and hooks

`install` may place CLI-first skills, durable instructions, and compatible lifecycle/context hooks for detected coding agents. New installs do **not** create MCP registrations or MCP-bound tier profiles/extensions.

Hooks are an optimization and guidance surface, not the only route to the graph. If a warm runtime is absent or a hook cannot augment context, it fails open and the ordinary CLI still works.

To configure detected agent integrations without replacing an externally managed binary:

```sh
codebase-memory-cli install --skip-binary
```

Use `install --plan` when you want to inspect planned writes before applying them.

## Installation

### From source

```sh
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
scripts/build.sh
./build/c/codebase-memory-cli --help
```

### Setup scripts

macOS/Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup.sh | bash
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup-windows.ps1 | iex
```

The setup scripts install the executable. They do not write MCP client configuration. Afterward, run `codebase-memory-cli install --skip-binary` if you want Codebase Memory to install its CLI-first skills/instructions/hooks for detected agents.

## Human and machine output

Human-readable output is the default. `--json` is the stable machine-oriented surface for the canonical commands in this slice.

Design rules:

- stdout is reserved for command results;
- progress and diagnostics belong on stderr;
- errors produce non-zero exit codes;
- canonical JSON hides the historical transport envelope;
- result ordering/pagination remain explicit where the underlying operation supports them.

## Project and trust boundaries

Codebase Memory can infer the current repository/project in common cases, but authorization is not inferred.

`allow-root` remains deliberately human-only. An agent/tool caller must not be able to expand its own indexing authorization boundary. `CBM_ALLOWED_ROOT` and the always-on sensitive/root-directory restrictions continue to constrain indexing scope.

See [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) for cache, runtime, extension mapping, and environment settings.

## Indexing and language support

The existing indexing engine is intentionally unchanged by the CLI-first migration. It includes:

- tree-sitter-based extraction across the project’s supported grammar set;
- hybrid LSP semantic enrichment for selected languages;
- persistent local indexes;
- call/import/definition and selected cross-service relationships;
- change detection, architecture, Cypher, ADR, trace-ingestion, and other operations still available through the temporary compatibility interface while canonical named commands are expanded.

The first vertical slice does **not** rewrite the graph schema, parser pipeline, store format, or existing indexes.

## Configuration

Common commands:

```sh
codebase-memory-cli config list
codebase-memory-cli config get auto_index
codebase-memory-cli config set auto_index true
codebase-memory-cli config set watcher_enabled false
codebase-memory-cli config reset auto_index
```

Important environment variables include:

| Variable | Purpose |
|---|---|
| `CBM_CACHE_DIR` | Override the local cache/index root. |
| `CBM_ALLOWED_ROOT` | Constrain permissible indexing roots. |
| `CBM_RUNTIME_DIR` | Override the secure local coordination rendezvous parent. |
| `CBM_WORKERS` | Override indexing worker count. |
| `CBM_LOG_LEVEL` | Control runtime logging. |

See [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) for the full reference.

## Architecture during the CLI-first migration

The intended direction is:

```text
human shell ─┐
agent skill ─┼──> CLI / hooks ──> protocol-neutral operations ──> graph/index engine
agent hooks ─┘
```

Vertical Slice 1 establishes the left-hand product surface first. Internally, some canonical commands and hooks still reach the existing dispatcher and schema registry in `src/mcp`; that dependency is explicit temporary debt, not the target architecture.

The next slice extracts the operation contract/result model so CLI, hooks, and optional runtime coordination call the engine without MCP types or result envelopes. Index supervision, project mutation locks, version-cohort checks, process containment, memory limits, cancellation, and secure local coordination are correctness mechanisms and are retained until they can be moved without weakening those guarantees.

## Build

The native production build is:

```sh
make -f Makefile.cbm cbm
```

or:

```sh
scripts/build.sh
```

The build remains C11-based and uses the existing vendored parser/storage dependencies.

## Security

Codebase Memory indexes source that the current account can read. Treat an allowed repository root as a real data-access boundary.

Key safeguards retained through the CLI migration include:

- human-controlled root enrollment;
- refusal of filesystem roots, broad system trees, home directories, and known credential directories as index roots;
- canonical cache/runtime paths and owner-private local coordination;
- exact-build/version-cohort admission;
- mutation locks around shared project state;
- supervised index workers with cancellation/resource controls;
- ownership-aware integration edits and removals.

See [`SECURITY.md`](SECURITY.md) and [`docs/SECURITY-DISCLOSURE.md`](docs/SECURITY-DISCLOSURE.md).

## Migration documents

- [`docs/CLI_FIRST_VERTICAL_SLICE_IMPLEMENTATION_PLAN.md`](docs/CLI_FIRST_VERTICAL_SLICE_IMPLEMENTATION_PLAN.md) — implementation-grade first-slice plan, critical review, checkpoints, and definition of done.
- [`docs/CLI_ONLY_MIGRATION_PLAN.md`](docs/CLI_ONLY_MIGRATION_PLAN.md) — original high-level CLI-only direction retained as historical input.

## License

MIT. See [`LICENSE`](LICENSE).
