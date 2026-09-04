# Configuration Reference

This page documents the configuration and persisted runtime files used by `codebase-memory-cli`. Several filesystem paths retain the historical `codebase-memory-mcp` name for compatibility; those paths do not represent an MCP product interface.

## At a Glance

| Purpose | Path | Format | Notes |
|---|---|---|---|
| Global custom extension mapping | `$XDG_CONFIG_HOME/codebase-memory-mcp/config.json` | JSON | Falls back to `~/.config/codebase-memory-mcp/config.json` when `XDG_CONFIG_HOME` is unset. |
| Per-project custom extension mapping | `{repo_root}/.codebase-memory.json` | JSON | Overrides conflicting global `extra_extensions` entries. |
| CLI-managed runtime settings | `${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/_config.db` | SQLite | Written by `codebase-memory-cli config set/reset`. |
| UI settings | `${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/config.json` | JSON | Stores `ui_enabled` and `ui_port`. |
| Daemon operation log | `${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/logs/cbm-daemon.log` | Structured log | Durable daemon lifecycle, watcher/indexing, UI, resource, and error events. |
| Admission conflict log | `${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/logs/daemon-conflicts.ndjson` | NDJSON | Exact-build, ABI, and canonical-cache conflicts. |
| Activation log | `${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/logs/activation-events.ndjson` | NDJSON | Install/update/uninstall activation progress and outcomes. |

CBM resolves `CBM_CACHE_DIR` to a canonical per-account path before using any of these locations. The log directory and files are private to the account.

## 1. Custom File Extension Mapping

Two optional JSON files let you map additional file extensions to built-in languages.

### Global config

Default path:

```text
$XDG_CONFIG_HOME/codebase-memory-mcp/config.json
```

Fallback when `XDG_CONFIG_HOME` is unset:

```text
~/.config/codebase-memory-mcp/config.json
```

### Per-project config

Place this file in the repository root:

```text
.codebase-memory.json
```

### Format

```json
{
  "extra_extensions": {
    ".blade.php": "php",
    ".mjs": "javascript",
    ".twig": "html"
  }
}
```

Notes:

- Extension keys must include the leading dot.
- Language names are case-insensitive.
- Unknown language names are skipped.
- Missing files are ignored.
- If the same extension appears in both files, the per-project file wins.

## 2. CLI-Managed Runtime Settings

The `config` subcommand stores runtime settings in a small SQLite database:

```text
${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/_config.db
```

Inspect or change values with the CLI:

```bash
codebase-memory-cli config list
codebase-memory-cli config get auto_index
codebase-memory-cli config set auto_index true
codebase-memory-cli config set auto_index_limit 50000
codebase-memory-cli config set watcher_enabled false
codebase-memory-cli config reset auto_index
```

Current keys:

| Key | Default | Meaning |
|---|---|---|
| `auto_index` | `false` | Permit the retained warm runtime to auto-index newly encountered projects. Canonical CLI usage can always index explicitly with `codebase-memory-cli index`. |
| `auto_index_limit` | `50000` | Maximum file count allowed for automatic indexing of a new project. |
| `auto_watch` | `true` | Register an active project with the retained background watcher when a warm runtime/hook integration supplies project context. The ordinary one-shot CLI does not require this. |
| `watcher_enabled` | `true` | Master switch for the retained background watcher subsystem. Set `false` to stop its poll thread/project registration. Reindex manually with `codebase-memory-cli index` when disabled. |

> **`watcher_enabled` vs `auto_watch`.** `watcher_enabled` controls whether the
> retained watcher subsystem starts at all. `auto_watch` is narrower: it controls
> whether an active project is registered with an already-running watcher.
> One-shot canonical CLI commands do not require a watcher or warm runtime.
>
> `watcher_enabled` is read when the retained runtime starts. If you change it
> while that runtime is active, retire it so the next runtime observes the new
> value. Disabling the watcher does not disable explicit `index`, `search`,
> `trace`, `snippet`, or `coverage` commands.

## 3. UI Settings

The optional built-in graph UI stores its settings in:

```text
${CBM_CACHE_DIR:-~/.cache/codebase-memory-mcp}/config.json
```

Current format:

```json
{
  "ui_enabled": false,
  "ui_port": 9749
}
```

Notes:

- If a UI-enabled binary finds its verified external asset pack and no UI config file exists yet, the UI auto-enables on first run. Missing or invalid assets keep the optional UI disabled; ordinary CLI commands remain available.
- `CBM_CACHE_DIR` changes both the UI config location and the runtime settings database location.
- CBM resolves `CBM_CACHE_DIR` to one canonical per-account cache root. A process configured with a different root fails while any CBM session or command is active; close them before switching roots.

## 4. Environment Variables

These environment variables affect runtime behavior:

| Variable | Default | Description |
|---|---|---|
| `CBM_ALLOWED_ROOT` | *(unset)* | Confine `index_repository` to paths within this directory. When set, a `repo_path` that resolves (after symlink / `..` resolution) outside this root is refused, and the same check now applies to the graph UI's `POST /api/index` route rather than only to one request path. Unset imposes no *containment* restriction — but see the always-on limits below, which apply whether or not this is set. Useful when agentic callers operate in a repository and indexing scope must remain human-controlled. |
| `CBM_CACHE_DIR` | `~/.cache/codebase-memory-mcp` | Override the cache directory used for indexes, `_config.db`, and UI `config.json`. |
| `CBM_DIAGNOSTICS` | `false` | Enable periodic `snapshot.json` and retained `trajectory.ndjson` below a fresh owner-private directory in the system temp directory. The daemon records the randomized paths in the `diagnostics.start` discovery record (a single JSON line) in `${CBM_CACHE_DIR}/logs/cbm-daemon.log`; that one record is emitted even when `CBM_LOG_LEVEL` suppresses ordinary logging, so the paths always remain discoverable. |
| `CBM_DOWNLOAD_URL` | GitHub releases | Override the update download URL. |
| `CBM_LOG_LEVEL` | `info` | Set the log level to `debug`, `info`, `warn`, `error`, or `none` (or `0`-`4`). One-shot CLI messages use that command's stderr; retained detached-runtime events use `${CBM_CACHE_DIR}/logs/cbm-daemon.log`. |
| `CBM_RUNTIME_DIR` | `%LOCALAPPDATA%` (Windows), `/private/tmp` (macOS), `/tmp` (other) | Parent directory for the daemon/CLI rendezvous directory, which CBM creates inside it as `cbm-daemon-<uid>` (`cbm-daemon-<key>` on Windows). Set it when the default ancestry cannot pass the private-directory check — see below. `CBM_CACHE_DIR` does **not** move the rendezvous. |
| `CBM_WORKERS` | auto-detected | Override the indexing worker count. |

### Relocating the daemon rendezvous directory

Before it is used, the rendezvous directory and every ancestor of it are checked:
each ancestor must be owned by you or by root, must not be world-writable (unless
it is the standard root-owned sticky directory such as `/tmp`), and must carry no
allow-ACL — on Windows, no ACE granting mutation rights to another identity. The
rendezvous directory itself is then forced to owner-only (`0700`, no extended ACL
/ an owner-only DACL).

That ancestry is not always acceptable in the default location. A Windows profile
that has acquired a capability-SID ACE with `WRITE_DAC` / `WRITE_OWNER` / `DELETE`
on `%LOCALAPPDATA%` — something an installed packaged app can add — fails the walk,
and so can an unusual `/tmp` or home directory on POSIX. When that happens *every*
command fails, `config list` included, so the settings surface cannot be reached
either:

```text
codebase-memory-cli: secure daemon endpoint could not be created
```

`CBM_RUNTIME_DIR` points the rendezvous at an ancestry you choose:

```bash
export CBM_RUNTIME_DIR="$HOME/cbm-runtime"   # any directory you own
```

```powershell
$env:CBM_RUNTIME_DIR = "D:\cbm-runtime"
```

The check is not relaxed for the directory you name: it goes through exactly the
same validation as the default, and a value that fails it is refused rather than
silently ignored. Because the rendezvous is how sessions find each other, every
process that should share one daemon must see the same value — set it consistently for any shells or agent processes that should share the same retained runtime coordination.

Environment used by retained runtime-owned components—such as diagnostics, logging, and process-wide indexing resource limits—is captured when that runtime starts. Later participants cannot replace those values. `CBM_ALLOWED_ROOT` remains caller-specific, a conflicting `CBM_CACHE_DIR` is rejected, and one-shot CLI commands use their own current environment.


### Roots that are always refused

Independently of `CBM_ALLOWED_ROOT`, some directories are refused as an indexing
root because they are too broad or too sensitive to index as a unit:

- a filesystem root, a Windows drive root, or a UNC share root;
- a top-level system tree — `/etc`, `/var`, `/usr`, `/home`, `/Users`, and on
  Windows `C:\Windows`, `C:\Users`, `C:\ProgramData`, `C:\Program Files`;
- your home directory itself (directories *below* it are fine);
- a credential directory at any depth — `.ssh`, `.aws`, `.gnupg`, `.kube`,
  `.docker`, `.netrc`, `.git-credentials`, `.password-store`, macOS `Keychains`.

Two limits are worth stating plainly. This constrains *scope*, not
*sensitivity*: inside a root that is allowed, every file the process can read may
be indexed and later returned. And the credential list is a denylist, so it
raises the cost of a mistake rather than closing the class — a directory it does
not name is permitted.

## 5. Agent and Editor Integration Files

The CLI-first `install` path can write Codebase Memory skills, durable CLI instructions, and compatible lifecycle/context hooks for detected agents. It does **not** create new MCP registrations or MCP-bound tier profiles/extensions. Legacy ownership-aware editors/removers remain present so update/uninstall can safely remove older Codebase Memory-owned MCP entries without deleting foreign or modified configuration.

Those target paths vary by tool and platform, so the easiest way to inspect the exact files for your machine is:

```bash
codebase-memory-cli install --dry-run
```

That prints the specific config files the installer would modify without writing anything.
