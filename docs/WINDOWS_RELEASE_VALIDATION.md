# Native Windows Release Validation

Status: **AUTHORITATIVE EXTERNAL QUALIFICATION PROCEDURE**

Authoritative host: `luigi.home.arpa` (Windows 11 x64).

This procedure validates the exact GitHub release-candidate bytes. It does not build the executable locally.

## Non-negotiable rules

- Download the candidate from the GitHub release/RC that is being qualified.
- Verify archive and executable SHA-256 before testing.
- Never replace the candidate executable with a locally compiled binary.
- Use a new qualification evidence directory for every RC attempt.
- Do not modify test scripts during a qualification run. A script defect invalidates the affected step; fix it in source, publish a new RC if required, then rerun.
- Preserve raw stdout/stderr and exit codes.
- Use isolated `CBM_CACHE_DIR` locations unless a test explicitly validates upgrade/persisted-state behavior.
- Record every environment override.

## Prerequisites

Record before testing:

- Windows edition/build;
- CPU model/logical processors;
- physical memory;
- filesystem and free space for repositories/cache/results;
- PowerShell version;
- current power plan;
- antivirus/Defender state if it cannot be held constant;
- Git version;
- Python version where Windows guard scripts require it.

Avoid Windows Update, repository clones, builds, virus scans, backups, or other heavy I/O during comparative tests.

## Stage 1 — Artifact identity

Create a new qualification root, for example:

```powershell
$Q = "C:\cbm-qualification\<release>-<timestamp>"
New-Item -ItemType Directory -Force $Q | Out-Null
```

Store:

- release URL/tag metadata;
- downloaded archive;
- published checksum material;
- locally computed archive SHA-256;
- extracted executable SHA-256;
- `codebase-memory-cli.exe --version`;
- source commit from release metadata.

Fail qualification immediately if hashes or release identity do not match.

## Stage 2 — Portable artifact smoke

Use the extracted executable directly.

Required checks:

- `--version`;
- `--help`;
- `doctor --json` in an isolated cache;
- permanent daemon start/status/stop;
- clean restart after daemon termination;
- failure exit codes are nonzero for invalid operations;
- JSON output parses successfully for supported `--json` commands.

## Stage 3 — Canonical repository workflow

Against a frozen benchmark/validation repository:

1. create fresh isolated cache;
2. index repository;
3. verify project/status visibility;
4. run narrow search;
5. retrieve snippet/source;
6. retrieve file outline;
7. run architecture/query operation;
8. run change detection;
9. run `doctor` and capture health/reliability state.

Validate semantic results as well as exit status. An empty/partial result caused by indexing less data is not a pass.

## Stage 4 — Existing native-Windows guards

Run the reconciled Windows test surfaces against the exact extracted binary. Prefer passing the candidate explicitly rather than allowing any script to compile a binary.

Required retained coverage includes:

- non-ASCII repository path;
- non-ASCII CLI argv;
- non-ASCII cache/dump behavior;
- daemon lifecycle/stability;
- hook augmentation;
- UI drive listing;
- update handoff behavior.

Where `scripts/test-windows.ps1` supports a `-Binary` parameter, release qualification must supply it. The run is invalid if the script rebuilds the executable.

## Stage 5 — Crash/recovery and concurrency

Exercise and retain evidence for:

- daemon crash during/around active index followed by recovery;
- repeated daemon stop/start;
- parallel read clients against permanent daemon;
- concurrent index admission/coalescing where the Windows harness exposes it;
- mutation serialization/lock behavior where practical;
- cancellation behavior covered by available Windows/integration surfaces;
- `doctor` after recovery, including reliability events if generated.

Do not manufacture corruption of user data unless the test explicitly operates in an isolated qualification cache.

## Stage 6 — Installed-product validation

Qualify the supported Windows installation path separately from the portable binary.

Required scenarios:

- clean install;
- installed `codebase-memory-cli --version` identity matches candidate;
- installed command discovery/PATH behavior;
- basic index/search/doctor workflow;
- update handoff where supported;
- uninstall;
- cleanup of Codebase Memory-owned legacy MCP configuration without deleting foreign/modified entries;
- no clean-install creation of MCP server registrations or retired executable names.

Capture before/after filesystem/config evidence for install and uninstall tests.

## Stage 7 — UI/hooks where applicable

Validate supported UI and hook behavior without using retired `/rpc`, JSON-RPC, MCP stdio, or tool-call envelopes.

The qualification agent should treat any newly generated MCP registration as a release-blocking defect.

## Stage 8 — Final health snapshot

Capture:

- daemon status;
- `doctor --json`;
- human `doctor` output;
- project status;
- final process list relevant to Codebase Memory;
- cache directory inventory;
- Windows Event Log excerpts only if a crash/failure requires them.

## Result classification

Each check must be one of:

- `PASS` — expected product behavior verified;
- `FAIL_PRODUCT` — candidate behavior is incorrect;
- `FAIL_TEST` — qualification harness/expectation is stale or incorrect;
- `FAIL_ENVIRONMENT` — host/environment prevented a valid result;
- `SKIP_NOT_APPLICABLE` — explicitly documented feature does not apply;
- `KNOWN_ACCEPTED` — known issue with explicit release disposition.

Never convert a failure to pass by editing evidence or silently rerunning only the successful portion.

## Completion criteria

Native Windows qualification is complete only when:

- exact candidate identity is proven;
- portable and installed forms are tested;
- retained Windows guards run against supplied candidate bytes;
- recovery/lifecycle checks complete;
- evidence bundle is complete;
- all non-pass results have explicit disposition.
