# Runtime Assurance and Concurrency Verification Plan

Status: planned reliability track for the CLI-first migration.

## Purpose

Codebase Memory must remain safe under concurrent agent use after the public surface moves from MCP to `codebase-memory-cli`. CLI-first must not mean coordination-free. The product should be able to prove, after a build and during normal operation, that its databases are healthy, writer coordination is working, readers remain usable during writes, and performance has not regressed unexpectedly.

## Architectural invariants

1. **The daemon is the authoritative coordinator for substantive writes.** Indexing, re-indexing, project deletion, trace ingestion, ADR mutation, schema/data migration, repair, quarantine, and rebuild operations must execute through the daemon or an equivalently coordinated supervised worker owned by it.
2. **Local CLI execution is read-only.** A canonical operation may execute directly in the CLI process only when the neutral operation registry marks it `read_only=true`. Mutating operations must not gain a direct local path merely by being added to the registry.
3. **One active mutation lease per project.** Multiple projects may mutate concurrently, subject to the global worker/memory budget, but two writers must not mutate the same project database simultaneously.
4. **SQLite remains the final integrity boundary.** Project locks/daemon queues reduce contention; SQLite WAL, transactions, busy handling and atomic publication remain mandatory defenses rather than being bypassed.
5. **Contention is not corruption.** `SQLITE_BUSY`, `SQLITE_LOCKED`, lock timeout, cancellation, or an already-active project mutation must be recorded as contention/retry outcomes. They must never trigger quarantine or rebuild by themselves.
6. **Readers may coexist with writers.** Read-only query connections should use SQLite WAL snapshots and must not issue write pragmas. A reader should normally observe the last committed database while a new index is staged/published.
7. **Publication is atomic and verified.** New index state is built/staged, integrity checked, sealed, and atomically published; partially written state must never become the live project database.
8. **Every forced rebuild has a reason code.** Rebuild/quarantine decisions must be attributable to confirmed corruption/incompatibility, never to an undifferentiated open failure.

## Existing protections to preserve

The current source already contains substantial safety mechanisms:

- SQLite `journal_mode=WAL` for live write connections.
- `PRAGMA busy_timeout = 10000` / `sqlite3_busy_timeout(..., 10000)` on relevant connections.
- Query-only opens that deliberately skip journal-mode/checkpoint/synchronous pragmas so reads do not mutate the database.
- `BEGIN IMMEDIATE` for write transactions, taking the writer reservation before mutation work proceeds.
- PASSIVE WAL checkpoints during live use and WAL-starvation warnings when readers prevent reclamation.
- Exclusive/sealed publication behavior for completed staging databases.
- `cbm_store_check_integrity_verdict()` that explicitly classifies `SQLITE_BUSY` and `SQLITE_LOCKED` as transient rather than corrupt.
- Per-project process-safe mutation leases plus a global project-set lock.
- Daemon index-job coalescing/subscriber tracking, worker limits, memory budgets, cancellation, and supervised worker containment.
- Version-cohort/lifetime coordination and durable daemon conflict logging.
- Existing structured logging with `debug`, `info`, `warn`, `error`, and `none` levels plus text/JSON formats.
- Existing pipeline timing logs and diagnostics snapshots/query timing counters.

The reliability track should expose and verify these mechanisms rather than replace them casually.

## User-facing assurance commands

### `codebase-memory-cli doctor`

Fast, safe health report suitable for humans and agents.

Default mode should be read-only and bounded. Proposed checks:

- executable/build identity and platform;
- cache-root ownership/permissions;
- daemon reachability and build/cohort compatibility;
- project discovery and root mapping;
- database openability in read-only mode;
- SQLite journal mode and schema compatibility;
- shallow integrity verdict;
- WAL/shm presence and WAL size;
- current daemon job/subscriber counts;
- project mutation-lock availability without taking a destructive lease;
- recent write-contention/conflict counts;
- recent rebuild/quarantine reasons;
- last successful index metadata and coverage summary.

Output formats: human text and versioned JSON.

### `codebase-memory-cli doctor --deep`

Explicit expensive verification. Adds at minimum:

- `PRAGMA quick_check` for every selected project;
- optional `PRAGMA integrity_check` when requested with an additional full-integrity flag;
- WAL checkpoint health observation without destructive truncation of a live WAL;
- store generation/meta consistency;
- artifact/staging-file sanity;
- orphan/stale temporary-file inspection.

Deep checks must classify BUSY/LOCKED as `transient`/`contended`, not `corrupt`.

### `codebase-memory-cli benchmark`

Repeatable performance/evaluation runner. It should record machine/build metadata and emit versioned JSON so results can be compared across commits.

Primary metrics:

- cold and warm full-index wall time;
- incremental-index wall time by changed-file count;
- files/s, definitions/s, edges/s;
- staging/publish/seal/checkpoint time;
- search/snippet/trace/status/coverage p50/p95/max latency;
- daemon queue wait and mutation-lock wait;
- peak worker memory and aggregate worker memory;
- WAL high-water mark and checkpoint effectiveness;
- DB size/indexed-file ratio;
- operation error/contention/cancellation counts.

## Concurrency stress evaluation

A dedicated scenario runner should exercise real processes, not mocked locks.

Required scenarios:

1. Two agents request a full index for the same project simultaneously. Expected: one physical mutation job; the second joins/coalesces or receives a stable busy outcome. Never two live writers and never a rebuild caused by contention.
2. Two agents request incompatible index options for the same project. Expected: deterministic conflict/busy response and a recorded conflict event.
3. Reader continuously searches while another process indexes the project. Expected: successful reads from committed snapshots; after publication, subsequent reads observe the new generation.
4. Many concurrent readers plus one writer. Expected: no corruption; bounded WAL behavior; WAL-starvation warning/metric if long-lived readers delay checkpointing.
5. Writers to different projects. Expected: permitted concurrency within physical-job and memory budgets.
6. Kill/crash an index worker mid-build. Expected: live DB remains valid; staging residue is recoverable/cleanable; no false corruption quarantine.
7. Kill the daemon during/around publication. Expected: atomic old-or-new live DB state and successful integrity verdict on restart.
8. Hold a SQLite/write lock artificially and run integrity/recovery paths. Expected: BUSY/LOCKED classified transient and zero quarantine/rebuild actions.
9. Repeated CLI processes from several agents. Expected: process-level project locks serialize mutations regardless of client lifetime.
10. Windows 10/11 equivalents of the same process-concurrency scenarios, including abrupt process termination and file-lock semantics.

## Logging and event policy

The current logger already supports `CBM_LOG_LEVEL=debug|info|warn|error|none` and `CBM_LOG_FORMAT=text|json`.

Target policy:

- ordinary CLI output remains clean on stdout;
- command errors are always surfaced on stderr unless logging is explicitly disabled;
- default CLI operational logging should be quiet/error-oriented;
- daemon default may retain concise lifecycle warnings/errors while detailed timings are opt-in;
- `--log-level` and `--log-format` should provide command-line equivalents to the environment controls;
- conflict/contention/rebuild/quarantine events should have stable event names and reason codes;
- an optional bounded local NDJSON reliability log should record mutation conflicts, SQLite busy/locked outcomes, rebuild/quarantine decisions, worker crashes, failed integrity checks, and WAL-starvation warnings;
- secrets/source contents must not be written to assurance logs.

## Suggested stable events

- `mutation.requested`
- `mutation.coalesced`
- `mutation.conflict`
- `mutation.lock_wait`
- `mutation.lock_timeout`
- `sqlite.busy`
- `sqlite.locked`
- `store.integrity.ok`
- `store.integrity.transient`
- `store.integrity.corrupt`
- `store.quarantine`
- `store.rebuild.requested`
- `store.rebuild.completed`
- `store.wal.starving`
- `store.checkpoint`
- `index.worker.crash`
- `index.publish`

Every quarantine/rebuild event should contain a machine-readable reason and the preceding integrity verdict.

## Acceptance gates for CLI-first release

A CLI-first release should not be considered complete until:

- the same-project concurrent-write stress test passes repeatedly without corruption or duplicate physical mutation;
- reader-during-write stress passes without read failures attributable to normal writer activity;
- injected BUSY/LOCKED conditions never cause quarantine/rebuild;
- `doctor --deep` reports healthy databases after concurrency stress;
- no direct local CLI mutator bypasses the daemon/project mutation lease;
- performance baselines are recorded for Linux, macOS, Windows 10/11 x64, and supported ARM64 targets where runners exist;
- regression thresholds are defined from measured baselines rather than arbitrary absolute numbers.

## Migration sequencing

This assurance work is a separate track and should not block every MCP extraction checkpoint. However, the architectural invariants above are effective immediately. The implementation sequence is:

1. preserve daemon-authoritative write routing while extracting read operations;
2. instrument stable mutation/contention/rebuild reason events;
3. implement `doctor` fast/deep read-only checks;
4. add real-process concurrency scenarios;
5. establish benchmark result schema and baselines;
6. only then extract mutating MCP tools, one at a time, through daemon-coordinated neutral operations;
7. delete MCP mutation machinery only after equivalent concurrency tests pass.
