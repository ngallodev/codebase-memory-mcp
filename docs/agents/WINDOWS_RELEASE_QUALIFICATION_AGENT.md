# Windows Release Qualification Agent Runbook

Status: **STANDALONE OPERATOR/AGENT INSTRUCTIONS**

You are qualifying a `codebase-memory-cli` release candidate on the authoritative Windows host `luigi.home.arpa`.

Your job is to collect evidence, not to improve the product during the run.

## Hard constraints

1. Use the exact GitHub-published candidate artifact supplied for qualification.
2. Do not compile or rebuild `codebase-memory-cli.exe` locally.
3. Verify and record SHA-256 before executing the candidate.
4. Never alter a frozen benchmark repository to make a test pass.
5. Never advance a benchmark repository commit during a comparison generation.
6. Use isolated cache/result directories for independent trials.
7. Preserve raw output and exit codes.
8. Do not edit a failing test during the qualification run. Classify the failure and stop the affected scenario.
9. Do not turn off correctness checks to obtain performance numbers.
10. Do not interpret MCP compatibility paths/legacy cleanup markers as an active MCP product feature.

## Inputs you must receive

- candidate GitHub release/tag or artifact URL;
- expected candidate/source commit;
- expected checksum material;
- baseline release/tag for comparative benchmarking, or an RC dispatch record explicitly declaring `BASELINE_ZERO`;
- qualification root path;
- frozen corpus manifest derived from `docs/qualification/BENCHMARK_CORPUS.json`;
- source checkout containing the matching qualification scripts/tests.

If any identity input is missing, record `BLOCKED_IDENTITY` and do not fabricate it.

## Step 1 — Create immutable run directory

Create a new timestamped run directory. Never reuse an older run directory.

Populate the evidence layout specified by `docs/RELEASE_QUALIFICATION_PLAN.md`.

## Step 2 — Record machine state

Capture:

- `Get-ComputerInfo` relevant Windows/version fields;
- processor and memory information;
- PowerShell version;
- active power plan;
- filesystem/free-space information;
- Git/Python versions;
- running Codebase Memory processes before the run;
- relevant `CBM_*` environment variables.

Do not expose unrelated secrets from the environment; capture only benchmark/product-relevant variables.

## Step 3 — Acquire and verify artifacts

Download the candidate from its GitHub release.

Record:

- download source;
- archive filename/hash;
- executable hash;
- version output;
- expected source commit/release identity.

If the published checksum and computed checksum disagree, classify `FAIL_PRODUCT`/artifact integrity and stop.

Acquire and verify the baseline artifact in the same manner when comparative benchmarking. If the RC dispatch record explicitly declares `BASELINE_ZERO`, do not fabricate or substitute a baseline artifact; record baseline establishment and skip comparative deltas for this release.

## Step 4 — Verify frozen corpus before any benchmark

For every repository in the corpus manifest:

- confirm local path exists;
- record `git remote -v`;
- verify `git rev-parse HEAD` equals manifest commit;
- verify clean working tree when `clean_required=true`;
- do not fetch/pull/reset automatically unless the operator explicitly instructed you to initialize the frozen corpus;
- write verification results into the evidence bundle.

If a repository does not match, mark benchmarking `BLOCKED_CORPUS_DRIFT`. Do not silently update the manifest or repository.

## Step 5 — Portable functional qualification

Follow `docs/WINDOWS_RELEASE_VALIDATION.md` against the extracted candidate.

Use isolated `CBM_CACHE_DIR` values.

Capture every command, exit code, stdout, and stderr.

Prefer structured JSON output where it helps assert semantics, but also capture selected human output such as `doctor`.

## Step 6 — Run retained native-Windows guards

Invoke the repository's Windows validation scripts with the exact candidate binary supplied explicitly.

If a script attempts to build because no binary was supplied, stop and correct invocation; a locally built executable invalidates release evidence.

Classify each guard independently.

## Step 7 — Installed-product qualification

Exercise supported install/update/uninstall paths separately from the portable binary.

Verify installed executable identity matches the candidate.

Pay particular attention to ownership-aware legacy cleanup: Codebase Memory-owned historical MCP configuration may be removed, but foreign or modified configuration must remain intact. Clean install must not create new MCP registrations.

## Step 8 — Comparative benchmarking

Follow `docs/WINDOWS_BENCHMARK_PROTOCOL.md` exactly.

For every corpus repository:

- baseline trial 1 with fresh cache;
- candidate trial 1 with fresh cache;
- baseline trial 2;
- candidate trial 2;
- baseline trial 3;
- candidate trial 3.

Do not reuse result/cache directories.

Before interpreting timings, verify correctness/shape parity.

Generate comparison output from raw trials. Never delete raw trials after producing summaries.

## Step 9 — Failure handling

Classify every non-pass as one of:

- `FAIL_PRODUCT`;
- `FAIL_TEST`;
- `FAIL_ENVIRONMENT`;
- `BLOCKED_IDENTITY`;
- `BLOCKED_CORPUS_DRIFT`;
- `KNOWN_ACCEPTED`;
- `SKIP_NOT_APPLICABLE`.

For a product failure, record:

- exact command/scenario;
- candidate identity;
- relevant repository/corpus identity;
- exit code;
- raw output paths;
- minimal reproduction steps;
- whether baseline reproduces the issue.

Do not patch the release candidate in place.

## Step 10 — Produce SUMMARY.md

The summary must state:

- candidate identity/hash;
- baseline identity/hash if used;
- machine identity;
- corpus generation;
- portable qualification result;
- installed qualification result;
- recovery/concurrency result;
- Windows guard result;
- benchmark comparison table;
- every non-pass disposition;
- whether evidence supports release, rejects release, or is incomplete.

Do not say "Windows validated" unless the exact GitHub candidate bytes completed the required native-Windows gates.

## Benchmark corpus initialization (one-time per generation)

When establishing a new corpus generation, the operator may explicitly authorize corpus initialization. For `windows-corpus-1`, the repository identities are already selected in `docs/qualification/BENCHMARK_CORPUS.json`; do not replace them with different repositories. In that one-time workflow:

1. locate or clone the five declared repository checkouts;
2. ensure each is at a deliberate stable commit;
3. verify each checkout's canonical remote matches the manifest and record its exact commit;
4. ensure clean state;
5. copy the finalized manifest into version control;
6. establish baseline measurements against the selected baseline artifact;
7. freeze the corpus.

After freeze, agents must never automatically advance it. A repository update requires a new corpus-generation identifier and new baseline capture.

## Step 11 — Attach promotion evidence to the GitHub draft

Only after every required gate is PASS (or the first qualified release legitimately uses `BASELINE_ZERO`):

1. write `qualification-manifest.json` with schema version 1;
2. copy the final human summary to `qualification-summary.md`;
3. compute SHA-256 of `qualification-summary.md` and store it as `summary_sha256`;
4. record the exact draft tag, Windows ZIP SHA-256, and extracted `codebase-memory-cli.exe` SHA-256 in `release`;
5. set `host` to `luigi.home.arpa` and `corpus_generation` to `windows-corpus-1`;
6. set `portable_result`, `installed_result`, `windows_guards_result`, and `recovery_result` to `PASS`;
7. set `benchmark_result` to `PASS`, or `BASELINE_ZERO` only when establishing the first qualified baseline;
8. upload both files to the existing GitHub DRAFT release with authenticated `gh release upload`;
9. do not un-draft the release and do not publish npm/PyPI yourself.

Promotion is owned by `.github/workflows/promote-qualified-release.yml`, which independently re-downloads the release ZIP/checksums and verifies that your evidence binds the exact shipped Windows bytes.

Required manifest shape:

```json
{
  "schema_version": 1,
  "result": "PASS",
  "host": "luigi.home.arpa",
  "corpus_generation": "windows-corpus-1",
  "portable_result": "PASS",
  "installed_result": "PASS",
  "windows_guards_result": "PASS",
  "recovery_result": "PASS",
  "benchmark_result": "PASS",
  "summary_sha256": "<sha256>",
  "release": {
    "tag": "<vX.Y.Z[-rc.N]>",
    "windows_archive_sha256": "<sha256>",
    "windows_executable_sha256": "<sha256>"
  }
}
```

If any required gate is not releasable, do not upload a PASS manifest. Keep the evidence bundle locally and report the blocker.
