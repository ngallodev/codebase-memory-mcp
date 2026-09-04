# Windows Comparative Benchmark Protocol

Status: **AUTHORITATIVE RELEASE BENCHMARK PROCEDURE**

Authoritative host: `luigi.home.arpa`.

## Goal

Produce repeatable comparative evidence between an immutable public baseline release and an immutable release candidate using frozen repository inputs.

## Binary inputs

- **BASELINE:** most recent publicly released production-worthy version.
- **CANDIDATE:** current GitHub release candidate.

Both must be GitHub-published artifacts with recorded SHA-256 identity. Do not compare a GitHub artifact against a locally built executable.

For the first CLI-first release, `v0.10.8` is not treated as a comparable performance baseline because it predates the canonical CLI workflow surface. The RC dispatch record must explicitly select `BASELINE_ZERO`. The first fully qualified CLI release becomes `baseline-generation-0`; comparative release gating begins with the next release. `BASELINE_ZERO` must never be inferred automatically.

## Frozen corpus contract

Benchmark repositories are frozen inputs, not moving examples.

The corpus is defined by `docs/qualification/BENCHMARK_CORPUS.json` and copied into each evidence bundle as `benchmark/corpus.json`.

Every entry records:

- stable corpus id;
- repository name;
- canonical remote URL;
- exact Git commit SHA;
- expected clean-tree state;
- intended scale/category;
- operations enabled for that repository;
- canonical local checkout path on `luigi.home.arpa` when established.

Rules:

1. Never advance a corpus repository during a baseline/candidate comparison series.
2. Never benchmark a dirty tree unless the corpus definition intentionally describes that exact dirty fixture.
3. Verify `HEAD` and clean state before every trial.
4. If any corpus commit changes, increment the corpus-generation identifier and establish new baseline results for the entire changed corpus generation.
5. Do not compare results across corpus generations as release regression evidence.

## Recommended corpus

Use the following stable five-repository corpus for `windows-corpus-1`:

1. `codebase-memory-cli` — self-hosting, medium/large multi-language workflow and regression relevance. Historical GitHub repository slug: `https://github.com/DeusData/codebase-memory-mcp.git`.
2. `specgen-aw` — realistic specification-authoring application from the user's development workflow: `https://github.com/ngallodev-software/specgen-aw.git`.
3. `agent-workflow` — realistic companion workflow application: `https://github.com/ngallodev-software/agent-workflow.git`.
4. `agent-workflow-spec-contracts` — the shared contract dependency used by SpecGen/Agent-Workflow integration: `https://github.com/ngallodev-software/agent-workflow-spec-contracts.git`.
5. `herdr` — larger open-source, multi-language workload used to add scale and language diversity: `https://github.com/herdrdev/herdr.git`.

The repository identities are fixed for this corpus generation. The exact commit for each entry is deliberately captured from the actual frozen checkout used to initialize `luigi.home.arpa`; do not substitute current remote HEAD during planning. Once those commits are recorded, changing any one of them creates a new corpus generation and requires a new baseline.

## Trial design

For each corpus repository, run at least three independent trials per binary.

Preferred order to reduce temporal bias:

```text
baseline trial 1
candidate trial 1
baseline trial 2
candidate trial 2
baseline trial 3
candidate trial 3
```

Each trial uses a new empty result directory and a new empty `CBM_CACHE_DIR`.

Do not reuse caches between baseline and candidate.

## Workload semantics

Use `scripts/benchmark-agent-workflows.sh` (or its Windows wrapper when added) with identical workload parameters for baseline and candidate.

Measure real agent workflows:

- full index;
- narrow search;
- architecture/query;
- snippet/source retrieval;
- file outline;
- change detection;
- status;
- optional cross-repository query for the declared paired repositories.

Repeated read operations receive one unrecorded warm-up before recorded timings. Indexing remains a fresh-cache measurement.

## Host controls

Run all comparative trials on `luigi.home.arpa` with:

- same Windows build;
- same power plan;
- AC power if applicable;
- same repository paths/filesystem;
- no concurrent source builds;
- no package/system updates;
- no intentional antivirus configuration change between binaries;
- no unrelated heavy CPU/disk workload.

Record CPU, memory, storage/filesystem, OS build, PowerShell, Git, Python, power plan, and relevant `CBM_*` variables.

## Correctness gate before timing comparison

A timing is valid only if both binaries successfully complete the operation and produce equivalent workload shape.

At minimum compare where available:

- indexed project identity;
- source/file counts;
- graph node/edge counts;
- operation exit status;
- expected result presence for fixed queries/symbols/files.

A candidate that is faster because it indexed less or returned incomplete data has failed correctness, not improved performance.

## Comparative metrics

For repeated read operations use median latency within each trial and compare the distribution of trial medians.

```text
absolute_delta_ms = candidate_median_ms - baseline_median_ms
relative_delta_pct = 100 * absolute_delta_ms / baseline_median_ms
```

For indexing compare fresh-cache trial wall times.

Interpretation guideline:

- <5%: normally noise unless consistent and operationally important;
- 5–15%: investigate when consistent;
- >15% unexplained regression on same input/host: release-blocking until explained, accepted deliberately, or disproven with additional trials.

These are triage thresholds, not product SLOs.

## Invalid comparison conditions

Invalidate and rerun affected trials when any of these differ unintentionally:

- binary/artifact identity;
- corpus generation or repository commit;
- dirty-tree state;
- harness hash or parameters;
- cache-state procedure;
- Windows build/power plan materially changed;
- failed operation;
- materially different indexed graph/file shape;
- heavy background activity compromised one side.

## Evidence retention

For every trial retain:

- raw stdout/stderr;
- timings;
- environment/provenance file;
- binary hashes;
- corpus verification output;
- cache directory used by that trial until evidence is reviewed;
- comparison summary generated from raw trials.

Do not keep only averaged numbers.
