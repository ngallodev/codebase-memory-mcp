# Agent-workflow performance baseline

Status: **HARNESS READY; NUMERIC BASELINE CAPTURE PENDING PRODUCTION BUILD**

This benchmark is intended to produce repeatable **comparative** evidence for real agent workflows.
A timing number without the environment, binary, repository revision, cache state, and exact harness
revision is not a valid baseline.

## What is measured

`scripts/benchmark-agent-workflows.sh` records:

- full repository indexing;
- narrow graph search;
- architecture retrieval;
- exact snippet retrieval;
- file outline retrieval;
- change detection in `files` mode;
- status lookup;
- optional secondary-repository indexing and cross-repository intelligence.

Read cases run repeatedly against an explicitly started permanent daemon and report minimum,
median, and maximum wall-clock latency. Index and cross-repository mutation cases run once per
benchmark trial because they intentionally mutate durable state. The harness uses a private
`CBM_CACHE_DIR` under the results directory so benchmark state does not mix with the operator's
normal cache.

## Reproducibility contract

A result is suitable for baseline/comparison use only when all of the following are retained.

### 1. Binary identity

Record:

- exact `codebase-memory-cli --version` output;
- SHA-256 of the executable;
- source commit when the source tree is Git-backed;
- build type/optimization level and compiler identity when available;
- whether the binary is a release candidate, release artifact, or local development build.

Never compare an optimized release candidate against a debug/unoptimized binary and interpret the
difference as a product regression.

### 2. Benchmark repository identity

For every benchmark repository record:

- canonical path used for the run;
- Git remote URL when available;
- exact Git commit SHA;
- dirty/clean working-tree state;
- source file count and repository byte size where practical.

Comparative runs must use the **same repository commit**. If the input revision changes, establish a
new baseline rather than comparing the numbers directly.

### 3. Harness identity and parameters

Record:

- SHA-256 of `scripts/benchmark-agent-workflows.sh`;
- repeat count;
- search query;
- snippet symbol;
- outline file;
- change-detection base branch;
- secondary repository and revision, if cross-repository benchmarking is enabled.

The same harness revision and parameter set should be used for the reference and candidate runs.
If the harness changes materially, rerun the reference baseline with the new harness before drawing
regression conclusions.

### 4. Cache and daemon state

The canonical workflow baseline uses a **fresh isolated cache** for indexing and then measures warm
read operations against the resulting committed index.

For each independent trial:

1. stop any benchmark daemon using the benchmark cache;
2. create a new empty result/cache directory;
3. start the permanent daemon;
4. perform one full index into that empty cache;
5. perform one unrecorded warm-up read for each repeated read case;
6. run the recorded repetitions;
7. stop the daemon;
8. retain the entire result directory.

Do not reuse a cache directory from an earlier candidate/reference run. Do not compare a cold read
from one build against a warmed read from another.

### 5. Host/environment controls

Reference and candidate measurements should run on the same physical machine whenever possible.
Record at minimum:

- OS/kernel version;
- architecture;
- CPU model and logical CPU count;
- total physical memory;
- filesystem containing the repository/results directory;
- power source/governor information where available;
- relevant `CBM_*` environment variables;
- benchmark start time.

For release-quality comparisons:

- close unrelated CPU/disk intensive workloads;
- avoid running package updates, virus scans, repository clones, or builds concurrently;
- use AC power for laptops;
- do not intentionally change CPU governor/power mode between reference and candidate runs;
- run reference and candidate close together in time when practical;
- prefer alternating `reference -> candidate -> reference -> candidate` trials if machine noise is a concern.

Virtual/CI machines may be used for trend detection, but noisy/shared runners should not be the sole
basis for a release-blocking performance decision.

## Canonical capture procedure

Use a production-optimized binary that has successfully linked.

```sh
rm -rf /tmp/cbm-bench-reference
scripts/benchmark-agent-workflows.sh \
  /path/to/reference/codebase-memory-cli \
  /path/to/pinned/repository \
  /tmp/cbm-bench-reference

rm -rf /tmp/cbm-bench-candidate
scripts/benchmark-agent-workflows.sh \
  /path/to/candidate/codebase-memory-cli \
  /path/to/pinned/repository \
  /tmp/cbm-bench-candidate
```

Recommended release capture uses at least **3 independent trials per binary**, each with a newly
created cache/result directory. Within each trial, the default repeated read count is 7 for release
comparisons; 5 is acceptable for development smoke measurements.

Example:

```sh
for trial in 1 2 3; do
  CBM_BENCH_REPEATS=7 scripts/benchmark-agent-workflows.sh \
    ./reference/codebase-memory-cli ./repo "/tmp/cbm-ref-$trial"
  CBM_BENCH_REPEATS=7 scripts/benchmark-agent-workflows.sh \
    ./candidate/codebase-memory-cli ./repo "/tmp/cbm-candidate-$trial"
done
```

Useful workload overrides must be identical for both binaries:

```sh
CBM_BENCH_QUERY='store recovery' \
CBM_BENCH_SYMBOL='main' \
CBM_BENCH_FILE='src/main.c' \
CBM_BENCH_BASE_BRANCH='main' \
CBM_BENCH_REPEATS=7 \
  scripts/benchmark-agent-workflows.sh build/c/codebase-memory-cli . /tmp/cbm-agent-bench
```

## Result files

Each result directory contains:

- `timings.tsv` — every recorded operation/run, elapsed milliseconds, and exit code;
- `summary.tsv` — successful-run min/median/max by operation;
- `environment.txt` — binary/repository/harness/host metadata;
- raw stdout/stderr for every recorded case;
- the private benchmark cache used by that trial.

The raw result directory is the evidence artifact. Do not retain only a copied summary table.

## Comparative metrics

Use the median as the primary latency comparator for repeated read operations. Keep min/max to
identify instability, but do not make release decisions from a single fastest run.

For each operation calculate:

```text
absolute_delta_ms = candidate_median_ms - reference_median_ms
relative_delta_pct = 100 * (candidate_median_ms - reference_median_ms) / reference_median_ms
```

For full indexing, compare independent-trial index wall times rather than repeated indexing against
the same cache. Also compare correctness/shape evidence such as node/edge/file counts when emitted;
a faster run caused by silently indexing less data is not a performance improvement.

Interpretation guideline:

- `< 5%` median change: normally noise unless highly consistent across trials;
- `5–15%` regression: investigate when consistent or important to a common agent path;
- `> 15%` regression on the same host/input: release-blocking until explained, intentionally accepted,
  or shown to be measurement noise by additional trials.

These are comparative triage thresholds, not product SLOs. A persistent small regression on a very
high-frequency operation may matter more than a larger regression on an uncommon operation.

## Validity checks before accepting a comparison

Reject or rerun the comparison when any of the following differ unintentionally:

- benchmark repository commit;
- harness revision/parameters;
- optimized vs. unoptimized build type;
- architecture;
- cache-state procedure;
- failed benchmark operations;
- materially different indexed node/edge/file counts;
- major host load/power-mode changes.

Any nonzero benchmark operation exit code must be investigated; failed runs are excluded from
summary statistics but make the trial incomplete until explained.

## Platform baselines

Do not compare Linux, macOS, and Windows timings as if they were interchangeable. Maintain separate
baselines per platform/machine class. Native Windows validation must capture its own benchmark
results; Linux numbers are not Windows evidence.

## CP59 execution note

The CP59 environment could not complete the production-optimized build because the monolithic
`internal/cbm/lsp_all.c` translation unit exceeded the available uninterrupted compiler window at
`-O2`. No compiler error was observed. Rather than publish timings from an unoptimized or older
binary, CP59 records the reproducible harness and leaves numeric baseline capture pending a
completed production build. This distinction is deliberate: **HARNESS READY** is not the same as
**BASELINE CAPTURED**.

The older `scripts/benchmark-index.sh` was also corrected to invoke the canonical
`codebase-memory-cli index` command instead of the deprecated raw `cli index_repository` shim.

## CP61 release-qualification boundary

For release gating on Windows, this general harness contract is subordinate to
`docs/WINDOWS_BENCHMARK_PROTOCOL.md`. The authoritative benchmark host is `luigi.home.arpa`, binary
inputs are immutable GitHub-published artifacts, and repository inputs come from the frozen corpus
manifest in `docs/qualification/BENCHMARK_CORPUS.json`.

Repository commits remain unchanged for the lifetime of a corpus generation. Advancing any corpus
repository requires a new corpus-generation identifier and fresh baseline capture; results from
different corpus generations are not valid regression comparisons.
