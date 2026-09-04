# GitHub RC Dispatch Agent Runbook

Status: **STANDALONE RELEASE-OPERATOR INSTRUCTIONS**

Use this runbook after Linux qualification has reached the release-candidate boundary and before native-Windows qualification on `luigi.home.arpa`.

## Objective

Create one immutable GitHub release candidate from one exact clean source commit, keep it in DRAFT after CI verification, and hand its exact Windows bytes to the external qualification agent.

Do not publish npm/PyPI and do not un-draft the release before external qualification passes.

## Release line

The most recent public release before the CLI-first qualification cycle is `v0.10.8`. For the CLI-first release, the recommended first candidate is `v0.11.0-rc.1` unless the operator deliberately selects a different next-version policy.

The version must always be supplied explicitly. Never derive or increment a tag automatically during dispatch.

## Preconditions

1. Work from the exact source tree intended for RC publication.
2. `git status --porcelain` must be empty.
3. The source commit must already be pushed and reachable on GitHub.
4. CP64-or-later external qualification hold/promotion workflow must be present.
5. `docs/qualification/BENCHMARK_CORPUS.json` must identify the frozen corpus generation.
6. Do not set `skip_tests=true` for the first RC from this qualification cycle.
7. Use `soak_level=full` and do not skip performance gates for the first RC unless a documented qualification decision explicitly says otherwise.

## Step 1 — Prepare dispatch metadata

From the clean source tree:

```bash
python3 scripts/ci/prepare-rc-dispatch.py \
  --version v0.11.0-rc.1 \
  --baseline-zero
```

The command refuses non-RC tags and dirty worktrees. It writes:

```text
qualification/rc-dispatch.json
```

The record binds:

- source commit;
- requested RC tag;
- release workflow hash;
- benchmark corpus generation and manifest hash;
- release inputs;
- authoritative Windows qualification host;
- expected post-CI release state (`draft`);
- benchmark baseline mode (`BASELINE_ZERO` for the first qualified CLI release, otherwise an explicit immutable baseline tag).

Preserve this file in the qualification evidence bundle. Do not commit a generated dispatch record containing a transient source commit unless deliberately retaining it as release evidence.


## Benchmark baseline decision for the first CLI release

`v0.10.8` is the latest public release, but it predates the qualified CLI-first product surface and is not a valid like-for-like benchmark baseline for the canonical CLI workflow harness.

For `v0.11.0-rc.1`, use `--baseline-zero`. This is an explicit release decision, not an automatic fallback. The first fully qualified CLI release becomes the retained baseline for the next release.

For later releases, do **not** use `--baseline-zero`; pass the exact immutable public CLI-first baseline with `--baseline-tag vX.Y.Z`.

## Step 2 — Verify tag availability

Before dispatching, explicitly confirm the requested tag and release name do not already exist. Immutable release names must never be guessed or reused.

```bash
gh release view v0.11.0-rc.1 --repo DeusData/codebase-memory-mcp

git ls-remote --tags https://github.com/DeusData/codebase-memory-mcp.git \
  refs/tags/v0.11.0-rc.1
```

For a new candidate both checks must show that the tag is unused. If either exists, choose the next deliberate RC number and regenerate `rc-dispatch.json`.

## Step 3 — Dispatch exactly the generated command

Run the `gh workflow run ...` command printed by `prepare-rc-dispatch.py` without weakening its external-qualification hold.

For the normal first candidate it is equivalent to:

```bash
gh workflow run release.yml \
  --repo DeusData/codebase-memory-mcp \
  --ref <exact-source-commit> \
  -f version=v0.11.0-rc.1 \
  -f soak_level=full \
  -f skip_perf=false \
  -f skip_tests=false \
  -f hold_for_external_qualification=true
```

## Step 4 — Observe the workflow

Record the workflow run ID and URL. The run must complete the required lint/test/build/smoke/soak/security/verify chain.

If GitHub CI fails, classify and fix the failure in source, create a new commit, and dispatch a new RC tag. Do not mutate or reinterpret a failed candidate as qualified.

## Step 5 — Verify held draft state

After CI succeeds:

- the release must exist;
- it must still be DRAFT;
- npm/PyPI publication must not have occurred through this workflow;
- the Windows ZIP, `checksums.txt`, selection evidence, SBOM/signature/provenance assets required by the release workflow must be present.

Record the release asset listing and release ID in the evidence bundle.

## Step 6 — Create Windows qualification handoff

Provide the Windows qualification agent on `luigi.home.arpa` with:

- release tag;
- release URL/ID;
- expected source commit;
- `qualification/rc-dispatch.json`;
- `checksums.txt` from the draft release;
- matching qualification source checkout containing `docs/WINDOWS_RELEASE_VALIDATION.md`, `docs/WINDOWS_BENCHMARK_PROTOCOL.md`, and `docs/agents/WINDOWS_RELEASE_QUALIFICATION_AGENT.md`.

The Windows agent must download the GitHub-published Windows ZIP itself and verify the exact bytes. Do not copy a locally built executable to the Windows host as a substitute.

## Step 7 — Promotion boundary

Only `.github/workflows/promote-qualified-release.yml` may move the held draft to public distribution after external evidence is attached and independently verified.

Do not manually:

- un-draft the release;
- publish npm;
- publish PyPI;
- alter qualification evidence to satisfy the promotion gate.
